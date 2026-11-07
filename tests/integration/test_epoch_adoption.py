"""Actual SQL665 -> large-lag provider666 -> private preparation -> real adoption."""
import contextlib
import io
import unittest
import psycopg
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource,acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.epoch_stage import prefix,job_key
from integration.test_network_profiles import Databases
from fixtures.epoch_transition import Environment,seed_source,PROFILE,S,FUTURE

class AdoptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dbs=Databases();cls.source_dsn,cls.source_alias=cls.dbs.create()
        with psycopg.connect(cls.source_dsn,autocommit=True) as db:seed_source(db)
    @classmethod
    def tearDownClass(cls):cls.dbs.close()
    def setUp(self):
        dsn,alias=self.dbs.create()
        with contextlib.redirect_stdout(io.StringIO()):self.env=Environment(dsn,alias,self.source_alias)
        self.addCleanup(self.env.close)
    def blocked(self):
        e=self.env
        with self.assertRaisesRegex(TransientSyncFailure,'epoch_preparation_not_ready'):e.driver.run_once(e.owner,e.health,e.stop)
        e.health.end_operation();e.health.update(sync='backoff')
        self.assertEqual(e.owner.generation()['target_height'],S);self.assertGreater(e.health.snapshot()['lag']['blocks'],9);self.assertEqual(e.health.health()[0],503)
        self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],0)
    def test_cold_large_lag_restart_prepares_privately_then_catches_up_exact_SQL(self):
        e=self.env;self.blocked();e.prepare(steps=20)
        spent=e.preparer.store.sync_job(job_key(666))['attempts'];e.restart_all();e.clock.now+=61;self.blocked();self.assertEqual(e.preparer.store.sync_job(job_key(666))['attempts'],spent)
        e.prepare();stage,manifest=e.preparer.store.load(666);self.assertTrue(stage.complete)
        self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],0)
        self.assertEqual(e.owner.db.execute('SELECT count(*) FROM public.epoch_stake WHERE epoch_no=666').fetchone()[0],0)
        e.clock.now+=61;e.health.end_operation();e.poller.tick(e.pollstore,e.gate);e.driver.run_once(e.owner,e.health,e.stop)
        self.assertEqual(e.owner.generation()['target_height'],S+100);verify_store(e.owner)
        from db_sync_alt.sources.blockfrost.relevance import qualified_parent
        self.assertIsNotNone(qualified_parent(e.owner,PROFILE));self.assertIn('relevance_coverage',e.owner.generation()['manifest'])
        self.assertEqual(e.preparer.store.sync_job(job_key(666))['state'],'completed')
        self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],2669)
        e.health.end_operation();e.poller.tick(e.pollstore,e.gate);e.driver.run_once(e.owner,e.health,e.stop)
        self.assertEqual(e.owner.generation()['target_height'],S+FUTURE);actual=verify_store(e.owner)['digests']
        self.assertIsNotNone(qualified_parent(e.owner,PROFILE));self.assertIn('relevance_coverage',e.owner.generation()['manifest'])
        reference_dsn,_=self.dbs.create()
        with PgStore(conninfo=reference_dsn,create=True,network='mainnet') as reference,contextlib.redirect_stdout(io.StringIO()):
            acquire(CountedSQLSource(self.source_alias,PROFILE,{'dbsync_version':'13.8.0.0','tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}),reference,PROFILE,target_height=S+FUTURE,window_blocks=10000)
            self.assertEqual(actual,reference.digests(reference.generation()['id']))
    def test_candidate_omission_rolls_back_nonce_weights_checkpoint_and_adoption_flag(self):
        e=self.env;self.blocked();e.prepare();original=e.preparer.adopt
        def omitted(*args,**kwargs):
            nonce,stakes,anchor,receipt=original(*args,**kwargs);return nonce,stakes[:-1],anchor,receipt
        e.preparer.adopt=omitted;e.clock.now+=61;e.health.end_operation();e.poller.tick(e.pollstore,e.gate)
        with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):e.driver.run_once(e.owner,e.health,e.stop)
        self.assertEqual(e.owner.generation()['target_height'],S);self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],0)
        self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_nonces WHERE epoch=666').fetchone()[0],0)
        self.assertEqual(e.preparer.store._meta(prefix(666)+'manifest')['state'],'complete');self.assertEqual(e.preparer.store.sync_job(job_key(666))['state'],'active')
    def test_corrupt_private_stage_refuses_without_future_fact_exposure(self):
        e=self.env;self.blocked();e.prepare(steps=20);key=prefix(666)+'record:00001';row=e.preparer.store._meta(key);row['response']['network_magic']=1;e.preparer.store._put(key,row)
        e.clock.now+=61;e.health.end_operation();e.poller.tick(e.pollstore,e.gate)
        with self.assertRaises(LifecycleFailure):e.driver.run_once(e.owner,e.health,e.stop)
        self.assertEqual(e.owner.generation()['target_height'],S);self.assertEqual(e.owner.db.execute('SELECT count(*) FROM public.epoch_stake WHERE epoch_no=666').fetchone()[0],0)
    def test_fresh_adoption_nonce_epoch_and_anchor_changes_refuse(self):
        e=self.env;self.blocked();e.prepare();e.clock.now+=61
        anchor=e.preparer.store.load(666)[0].result()['anchor']['height']
        for kind in ('nonce','epoch','anchor'):
            def mutate(path,row):
                if kind=='nonce' and path=='/epochs/666/parameters':row['nonce']='ab'*32
                if kind=='epoch' and path=='/blocks/latest':row['epoch']=667
                if kind=='anchor' and path=='/blocks/'+str(anchor):row['hash']='cd'*32
                return row
            e.client.mutate=mutate;e.health.end_operation()
            with self.subTest(kind=kind),self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):e.driver.run_once(e.owner,e.health,e.stop)
            self.assertEqual(e.owner.generation()['target_height'],S);self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],0)
        e.client.mutate=None
    def test_real_worker_stop_closes_narrow_owner_and_keeps_private_progress(self):
        import time
        from db_sync_alt.sources.blockfrost.epoch_stage import EpochStageStore
        e=self.env;self.blocked();e.preparer.interval=.01;thread=e.preparer.start();deadline=time.time()+8
        while time.time()<deadline:
            value=e.health.snapshot().get('epoch_preparation') or {}
            if (value.get('attempts') or 0)>=2:break
            time.sleep(.05)
        else:self.fail('preparation worker made no durable progress')
        e.stop.set();thread.join(5);self.assertFalse(thread.is_alive());self.assertIsNone(e.preparer.store)
        reopened=EpochStageStore(None,e.binding,PROFILE,conninfo=e.dsn)
        try:
            self.assertGreaterEqual(reopened.sync_job(job_key(666))['attempts'],2);reopened.load(666)
            self.assertEqual(e.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],0)
        finally:reopened.close()
