"""Real PostgreSQL operational durability; no live provider or canonical future facts."""
import threading
import unittest
from psycopg.types.json import Jsonb

from db_sync_alt.contract import Refusal
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure
from db_sync_alt.sources.blockfrost.epoch_stage import EpochStageStore, job_key, prefix
from db_sync_alt.sources.blockfrost.poller import owner_binding, PollHealth
from db_sync_alt.sources.blockfrost.sync import AccountedProvider
from db_sync_alt.service.health import HealthState
from integration.test_network_profiles import Databases
from fixtures import synthetic
from unit.test_epoch_pools import PROFILE, ReplayProvider

class Client(ReplayProvider):
    def status(self):return {'daily_limit':50000,'budget_used':len(self.calls)}
    def get(self,path,purpose):
        if path=='/metrics':
            self.calls.append(path);return []
        return super().get(path)

class StageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.dbs=Databases()
    @classmethod
    def tearDownClass(cls):cls.dbs.close()
    def setUp(self):
        self.dsn,self.alias=self.dbs.create();self.owner=synthetic.create(self.dsn,PROFILE.identity);self.addCleanup(self.owner.close)
        self.binding=owner_binding(self.owner);self.stage=EpochStageStore(None,self.binding,PROFILE,conninfo=self.dsn);self.addCleanup(self.stage.close)
        self.client=Client();self.stop=threading.Event();self.health=HealthState(service_version='1',schema_version=1,network='mainnet',profile=PROFILE.identity['profile'])
    def provider(self):return AccountedProvider(self.stage,job_key(666),self.stage.sync_job(job_key(666)),self.client,PollHealth(self.health),self.stop,network=PROFILE.network)
    def one(self):
        self.stage.ensure(666);collector,manifest=self.stage.load(666);collector.step(self.provider());self.stage.append(666,manifest,collector.records[-1],collector)
        return collector
    def reopen(self):
        self.stage.close();self.stage=EpochStageStore(None,self.binding,PROFILE,conninfo=self.dsn);self.addCleanup(self.stage.close)
    def test_real_PG_private_record_resume_and_durable_counter(self):
        collector=self.one();attempts=self.stage.sync_job(job_key(666))['attempts'];self.assertEqual(attempts,2)
        self.reopen();new,manifest=self.stage.load(666);self.assertEqual(new.records,collector.records);self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],attempts)
        self.assertEqual(self.owner.db.execute('SELECT count(*) FROM chain.epoch_stakes WHERE epoch=666').fetchone()[0],0)
        self.assertEqual(self.owner.db.execute('SELECT count(*) FROM chain.epoch_nonces WHERE epoch=666').fetchone()[0],0)
    def test_crash_after_reservation_stays_spent(self):
        job=self.stage.ensure(666);job.update(attempts=1,reserved_at=1);self.stage.save_sync_job(job_key(666),job);self.reopen()
        self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],1);self.one();self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],3)
    def test_counter_decrease_and_budget_change_refused(self):
        self.one();job=self.stage.sync_job(job_key(666))
        for updates in ({'attempts':0},{'budget':7999}):
            with self.subTest(updates=updates),self.assertRaises((Refusal,LifecycleFailure)):self.stage.save_sync_job(job_key(666),{**job,**updates})
    def test_only_one_worker_and_owner_loss(self):
        with self.assertRaisesRegex(LifecycleFailure,'writer_busy'):EpochStageStore(None,self.binding,PROFILE,conninfo=self.dsn)
        self.owner.close()
        with self.assertRaisesRegex(LifecycleFailure,'database_unavailable'):self.stage.ensure(666)
    def test_record_and_manifest_corruption_refused(self):
        self.one();key=prefix(666)+'record:00001';record=self.stage._meta(key)
        self.stage._put(key,{**record,'endpoint':'/evil'})
        with self.assertRaises(Refusal):self.stage.load(666)
    def test_profile_binding_corruption_refused(self):
        self.stage.ensure(666);key=prefix(666)+'policy';policy=self.stage._meta(key);policy['profile_sha256']='ab'*32;self.stage._put(key,policy)
        with self.assertRaises(Refusal):self.stage.ensure(666)
    def test_physical_database_copy_does_not_mint_new_budget(self):
        self.one();dsn,_=self.dbs.create()
        with synthetic.create(dsn,PROFILE.identity) as other:
            with other.db.transaction():
                for key,value in self.stage.db.execute("SELECT id,body FROM chain.sync_jobs WHERE id LIKE 'epoch-preparation-v1:%'"):other.db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s)',(key,Jsonb(value)))
                row=self.stage.sync_job(job_key(666));other.db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s)',(job_key(666),Jsonb(row)))
            copied=EpochStageStore(None,owner_binding(other),PROFILE,conninfo=dsn)
            try:
                with self.assertRaises(Refusal):copied.ensure(666)
            finally:copied.close()
    def test_budget_exhaustion_and_stop_do_not_send(self):
        self.stage.ensure(666);job=self.stage.sync_job(job_key(666))
        from db_sync_alt.contract import digest
        root='0'*64;records=[]
        for number in range(1,8001):
            record={'version':1,'job_id':job['job_id'],'attempt':number,'budget':8000,'at':1}
            root=digest({'previous':root,'reservation':record});records.append((prefix(666)+'reservation:'+str(number).zfill(5),Jsonb(record)))
        with self.stage.db.transaction():
            for key,value in records:self.stage._put(key,value.obj)
            job.update(attempts=8000,reserved_at=1,reservation_root=root)
            self.stage.db.execute('UPDATE chain.sync_jobs SET body=%s WHERE id=%s',(Jsonb(job),job_key(666)))
        with self.assertRaises(LifecycleFailure):self.provider().get('/genesis')
        self.assertEqual(self.client.calls,[]);self.assertEqual(self.stage.sync_job(job_key(666))['state'],'exhausted')
    def test_global_quota_and_cooldown_preserve_stage(self):
        self.stage.ensure(666)
        self.client.status=lambda:{'daily_limit':50000,'budget_used':50000}
        with self.assertRaises(TransientSyncFailure):self.provider().get('/genesis')
        self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],0);self.assertEqual(self.client.calls,[])
    def test_stop_before_send_preserves_unspent_counter(self):
        self.stage.ensure(666);self.stop.set()
        with self.assertRaisesRegex(Refusal,'interrupted'):self.provider().get('/genesis')
        self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],0)
    def test_pruning_retains_spent_budget_tombstone(self):
        self.one();attempts=self.stage.sync_job(job_key(666))['attempts'];self.stage.prune(668)
        self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],attempts)
        with self.assertRaises(Refusal):self.stage.load(666)
        self.stage.prune(668);self.assertEqual(self.stage._meta(prefix(666)+'manifest')['prior_root']!= '0'*64,True)

    def test_cooldown_before_send_is_durable(self):
        import time
        self.stage.ensure(666);until=time.time()+120
        self.client.status=lambda:{'daily_limit':50000,'budget_used':0,'cooldown_until':until}
        with self.assertRaises(TransientSyncFailure):self.provider().get('/genesis')
        job=self.stage.sync_job(job_key(666));self.assertGreaterEqual(job['retry_at'],until);self.assertEqual(job['attempts'],0);self.assertEqual(self.client.calls,[])
    def test_provider_failure_spends_once_and_no_immediate_retry(self):
        self.stage.ensure(666)
        def failure(path,purpose):self.client.calls.append(path);raise RuntimeError('HTTP 503; counted')
        self.client.get=failure
        with self.assertRaises(TransientSyncFailure):self.provider().get('/genesis')
        self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],1)
        with self.assertRaises(TransientSyncFailure):self.provider().get('/genesis')
        self.assertEqual(self.stage.sync_job(job_key(666))['attempts'],1);self.assertEqual(len(self.client.calls),1)

    def test_reserved_counter_corruption_cannot_hide_spent_attempt(self):
        job=self.stage.ensure(666);job.update(attempts=1,reserved_at=1);self.stage.save_sync_job(job_key(666),job)
        corrupt={**job,'attempts':0,'reservation_root':'0'*64}
        self.stage.db.execute('UPDATE chain.sync_jobs SET body=%s WHERE id=%s',(Jsonb(corrupt),job_key(666)))
        self.reopen()
        with self.assertRaises(Refusal):self.stage.ensure(666)
    def test_UTC_day_change_keeps_epoch_budget_and_job(self):
        import time
        self.one();old=self.stage.sync_job(job_key(666));self.reopen();collector,manifest=self.stage.load(666)
        provider=AccountedProvider(self.stage,job_key(666),self.stage.sync_job(job_key(666)),self.client,PollHealth(self.health),self.stop,network=PROFILE.network,clock=lambda:time.time()+86400)
        collector.step(provider);self.stage.append(666,manifest,collector.records[-1],collector)
        new=self.stage.sync_job(job_key(666));self.assertEqual(new['job_id'],old['job_id']);self.assertEqual(new['budget'],8000);self.assertEqual(new['attempts'],old['attempts']+2)

    def test_malformed_epoch_does_not_poison_high_watermark_or_create_job(self):
        for epoch in (True,-1,2**31):
            with self.subTest(epoch=epoch),self.assertRaises(Refusal):self.stage.ensure(epoch)
        self.assertIsNone(self.stage._meta('epoch-preparation-v1:highest-epoch'))
        self.assertEqual(self.stage.db.execute("SELECT count(*) FROM chain.sync_jobs WHERE id LIKE 'provider-epoch-v1-%'").fetchone()[0],0)
