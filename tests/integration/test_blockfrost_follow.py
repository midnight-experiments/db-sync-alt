"""Actual PostgreSQL fast adapter against nonempty synthetic provider inventories.

No live provider. The full update (`adapter.update`) and unchanged full canonical
digests are independent references.
"""
import copy
import os
import threading
import time
from pathlib import Path
from dataclasses import replace
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import psycopg
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
import fixtures.database as real
import fixtures.blockfrost_generation as gen_fixture
from fixtures.networks import PREPROD


class FollowSourcePGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
    @classmethod
    def tearDownClass(cls):cls.admin.close()
    setUp=real.SyncFixture.setUp
    tearDown=real.SyncFixture.tearDown
    database=real.SyncFixture.database
    make=real.SyncFixture.make
    state=real.SyncFixture.state
    reference=real.SyncFixture.reference
    compare_reference=real.SyncFixture.compare_reference

    def prepare(self,transition=False):
        dsn=self.make(transition);routes,_=gen_fixture.fixtures(transition=transition)
        store=PgStore(conninfo=dsn);self.addCleanup(store.close)
        upgrade_store(store);store.certify_ready_full()
        return dsn,store,routes

    def run_follow(self,store,dsn,routes,target=None):
        health=self.state();stop=threading.Event();client=real.AccountedFake(routes)
        pollstore=PollBudgetStore(None,owner_binding(store),conninfo=dsn)
        self.addCleanup(pollstore.close)
        poller=HeadPoller(lambda:pollstore,lambda:client,health,stop,network=PREPROD)
        poller.tick(pollstore,client)
        driver=SyncDriver(SimpleNamespace(sync={}),client=client,profile=self.profile,fixed_target=target)
        driver._poller=poller
        driver.run_once(store,health,stop)
        pollstore.close()
        return client,driver

    def test_nonempty_spend_mint_all_families_equal_full_update(self):
        dsn,store,routes=self.prepare()
        old=copy.deepcopy(store.generation());oldhash=store.digests(old['id'])
        client,driver=self.run_follow(store,dsn,routes)
        report=verify_store(store)
        self.assertEqual(report['digests'],store.digests(store.generation()['id']))
        self.compare_reference(routes,store)
        self.assertEqual(store.generation()['manifest']['integrity']['version'],2)
        self.assertEqual(store.generation(0),old);self.assertEqual(store.digests(0),oldhash)
        pages=[p for p in client.calls if '/next?' in p]
        self.assertEqual(len(pages),1)
        self.assertEqual(store.sync_job('provider-active')['state'],'completed')
        self.assertTrue(store.completed(1,'handoff','initial'))

    def test_twice_native_restore_preserves_poll_job_and_chains_explicit_receipts(self):
        from db_sync_alt.backup import backup,restore
        from db_sync_alt.sources.blockfrost.poller import _admission,identity,bind_restored_poll_policy
        from db_sync_alt.errors import LifecycleFailure
        dsn,store,routes=self.prepare()
        self.run_follow(store,dsn,routes)
        key,row=store.db.execute("SELECT id,body FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%'").fetchone()
        # Persist a real backoff/history state; native recovery must not clear it.
        row={**row,'retry_at':time.time()+120,'failure_count':1,'error_code':'provider_unavailable'}
        store.save_sync_job(key,row)
        original=store.db.execute("SELECT key,value FROM chain.metadata WHERE key LIKE 'poll-budget-%'").fetchall()
        store.close()
        for number in (1,2):
            archive=Path(self.tmp.name)/('poll-restore-'+str(number)+'.dump')
            manifest=archive.with_suffix('.json')
            backup(archive,manifest,conninfo=dsn)
            destination=self.database()
            result=restore(archive,manifest,conninfo=destination)
            self.assertTrue(result['poll_restore_binding']['accounting_rows_unchanged'])
            self.assertEqual(len(result['poll_restore_binding']['added_metadata']),1)
            with PgStore(conninfo=destination) as restored:
                self.assertEqual(restored.sync_job(key),row)
                for name,value in original:self.assertEqual(restored.meta(name),value)
                self.assertIsNone(_admission(restored.db,key,row,identity(restored.db)))
                self.assertEqual(restored.db.execute("SELECT count(*) FROM chain.metadata WHERE key LIKE 'poll-budget-restore:%'").fetchone()[0],number)
                with self.assertRaises(LifecycleFailure):bind_restored_poll_policy(restored,'a'*64)
                # Copied receipt changes or imported unrelated archives cannot
                # be accepted as an online physical rebind.
                with self.assertRaises(Exception):
                    with restored.db.transaction():
                        restored.db.execute("UPDATE chain.metadata SET value='{}' WHERE key LIKE 'poll-budget-restore:%'")
            dsn=destination

    def test_one_shot_then_managed_poll_and_native_restore_preserve_policy(self):
        from db_sync_alt.cli.acquire import blockfrost_once
        from db_sync_alt.backup import backup,restore
        from db_sync_alt.sources.blockfrost.poller import day_at
        dsn,store,routes=self.prepare()
        blockfrost_once(store,self.profile,target_height=gen_fixture.OFFSET+11,client=real.AccountedFake(routes))
        key='provider-poll-'+day_at(time.time());before=copy.deepcopy(store.sync_job(key))
        poll=PollBudgetStore(None,owner_binding(store),conninfo=dsn)
        try:
            self.assertEqual(poll.sync_job(key),before)
            blocked=real.AccountedFake(routes)
            with self.assertRaisesRegex(Exception,'writer_busy'):
                blockfrost_once(store,self.profile,target_height=gen_fixture.OFFSET+12,client=blocked)
            self.assertEqual(blocked.calls,[])
            client=real.AccountedFake(routes);health=self.state()
            HeadPoller(lambda:poll,lambda:client,health,threading.Event(),network=PREPROD).tick(poll,client)
            advanced=poll.sync_job(key)
            self.assertEqual(advanced['job_id'],before['job_id'])
            self.assertEqual(advanced['attempts'],before['attempts']+2)
            before=copy.deepcopy(advanced)
        finally:poll.close()
        store.close();archive=Path(self.tmp.name)/'oneshot.dump';manifest=archive.with_suffix('.json')
        backup(archive,manifest,conninfo=dsn);destination=self.database()
        result=restore(archive,manifest,conninfo=destination)
        self.assertEqual(len(result['poll_restore_binding']['added_metadata']),1)
        with PgStore(conninfo=destination) as recovered:
            poll=PollBudgetStore(None,owner_binding(recovered),conninfo=destination)
            try:self.assertEqual(poll.sync_job(key),before)
            finally:poll.close()

    def test_schema1_direct_poll_restore_then_upgrade_preserves_policy(self):
        from db_sync_alt.cli.acquire import blockfrost_once
        from db_sync_alt.backup import backup,restore
        from db_sync_alt.sources.blockfrost.poller import day_at
        dsn=self.make();routes,_=gen_fixture.fixtures()
        with PgStore(conninfo=dsn) as store:
            self.assertFalse(store.supports_incremental)
            blockfrost_once(store,self.profile,target_height=gen_fixture.OFFSET+11,client=real.AccountedFake(routes))
            key='provider-poll-'+day_at(time.time());before=copy.deepcopy(store.sync_job(key))
            original=copy.deepcopy(store.meta('poll-budget-policy:'+before['day']))
        archive=Path(self.tmp.name)/'schema1-native-poll.dump';manifest=archive.with_suffix('.json')
        backup(archive,manifest,conninfo=dsn);destination=self.database()
        result=restore(archive,manifest,conninfo=destination)
        self.assertEqual(len(result['poll_restore_binding']['added_metadata']),1)
        with PgStore(conninfo=destination) as recovered:
            self.assertFalse(recovered.supports_incremental)
            self.assertEqual(recovered.sync_job(key),before)
            self.assertEqual(recovered.meta('poll-budget-policy:'+before['day']),original)
            upgrade_store(recovered);recovered.certify_ready_full()
            poll=PollBudgetStore(None,owner_binding(recovered),conninfo=destination)
            try:self.assertEqual(poll.sync_job(key),before)
            finally:poll.close()

    def test_epoch_transition_equals_full_update(self):
        dsn,store,routes=self.prepare(transition=True)
        self.run_follow(store,dsn,routes)
        self.compare_reference(routes,store,transition=True)
        self.assertEqual(verify_store(store)['digests'],store.digests(1))
        self.assertTrue(store.completed(1,'epochs',3))

    def test_provisional_fork_rebuilds_only_uncommitted_tail(self):
        dsn,store,_=self.prepare();routes,_=gen_fixture.fixtures(fork=1)
        old=store.digests(0);self.run_follow(store,dsn,routes)
        self.assertEqual(store.digests(0),old)
        self.assertNotEqual(store.block_at(0,gen_fixture.OFFSET+11)['hash'],store.block_at(1,gen_fixture.OFFSET+11)['hash'])
        self.compare_reference(routes,store)
        reuse=store.checkpoint_rows(generation=1,phase='metadata',unit='inherited')[0]['receipt']
        self.assertEqual(reuse['hi'],gen_fixture.OFFSET+10)

    def test_committed_conflict_revokes_before_new_pending(self):
        dsn,store,routes=self.prepare()
        routes['/blocks/'+str(gen_fixture.OFFSET+10)]['hash']='ff'*32
        with self.assertRaisesRegex(Exception,'canonical_conflict'):self.run_follow(store,dsn,routes)
        self.assertTrue(store.state()['revoked']);self.assertIsNone(store.pending())

    def test_two_successors_reuse_handoff_and_do_not_run_full_verifier(self):
        dsn,store,routes=self.prepare()
        first,_=self.run_follow(store,dsn,routes,gen_fixture.OFFSET+11)
        self.assertIn('/genesis',first.calls)
        with patch('db_sync_alt.verify.store.verify_store',side_effect=AssertionError('unexpected full history verification')):
            second,_=self.run_follow(store,dsn,routes,gen_fixture.OFFSET+12)
        self.assertNotIn('/genesis',second.calls)
        self.assertFalse(any(p.endswith('/cbor') for p in second.calls))
        self.assertEqual(len([p for p in second.calls if '/next?' in p]),1)
        acquisition=second.calls[2:]  # Existing poll journal needs no second metric.
        self.assertEqual(second.calls[:2],['/blocks/latest','/blocks/'+str(gen_fixture.OFFSET+12)])
        fixed=[p for p in acquisition if p=='/metrics' or '/next?' in p or
               p in {'/blocks/'+str(gen_fixture.OFFSET+n) for n in (11,12,2171,2172)}]
        self.assertEqual(len(fixed),6)  # metric + four fresh anchors + suffix page.
        self.assertEqual(store.generation()['target_height'],gen_fixture.OFFSET+12)
        verify_store(store)

    def test_actual_service_polls_during_blocked_verification_and_stops_cleanly(self):
        from db_sync_alt.service.runtime import Service, PostgresRuntime
        from psycopg import sql
        from fixtures.service import config, real_reader
        dsn,store,routes=self.prepare();store.close()
        servicefile=Path(self.tmp.name)/'follow-services.conf'
        values=psycopg.conninfo.conninfo_to_dict(dsn)
        servicefile.write_text('[follow_test]\n'+''.join(k+'='+v+'\n' for k,v in values.items())+'\n')
        reader=real_reader(self.admin,dsn,servicefile)
        self.addCleanup(self.admin.execute,sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(reader['role'])))
        previous=os.environ.get('PGSERVICEFILE');os.environ['PGSERVICEFILE']=str(servicefile)
        entered=threading.Event();release=threading.Event()
        cfg=replace(config(),pg_service='follow_test',reader=reader,shutdown_seconds=5,storage_reserves={'/tmp':1})
        cfg=replace(cfg,policy=replace(cfg.policy,poll_seconds=.05,provider_operation_seconds=5,local_operation_seconds=30))
        class Runtime(PostgresRuntime):
            def open_writer(inner,health):
                writer=super().open_writer(health);original=writer.verify_pending_incremental
                def verify(gen):
                    entered.set()
                    if not release.wait(10):raise AssertionError('missing test release')
                    return original(gen)
                writer.verify_pending_incremental=verify
                return writer
        client=real.AccountedFake(routes)
        driver=SyncDriver(cfg,client=client,profile=self.profile)
        service=Service(cfg,runtime=Runtime(cfg),sync_driver=driver)
        try:
            service.start();self.assertTrue(entered.wait(15),service.failure)
            self.assertEqual(service.health.snapshot()['schema_version'],'3')
            before=driver._poller.latest().sequence
            deadline=time.monotonic()+3
            while driver._poller.latest().sequence<before+2 and time.monotonic()<deadline:time.sleep(.01)
            self.assertGreaterEqual(driver._poller.latest().sequence,before+2)
            self.assertEqual(service.health.snapshot()['operation']['stage'],'verification')
            release.set();deadline=time.monotonic()+10
            while not service.failed.is_set() and time.monotonic()<deadline:
                with PgStore(conninfo=dsn,readonly=True) as view:
                    if view.generation()['id']==1:
                        # The node's reader sees the new generation straight from PostgreSQL.
                        with psycopg.connect(service=reader['pg_service']) as node:
                            self.assertEqual(node.execute('SELECT max(block_no) FROM block').fetchone()[0],view.generation()['witness_height'])
                        break
                time.sleep(.05)
            else:self.fail('publication not observed')
            self.assertIsNone(service.failure)
            self.assertTrue(service.stop());self.assertIsNone(service.failure)
            self.assertFalse(driver._poller.thread.is_alive())
        finally:
            release.set();service.stop()
            if previous is None:os.environ.pop('PGSERVICEFILE',None)
            else:os.environ['PGSERVICEFILE']=previous

    def test_changed_fixed_pending_anchor_does_not_retarget_or_reset(self):
        dsn,store,routes=self.prepare()
        from db_sync_alt.sources.blockfrost.sync import PgHooks
        original=PgHooks.before_publish
        def stop(hooks):hooks.stop_event.set();original(hooks)
        with patch.object(PgHooks,'before_publish',stop):self.run_follow(store,dsn,routes)
        pending=copy.deepcopy(store.pending());job=copy.deepcopy(store.sync_job('provider-active'))
        bad=copy.deepcopy(routes);bad['/blocks/'+str(pending['target_height'])]['hash']='ab'*32
        with self.assertRaises(Exception):self.run_follow(store,dsn,bad)
        self.assertEqual(store.sync_job('provider-active')['last_failure']['code'],'pending_target_changed')
        self.assertEqual(store.pending(),pending)
        after=store.sync_job('provider-active')
        self.assertEqual((after['job_id'],after['target'],after['budget']),(job['job_id'],job['target'],job['budget']))
        self.assertGreaterEqual(after['attempts'],job['attempts'])
        self.assertEqual(store.generation()['id'],0)

    def test_stopped_before_publication_preserves_fixed_pending_and_resumes(self):
        dsn,store,routes=self.prepare()
        from db_sync_alt.sources.blockfrost.sync import PgHooks
        original=PgHooks.before_publish
        def stop(hooks):hooks.stop_event.set();original(hooks)
        with patch.object(PgHooks,'before_publish',stop):
            self.run_follow(store,dsn,routes)
        self.assertEqual(store.generation()['id'],0)
        pending=store.pending();job=store.sync_job('provider-active');self.assertIsNotNone(pending)
        self.run_follow(store,dsn,routes)
        self.assertEqual(store.generation()['id'],pending['id'])
        self.assertEqual(store.sync_job('provider-active')['job_id'],job['job_id'])
        self.assertGreater(store.sync_job('provider-active')['attempts'],job['attempts'])
        verify_store(store)
