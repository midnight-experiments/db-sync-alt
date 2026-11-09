"""Real PostgreSQL crash, recovery, cache and option regressions; fake provider."""
import contextlib
import copy
import unittest
from unittest.mock import patch

from db_sync_alt.contract import Refusal
from db_sync_alt.store.database import PgStore
from db_sync_alt.sources.blockfrost import relevance
from db_sync_alt.verify.store import verify_store
from fixtures import relevance as f
from fixtures.blockfrost import Fake

class RelevanceRecoveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):f.configure('mainnet');f.SQLFixture.setup()
    @classmethod
    def tearDownClass(cls):f.SQLFixture.teardown()
    def prepare(self):
        row=f.make_tx(200,relevant=True);store,routes=f.SQLFixture.prepare([[row]],reuse_root=True)
        self.addCleanup(store.close);return store,routes
    def leave_pending(self,store,routes,*,enabled=True,missing=False):
        original=store.checkpoint
        def checkpoint(generation,phase,unit,receipt):
            if missing and phase=='relevance':return
            return original(generation,phase,unit,receipt)
        with patch.object(store,'checkpoint',side_effect=checkpoint),patch.object(store,'stage_metadata_reuse',side_effect=Refusal('interrupted')):
            with self.assertRaisesRegex(Refusal,'interrupted'):f.SQLFixture.follow(store,routes,f.S+1,relevance_enabled=enabled)
        self.assertIsNotNone(store.pending())
    def test_atomic_generation_and_coverage_rollback_at_three_crash_points(self):
        for point in ('after_begin','before_checkpoint','before_commit'):
            with self.subTest(point=point):
                store,routes=self.prepare();before=store.digests(0);begin=store.begin_continuation;checkpoint=store.checkpoint;atomic=store.atomic
                def crash_begin(**args):
                    value=begin(**args)
                    if point=='after_begin':raise Refusal('injected_crash')
                    return value
                def crash_checkpoint(generation,phase,unit,receipt):
                    if phase=='relevance' and point=='before_checkpoint':raise Refusal('injected_crash')
                    return checkpoint(generation,phase,unit,receipt)
                @contextlib.contextmanager
                def crash_atomic():
                    with atomic():
                        yield
                        if point=='before_commit' and store.pending() and store.completed(1,'relevance','coverage'):raise Refusal('injected_crash')
                with patch.object(store,'begin_continuation',side_effect=crash_begin),patch.object(store,'checkpoint',side_effect=crash_checkpoint),patch.object(store,'atomic',crash_atomic):
                    with self.assertRaisesRegex(Refusal,'injected_crash'):f.SQLFixture.follow(store,routes,f.S+1)
                self.assertIsNone(store.pending());self.assertFalse(store.checkpoint_rows(generation=1));self.assertEqual(store.digests(0),before)
                # A new writer certifies the unchanged parent and completes once.
                dsn=store.db.info.dsn;store.close()
                with PgStore(conninfo=dsn) as restarted:
                    restarted.certify_ready_full();f.SQLFixture.follow(restarted,routes,f.S+1);verify_store(restarted)
                    self.assertEqual(len(restarted.checkpoint_rows(generation=1,phase='relevance',unit='coverage')),1)
    def test_committed_pair_survives_process_restart_before_metadata(self):
        store,routes=self.prepare();self.leave_pending(store,routes)
        self.assertEqual(store.pending()['manifest']['relevance_coverage'],store.checkpoint_rows(generation=1,phase='relevance',unit='coverage')[0]['receipt'])
        dsn=store.db.info.dsn;store.close()
        with PgStore(conninfo=dsn) as restarted:
            restarted.certify_ready_full();f.SQLFixture.follow(restarted,routes,f.S+1);verify_store(restarted)
    def test_legacy_missing_row_repairs_exact_proof_without_provider_or_counter_reset(self):
        store,routes=self.prepare();self.leave_pending(store,routes,missing=True)
        pending=store.pending();self.assertFalse(store.completed(1,'relevance','coverage'))
        with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):relevance.verify_generation(store,1)
        self.assertFalse(store.completed(1,'relevance','coverage'))
        # The durable budget is unrelated to repairing the proof row.
        marker={'job_id':'fixture-budget','attempts':7,'budget':100,'state':'active'};store.save_sync_job('fixture-preserved',marker)
        client=Fake(routes)
        with patch.object(client,'get',side_effect=AssertionError('repair must not call provider')):
            cert=relevance.pending_certificate(store,f.PROFILE,1)
        self.assertEqual(cert,pending['manifest']['relevance_coverage']);self.assertEqual(store.sync_job('fixture-preserved'),marker)
        self.assertEqual(client.calls,[]);self.assertEqual(len(store.checkpoint_rows(generation=1,phase='relevance',unit='coverage')),1)
        f.SQLFixture.follow(store,routes,f.S+1);verify_store(store)
    def test_conflicting_legacy_row_refuses_without_provider_or_fact_changes(self):
        store,routes=self.prepare();self.leave_pending(store,routes,missing=True)
        forged=copy.deepcopy(store.pending()['manifest']['relevance_coverage']);forged['profile']='00'*32
        store.checkpoint(1,'relevance','coverage',forged);before=store.digests(0);client=Fake(routes)
        from db_sync_alt.sources.blockfrost.follow import update_follow
        with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
            update_follow(store,f.Provider(50000,client),f.PROFILE,f.S+1,hooks=f.FixtureHooks(),relevance_enabled=True)
        self.assertFalse(client.calls);self.assertEqual(store.digests(0),before);self.assertEqual(store.checkpoint_rows(generation=1,phase='relevance',unit='coverage')[0]['receipt'],forged)
    def test_forged_missing_certificate_is_not_reconstructed(self):
        from psycopg.types.json import Jsonb
        store,routes=self.prepare();self.leave_pending(store,routes,missing=True)
        manifest=store.pending()['manifest'];manifest['relevance_coverage']['range'][0]-=1
        store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=1',(Jsonb(manifest),))
        with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):relevance.pending_certificate(store,f.PROFILE,1)
        self.assertFalse(store.completed(1,'relevance','coverage'))
    def test_resume_mode_changes_refuse_before_any_provider_call(self):
        from db_sync_alt.sources.blockfrost.follow import update_follow
        for enabled in (True,False):
            store,routes=self.prepare();self.leave_pending(store,routes,enabled=enabled);client=Fake(routes)
            with self.assertRaisesRegex(Refusal,'relevance_mode_mismatch'):
                update_follow(store,f.Provider(50000,client),f.PROFILE,f.S+1,hooks=f.FixtureHooks(),relevance_enabled=not enabled)
            self.assertFalse(client.calls)
            import threading
            from types import SimpleNamespace
            from db_sync_alt.errors import LifecycleFailure
            from db_sync_alt.sources.blockfrost.sync import SyncDriver
            from fixtures.database import AccountedFake
            from fixtures.sync import health
            accounted=AccountedFake(routes);status=health()
            driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':not enabled}),client=accounted,profile=f.PROFILE)
            with self.assertRaisesRegex(LifecycleFailure,'relevance_mode_mismatch'):driver.run_once(store,status,threading.Event())
            self.assertFalse(accounted.calls);self.assertIsNone(store.sync_job('provider-active'))
            self.assertEqual(status.snapshot()['relevance']['reason'],'relevance_mode_mismatch')
    def test_default_fullscan_has_zero_new_relevance_reads(self):
        store,routes=self.prepare()
        with patch.object(relevance,'certificate',side_effect=AssertionError('off path certificate')),patch.object(relevance,'qualified_parent',side_effect=AssertionError('off path lineage')):
            _,calls=f.SQLFixture.follow(store,routes,f.S+1,relevance_enabled=False)
        self.assertGreater(sum('/utxos' in p for p in calls),0);self.assertNotIn('relevance_coverage',store.generation()['manifest']);verify_store(store)
    def test_hot_cache_avoids_lineage_reads_but_revision_profile_and_revocation_fence(self):
        store,routes=self.prepare();relevance.qualified_parent(store,f.PROFILE)
        with patch.object(store,'checkpoint_rows',side_effect=AssertionError('hot lineage walk')):
            self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
        self.leave_pending(store,routes)  # Legitimate mutations advance the revision through guarded triggers.
        with patch('db_sync_alt.verify.completion.validate_completion',wraps=__import__('db_sync_alt.verify.completion',fromlist=['validate_completion']).validate_completion) as validation:
            self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE));self.assertGreater(validation.call_count,0)
        wrong=copy.deepcopy(f.PROFILE);wrong.identity['config_sha256']='00'*32
        with self.assertRaisesRegex(Refusal,'relevance_parent_changed'):relevance.qualified_parent(store,wrong)
        store.mark_conflict();self.assertIsNone(relevance.qualified_parent(store,f.PROFILE))
    def test_real_driver_refuses_uncertified_provider_lineage_before_accounted_request(self):
        import threading
        from types import SimpleNamespace
        from db_sync_alt.errors import LifecycleFailure
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        from fixtures.database import AccountedFake
        from fixtures.sync import health
        store,routes=self.prepare();f.SQLFixture.follow(store,routes,f.S+1,relevance_enabled=False)
        client=AccountedFake(routes);status=health()
        driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),client=client,profile=f.PROFILE)
        with self.assertRaisesRegex(LifecycleFailure,'relevance_parent_unsupported'):driver.run_once(store,status,threading.Event())
        self.assertFalse(client.calls);self.assertIsNone(store.sync_job('provider-active'))
        self.assertEqual(status.snapshot()['relevance']['reason'],'relevance_parent_unsupported')
    def test_real_driver_preserves_relevance_refusal_and_accounted_job_without_publication(self):
        import threading
        from types import SimpleNamespace
        from db_sync_alt.errors import LifecycleFailure
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        from fixtures.database import AccountedFake
        from fixtures.sync import health
        store,routes=self.prepare();client=AccountedFake(routes);status=health()
        before=copy.deepcopy(store.generation());active=[]
        driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),client=client,profile=f.PROFILE,fixed_target=f.S+1)
        def refuse(*args,**kwargs):
            active.append(copy.deepcopy(store.sync_job('provider-active')))
            raise Refusal('relevance_inventory_invalid')
        with patch('db_sync_alt.sources.blockfrost.follow.update_follow',side_effect=refuse):
            with self.assertRaisesRegex(LifecycleFailure,'relevance_inventory_invalid'):
                driver.run_once(store,status,threading.Event())
        self.assertEqual(store.generation(),before);self.assertIsNone(store.pending())
        saved=store.sync_job('provider-active');self.assertEqual(len(active),1)
        for key in ('job_id','parent','target','budget','attempts','state'):
            self.assertEqual(saved[key],active[0][key],key)
        verify_store(store)
    def test_real_driver_reports_relevance_and_preserves_accounted_job_on_resume(self):
        import threading
        from types import SimpleNamespace
        from db_sync_alt.service.health import HealthState
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        from fixtures.database import AccountedFake
        store,routes=self.prepare();client=AccountedFake(routes)
        status=HealthState(service_version='test',schema_version=3,network='mainnet',profile='fixture')
        driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),client=client,profile=f.PROFILE,fixed_target=f.S+1)
        driver.run_once(store,status,threading.Event())
        self.assertEqual(status.snapshot()['relevance'],{'requested':'relevance','effective':'relevance','reason':'verified'})
        self.assertEqual(store.sync_job('provider-active')['state'],'completed');verify_store(store)
        calls=len(client.calls);job=copy.deepcopy(store.sync_job('provider-active'))
        driver.run_once(store,status,threading.Event())
        self.assertEqual(store.sync_job('provider-active'),job)
        self.assertEqual(len(client.calls),calls+2)  # Only the next idle head/anchor poll.
    def test_switching_off_does_not_disable_persisted_proof_verification(self):
        from psycopg.types.json import Jsonb
        store,routes=self.prepare();f.SQLFixture.follow(store,routes,f.S+1)
        manifest=store.generation()['manifest'];manifest['relevance_coverage']['profile']='00'*32
        store.db.execute('ALTER TABLE chain.generations DISABLE TRIGGER USER')
        try:store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=1',(Jsonb(manifest),))
        finally:store.db.execute('ALTER TABLE chain.generations ENABLE TRIGGER USER')
        with self.assertRaises(Refusal):verify_store(store)

class AllProfileRelevanceTests(unittest.TestCase):
    def test_enabled_and_disabled_equivalence_for_each_packaged_profile(self):
        from db_sync_alt.networks import names
        for network in names():
            with self.subTest(network=network):
                f.configure(network);f.SQLFixture.setup()
                try:
                    rows=[[f.make_tx(220,relevant=True),f.make_tx(221)]];optimized,routes=f.SQLFixture.prepare(rows,reuse_root=True);reference,other=f.SQLFixture.prepare(rows,reuse_root=True)
                    try:
                        _,calls=f.SQLFixture.follow(optimized,routes,f.T+1,relevance_enabled=True)
                        _,fullcalls=f.SQLFixture.follow(reference,other,f.T+1,relevance_enabled=False)
                        verify_store(optimized);verify_store(reference)
                        self.assertEqual(optimized.digests(1),reference.digests(1))
                        irrelevant='/txs/'+rows[0][1]['tx_hash']+'/utxos'
                        self.assertNotIn(irrelevant,calls);self.assertIn(irrelevant,fullcalls)
                        from db_sync_alt.store.incremental import profile_digest
                        from db_sync_alt.contract import digest
                        self.assertEqual(profile_digest(optimized.generation()),digest(f.PROFILE.identity))
                        self.assertEqual(optimized.network.network,network)
                    finally:optimized.close();reference.close()
                finally:f.SQLFixture.teardown();f.configure('mainnet')
