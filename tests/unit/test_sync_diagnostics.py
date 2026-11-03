"""Sanitized operational diagnostics, including actual rollback/reopen boundaries."""
import copy
import json
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from db_sync_alt.sources.blockfrost.adapter import inventory
from db_sync_alt.contract import Refusal
from db_sync_alt.errors import LifecycleFailure,TransientSyncFailure
from db_sync_alt.sources.blockfrost.sync import AccountedProvider,SyncDriver,safe_failure
from fixtures.sync import Store,Client,Clock,health,FixtureUpdate
from fixtures.networks import PREPROD, PREPROD_PROFILE


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.store=Store();self.clock=Clock();self.client=Client(self.clock,confirmed=101)
        self.stop=threading.Event();self.health=health();self.update=FixtureUpdate()
        self.driver=SyncDriver(SimpleNamespace(sync={}),client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=self.update)
    def run_driver(self):return self.driver.run_once(self.store,self.health,self.stop)
    def test_exact_refusal_before_mapping_and_restart_preserves_job(self):
        self.assertIsNotNone(safe_failure({'version':1,'code':'pending_anchor_changed','stage':'anchors','attempts':1,'at':1700000000}))
        self.update.error=Refusal('cbor_unsupported_simple')
        with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):self.run_driver()
        old=copy.deepcopy(self.store.jobs['provider-active']);failure=old['last_failure']
        self.assertEqual(failure['code'],'cbor_unsupported_simple');self.assertEqual(failure['attempts'],old['attempts'])
        self.assertEqual(old['error_code'],None);self.assertEqual(old['retry_at'],0)
        self.update.error=None
        self.driver=SyncDriver(SimpleNamespace(sync={}),client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=self.update)
        self.run_driver();new=self.store.jobs['provider-active']
        self.assertEqual(new['job_id'],old['job_id']);self.assertEqual(new['budget'],old['budget'])
        self.assertGreater(new['attempts'],old['attempts']);self.assertEqual(new['last_failure'],failure)
    def test_fatal_conflict_keeps_precise_record_and_existing_revocation(self):
        self.update.error=Refusal('provider_origin_conflict')
        with self.assertRaisesRegex(LifecycleFailure,'canonical_conflict'):self.run_driver()
        self.assertTrue(self.store.revoked)
        self.assertEqual(self.store.jobs['provider-active']['last_failure']['code'],'provider_origin_conflict')
    def test_pending_anchor_refusals_keep_their_own_lifecycle_code(self):
        for code in ('pending_target_changed','pending_suffix_base_changed','pending_anchor_changed'):
            with self.subTest(code=code):
                self.setUp();self.update.error=Refusal(code)
                with self.assertRaises(LifecycleFailure) as failure:self.run_driver()
                self.assertIs(type(failure.exception),LifecycleFailure);self.assertEqual(failure.exception.code,code)
                job=self.store.jobs['provider-active']
                self.assertEqual(job['last_failure']['code'],code);self.assertFalse(self.store.revoked)
                self.assertEqual((job['state'],job['retry_at'],job['error_code']),('active',0,None))
        self.setUp();self.update.error=Refusal('provider_metadata_gap')
        with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):self.run_driver()
    def test_transient_backoff_and_exact_epoch_failure_are_independent(self):
        self.update.error=Refusal('provider_epoch_not_ready')
        with self.assertRaises(TransientSyncFailure):self.run_driver()
        job=self.store.jobs['provider-active'];self.assertEqual(job['last_failure']['code'],'provider_epoch_not_ready')
        self.assertEqual(job['error_code'],'provider_unavailable');self.assertGreater(job['retry_at'],self.clock())
    def test_unknown_exception_and_secret_context_do_not_serialize(self):
        secret='postgres://user:secret@private/key'
        def failing(store,provider,profile,target,*,hooks):
            provider.set_failure_context(secret,generation=True,block_height=-1,block_hash=secret,transaction_hash=secret,transaction_index=10**1000,arbitrary=secret)
            provider.get('/unit');raise Refusal(secret)
        self.driver.update_fn=failing
        with self.assertLogs('db_sync_alt.sources.blockfrost.sync',level='WARNING') as logs:
            with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):self.run_driver()
        record=self.store.jobs['provider-active']['last_failure'];self.assertEqual(record['code'],'unclassified_failure')
        self.assertEqual(record['stage'],'unknown');self.assertEqual(set(record),{'version','code','stage','attempts','at'})
        self.assertNotIn(secret,json.dumps(record)+str(logs.output))
        self.store=Store();self.driver.update_fn=lambda *a,**k:(_ for _ in ()).throw(RuntimeError(secret))
        with self.assertRaisesRegex(RuntimeError,'postgres'):self.run_driver()
        self.assertEqual(self.store.jobs['provider-active']['last_failure']['code'],'unclassified_failure')
    def test_recording_failure_logs_fixed_code_without_masking_original(self):
        save=self.store.save_sync_job
        def fail_diagnostic(key,body):
            if 'last_failure' in body:raise RuntimeError('secret write detail')
            return save(key,body)
        self.store.save_sync_job=fail_diagnostic;self.update.error=Refusal('cbor_truncated')
        with self.assertLogs('db_sync_alt.sources.blockfrost.sync',level='WARNING') as logs:
            with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):self.run_driver()
        self.assertEqual(logs.output,['WARNING:db_sync_alt.sources.blockfrost.sync:failure_record_unavailable'])
        self.assertNotIn('last_failure',self.store.jobs['provider-active'])
    def test_corrupt_historical_inspection_is_bounded_and_never_admission(self):
        good={'version':1,'code':'cbor_truncated','stage':'events.inventory','attempts':3,'at':1700000000.0}
        for field,value in [('version',True),('code',{}),('code',[]),('stage',[]),('stage',{}),('at',10**1000),('at',float('nan')),('at',float('inf')),('attempts',True),('attempts',-1)]:
            with self.subTest(field=field,value=type(value).__name__):self.assertIsNone(safe_failure(dict(good,**{field:value})))
        self.assertEqual(safe_failure(dict(good,secret='credential',transaction_hash='bad',generation=True)),good)
        self.update.error=Refusal('cbor_truncated')
        with self.assertRaises(LifecycleFailure):self.run_driver()
        self.store.jobs['provider-active']['last_failure']={'secret':'malformed historical object'}
        self.update.error=None;self.run_driver();self.assertEqual(self.store.jobs['provider-active']['state'],'completed')
    def test_inventory_decode_records_tx_but_next_page_or_count_failure_clears_it(self):
        job={'job_id':'fixture','attempts':0,'budget':50,'state':'active','kind':'poll'}
        self.store.save_sync_job('test',job)
        provider=AccountedProvider(self.store,'test',job,self.client,self.health,self.stop,clock=self.clock,network=PREPROD)
        provider.set_failure_context('events',generation=12)
        header={'height':99,'hash':b'b'*32,'tx_count':2}
        row={'cbor':'00','tx_hash':'a'*64}
        provider.pages=lambda path:iter([[row]])
        with patch('db_sync_alt.sources.blockfrost.adapter.decode_transaction',side_effect=Refusal('cbor_truncated')):
            with self.assertRaises(Refusal):inventory(provider,header)
        self.assertEqual(provider.failure_context['transaction_hash'],'a'*64)
        self.assertEqual(provider.failure_context['transaction_index'],0)
        decoded={'hash':b'a'*32,'body_bytes':1}
        with patch('db_sync_alt.sources.blockfrost.adapter.decode_transaction',return_value=decoded):
            with self.assertRaisesRegex(Refusal,'provider_transaction_count_mismatch'):inventory(provider,header)
        self.assertNotIn('transaction_hash',provider.failure_context)
        def pages(path):yield [row];raise Refusal('provider_bad_page')
        provider.pages=pages
        with patch('db_sync_alt.sources.blockfrost.adapter.decode_transaction',return_value=decoded):
            with self.assertRaisesRegex(Refusal,'provider_bad_page'):inventory(provider,header)
        self.assertNotIn('transaction_hash',provider.failure_context)
        provider.set_failure_context(['secret'],generation=False,transaction_hash={'secret':'x'})
        self.assertEqual(provider.failure_context,{'stage':'unknown'})
    def test_confirmed_height_mismatch_is_recorded_after_poll_job_starts(self):
        get=self.client.get
        def bad(path,purpose):
            value=get(path,purpose)
            if path=='/blocks/101':value['height']=102
            return value
        self.client.get=bad
        with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):self.run_driver()
        self.assertNotIn('provider-active',self.store.jobs)
        job=next(iter(self.store.jobs.values()));f=job['last_failure']
        self.assertEqual(f['code'],'invalid_coverage');self.assertEqual(f['stage'],'poll');self.assertEqual(f['block_height'],101)

    def test_optional_context_callback_cannot_change_inventory_contract(self):
        provider=SimpleNamespace(set_failure_context=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('secret callback')))
        with self.assertLogs('db_sync_alt.sources.blockfrost.adapter',level='WARNING') as logs:
            self.assertEqual(inventory(provider,{'tx_count':0}),[])
        self.assertEqual(logs.output,['WARNING:db_sync_alt.sources.blockfrost.adapter:failure_context_unavailable'])

    def test_poll_failure_keeps_exact_code_without_acquisition_job(self):
        self.client.get=lambda *a,**k: {'bad':'secret'}
        with self.assertRaises(Refusal):self.run_driver()
        self.assertNotIn('provider-active',self.store.jobs)
        job=next(iter(self.store.jobs.values()));self.assertEqual(job['last_failure']['code'],'provider_bad_block')


# Reuse only fixture helpers, not the unrelated inherited test methods.

if __name__=='__main__':unittest.main(verbosity=2)
