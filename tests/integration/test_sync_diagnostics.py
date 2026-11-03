"""Sanitized operational diagnostics, including actual rollback/reopen boundaries."""
import copy
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch
from db_sync_alt.contract import Refusal
from db_sync_alt.errors import LifecycleFailure




# Reuse only fixture helpers, not the unrelated inherited test methods.
import fixtures.database as fixture
class DiagnosticStoreTests(unittest.TestCase):
    setUpClass=classmethod(fixture.SyncFixture.setUpClass.__func__)
    tearDownClass=classmethod(fixture.SyncFixture.tearDownClass.__func__)
    setUp=fixture.SyncFixture.setUp
    tearDown=fixture.SyncFixture.tearDown
    database=fixture.SyncFixture.database
    make=fixture.SyncFixture.make
    state=fixture.SyncFixture.state
    run_driver=fixture.SyncFixture.run_driver
    def test_inventory_failure_survives_process_restart_and_resumes_same_job(self):
        from db_sync_alt.store.database import PgStore
        dsn=self.make();routes,_=fixture.gen_fixture.fixtures();broken=copy.deepcopy(routes)
        endpoint='/blocks/'+fixture.gen_fixture.block(12)['hash']+'/txs/cbor?count=100&page=1&order=asc'
        broken[endpoint][0]['cbor']='f8ff'
        with PgStore(conninfo=dsn) as store:
            before=store.digests(0)
            with self.assertRaisesRegex(LifecycleFailure,'invalid_coverage'):self.run_driver(store,broken)
            saved=store.sync_job('provider-active');failure=saved['last_failure']
            self.assertEqual(failure['code'],'cbor_unsupported_simple');self.assertEqual(failure['stage'],'events.inventory')
            self.assertEqual(failure['generation'],1);self.assertEqual(failure['block_height'],fixture.gen_fixture.OFFSET+12)
            self.assertEqual(failure['transaction_index'],0);self.assertEqual(failure['transaction_hash'],broken[endpoint][0]['tx_hash'])
            self.assertEqual(store.digests(0),before);self.assertTrue(store.completed(1,'events',fixture.gen_fixture.OFFSET+11));self.assertFalse(store.completed(1,'events',fixture.gen_fixture.OFFSET+12))
        env=dict(os.environ,DIAGNOSTIC_DSN=dsn)
        script="import os,json;from db_sync_alt.store.database import PgStore;from db_sync_alt.sources.blockfrost.sync import safe_failure\nwith PgStore(conninfo=os.environ['DIAGNOSTIC_DSN'],readonly=True) as s: print(json.dumps(safe_failure(s.sync_job('provider-active')['last_failure'])))"
        result=subprocess.run([sys.executable,'-c',script],env=env,capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr);self.assertEqual(json.loads(result.stdout),failure)
        with PgStore(conninfo=dsn) as store:
            self.run_driver(store,routes);done=store.sync_job('provider-active')
            self.assertEqual(done['last_failure'],failure);self.assertEqual(done['job_id'],saved['job_id']);self.assertEqual(done['budget'],saved['budget']);self.assertGreater(done['attempts'],saved['attempts'])
            self.assertEqual(store.generation()['target_height'],fixture.gen_fixture.OFFSET+12);self.assertEqual(store.digests(0),before)
    def test_fact_transaction_rollback_still_records_precise_failure(self):
        from db_sync_alt.store.database import PgStore
        dsn=self.make();routes,_=fixture.gen_fixture.fixtures()
        with PgStore(conninfo=dsn) as store:
            before=store.digests(0);insert=store.insert
            def failing(table,row,generation):
                if table=='inputs':raise Refusal('immutable_fact_conflict')
                return insert(table,row,generation)
            with patch.object(store,'insert',side_effect=failing):
                with self.assertRaisesRegex(LifecycleFailure,'canonical_conflict'):self.run_driver(store,routes)
            job=store.sync_job('provider-active');f=job['last_failure']
            self.assertEqual(f['code'],'immutable_fact_conflict');self.assertEqual(f['stage'],'events.commit')
            self.assertEqual(f['block_height'],fixture.gen_fixture.OFFSET+11);self.assertIn('transaction_hash',f)
            self.assertFalse(store.completed(1,'events',fixture.gen_fixture.OFFSET+11));self.assertEqual(store.digests(0),before)
            self.assertEqual(store.db.execute('SELECT count(*) FROM chain.transactions WHERE first_generation=1').fetchone()[0],0)
            self.assertTrue(store.state()['revoked'])
        with PgStore(conninfo=dsn) as store:self.assertEqual(store.sync_job('provider-active')['last_failure'],f)

if __name__=='__main__':unittest.main(verbosity=2)
