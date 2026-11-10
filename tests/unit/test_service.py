# SPDX-License-Identifier: Apache-2.0
"""Lifecycle failure injection; Docker network-none with isolated loopback HTTP."""
from dataclasses import replace
import json
import os
import subprocess
from pathlib import Path
import socket
import sys
import tempfile
import threading
import time
import unittest
import weakref
from types import SimpleNamespace
from unittest.mock import patch

from db_sync_alt.service.runtime import (LifecycleFailure, Service, ServiceConfig,
                        TransientSyncFailure, failure_code, load_sync_driver)
from fixtures.service import READER, REPORT, Runtime, config, eventually, get, point












class Sync:
    def __init__(self, runtime):
        self.runtime = runtime
        self.calls = 0
        self.entered = threading.Event()
        self.block = None
        self.error = None

    def run_once(self, store, health, stop_event):
        if threading.get_ident() != store.owner:
            raise AssertionError('sync did not run on writer-owning thread')
        self.calls += 1
        self.entered.set()
        health.observe_provider(point(self.runtime.target + 2160), point(self.runtime.target))
        if self.block:
            self.block.wait(2)
        if self.error:
            raise self.error
        if stop_event.is_set():
            return






class LifecycleTests(unittest.TestCase):
    def make(self, cfg=None, runtime=None, sync=None):
        runtime = runtime or Runtime()
        sync = sync or Sync(runtime)
        service = Service(cfg or config(), runtime=runtime, sync_driver=sync)
        self.addCleanup(service.stop)
        service.start()
        return service, runtime, sync

    def test_unused_startup_history_is_not_retained_while_serving(self):
        class Inspection(dict):pass
        runtime=Runtime();refs=[]
        original=runtime.inspect
        def inspect():
            value=Inspection(original())
            value['generations']=[{'manifest':{'history':'x'*4096}}]
            refs.append(weakref.ref(value))
            return value
        runtime.inspect=inspect
        service,runtime,sync=self.make(runtime=runtime)
        eventually(sync.entered.is_set)
        self.assertEqual(runtime.verifications,1)
        self.assertIsNone(refs[0](), 'unused startup inspection survived into the serving loop')
        self.assertTrue(service.stop())
        self.assertEqual(runtime.closed,1)

    def test_ready_idle_reader_check_and_graceful_owner_thread_close(self):
        service, runtime, sync = self.make()
        eventually(lambda: service.health.health()[0] == 200)
        code, status = get(service, '/health')
        self.assertEqual(code, 200)
        self.assertEqual(status['reader'], {k: REPORT[k] for k in ('role', 'privileges', 'settings', 'warnings')})
        self.assertEqual(status['warnings'], ['node_requirements_unchecked'])
        self.assertEqual((runtime.verifications, runtime.bound), (1, 1))
        self.assertTrue(service.stop())
        self.assertEqual(runtime.closed, 1)
        self.assertEqual(service.health.snapshot()['lifecycle'], 'stopped')

    def test_empty_database_restores_once_then_restart_validates_without_restore(self):
        runtime = Runtime()
        runtime.inspection = {'empty': True}
        cfg = config(backup_archive='/tmp/verified.dump', backup_manifest='/tmp/verified.json')
        first, _, _ = self.make(cfg=cfg, runtime=runtime)
        eventually(lambda: first.ready.is_set())
        self.assertTrue(first.stop())
        second, _, _ = self.make(runtime=runtime)
        eventually(lambda: second.ready.is_set())
        self.assertEqual(runtime.restores, 1)
        self.assertEqual(runtime.verifications, 2)

    def test_missing_invalid_and_incomplete_restore_fail_without_serving(self):
        for kind, expected in (('missing', 'missing_backup'), ('corrupt', 'invalid_backup'),
                               ('partial', 'incomplete_restore'), ('foreign', 'invalid_identity')):
            with self.subTest(kind=kind):
                runtime = Runtime()
                cfg = config()
                if kind in {'missing', 'corrupt'}:
                    runtime.inspection = {'empty': True}
                if kind == 'corrupt':
                    cfg = replace(cfg, backup_archive='/tmp/bad.dump', backup_manifest='/tmp/bad.json')
                    runtime.restore_error = LifecycleFailure('invalid_backup')
                if kind == 'partial':
                    runtime.inspection['migration_state'] = 'restoring'
                if kind == 'foreign':
                    runtime.inspect_error = RuntimeError('unowned_database')
                service, _, _ = self.make(cfg=cfg, runtime=runtime)
                eventually(service.failed.is_set)
                self.assertEqual(service.failure, expected)
                self.assertEqual(runtime.bound, 0)
                self.assertEqual(get(service, '/health')[0], 503)
                self.assertEqual(runtime.verifications, 0)
                service.stop()

    def test_duplicate_startup_does_not_start_fetch_or_grant(self):
        runtime = Runtime()
        first, _, _ = self.make(runtime=runtime)
        eventually(lambda: first.ready.is_set())
        second, _, sync = self.make(runtime=runtime)
        eventually(second.failed.is_set)
        self.assertEqual(second.failure, 'writer_busy')
        self.assertEqual(sync.calls, 0)
        self.assertEqual(runtime.bound, 1)
        self.assertTrue(first.ready.is_set())

    def test_database_loss_changes_health_and_reader_loss_not_hidden_by_alive_loop(self):
        service, runtime, _ = self.make()
        eventually(lambda: service.health.health()[0] == 200)
        runtime.reader_working = False
        eventually(lambda: service.health.snapshot()['serving'] == 'unready')
        self.assertEqual(service.health.health()[0], 503)
        self.assertEqual(service.health.snapshot()['error_code'], 'sql_unavailable')
        runtime.reader_working = LifecycleFailure('backend_identity_mismatch')
        eventually(lambda: service.health.snapshot()['error_code'] == 'backend_identity_mismatch')
        self.assertEqual(service.health.health()[0], 503)
        runtime.reader_working = RuntimeError('postgres://alice:do-not-print@private-secret-directory')
        eventually(lambda: service.health.snapshot()['error_code'] == 'sql_unavailable')
        runtime.reader_working = True
        eventually(lambda: service.health.health()[0] == 200)
        runtime.reachable = False
        eventually(lambda: service.health.snapshot()['database']['reachable'] is False)
        self.assertEqual(service.health.health()[0], 503)
        runtime.reachable = True
        eventually(lambda: service.health.health()[0] == 200)

    def test_blocked_startup_keeps_http_responsive_and_shutdown_bounded(self):
        runtime = Runtime()
        runtime.block_inspect = threading.Event()
        service, _, _ = self.make(runtime=runtime)
        try:
            started = time.monotonic()
            self.assertEqual(get(service, '/status')[0], 200)
            self.assertEqual(get(service, '/health')[0], 503)
            self.assertLess(time.monotonic() - started, 1)
            started = time.monotonic()
            self.assertFalse(service.stop())
            self.assertLess(time.monotonic() - started, 0.6)
            self.assertEqual(service.failure, 'shutdown_timeout')
            self.assertEqual(runtime.bound, 0)
        finally:
            runtime.block_inspect.set()
            eventually(lambda: all(not thread.is_alive() for thread in service._threads))

    def test_blocked_provider_cannot_be_hidden_by_fresh_supervisor_heartbeat(self):
        runtime = Runtime()
        sync = Sync(runtime)
        sync.block = threading.Event()
        service, _, _ = self.make(runtime=runtime, sync=sync)
        try:
            self.assertTrue(sync.entered.wait(1))
            eventually(lambda: 'operation_stalled' in service.health.snapshot()['reason_codes'])
            value = get(service, '/health')
            self.assertEqual(value[0], 503)
            self.assertLess(value[1]['workers']['sync']['heartbeat_age_seconds'], 0.1)
            self.assertEqual(get(service, '/status')[0], 200)
        finally:
            sync.block.set()

    def test_transient_and_quota_retry_keep_coverage_without_polling_provider(self):
        for code in ('provider_unavailable', 'quota_exhausted'):
            with self.subTest(code=code):
                runtime = Runtime()
                sync = Sync(runtime)
                sync.error = TransientSyncFailure(code, retry_at=time.time() + 1)
                service, _, _ = self.make(runtime=runtime, sync=sync)
                eventually(lambda: service.health.snapshot()['sync'] in {'backoff', 'quota_wait'})
                eventually(lambda: service.health.health()[0] == 200)
                before = sync.calls
                for _ in range(5):
                    self.assertEqual(get(service, '/status')[0], 200)
                self.assertEqual(sync.calls, before)
                self.assertIsNone(service.health.snapshot()['operation'])
                service.stop()

    def test_conflict_reports_revoked_without_late_ready_overwrite(self):
        service, runtime, _ = self.make()
        eventually(lambda: service.health.health()[0] == 200)
        runtime.revoked = True
        eventually(service.failed.is_set)
        time.sleep(0.05)
        value = service.health.snapshot()
        self.assertEqual((value['lifecycle'], value['sync'], value['serving']), ('halted', 'halted', 'revoked'))
        self.assertEqual(value['error_code'], 'canonical_conflict')

    def test_wrong_profile_verification_failure_closes_writer(self):
        runtime = Runtime()
        runtime.verify_error = RuntimeError('untrusted_profile_identity')
        service, _, sync = self.make(runtime=runtime)
        eventually(service.failed.is_set)
        eventually(lambda: runtime.closed == 1)
        self.assertEqual(service.failure, 'invalid_identity')
        self.assertEqual(runtime.bound, 0)
        self.assertEqual(sync.calls, 0)

    def test_reader_that_cannot_be_granted_or_verified_fails_before_sync(self):
        for error, code in ((LifecycleFailure('invalid_reader_role'), 'invalid_reader_role'),
                            (LifecycleFailure('schema_upgrade_required'), 'schema_upgrade_required'),
                            (RuntimeError('reader_privileges_not_exact'), 'invalid_reader_role')):
            with self.subTest(code=code):
                runtime = Runtime()
                runtime.bind_error = error
                service, _, sync = self.make(runtime=runtime)
                eventually(service.failed.is_set)
                self.assertEqual(service.failure, code)
                value = service.health.snapshot()
                # Not revoked: what PostgreSQL serves does not depend on this process.
                self.assertEqual((value['lifecycle'], value['serving'], value['reader']), ('failed', 'unknown', None))
                self.assertEqual(sync.calls, 0)
                service.stop()

    def test_reader_setting_warnings_are_reported_but_do_not_fail_health(self):
        runtime = Runtime()
        report = dict(REPORT, warnings=['reader_statement_timeout_unset', 'unknown_text_never_echoed'],
                      settings=dict(REPORT['settings'], statement_timeout=None, search_path='x;\nevil'))
        runtime.bind_reader = lambda store: (setattr(runtime, 'bound', 1), report)[1]
        service, _, _ = self.make(runtime=runtime)
        eventually(lambda: service.health.health()[0] == 200)
        status = get(service, '/health')[1]
        self.assertEqual(status['warnings'], ['reader_statement_timeout_unset', 'node_requirements_unchecked'])
        self.assertEqual((status['reader']['settings']['statement_timeout'], status['reader']['settings']['search_path']), (None, 'set'))

    def test_blocked_database_observer_does_not_starve_writer_heartbeat(self):
        service, runtime, _ = self.make()
        eventually(lambda: service.health.health()[0] == 200)
        runtime.block_observe = threading.Event()
        try:
            time.sleep(0.15)
            value = service.health.snapshot()
            self.assertLess(value['workers']['sync']['heartbeat_age_seconds'], 0.05)
            self.assertEqual(get(service, '/status')[0], 200)
        finally:
            runtime.block_observe.set()

    def test_missing_p2_never_starts_a_fake_healthy_sync_loop(self):
        with patch('db_sync_alt.service.runtime.importlib.import_module', side_effect=ImportError('private secret path')):
            with self.assertRaises(LifecycleFailure) as captured:
                load_sync_driver(config())
        self.assertEqual(captured.exception.code, 'sync_unavailable')

    def test_epoch_parameter_refusals_map_to_reported_codes(self):
        from db_sync_alt.contract import Refusal
        for code in ('epoch_parameters_upgrade_required', 'epoch_parameters_source_required', 'upgrade_required_before_backup'):
            self.assertEqual(failure_code(Refusal(code)), 'schema_upgrade_required')
        for code in ('invalid_epoch_max_tx_size', 'epoch_parameter_receipt_mismatch', 'epoch_parameters_upgrade_invalid'):
            self.assertEqual(failure_code(Refusal(code)), 'invalid_coverage')

    def test_secret_exceptions_never_enter_status_or_failure_code(self):
        runtime = Runtime()
        runtime.verify_error = RuntimeError('postgres://alice:do-not-print@private-secret-directory')
        service, _, _ = self.make(runtime=runtime)
        eventually(service.failed.is_set)
        self.assertEqual(service.failure, 'internal_error')
        self.assertNotIn('do-not-print', json.dumps(get(service, '/status')[1]))


    def test_relevance_failure_codes_remain_visible_in_service_health(self):
        for code in ('relevance_parent_unsupported', 'relevance_mode_mismatch', 'relevance_coverage_invalid',
                     'relevance_parent_changed', 'relevance_inventory_invalid', 'relevance_output_unqualified',
                     'relevance_profile_address_invalid', 'relevance_work_unit_limit'):
            with self.subTest(code=code):
                runtime = Runtime(); sync = Sync(runtime); sync.error = LifecycleFailure(code)
                service, _, _ = self.make(runtime=runtime, sync=sync)
                eventually(service.failed.is_set)
                self.assertEqual(service.failure, code)
                status, body = get(service, '/health')
                self.assertEqual((status, body['error_code']), (503, code))
                self.assertTrue(service.stop())

class ConfigTests(unittest.TestCase):
    def test_node_requirements_are_compared_with_the_profile_and_only_reported(self):
        from db_sync_alt.networks import directory
        from db_sync_alt.service.runtime import node_requirement_warnings
        profile = directory('preprod')
        names = sorted(json.loads((profile / 'source-manifest.json').read_text())['files'])
        with tempfile.TemporaryDirectory() as tmp:
            node_dir = Path(tmp)
            for name in names:
                # The node's own copy: same JSON, different formatting.
                (node_dir / name).write_text(json.dumps(json.loads((profile / name).read_text()), indent=4))
            same = config(node={'security_parameter': 2160, 'configuration_dir': str(node_dir)})
            self.assertEqual(node_requirement_warnings(same), [])
            self.assertEqual(node_requirement_warnings(config(node={'security_parameter': 432})), ['node_security_parameter_mismatch'])
            changed = json.loads((node_dir / 'cnight-config.json').read_text())
            changed['mapping_validator_address'] = 'addr_test1_moved'
            (node_dir / 'cnight-config.json').write_text(json.dumps(changed))
            self.assertEqual(node_requirement_warnings(config(node={'security_parameter': 432, 'configuration_dir': str(node_dir)})),
                             ['node_security_parameter_mismatch', 'node_configuration_mismatch'])
            (node_dir / 'cnight-config.json').unlink()
            self.assertEqual(node_requirement_warnings(config(node={'configuration_dir': str(node_dir)})), ['node_configuration_mismatch'])
        self.assertEqual(node_requirement_warnings(config()), ['node_requirements_unchecked'])
        for node in ({}, {'security_parameter': '2160'}, {'security_parameter': 0}, {'configuration_dir': 'relative/res'}, {'k': 2160}, [],
                     {'bridge_checkpoint': 'Block(1)'}, {'bridge_checkpoint': 1}, {'bridge_checkpoint': 'tx:AB'}):
            with self.subTest(node=node), self.assertRaises(ValueError):
                config(node=node)
        # The on-chain bridge checkpoint is a stated node value; it is reported against the metadata coverage.
        for checkpoint in ('block:4500300', 'tx:' + 'ab' * 32):
            self.assertEqual(config(node={'bridge_checkpoint': checkpoint}).node, {'bridge_checkpoint': checkpoint})
        self.assertEqual(node_requirement_warnings(config(node={'security_parameter': 2160, 'bridge_checkpoint': 'block:1'})), [])

    def test_reader_configuration_is_not_in_repr(self):
        self.assertNotIn('private_reader_service', repr(ServiceConfig('test', {'role': 'node_reader', 'pg_service': 'private_reader_service'}, network='preprod')))

    def test_actual_main_entrypoint_preserves_transient_exception_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'sitecustomize.py').write_text((Path(__file__).resolve().parents[1]/'fixtures'/'service_entrypoint.py').read_text())
            secret=root/'password';secret.write_text('fixture-only');secret.chmod(0o600)
            config_path=root/'service.json'
            config_path.write_text(json.dumps({'pg_service':'fixture','network':'preprod','http_port':config().http_port,
                'reader':{'role':'node_reader','pg_service':'fixture_reader'},
                'policy':{'heartbeat_seconds':0.01},'shutdown_seconds':0.5}))
            env=dict(os.environ,PYTHONPATH=str(root),ENTRYPOINT_MARKER=str(root/'marker'))
            result=subprocess.run([sys.executable,'-m','db_sync_alt','service','--config',str(config_path)],
                                  env=env,capture_output=True,text=True,timeout=3)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertEqual((root/'marker').read_text(),'transient survived')
            self.assertEqual(result.stderr,'')
            anchors=[('lifecycle:'+code,1,code) for code in ('pending_target_changed','pending_suffix_base_changed','pending_anchor_changed')]
            for mode,code,reason in [('db_loss',75,'database_unavailable'),('db_wait_loss',75,'database_unavailable'),('db_wait_timeout',75,'database_timeout'),('permanent',1,'invalid_identity'),('lifecycle:invalid_coverage',1,'invalid_coverage')]+anchors:
                env['ENTRYPOINT_MODE']=mode
                result=subprocess.run([sys.executable,'-m','db_sync_alt','service','--config',str(config_path)],
                                      env=env,capture_output=True,text=True,timeout=3)
                self.assertEqual(result.returncode,code,result.stderr)
                self.assertEqual(json.loads(result.stderr),{'error_code':reason})

    def test_writer_probe_client_deadline_has_no_background_worker(self):
        from db_sync_alt.service.runtime import PostgresRuntime
        left,right=socket.socketpair();calls=[]
        class Stalled:
            nonblocking=1;socket=left.fileno()
            def send_query(self,query):calls.append(query)
            def flush(self):return 0
            def consume_input(self):pass
            def is_busy(self):return True
        db=SimpleNamespace(closed=False,autocommit=True,info=SimpleNamespace(transaction_status=0),
                           lock=threading.Lock(),pgconn=Stalled())
        store=SimpleNamespace(db=db,require_writer=lambda:None)
        threads={t.ident for t in threading.enumerate()};start=time.monotonic()
        try:
            with self.assertRaises(LifecycleFailure) as failure:PostgresRuntime(config()).writer_check(store,.05)
            self.assertEqual(failure.exception.code,'database_timeout')
            self.assertGreaterEqual(time.monotonic()-start,.04);self.assertLess(time.monotonic()-start,.5)
            self.assertEqual(calls,[b'SELECT 1']);self.assertEqual({t.ident for t in threading.enumerate()},threads)
            db.closed=True
            with self.assertRaises(LifecycleFailure) as failure:PostgresRuntime(config()).writer_check(store,.05)
            self.assertEqual(failure.exception.code,'database_unavailable');self.assertEqual(len(calls),1)
        finally:left.close();right.close()

    def test_configuration_refuses_unknown_identity_and_backup_halves(self):
        with self.assertRaises(ValueError):
            ServiceConfig('test', READER, network='testnet')
        with self.assertRaises(ValueError):
            ServiceConfig('test', READER)
        with self.assertRaises(ValueError):
            ServiceConfig('test', READER, network='preprod', profile='midnight-preprod-node-1.0.0')
        with self.assertRaises(ValueError):
            ServiceConfig('test', READER, network='preprod', backup_archive='/tmp/only-one-half')
        with self.assertRaises(ValueError):
            ServiceConfig('postgres://secret', READER, network='preprod')
        for reader in ({}, {'role': 'node_reader'}, {'role': 'bad name', 'pg_service': 'x'}, {'role': 'node_reader', 'pg_service': 'test'},
                       {'role': 'node_reader', 'pg_service': 'x', 'password': 'p'}, None):
            with self.subTest(reader=reader), self.assertRaises(ValueError):
                ServiceConfig('test', reader, network='preprod')
        self.assertEqual(failure_code(RuntimeError('reader_role_missing')), 'invalid_reader_role')
        self.assertEqual(failure_code(RuntimeError('reader_requires_current_schema')), 'schema_upgrade_required')
        self.assertEqual(failure_code(RuntimeError('private connection string')), 'internal_error')
        for code in ('pending_target_changed', 'pending_suffix_base_changed', 'pending_anchor_changed'):
            self.assertEqual(failure_code(LifecycleFailure(code)), code)
        self.assertEqual(failure_code(LifecycleFailure('provider_metadata_gap')), 'internal_error')
        for code in ('historical_selected_event_changed', 'historical_state_changed', 'historical_epoch_changed'):
            self.assertEqual(failure_code(RuntimeError(code)), 'canonical_conflict')
        self.assertEqual(failure_code(RuntimeError('unsupported_candidate_activity')), 'unsupported_capability')
        pg_error = type('OperationalError', (Exception,), {'__module__': 'psycopg'})
        self.assertEqual(failure_code(pg_error('secret connection string')), 'database_unavailable')
        shutdown_error=type('AdminShutdown',(pg_error,),{'__module__':'psycopg.errors'})
        self.assertEqual(failure_code(shutdown_error('private server detail')), 'database_unavailable')


if __name__ == '__main__':
    unittest.main(verbosity=2)
