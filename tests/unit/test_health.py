# SPDX-License-Identifier: Apache-2.0
"""Pure policy plus isolated-loopback HTTP tests; no PG/provider/source calls."""
import contextlib
import http.client
import io
import json
import random
import socket
import threading
import time
import unittest

from fixtures.networks import PREPROD
from db_sync_alt.service.health import (BoundedDatabaseProbe, ChainPoint, DatabaseStatus, HealthHTTPServer,
                       HealthPolicy, HealthState, PendingProgress, QuotaStatus)


class Clock:
    def __init__(self):
        self.elapsed, self.wall = 1000.0, 1700000000.0

    def monotonic(self):
        return self.elapsed

    def time(self):
        return self.wall

    def advance(self, seconds):
        self.elapsed += seconds
        self.wall += seconds


def point(height, timestamp=None):
    return ChainPoint(height, f'{height:064x}', timestamp)


def state(clock=None):
    clock = clock or Clock()
    result = HealthState(service_version='0.1.0', schema_version=1, network='preprod',
                         profile='midnight-1x', monotonic=clock.monotonic, wall_clock=clock.time)
    return result, clock


def healthy():
    result, clock = state()
    result.update(lifecycle='ready', sync='idle', serving='ready')
    # Provider head has the mandated 2160-block lead; caught up lag is still zero.
    result.observe_provider(point(3160, 63200), point(1000, 20000))
    result.set_coverage(committed=point(1000, 20000), served=point(1000, 20000), witness=point(3160, 63200))
    result.database_observed(True)
    result.heartbeat('sync')
    result.heartbeat('sql')
    return result, clock


def refresh_local(result):
    result.database_observed(True)
    result.heartbeat('sync')
    result.heartbeat('sql')


class StateTests(unittest.TestCase):
    def test_startup_unknowns_are_null_and_never_healthy(self):
        result, _ = state()
        code, value = result.health()
        self.assertEqual(code, 503)
        for field in ('provider_head', 'provider_observed_at', 'confirmed_target', 'committed_target',
                      'served_target', 'witness', 'last_successful_sync', 'retry_at', 'error_code'):
            self.assertIsNone(value[field], field)
        self.assertEqual(value['lag'], {'blocks': None, 'seconds': None})
        self.assertEqual(value['quota'], {'used': None, 'limit': None, 'reset_at': None})
        self.assertIsNone(value['database']['reachable'])
        self.assertIn('provider_observation_unknown', value['reason_codes'])
        self.assertIn('worker_sync_unknown', value['reason_codes'])

    def test_status_schema_is_fixed_and_caught_up_idle_is_healthy(self):
        result, _ = healthy()
        code, value = result.health()
        self.assertEqual(code, 200)
        self.assertEqual(value['reason_codes'], [])
        self.assertEqual(value['lag'], {'blocks': 0, 'seconds': 0})
        self.assertEqual(set(value), {'service_version', 'schema_version', 'network', 'profile', 'follow_mode', 'poll_interval_seconds', 'poll_timing', 'poll_error_code',
            'lifecycle', 'sync', 'serving', 'provider_head', 'provider_observed_at',
            'confirmed_target', 'committed_target', 'served_target', 'witness', 'pending',
            'lag', 'freshness', 'last_successful_sync', 'retry_at', 'quota', 'database',
            'workers', 'operation', 'error_code', 'policy', 'healthy', 'reason_codes', 'reader', 'warnings', 'integrity', 'relevance'})

    def test_integrity_base_and_checkpointing_are_reported(self):
        result, _ = healthy()
        self.assertEqual(result.snapshot()['integrity'], {'base': None, 'checkpointed_at': None})
        result.integrity(base=7, checkpointed_at=None)
        self.assertEqual(result.snapshot()['integrity'], {'base': 7, 'checkpointed_at': None})
        result.integrity(base=9, checkpointed_at=1700000000.5)
        self.assertEqual(result.snapshot()['integrity'], {'base': 9, 'checkpointed_at': 1700000000.5})
        for base, at in ((-1, None), (True, None), ('9', None), (9, float('nan')), (9, -1), (9, '2026-11-07')):
            with self.subTest(base=base, at=at), self.assertRaises(ValueError):
                result.integrity(base=base, checkpointed_at=at)
        # A periodic checkpoint pauses publication briefly; health stays green while lag is within policy.
        result.update(sync='checkpointing')
        code, value = result.health()
        self.assertEqual((code, value['sync'], value['reason_codes']), (200, 'checkpointing', []))
        result.verified_schema(4)
        self.assertEqual(result.snapshot()['schema_version'], '4')
        with self.assertRaises(ValueError):
            result.verified_schema(5)

    def test_every_unready_lifecycle_is_503(self):
        for lifecycle in ('starting', 'restoring', 'stopping', 'stopped', 'failed', 'halted'):
            with self.subTest(lifecycle=lifecycle):
                result, _ = healthy()
                result.update(lifecycle=lifecycle)
                self.assertEqual(result.health()[0], 503)
                self.assertIn('lifecycle_' + lifecycle, result.snapshot()['reason_codes'])

    def test_restore_requires_fresh_observation(self):
        result, _ = state()
        result.update(lifecycle='ready', sync='idle', serving='ready')
        result.set_coverage(committed=point(1000), served=point(1000), witness=point(3160))
        refresh_local(result)
        self.assertEqual(result.health()[0], 503)
        self.assertIn('serving_lag_unknown', result.snapshot()['reason_codes'])

    def test_pending_progress_never_advances_served_coverage(self):
        result, _ = healthy()
        result.update(sync='syncing', pending=PendingProgress(point(2000, 40000), 300, 1000))
        value = result.snapshot()
        self.assertEqual(value['pending']['target']['height'], 2000)
        self.assertEqual(value['served_target']['height'], 1000)
        self.assertEqual(value['committed_target']['height'], 1000)
        self.assertEqual(result.health()[0], 200)

    def test_transient_provider_outage_degrades_without_immediate_revocation(self):
        result, clock = healthy()
        result.set_error('provider_unavailable')
        result.update(sync='degraded')
        clock.advance(120)
        refresh_local(result)
        self.assertEqual(result.health()[0], 200)
        clock.advance(0.01)
        refresh_local(result)
        code, value = result.health()
        self.assertEqual(code, 503)
        self.assertIn('provider_observation_stale', value['reason_codes'])
        self.assertEqual(value['serving'], 'ready')
        self.assertEqual(value['served_target']['height'], 1000)

    def test_confirmation_to_served_lag_threshold_and_unknown_timestamp(self):
        result, _ = healthy()
        lag = PREPROD.serving_lag_max_blocks
        result.observe_provider(point(3160 + lag, 80000), point(1000 + lag, 20600))
        self.assertEqual(result.health()[0], 200)
        result.observe_provider(point(3160 + lag, 80000), point(1000 + lag, 20601))
        self.assertIn('serving_lag_exceeded', result.snapshot()['reason_codes'])
        result.observe_provider(point(4160), point(2000))
        self.assertIn('serving_lag_unknown', result.snapshot()['reason_codes'])

    def test_block_lag_boundary_and_strict_policy_type(self):
        for bad in (True, False, 0, -1, 1.5, 15.0, float('nan'), float('inf'), '15'):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                HealthPolicy(serving_lag_max_blocks=bad)
        result, _ = healthy()
        # The default limit is the network profile's, below the node's stability margin.
        lag = PREPROD.serving_lag_max_blocks
        self.assertEqual(result.policy.serving_lag_max_blocks, lag)
        self.assertLess(lag, PREPROD.stability_margin)
        result.observe_provider(point(3160 + lag, 63200), point(1000 + lag, 20000))
        self.assertEqual(result.health()[0], 200)
        result.observe_provider(point(3161 + lag, 63200), point(1001 + lag, 20000))
        self.assertEqual(result.health()[0], 503)
        self.assertIn('serving_block_lag_exceeded', result.snapshot()['reason_codes'])
        self.assertEqual(result.snapshot()['serving'], 'ready')
        unknown, _ = state()
        self.assertIn('serving_block_lag_unknown', unknown.snapshot()['reason_codes'])

    def test_native_failure_snapshot_is_unhealthy_without_revoking_sql_coverage(self):
        result, clock = healthy()
        result.observe_provider(point(5360756, 1793532286), point(5358596, 1793483588))
        result.set_coverage(committed=point(5358552, 1793482465),
                            served=point(5358552, 1793482465), witness=point(5360712, 1793531366))
        result.update(sync='syncing', pending=PendingProgress(point(5358596, 1793483588), 44, 44))
        result.begin_operation('verification')
        clock.advance(218.77002485695994)
        refresh_local(result)
        result.operation_progress()
        with listener(result) as server:
            code, payload = get(server, '/health')
            self.assertEqual(code, 503)
            self.assertIn('serving_block_lag_exceeded', payload['reason_codes'])
            self.assertIn('provider_observation_stale', payload['reason_codes'])
            self.assertEqual(payload['lag']['blocks'], 44)
            status, current = get(server, '/status')
            self.assertEqual(status, 200)
            self.assertEqual(current['serving'], 'ready')
            self.assertEqual(current['served_target']['height'], 5358552)
            self.assertEqual(current['witness']['height'], 5360712)
            self.assertEqual(current['pending']['completed'], 44)

    def test_worker_stall_is_independent_of_provider_observation_and_db(self):
        result, clock = healthy()
        clock.advance(120)
        result.database_observed(True)
        self.assertEqual(result.health()[0], 200)
        clock.advance(0.01)
        result.database_observed(True)
        result.heartbeat('sql')
        self.assertIn('worker_sync_stalled', result.snapshot()['reason_codes'])

    def test_fresh_supervisor_heartbeat_does_not_hide_blocked_operation(self):
        for stage, deadline in (('provider', 120), ('verification', 300), ('database', 300)):
            with self.subTest(stage=stage):
                result, clock = healthy()
                result.begin_operation(stage)
                clock.advance(deadline)
                result.observe_provider(point(3160, 63200), point(1000, 20000))
                refresh_local(result)
                self.assertEqual(result.health()[0], 200)
                clock.advance(0.01)
                refresh_local(result)
                self.assertIn('operation_stalled', result.snapshot()['reason_codes'])
                result.operation_progress()
                self.assertEqual(result.health()[0], 200)
                result.end_operation()
                self.assertIsNone(result.snapshot()['operation'])

    def test_explicit_stage_deadline_override_is_visible_and_enforced(self):
        result, clock = healthy()
        result.begin_operation('verification', deadline_seconds=7)
        self.assertEqual(result.snapshot()['operation']['deadline_seconds'], 7)
        with self.assertRaises(ValueError):
            result.begin_operation('provider')
        clock.advance(8)
        refresh_local(result)
        self.assertIn('operation_stalled', result.snapshot()['reason_codes'])

    def test_quota_and_backoff_pauses_are_not_stuck_but_freshness_applies(self):
        for reason in ('quota_wait', 'backoff'):
            with self.subTest(reason=reason):
                result, clock = healthy()
                result.begin_operation('provider')
                result.pause(reason, resume_at=clock.wall + 3600)
                result.update(quota=QuotaStatus(50000, 50000, clock.wall + 3600))
                clock.advance(90)
                refresh_local(result)
                self.assertEqual(result.health()[0], 200)
                self.assertIsNone(result.snapshot()['operation'])
                clock.advance(31)
                refresh_local(result)
                self.assertIn('provider_observation_stale', result.snapshot()['reason_codes'])
                self.assertNotIn('operation_stalled', result.snapshot()['reason_codes'])

    def test_expired_pause_is_not_an_indefinite_liveness_exemption(self):
        result, clock = healthy()
        result.pause('backoff', resume_at=clock.wall + 10)
        clock.advance(131)
        refresh_local(result)
        self.assertIn('retry_overdue', result.snapshot()['reason_codes'])
        result.begin_operation('provider')
        self.assertNotIn('retry_overdue', result.snapshot()['reason_codes'])

    def test_wall_clock_jumps_do_not_reset_deadlines_or_freshness(self):
        result, clock = healthy()
        result.begin_operation('provider')
        clock.wall += 100000
        self.assertEqual(result.health()[0], 200)
        clock.wall -= 200000
        clock.advance(121)
        refresh_local(result)
        self.assertIn('operation_stalled', result.snapshot()['reason_codes'])

    def test_database_loss_unknown_and_stale_are_unhealthy(self):
        result, clock = healthy()
        result.database_observed(False)
        self.assertIn('database_unavailable', result.snapshot()['reason_codes'])
        self.assertIn('database_unknown', result.snapshot(DatabaseStatus())['reason_codes'])
        result.database_observed(True)
        clock.advance(5.01)
        self.assertIn('database_observation_stale', result.snapshot()['reason_codes'])

    def test_canonical_conflict_revokes_health(self):
        result, _ = healthy()
        result.update(lifecycle='halted', sync='halted', serving='revoked')
        result.set_error('canonical_conflict')
        self.assertEqual(result.health()[0], 503)
        self.assertIn('serving_revoked', result.snapshot()['reason_codes'])
        self.assertEqual(result.snapshot()['error_code'], 'canonical_conflict')

    def test_success_error_and_json_redaction(self):
        result, _ = healthy()
        secret = 'postgres://alice:password@private.example/secret-directory'
        result.set_error(secret)
        self.assertNotIn(secret, json.dumps(result.snapshot()))
        self.assertEqual(result.snapshot()['error_code'], 'internal_error')
        result.set_error(RuntimeError(secret))
        self.assertEqual(result.snapshot()['error_code'], 'internal_error')
        for code in ('pending_target_changed', 'pending_suffix_base_changed', 'pending_anchor_changed'):
            result.set_error(code)
            self.assertEqual(result.snapshot()['error_code'], code)
        result.set_error('provider_metadata_gap')
        self.assertEqual(result.snapshot()['error_code'], 'internal_error')
        result.record_success()
        self.assertIsNotNone(result.snapshot()['last_successful_sync'])
        self.assertIsNone(result.snapshot()['error_code'])
        with self.assertRaises(ValueError):
            result.update(sync=secret)
        with self.assertRaises(ValueError):
            HealthState(service_version='1', schema_version=1, network='preprod', profile=secret)

    def test_atomic_updates_and_detached_snapshots(self):
        result, _ = healthy()
        errors = []
        def writer():
            for _ in range(1000):
                result.update(lifecycle='halted', serving='revoked')
                result.update(lifecycle='ready', serving='ready')
        thread = threading.Thread(target=writer)
        thread.start()
        for _ in range(1000):
            value = result.snapshot()
            if (value['lifecycle'], value['serving']) not in {('halted', 'revoked'), ('ready', 'ready')}:
                errors.append(value)
            value['workers']['sync']['heartbeat_at'] = 'mutated'
        thread.join()
        self.assertFalse(errors)
        self.assertNotEqual(result.snapshot()['workers']['sync']['heartbeat_at'], 'mutated')

    def test_invalid_values_cannot_enter_public_state(self):
        result, _ = healthy()
        for invalid in (float('nan'), float('inf'), -1, True):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                HealthPolicy(worker_stall_seconds=invalid)
        with self.assertRaises(ValueError):
            PendingProgress(completed=5, total=4)
        with self.assertRaises(ValueError):
            ChainPoint(1, 'raw secret')
        with self.assertRaises(ValueError):
            result.set_coverage(committed=point(10), served=point(11), witness=point(12))
        with self.assertRaises(ValueError):
            result.observe_provider(point(9), point(10))


class ProbeTests(unittest.TestCase):
    def test_database_failure_is_sanitized_and_recoverable(self):
        calls = []
        def check(timeout):
            calls.append(timeout)
            if len(calls) == 1:
                raise RuntimeError('postgres://alice:secret@hostname/private-directory')
            return True
        probe = BoundedDatabaseProbe(check, cache_seconds=0)
        first = probe.check()
        self.assertFalse(first.reachable)
        self.assertEqual(first.error_code, 'database_unavailable')
        self.assertTrue(probe.check().reachable)
        probe.close()
        self.assertFalse(probe.check().reachable)

    def test_repeated_timeouts_have_one_inflight_call_and_bounded_shutdown(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        def check(timeout):
            calls.append(timeout)
            entered.set()
            release.wait(5)
            return True
        probe = BoundedDatabaseProbe(check, timeout_seconds=0.03)
        try:
            start = time.monotonic()
            first = probe.check()
            self.assertTrue(entered.is_set())
            self.assertEqual(first.error_code, 'database_timeout')
            results = []
            threads = [threading.Thread(target=lambda: results.append(probe.check())) for _ in range(20)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(1)
                self.assertFalse(thread.is_alive())
            self.assertEqual(len(calls), 1)
            self.assertTrue(all(value.error_code == 'database_timeout' for value in results))
            probe.close()
            self.assertLess(time.monotonic() - start, 1)
        finally:
            release.set()


@contextlib.contextmanager
def listener(result, **kwargs):
    # Random free high port inside Docker's isolated loopback; no host publish.
    server = None
    for _ in range(20):
        try:
            server = HealthHTTPServer(result, port=random.randint(10001, 60000), **kwargs)
            break
        except OSError:
            continue
    if server is None:
        raise RuntimeError('no free test listener port')
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': 0.01}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(1)
        if thread.is_alive():
            raise AssertionError('HTTP server failed to shut down')


def get(server, path):
    connection = http.client.HTTPConnection(*server.server_address, timeout=2)
    try:
        connection.request('GET', path)
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


class HTTPTests(unittest.TestCase):
    def test_status_and_health_healthy_then_revoked_and_no_provider_calls(self):
        result, _ = healthy()
        with listener(result) as server:
            started = time.monotonic()
            for _ in range(10):
                self.assertEqual(get(server, '/status')[0], 200)
                self.assertEqual(get(server, '/health')[0], 200)
            self.assertLess(time.monotonic() - started, 1)
            # The HTTP component has no provider callback or import at all.
            result.update(serving='revoked')
            self.assertEqual(get(server, '/status')[0], 200)
            code, payload = get(server, '/health')
            self.assertEqual(code, 503)
            self.assertIn('serving_revoked', payload['reason_codes'])

    def test_broken_dependency_is_bounded_and_status_stays_200(self):
        result, _ = healthy()
        release = threading.Event()
        calls = []
        def check(timeout):
            calls.append(timeout)
            release.wait(5)
            return True
        probe = BoundedDatabaseProbe(check, timeout_seconds=0.05)
        try:
            with listener(result, probe=probe) as server:
                for path, expected in (('/health', 503), ('/status', 200), ('/health', 503)):
                    started = time.monotonic()
                    code, payload = get(server, path)
                    self.assertEqual(code, expected)
                    self.assertIn('database_timeout', payload['reason_codes'])
                    self.assertLess(time.monotonic() - started, 2)
                self.assertEqual(len(calls), 1)
        finally:
            release.set()

    def test_errors_and_request_targets_never_echo_secrets(self):
        result, _ = healthy()
        captured = io.StringIO()
        with contextlib.redirect_stderr(captured), listener(result) as server:
            code, payload = get(server, '/status?credential=do-not-print-this')
            self.assertEqual(code, 404)
            self.assertNotIn('do-not-print-this', json.dumps(payload))
            connection = http.client.HTTPConnection(*server.server_address, timeout=2)
            connection.request('SECRET-METHOD', '/secret-directory')
            response = connection.getresponse()
            self.assertEqual(response.status, 501)
            self.assertEqual(json.loads(response.read()), {'error_code': 'invalid_http_request'})
            connection.close()
        self.assertEqual(captured.getvalue(), '')

    def test_slow_partial_request_does_not_block_monitoring(self):
        result, _ = healthy()
        with listener(result, max_requests=2, socket_timeout=0.05) as server:
            with socket.create_connection(server.server_address, timeout=1) as slow:
                slow.sendall(b'GET /status HTTP/1.1\r\nX-Partial:')
                self.assertEqual(get(server, '/health')[0], 200)
                self.assertEqual(slow.recv(1), b'')

    def test_absolute_deadline_closes_slow_drip_headers(self):
        result, _ = healthy()
        with listener(result, socket_timeout=0.2, request_timeout=0.12) as server:
            with socket.create_connection(server.server_address, timeout=1) as slow:
                started = time.monotonic()
                slow.sendall(b'GET /status HTTP/1.1\r\nX-Partial:')
                for _ in range(3):
                    time.sleep(0.025)
                    slow.sendall(b'x')
                self.assertEqual(slow.recv(1), b'')
                self.assertLess(time.monotonic() - started, 0.5)
            self.assertEqual(get(server, '/health')[0], 200)

    def test_worker_thread_cap_and_explicit_external_binding(self):
        result, _ = healthy()
        with self.assertRaises(ValueError):
            HealthHTTPServer(result, host='0.0.0.0')
        with listener(result, max_requests=1, socket_timeout=0.05) as server:
            self.assertTrue(server._slots.acquire(blocking=False))
            try:
                with socket.create_connection(server.server_address, timeout=1) as excess:
                    self.assertEqual(excess.recv(1), b'')
            finally:
                server._slots.release()
            self.assertEqual(get(server, '/health')[0], 200)


if __name__ == '__main__':
    unittest.main(verbosity=2)
