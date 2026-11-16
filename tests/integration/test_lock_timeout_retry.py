# SPDX-License-Identifier: Apache-2.0
"""Another session holds a job row or the serving lock past the service's lock_timeout: the service waits and retries.

Real PostgreSQL, the service runtime (writer, poller, observer, HTTP) and the two-generation Blockfrost fixture:
generation 0 targets h=10 and the provider confirms h=12, so the service follows one update and publishes it.
At the update's first transaction request a second session takes the lock for HOLD_SECONDS, longer than the
writer's (2 s) and the poller's (1 s) lock_timeout.
"""
import contextlib
import io
import json
import logging
import os
from pathlib import Path
import signal
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

import psycopg
from psycopg import sql

from db_sync_alt import errors
from db_sync_alt.service import runtime as service_runtime
from db_sync_alt.service.health import HealthPolicy
from db_sync_alt.service.runtime import PostgresRuntime, Service, ServiceConfig
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.store.database import METRICS_MARKER, SERVING_LOCK, PgStore
from db_sync_alt.store.upgrade import upgrade_store
import fixtures.blockfrost_generation as gen_fixture
from fixtures.database import AccountedFake
from fixtures.service import config as test_config, eventually, get, real_reader

HOLD_SECONDS = 3
TARGET = gen_fixture.OFFSET + 12


class Client(AccountedFake):
    """The fixture provider; `trigger` runs once, on the requesting thread, before the first update transaction read."""
    def __init__(self, routes, trigger):
        super().__init__(routes)
        self.trigger, self.lock = trigger, threading.Lock()

    def get(self, p, **kw):
        with self.lock:
            trigger, self.trigger = (self.trigger, None) if '/txs/' in p else (None, self.trigger)
        if trigger:
            trigger()
        return super().get(p, **kw)


class LockTimeoutRetryTests(unittest.TestCase):
    def setUp(self):
        self.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        self.tmp = tempfile.TemporaryDirectory()
        servicefile = Path(self.tmp.name) / 'pg_service.conf'
        self.name = 'db_sync_alt_test_lock_' + uuid.uuid4().hex[:8]
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.name)))
        self.dsn = psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=self.name)
        with gen_fixture.baseline(self.dsn):
            pass
        with PgStore(conninfo=self.dsn) as store:
            upgrade_store(store)
        self.old_servicefile = os.environ.get('PGSERVICEFILE')
        os.environ['PGSERVICEFILE'] = str(servicefile)
        self.alias = 'service_' + uuid.uuid4().hex[:8]
        with servicefile.open('a') as out:
            out.write('[' + self.alias + ']\n' + ''.join(k + '=' + str(v) + '\n' for k, v in psycopg.conninfo.conninfo_to_dict(self.dsn).items()) + '\n')
        self.reader = real_reader(self.admin, self.dsn, servicefile)
        self.policy = {'poll_seconds': .25, 'heartbeat_seconds': .05}
        self.routes, _ = gen_fixture.fixtures()
        self.holds, self.services = [], []

    def tearDown(self):
        for hold in self.holds:
            hold['release'].set()
            hold['thread'].join(5)
        for service in self.services:
            service.stop()
        if self.old_servicefile is None:
            os.environ.pop('PGSERVICEFILE', None)
        else:
            os.environ['PGSERVICEFILE'] = self.old_servicefile
        self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(self.name)))
        self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(self.reader['role'])))
        self.admin.close()
        self.tmp.cleanup()

    def hold(self, key, seconds=HOLD_SECONDS):
        """Another session locks job row `key` FOR UPDATE, or with `key` None takes the serving lock, for `seconds`."""
        record = {'key': key, 'release': threading.Event(), 'found': False}
        db = psycopg.connect(self.dsn, autocommit=key is None)
        if key is None:
            record['found'] = db.execute('SELECT pg_advisory_lock(%s,%s)', SERVING_LOCK).fetchone() is not None
        else:
            record['found'] = db.execute('SELECT 1 FROM chain.sync_jobs WHERE id=%s FOR UPDATE', (key,)).fetchone() is not None
        record['started'] = time.monotonic()

        def release():
            record['release'].wait(seconds)
            db.close()  # ends the transaction or the session: the lock is released
            record['ended'] = time.monotonic()
        record['thread'] = threading.Thread(target=release, daemon=True)
        record['thread'].start()
        self.holds.append(record)
        return record

    def poll_key(self):
        with psycopg.connect(self.dsn) as db:
            return db.execute("SELECT max(id) FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%'").fetchone()[0]

    def configuration(self):
        return ServiceConfig(self.alias, self.reader, http_port=test_config().http_port, network='preprod',
                             storage_reserves={'/tmp': 1}, policy=HealthPolicy(**self.policy))

    def published(self):
        with PgStore(conninfo=self.dsn, readonly=True) as store:
            return store.generation()['target_height'] == TARGET and store.pending() is None

    def reference_digests(self):
        """Fact digests of the full update to h=12 on a fresh copy of generation 0."""
        name = 'db_sync_alt_test_lockref_' + uuid.uuid4().hex[:8]
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        try:
            with gen_fixture.baseline(psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)) as reference:
                return gen_fixture.advance(reference, self.routes, budget=500)['digests']
        finally:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))

    def follow(self, lock, *, runtime=None):
        """Run the service until it publishes h=12 while `lock()` (called at the update's first transaction read)
        holds a lock; returns the /status samples [(monotonic, snapshot)] and the warning lines."""
        config = self.configuration()
        client = Client(self.routes, lock)
        service = Service(config, runtime=runtime or PostgresRuntime(config),
                          sync_driver=SyncDriver(config, client=client, profile=gen_fixture.PROFILE))
        self.services.append(service)
        samples, done = [], threading.Event()

        def sample():
            while not done.wait(.05):
                with contextlib.suppress(Exception):
                    samples.append((time.monotonic(), get(service, '/status')[1]))
        sampler = threading.Thread(target=sample, daemon=True)

        def settled():
            # Published, and the error codes cleared by the next successful update and poll after the hold.
            if not samples or not self.holds or 'ended' not in self.holds[0] or samples[-1][0] <= self.holds[0]['ended']:
                return False
            last = samples[-1][1]
            return (last['error_code'] is None and last['poll_error_code'] is None
                    and (last['committed_target'] or {}).get('height') == TARGET)
        with self.assertLogs('db_sync_alt.errors', 'WARNING') as logs:
            service.start()
            sampler.start()
            eventually(lambda: service.failed.is_set() or settled(), seconds=90)
            self.assertIsNone(service.failure, 'the service stopped with ' + str(service.failure))
        done.set()
        sampler.join(2)
        self.assertEqual(len(self.holds), 1)
        self.assertTrue(self.holds[0]['found'], 'nothing to hold')
        self.assertTrue(self.published())
        self.assertIsNone(service.failure)
        self.assertTrue(service.stop(), 'the service did not stop in time')
        with PgStore(conninfo=self.dsn, readonly=True) as store:
            self.assertEqual(store.digests(store.generation()['id']), self.reference_digests())
            # A lock timeout writes no failure record.
            self.assertEqual(store.db.execute("SELECT count(*) FROM chain.sync_jobs WHERE body ? 'last_failure'").fetchone()[0], 0)
            self.assertEqual(store.sync_job('provider-active')['state'], 'completed')
        return samples, logs.output

    def assert_reported(self, samples, field):
        """`field` of /status read `database_lock_timeout` while the lock was held."""
        hold = self.holds[0]
        during = [snapshot[field] for at, snapshot in samples if hold['started'] <= at <= hold['ended']]
        self.assertIn('database_lock_timeout', during, field + ' never reported the lock timeout during the hold')

    def test_update_job_row_held(self):
        samples, logs = self.follow(lambda: self.hold('provider-active'))
        self.assertTrue(any('database_lock_timeout_retry worker=sync' in line for line in logs), logs)
        self.assert_reported(samples, 'error_code')
        self.assertIn('backoff', [snapshot['sync'] for _, snapshot in samples])

    def test_metrics_marker_row_held(self):
        samples, logs = self.follow(lambda: self.hold(METRICS_MARKER))
        # Both the writer and the poller write the marker before each request.
        self.assertTrue(any('database_lock_timeout_retry worker=sync' in line for line in logs), logs)
        self.assertTrue(any('database_lock_timeout_retry worker=poll' in line for line in logs), logs)
        self.assert_reported(samples, 'error_code')
        self.assert_reported(samples, 'poll_error_code')

    def test_poll_job_row_held(self):
        samples, logs = self.follow(lambda: self.hold(self.poll_key()))
        self.assertTrue(any('database_lock_timeout_retry worker=poll' in line for line in logs), logs)
        self.assert_reported(samples, 'poll_error_code')
        reasons = [snapshot['reason_codes'] for _, snapshot in samples]
        self.assertTrue(any('provider_poll_database_lock_timeout' in r for r in reasons))

    def test_serving_lock_held_at_publication(self):
        test = self

        class Runtime(PostgresRuntime):
            def open_writer(self, health):
                store = super().open_writer(health)
                publish = store.publish_incremental

                def publish_while_held(*args, **kwargs):
                    if not test.holds:
                        test.hold(None)
                    return publish(*args, **kwargs)
                store.publish_incremental = publish_while_held
                return store
        # The publication's transaction waits for the serving lock: it rolls back and the update resumes.
        samples, logs = self.follow(lambda: None, runtime=Runtime(self.configuration()))
        self.assertTrue(any('database_lock_timeout_retry worker=sync' in line for line in logs), logs)
        self.assert_reported(samples, 'error_code')

    def test_held_past_the_limit_the_service_exits_75(self):
        """The service entry point, with the limit lowered to 6 s: the hold outlasts it."""
        path = Path(self.tmp.name) / 'service.json'
        path.write_text(json.dumps({'pg_service': self.alias, 'reader': self.reader, 'network': 'preprod',
                                    'http_port': test_config().http_port, 'storage_reserves': {'/tmp': 1},
                                    'policy': self.policy, 'shutdown_seconds': 5}))
        client = Client(self.routes, lambda: self.hold('provider-active', seconds=60))
        handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        self.addCleanup(lambda: [signal.signal(sig, handler) for sig, handler in handlers.items()])
        stderr = io.StringIO()
        with patch.object(errors, 'LOCK_TIMEOUT_LIMIT_SECONDS', 6), \
                patch.object(service_runtime, 'load_sync_driver', lambda config: SyncDriver(config, client=client, profile=gen_fixture.PROFILE)), \
                self.assertLogs('db_sync_alt.errors', 'WARNING') as logs, contextlib.redirect_stderr(stderr):
            code = service_runtime.main(['--config', str(path)])
        self.assertEqual(code, 75)
        self.assertEqual(json.loads(stderr.getvalue().strip().splitlines()[-1]), {'error_code': 'database_unavailable'})
        self.assertTrue(self.holds and self.holds[0]['found'])
        self.assertTrue(any('database_lock_timeout_retry worker=sync' in line for line in logs.output), logs.output)
        persisted = [line for line in logs.output if 'database_lock_timeout_persisted worker=sync' in line]
        self.assertEqual(len(persisted), 1, logs.output)
        self.assertGreaterEqual(int(persisted[0].rsplit('seconds=', 1)[1]), 6)
        self.assertGreaterEqual(time.monotonic() - self.holds[0]['started'], 6)
        self.assertFalse(self.published())

    def test_acquire_reports_a_lock_timeout_as_temporary(self):
        """The one-shot `acquire --source blockfrost` does not wait: it stops with `database_lock_timeout`, exit code 75."""
        from db_sync_alt.cli import acquire
        import db_sync_alt.sources.blockfrost.sync as sync
        client = Client(self.routes, lambda: self.hold('provider-active'))
        handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        self.addCleanup(lambda: [signal.signal(sig, handler) for sig, handler in handlers.items()])
        stderr, records = io.StringIO(), []
        handler = logging.Handler()
        handler.emit = records.append
        logging.getLogger('db_sync_alt.errors').addHandler(handler)
        self.addCleanup(logging.getLogger('db_sync_alt.errors').removeHandler, handler)
        with patch.object(sync, 'shared_client', lambda network: client), contextlib.redirect_stderr(stderr):
            code = acquire.main(['--source', 'blockfrost', '--destination-service', self.alias, '--network', 'preprod',
                                 '--target-height', str(TARGET), '--reserve', '/tmp:1'])
        self.assertEqual(code, 75, stderr.getvalue()[-300:])
        report = json.loads(stderr.getvalue().strip().splitlines()[-1])
        self.assertEqual(report['error_code'], 'database_lock_timeout')
        self.assertTrue(self.holds and self.holds[0]['found'])
        self.assertTrue(any('database_lock_timeout_retry worker=sync' in r.getMessage() for r in records))
        self.assertFalse(self.published())
