"""Actual PG verification cancellation; deterministic provider, no live access."""
from dataclasses import replace
import os
from pathlib import Path
import threading
import time
import unittest

import psycopg
from psycopg import sql
from db_sync_alt.sources.blockfrost.adapter import update
from db_sync_alt.service.runtime import Service, PostgresRuntime
from db_sync_alt.store.database import PgStore
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.verify.store import verify_store
from db_sync_alt.store.upgrade import upgrade_store
from fixtures.service import config, eventually, real_reader
import fixtures.database as integration


class CancellationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        integration.SyncFixture.setUpClass()

    @classmethod
    def tearDownClass(cls):
        integration.SyncFixture.tearDownClass()

    def setUp(self):
        self.fixture = integration.SyncFixture()
        self.fixture.setUp()
        self.dsn = self.fixture.make(transition=True)
        self.routes, _ = integration.gen_fixture.fixtures(transition=True)
        with PgStore(conninfo=self.dsn) as store:upgrade_store(store)
        self.services = []
        self.release = threading.Event()
        self.old_servicefile = os.environ.get('PGSERVICEFILE')
        servicefile = Path(self.fixture.tmp.name) / 'pg_service.conf'
        settings = psycopg.conninfo.conninfo_to_dict(self.dsn)
        servicefile.write_text('[cancellation]\n' + ''.join(k+'='+v+'\n' for k,v in settings.items()) + '\n')
        self.reader = real_reader(self.fixture.admin, self.dsn, servicefile)
        os.environ['PGSERVICEFILE'] = str(servicefile)
        self.cfg = replace(config(), pg_service='cancellation', reader=self.reader, shutdown_seconds=5,
                           storage_reserves={'/tmp': 1}, policy=replace(config().policy,poll_seconds=3600))

    def tearDown(self):
        self.release.set()
        for service in self.services:
            service.stop()
        if self.old_servicefile is None:
            os.environ.pop('PGSERVICEFILE', None)
        else:
            os.environ['PGSERVICEFILE'] = self.old_servicefile
        self.fixture.tearDown()
        self.fixture.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(self.reader['role'])))

    def snapshot(self):
        # Include native IDs, pending checkpoints, metadata and durable counters.
        with PgStore(conninfo=self.dsn, readonly=True) as store:
            tables = store.db.execute("SELECT tablename FROM pg_tables WHERE schemaname='chain' ORDER BY tablename").fetchall()
            return {name: store.db.execute('SELECT to_jsonb(t) FROM chain.'+name+' t ORDER BY to_jsonb(t)::text').fetchall()
                    for (name,) in tables}

    def runtime(self, *, intercept=None, blocked=False, callback_error=None):
        test = self
        class Runtime(PostgresRuntime):
            def open_writer(inner, health):
                store = super().open_writer(health)
                inner.store = store
                inner.owner = threading.get_ident()
                original_close = store.close
                def close():
                    test.assertEqual(threading.get_ident(), inner.owner)
                    inner.closed = True
                    original_close()
                store.close = close
                original_callback = store.progress_callback
                def callback(phase, **fields):
                    inner.callbacks = getattr(inner, 'callbacks', 0) + 1
                    if callback_error:
                        raise callback_error
                    original_callback(phase, **fields)
                store.progress_callback = callback
                if intercept:
                    # Observation barrier around the real checkpoint. The work
                    # after it represents remaining local verification: a stop
                    # must unwind instead of returning into that continuation.
                    original_progress = store.progress
                    def progress(phase, **fields):
                        if intercept(phase, fields):
                            inner.checkpoint_transaction = store.db.info.transaction_status.name
                            inner.entered.set()
                            if not inner.service.stop_event.wait(15):
                                raise AssertionError('test stop not delivered')
                            original_progress(phase, **fields)
                            inner.returned_after_stop = True
                            test.release.wait(8)
                        else:
                            original_progress(phase, **fields)
                    store.progress = progress
                return store

            def verify(inner, store):
                if blocked:
                    inner.entered.set()
                    store.db.execute('SELECT pg_sleep(20)')
                return super().verify(store)
        runtime = Runtime(self.cfg)
        runtime.entered = threading.Event()
        runtime.returned_after_stop = False
        runtime.closed = False
        return runtime

    def start(self, runtime, *, update_fn=update, before_publish=None):
        client = integration.AccountedFake(self.routes)
        def actual(*args, **kwargs):
            if before_publish:
                kwargs['hooks'].before_publish = before_publish
            return update_fn(*args, **kwargs)
        driver = SyncDriver(self.cfg, client=client, profile=self.fixture.profile,
                            clock=lambda: 1700000000., update_fn=actual)
        service = Service(self.cfg, runtime=runtime, sync_driver=driver)
        runtime.service = service
        self.services.append(service)
        service.start()
        return service, client

    def test_startup_verification_stop_closes_owner_without_starting_provider(self):
        runtime = self.runtime(intercept=lambda p, f: p == 'verify_metadata_complete')
        before = self.snapshot()
        service, client = self.start(runtime)
        self.assertTrue(runtime.entered.wait(15))
        start = time.monotonic()
        self.assertTrue(service.stop())
        self.assertLess(time.monotonic()-start, 5)
        self.assertFalse(runtime.returned_after_stop)
        self.assertTrue(runtime.closed)
        self.assertTrue(runtime.store.db.closed)
        self.assertIsNone(service.failure)
        self.assertFalse(service.failed.is_set())
        self.assertIsNone(service.health.snapshot()['reader'])
        self.assertEqual(client.calls, [])
        self.assertEqual(self.snapshot(), before)

    def test_pending_verification_stop_rolls_back_and_normal_resume_publishes_once(self):
        runtime = self.runtime(intercept=lambda p, f: p == 'verify_metadata_complete' and f['generation'] == 1)
        def with_transaction(*args, **kwargs):
            hooks = kwargs['hooks']
            original = hooks.verify
            def verify(store, generation=None, **options):
                if generation == 1 and not options.get('require_ready', True):
                    # A real outer transaction and named cursor must unwind,
                    # including a deliberately uncommitted metadata mutation.
                    with store.atomic():
                        store.db.execute("INSERT INTO chain.sync_jobs VALUES('cancel-uncommitted-probe','{}')")
                        with store.db.cursor(name='cancel_outer_cursor') as cur:
                            cur.execute('SELECT generate_series(1, 3)')
                            self.assertEqual(cur.fetchone(), (1,))
                            return original(store, generation, **options)
                return original(store, generation, **options)
            hooks.verify = verify
            return update(*args, **kwargs)
        service, client = self.start(runtime, update_fn=with_transaction)
        self.assertTrue(runtime.entered.wait(20), service.failure)
        self.assertEqual(runtime.checkpoint_transaction, 'INTRANS')
        before = self.snapshot()
        calls = list(client.calls)
        with PgStore(conninfo=self.dsn, readonly=True) as store:
            job = store.sync_job('provider-active')
            serial = store.state()['publication_serial']
            pending = store.pending()
            self.assertEqual(pending['id'], 1)
            self.assertNotIn('completion', pending['manifest'])
            self.assertIsNone(store.sync_job('cancel-uncommitted-probe'))
        start = time.monotonic()
        self.assertTrue(service.stop())
        self.assertLess(time.monotonic()-start, 5)
        self.assertFalse(runtime.returned_after_stop)
        self.assertTrue(runtime.closed)
        self.assertIsNone(service.failure)
        self.assertEqual(client.calls, calls)
        self.assertEqual(self.snapshot(), before)
        # A fresh lifetime writer runs the unmodified real updater to publication.
        resumed_runtime = self.runtime()
        resumed, _ = self.start(resumed_runtime)
        def published():
            with PgStore(conninfo=self.dsn, readonly=True) as store:
                return store.meta('canonical_ready') == 1 and store.sync_job('provider-active')['state'] == 'completed'
        eventually(lambda: published() or resumed.failed.is_set(), seconds=20)
        self.assertFalse(resumed.failed.is_set(), resumed.failure)
        self.assertTrue(resumed.stop())
        with PgStore(conninfo=self.dsn) as store:
            self.assertIsNone(store.pending())
            self.assertEqual(store.generation()['id'], 1)
            self.assertEqual(store.state()['publication_serial'], serial+1)
            new_job = store.sync_job('provider-active')
            self.assertEqual(new_job['job_id'], job['job_id'])
            self.assertEqual(new_job['budget'], job['budget'])
            self.assertGreater(new_job['attempts'], job['attempts'])
            self.assertIsNone(store.sync_job('cancel-uncommitted-probe'))
            self.fixture.compare_reference(self.routes, store, transition=True)
            verify_store(store)

    def test_unrelated_progress_error_remains_failure(self):
        runtime = self.runtime(callback_error=ValueError('deliberate fixture error'))
        service, client = self.start(runtime)
        eventually(service.failed.is_set, seconds=15)
        self.assertEqual(service.failure, 'internal_error')
        self.assertTrue(service.stop())
        self.assertTrue(runtime.closed)
        self.assertEqual(client.calls, [])

    def test_blocked_database_operation_still_fails_original_five_second_shutdown(self):
        runtime = self.runtime(blocked=True)
        service, client = self.start(runtime)
        self.assertTrue(runtime.entered.wait(15))
        # The request is actually running in PostgreSQL, not a fake timer.
        eventually(lambda: self.fixture.admin.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE pid=%s AND query='SELECT pg_sleep(20)' AND state='active'",
            (runtime.store.db.info.backend_pid,)).fetchone()[0] == 1)
        start = time.monotonic()
        self.assertFalse(service.stop())
        elapsed = time.monotonic()-start
        self.assertGreaterEqual(elapsed, 4.8)
        self.assertLess(elapsed, 5.5)
        self.assertEqual(service.failure, 'shutdown_timeout')
        self.assertFalse(runtime.closed)
        self.assertEqual(client.calls, [])
        self.fixture.admin.execute('SELECT pg_cancel_backend(%s)', (runtime.store.db.info.backend_pid,))
        eventually(lambda: runtime.closed, seconds=5)
        self.assertTrue(runtime.store.db.closed)
        self.assertEqual(service.failure, 'shutdown_timeout')


if __name__ == '__main__':
    unittest.main(verbosity=2)
