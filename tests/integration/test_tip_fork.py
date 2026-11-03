"""Cardano tip forks around an in-flight follow job, against actual PostgreSQL.

Deterministic fixture providers only; no live provider. The newest Cardano block
(the witness at target+2160) is unstable, exactly as it is for cardano-db-sync:
the job's own stored parent-linked header chain is authoritative for it. The
2160-deep target remains a strict, fail-closed anchor.

Fixture shape: ready generation target OFFSET+10 / witness OFFSET+2170; the job
targets OFFSET+13, so its witness is OFFSET+2173 and it reuses the parent's
headers through W-3.
"""
import copy
from dataclasses import replace
import os
from pathlib import Path
import threading
import time
import unittest
from types import SimpleNamespace

import psycopg
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.provider import ProviderError
import fixtures.database as real
import fixtures.blockfrost_generation as gen_fixture
from fixtures.networks import PREPROD

OFFSET = gen_fixture.OFFSET
TOP = 2173                 # fixture index of the job witness
TARGET = OFFSET + TOP - 2160
WITNESS = OFFSET + TOP
PARENT_WITNESS = OFFSET + 2170
START_HEIGHT = OFFSET + 10


def chain_routes(top=TOP):
    """Canonical fixture chain through fixture index ``top``."""
    routes, _ = gen_fixture.fixtures()
    for h in range(8, top + 1):
        b = gen_fixture.block(h)
        routes['/blocks/' + str(OFFSET + h)] = b
        routes['/blocks/' + b['hash']] = b
        routes['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(n) for n in range(h + 1, min(h + 101, top + 1))]
    routes['/blocks/latest'] = gen_fixture.block(top)
    return routes


def orphan(h):
    """Same-slot competitor at fixture index ``h`` that loses the slot battle."""
    return dict(gen_fixture.block(h), hash=((0xf0 << 248) | h).to_bytes(32, 'big').hex())


def fork_routes():
    """Poll head is the orphan X at W; ``/next`` and ``/blocks/W`` give winner Y."""
    routes = chain_routes()
    routes['/blocks/latest'] = orphan(TOP)
    return routes


def late_fork_routes():
    """Poll head and the stored suffix are X at W; the final ``/blocks/W`` gives Y."""
    routes = chain_routes()
    routes['/blocks/latest'] = orphan(TOP)
    base = gen_fixture.block(2170)
    routes['/blocks/' + base['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(2171), gen_fixture.block(2172), orphan(TOP)]
    return routes


def missing():
    raise RuntimeError('HTTP 404; request counted, no automatic retry')


def missing_status():
    """HTTP 404 as the provider client reports it: an HTTP status, no machine text."""
    raise ProviderError('provider_request_failed_counted', http_status=404)


def deep(h):
    """Provider block of a competing branch that replaces fixture blocks from 2170 up."""
    key = lambda n: ((0xe0 << 248) | n).to_bytes(32, 'big').hex()
    return dict(gen_fixture.block(h), hash=key(h), previous_block=gen_fixture.block(h - 1)['hash'] if h == 2170 else key(h - 1))


def deep_fork_routes():
    routes = chain_routes()
    for h in range(2170, TOP + 1):
        b = deep(h); routes['/blocks/' + str(OFFSET + h)] = b; routes['/blocks/' + b['hash']] = b
        routes['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [deep(n) for n in range(h + 1, TOP + 1)]
    routes['/blocks/' + gen_fixture.block(2169)['hash'] + '/next?count=100&page=1'] = [deep(n) for n in range(2170, TOP + 1)]
    routes['/blocks/latest'] = deep(TOP)
    return routes


class TipForkPGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
    @classmethod
    def tearDownClass(cls): cls.admin.close()
    setUp = real.SyncFixture.setUp
    tearDown = real.SyncFixture.tearDown
    database = real.SyncFixture.database
    make = real.SyncFixture.make
    state = real.SyncFixture.state

    X = orphan(TOP)['hash']
    Y = gen_fixture.block(TOP)['hash']

    def prepare(self):
        dsn = self.make()
        store = PgStore(conninfo=dsn); self.addCleanup(store.close)
        upgrade_store(store); store.certify_ready_full()
        return dsn, store

    def restart(self, store, dsn):
        """New writer session, as a process restart opens it."""
        store.close()
        store = PgStore(conninfo=dsn); self.addCleanup(store.close)
        store.certify_ready_full()
        return store

    def run_follow(self, store, dsn, routes, *, stop=None, clock=None):
        health = self.state(); stop = stop or threading.Event(); client = real.AccountedFake(routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            kw = {} if clock is None else {'clock': clock}
            driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=self.profile, **kw)
            driver._poller = poller
            driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        return client, health

    def attempt(self, store, dsn, routes, **kw):
        """Run one follow step and return its observable outcome."""
        error = None; client = None
        try:
            client, _ = self.run_follow(store, dsn, routes, **kw)
        except Exception as exc:
            error = getattr(exc, 'code', type(exc).__name__)
        job = store.sync_job('provider-active') or {}
        pending = store.pending()
        return {'error': error, 'ready': store.generation()['id'], 'pending': pending and pending['id'],
                'last_failure': job.get('last_failure'), 'job_id': job.get('job_id'),
                'attempts': job.get('attempts'), 'budget': job.get('budget'), 'job_state': job.get('state'),
                'calls': None if client is None else list(client.calls)}

    def served_hash(self, dsn, height):
        """What the node reads: the gated node-facing relation (it raises unless the database serves)."""
        with psycopg.connect(dsn, autocommit=True) as db:
            row = db.execute('SELECT hash FROM public.block WHERE block_no=%s', (height,)).fetchone()
            return None if row is None else bytes(row[0]).hex()

    def final_anchor_receipt(self, store, generation):
        rows = store.checkpoint_rows(generation=generation, phase='final-anchors', unit='complete')
        self.assertEqual(len(rows), 1)
        return rows[0]['receipt']

    def assert_published_stored_witness(self, store, dsn, *, initial):
        g = store.generation()
        self.assertEqual((g['id'], g['target_height'], g['witness_height']), (1, TARGET, WITNESS))
        self.assertEqual(g['manifest']['anchors']['witness'], {'height': WITNESS, 'hash': self.Y})
        self.assertEqual(store.block_at(1, WITNESS)['hash'].hex(), self.Y)
        provisional = self.final_anchor_receipt(store, 1)['provisional']
        self.assertEqual(provisional['witness'], {'height': WITNESS, 'manifest_initial': initial, 'stored': self.Y})
        self.assertEqual(provisional['observations'][-1], {'stage': 'final', 'height': WITNESS, 'stored': self.Y, 'provider': self.Y})
        self.assertFalse(store.state()['revoked'])
        self.assertEqual(self.served_hash(dsn, WITNESS), self.Y)
        verify_store(store)

    def test_tip_fork_between_poll_and_suffix_publishes_stored_witness(self):
        dsn, store = self.prepare()
        outcome = self.attempt(store, dsn, fork_routes())
        self.assertIsNone(outcome['error'], outcome)
        self.assertIsNone(outcome['last_failure'], outcome)
        self.assertEqual((outcome['ready'], outcome['pending'], outcome['job_state']), (1, None, 'completed'), outcome)
        self.assert_published_stored_witness(store, dsn, initial=self.X)

    def run_service(self, dsn, routes, timeout=30):
        """Actual service lifecycle until publication of generation 1 or a failure."""
        from db_sync_alt.service.runtime import Service, PostgresRuntime
        from psycopg import sql
        from fixtures.service import config, real_reader
        servicefile = Path(self.tmp.name) / 'tip-fork-services.conf'
        values = psycopg.conninfo.conninfo_to_dict(dsn)
        servicefile.write_text('[tip_fork]\n' + ''.join(k + '=' + v + '\n' for k, v in values.items()) + '\n')
        reader = real_reader(self.admin, dsn, servicefile)
        self.addCleanup(self.admin.execute, sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(reader['role'])))
        previous = os.environ.get('PGSERVICEFILE'); os.environ['PGSERVICEFILE'] = str(servicefile)
        cfg = replace(config(), pg_service='tip_fork', reader=reader, shutdown_seconds=5, storage_reserves={'/tmp': 1})
        cfg = replace(cfg, policy=replace(cfg.policy, poll_seconds=.05, provider_operation_seconds=5, local_operation_seconds=30))
        driver = SyncDriver(cfg, client=real.AccountedFake(routes), profile=self.profile)
        service = Service(cfg, runtime=PostgresRuntime(cfg), sync_driver=driver)
        published = False
        try:
            service.start(); deadline = time.monotonic() + timeout
            while not service.failed.is_set() and time.monotonic() < deadline:
                with PgStore(conninfo=dsn, readonly=True) as view:
                    if view.generation()['id'] == 1:
                        published = True; break
                time.sleep(.05)
            snapshot = service.health.snapshot()
            with PgStore(conninfo=dsn, readonly=True) as view:
                job = view.sync_job('provider-active') or {}
                revoked = view.state()['revoked']
            outcome = {'published': published, 'failure': service.failure, 'health_error_code': snapshot['error_code'],
                       'lifecycle': snapshot['lifecycle'], 'last_failure': job.get('last_failure'), 'revoked': revoked}
            outcome['clean_stop'] = service.stop()
            outcome['failure_after_stop'] = service.failure
        finally:
            service.stop()
            if previous is None: os.environ.pop('PGSERVICEFILE', None)
            else: os.environ['PGSERVICEFILE'] = previous
        return outcome

    def test_service_survives_tip_fork_between_poll_and_suffix(self):
        dsn, store = self.prepare(); store.close()
        outcome = self.run_service(dsn, fork_routes())
        self.assertTrue(outcome['published'], outcome)
        self.assertIsNone(outcome['failure'], outcome)
        self.assertEqual((outcome['clean_stop'], outcome['failure_after_stop']), (True, None), outcome)
        with PgStore(conninfo=dsn) as store:
            store.certify_ready_full()
            self.assert_published_stored_witness(store, dsn, initial=self.X)

    def test_service_reports_target_change_under_its_own_code(self):
        dsn, store = self.prepare(); store.close()
        routes = chain_routes(); path = '/blocks/' + str(TARGET); row = routes[path]; started = []
        page = '/blocks/' + gen_fixture.block(2170)['hash'] + '/next?count=100&page=1'; rows = routes[page]
        def next_page():
            started.append(1); return copy.deepcopy(rows)
        def target():
            # Polls keep running at 50 ms; the change lands once the job is under way.
            return dict(row, hash='ab' * 32) if started else copy.deepcopy(row)
        routes[page] = next_page; routes[path] = target
        outcome = self.run_service(dsn, routes)
        self.assertFalse(outcome['published'], outcome)
        self.assertEqual((outcome['failure'], outcome['health_error_code'], outcome['lifecycle']),
                         ('pending_target_changed', 'pending_target_changed', 'failed'), outcome)
        self.assertEqual(outcome['last_failure']['code'], 'pending_target_changed', outcome)
        self.assertFalse(outcome['revoked'], outcome)

    def build_stuck_pending(self):
        """Pending generation exactly as a refused job leaves it.

        Manifest witness is the orphan X frozen from the poll head, the stored
        header at W is the winner Y, events are complete, reuse is through W-3,
        nothing is sealed and the active job carries the anchor refusal.
        """
        dsn, store = self.prepare()
        routes = fork_routes(); stop = threading.Event(); target = '/blocks/' + str(TARGET)
        row = routes[target]; calls = []
        def final_target():
            # First request is the poll's confirmed read; the second is the final anchor.
            calls.append(1)
            if len(calls) == 2: stop.set()
            return copy.deepcopy(row)
        routes[target] = final_target
        outcome = self.attempt(store, dsn, routes, stop=stop)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), (None, 0, 1), outcome)
        g = store.pending()
        self.assertNotIn('completion', g['manifest'])
        if g['manifest']['anchors']['witness']['hash'] != self.X:
            store.db.execute("UPDATE chain.generations SET manifest=jsonb_set(manifest,'{anchors,witness,hash}',to_jsonb(%s::text)) WHERE id=1 AND status='pending'", (self.X,))
        job = store.sync_job('provider-active')
        attempts = max(13, job['attempts'])
        record = {'version': 1, 'code': 'pending_anchor_changed', 'stage': 'anchors', 'attempts': attempts,
                  'at': time.time(), 'generation': 1, 'block_height': WITNESS, 'block_hash': self.X}
        store.save_sync_job('provider-active', dict(job, attempts=attempts, last_failure=record))
        g = store.pending()
        self.assertEqual(g['manifest']['anchors']['witness'], {'height': WITNESS, 'hash': self.X})
        self.assertEqual(store.block_at(1, WITNESS)['hash'].hex(), self.Y)
        self.assertEqual(store.checkpoint_rows(generation=1, phase='metadata', unit='inherited')[0]['receipt']['hi'], WITNESS - 3)
        self.assertEqual(store.last_event_height(1), TARGET)
        self.assertEqual(store.checkpoint_rows(generation=1, phase='final-anchors'), [])
        return dsn, store

    def test_witness_rebind_changes_only_the_unsealed_witness_hash(self):
        from db_sync_alt.contract import Refusal
        dsn, store = self.build_stuck_pending()
        before = copy.deepcopy(store.pending()['manifest'])
        revision = store.db.execute('SELECT revision FROM chain.integrity_state').fetchone()[0]
        after = store.rebind_pending_witness(1)
        self.assertEqual(after['anchors']['witness'], {'height': WITNESS, 'hash': self.Y})
        self.assertEqual({**after, 'anchors': None}, {**before, 'anchors': None})
        self.assertEqual({k: v for k, v in after['anchors'].items() if k != 'witness'},
                         {k: v for k, v in before['anchors'].items() if k != 'witness'})
        self.assertEqual(store.pending()['manifest'], after)
        bumped = store.db.execute('SELECT revision FROM chain.integrity_state').fetchone()[0]
        self.assertGreater(bumped, revision)
        self.assertEqual(store.rebind_pending_witness(1), after)
        self.assertEqual(store.db.execute('SELECT revision FROM chain.integrity_state').fetchone()[0], bumped)
        store = self.restart(store, dsn)
        self.assertIsNone(self.attempt(store, dsn, chain_routes())['error'])
        with self.assertRaises(Refusal):
            store.rebind_pending_witness(1)

    def test_restart_with_orphan_manifest_witness_publishes_without_reset(self):
        dsn, store = self.build_stuck_pending()
        before = copy.deepcopy(store.sync_job('provider-active'))
        store = self.restart(store, dsn)
        outcome = self.attempt(store, dsn, chain_routes())
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['pending'], outcome['job_state']), (1, None, 'completed'), outcome)
        self.assertEqual((outcome['job_id'], outcome['budget']), (before['job_id'], before['budget']), outcome)
        self.assertGreater(outcome['attempts'], before['attempts'], outcome)
        self.assert_published_stored_witness(store, dsn, initial=self.X)
        observed = self.final_anchor_receipt(store, 1)['provisional']['observations']
        self.assertEqual([(o['stage'], o['height']) for o in observed],
                         [('resume', WITNESS), ('resume', PARENT_WITNESS), ('final', WITNESS)])

    def test_fork_after_suffix_publishes_provisional_witness_and_next_job_replaces_it(self):
        dsn, store = self.prepare()
        outcome = self.attempt(store, dsn, late_fork_routes())
        self.assertIsNone(outcome['error'], outcome)
        g = store.generation()
        self.assertEqual((g['id'], outcome['pending']), (1, None), outcome)
        self.assertEqual(g['manifest']['anchors']['witness'], {'height': WITNESS, 'hash': self.X})
        provisional = self.final_anchor_receipt(store, 1)['provisional']
        self.assertEqual(provisional['witness'], {'height': WITNESS, 'manifest_initial': self.X, 'stored': self.X})
        self.assertEqual(provisional['observations'], [{'stage': 'final', 'height': WITNESS, 'stored': self.X, 'provider': self.Y}])
        self.assertEqual(self.served_hash(dsn, WITNESS), self.X)
        self.assertFalse(store.state()['revoked'])
        verify_store(store)
        # The next job sees Y at the old witness and rebuilds the replaced tail.
        outcome = self.attempt(store, dsn, chain_routes(TOP + 1))
        self.assertIsNone(outcome['error'], outcome)
        g = store.generation()
        self.assertEqual((g['id'], g['target_height'], g['witness_height']), (2, TARGET + 1, WITNESS + 1))
        reuse = store.checkpoint_rows(generation=2, phase='metadata', unit='inherited')[0]['receipt']
        self.assertEqual(reuse['hi'], WITNESS - 1)
        self.assertEqual(self.served_hash(dsn, WITNESS), self.Y)
        self.assertEqual(self.served_hash(dsn, WITNESS + 1), gen_fixture.block(TOP + 1)['hash'])
        self.assertEqual(store.block_at(1, WITNESS)['hash'].hex(), self.X)
        self.assertFalse(store.state()['revoked'])
        verify_store(store)

    def test_resume_of_sealed_pending_with_replaced_witness_publishes(self):
        from db_sync_alt.sources.blockfrost.sync import PgHooks
        from unittest.mock import patch
        dsn, store = self.prepare()
        original = PgHooks.before_publish
        def stop(hooks): hooks.stop_event.set(); original(hooks)
        with patch.object(PgHooks, 'before_publish', stop):
            self.assertIsNone(self.attempt(store, dsn, chain_routes())['error'])
        self.assertIn('completion', store.pending()['manifest'])
        before = copy.deepcopy(store.sync_job('provider-active'))
        replaced = chain_routes(); replaced['/blocks/' + str(WITNESS)] = orphan(TOP)
        outcome = self.attempt(store, dsn, replaced)
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id']), (1, before['job_id']), outcome)
        self.assertGreater(outcome['attempts'], before['attempts'])
        latest = store.checkpoint_rows(generation=1, phase='completion-revalidation', unit='latest')[0]['receipt']['receipt']
        self.assertEqual([o['provider'] for o in latest['provisional']['observations'] if o['height'] == WITNESS], [self.X, self.X])
        self.assertEqual(store.generation()['manifest']['anchors']['witness'], {'height': WITNESS, 'hash': self.Y})
        verify_store(store)

    def test_target_change_fails_closed_without_publication_or_reset(self):
        dsn, store = self.prepare()
        routes = chain_routes(); path = '/blocks/' + str(TARGET); row = routes[path]; calls = []
        def target():
            calls.append(1)
            return copy.deepcopy(row) if len(calls) == 1 else dict(row, hash='ab' * 32)
        routes[path] = target
        outcome = self.attempt(store, dsn, routes)
        self.assertEqual(outcome['error'], 'pending_target_changed', outcome)
        self.assertEqual((outcome['last_failure']['code'], outcome['last_failure']['stage']), ('pending_target_changed', 'anchors'), outcome)
        self.assertEqual((outcome['ready'], outcome['pending']), (0, 1), outcome)
        self.assertFalse(store.state()['revoked'])
        before = copy.deepcopy(store.sync_job('provider-active')); pending = copy.deepcopy(store.pending())
        changed = chain_routes(); changed[path] = dict(row, hash='ab' * 32)
        outcome = self.attempt(store, dsn, changed)
        self.assertEqual(outcome['error'], 'pending_target_changed', outcome)
        self.assertEqual(outcome['last_failure']['code'], 'pending_target_changed', outcome)
        self.assertEqual((outcome['job_id'], outcome['budget']), (before['job_id'], before['budget']))
        self.assertGreater(outcome['attempts'], before['attempts'])
        self.assertEqual(store.pending(), pending)
        self.assertEqual(store.generation()['id'], 0)
        self.assertFalse(store.state()['revoked'])

    def test_provider_without_witness_height_waits_then_publishes(self):
        dsn, store = self.prepare()
        routes = chain_routes(); routes['/blocks/' + str(WITNESS)] = missing
        now = [time.time()]; clock = lambda: now[0]
        outcome = self.attempt(store, dsn, routes, clock=clock)
        self.assertEqual(outcome['error'], 'provider_unavailable', outcome)
        job = store.sync_job('provider-active')
        self.assertEqual((job['error_code'], job['failure_count']), ('provider_unavailable', 1))
        self.assertGreater(job['retry_at'], now[0])
        self.assertEqual((job['last_failure']['code'], job['last_failure']['stage'], job['last_failure']['block_height']),
                         ('provider_unavailable', 'anchors', WITNESS))
        self.assertEqual((outcome['ready'], outcome['pending']), (0, 1))
        # Resume while the provider still lacks W: the same transient wait, longer backoff.
        now[0] = job['retry_at'] + 1
        outcome = self.attempt(store, dsn, routes, clock=clock)
        self.assertEqual(outcome['error'], 'provider_unavailable', outcome)
        job = store.sync_job('provider-active')
        self.assertEqual(job['failure_count'], 2)
        now[0] = job['retry_at'] + 1
        outcome = self.attempt(store, dsn, chain_routes(), clock=clock)
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id'], outcome['job_state']), (1, job['job_id'], 'completed'))
        self.assertGreater(outcome['attempts'], job['attempts'])
        self.assert_published_stored_witness(store, dsn, initial=self.Y)

    def test_provider_client_status_404_at_witness_waits_then_publishes(self):
        dsn, store = self.prepare()
        routes = chain_routes(); routes['/blocks/' + str(WITNESS)] = missing_status
        now = [time.time()]; clock = lambda: now[0]
        outcome = self.attempt(store, dsn, routes, clock=clock)
        self.assertEqual(outcome['error'], 'provider_unavailable', outcome)
        job = store.sync_job('provider-active')
        self.assertEqual((job['error_code'], job['failure_count']), ('provider_unavailable', 1))
        self.assertGreater(job['retry_at'], now[0])
        self.assertEqual((job['last_failure']['code'], job['last_failure']['stage'], job['last_failure']['block_height']),
                         ('provider_unavailable', 'anchors', WITNESS))
        self.assertEqual((outcome['ready'], outcome['pending']), (0, 1))
        self.assertFalse(store.state()['revoked'])
        now[0] = job['retry_at'] + 1
        outcome = self.attempt(store, dsn, chain_routes(), clock=clock)
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id'], outcome['job_state']), (1, job['job_id'], 'completed'))
        self.assertEqual(outcome['budget'], job['budget'])
        self.assertGreater(outcome['attempts'], job['attempts'])
        self.assert_published_stored_witness(store, dsn, initial=self.Y)

    def test_provider_client_status_404_on_suffix_page_waits_without_pending(self):
        dsn, store = self.prepare()
        routes = chain_routes()
        routes['/blocks/' + gen_fixture.block(2170)['hash'] + '/next?count=100&page=1'] = missing_status
        now = [time.time()]; clock = lambda: now[0]
        outcome = self.attempt(store, dsn, routes, clock=clock)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), ('provider_unavailable', 0, None), outcome)
        self.assertEqual((outcome['last_failure']['code'], outcome['last_failure']['stage']), ('provider_unavailable', 'metadata'))
        job = store.sync_job('provider-active')
        self.assertEqual((job['state'], job['target'], job['error_code'], job['failure_count']),
                         ('active', TARGET, 'provider_unavailable', 1))
        now[0] = job['retry_at'] + 1
        outcome = self.attempt(store, dsn, chain_routes(), clock=clock)
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id'], outcome['budget']), (1, job['job_id'], job['budget']))
        self.assertGreater(outcome['attempts'], job['attempts'])
        self.assert_published_stored_witness(store, dsn, initial=self.Y)

    def crash_before_suffix_store(self, store, dsn, routes):
        """The process dies after the pending generation exists, before its suffix is stored."""
        import db_sync_alt.sources.blockfrost.follow as pg_follow_source
        from unittest.mock import patch
        def crash(*a, **kw): raise RuntimeError('simulated process death')
        with patch.object(pg_follow_source, '_store_suffix', side_effect=crash):
            outcome = self.attempt(store, dsn, routes)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), ('RuntimeError', 0, 1), outcome)
        self.assertEqual(store.last_metadata_height(1), PARENT_WITNESS)
        self.assertEqual(store.checkpoint_rows(generation=1, phase='metadata', unit=str(PARENT_WITNESS + 1)), [])
        self.assertTrue(store.completed(1, 'handoff', 'initial'))
        return outcome

    def test_crash_between_pending_creation_and_suffix_store_resumes(self):
        dsn, store = self.prepare()
        crashed = self.crash_before_suffix_store(store, dsn, chain_routes())
        store = self.restart(store, dsn)
        outcome = self.attempt(store, dsn, chain_routes())
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id'], outcome['job_state']), (1, crashed['job_id'], 'completed'))
        self.assertGreater(outcome['attempts'], crashed['attempts'])
        self.assert_published_stored_witness(store, dsn, initial=self.Y)
        pages = store.checkpoint_rows(generation=1, phase='metadata')
        self.assertEqual([(p['unit'], p['receipt'].get('lo'), p['receipt'].get('hi')) for p in pages],
                         [(str(PARENT_WITNESS + 1), PARENT_WITNESS + 1, WITNESS), ('inherited', START_HEIGHT, PARENT_WITNESS)])
        self.assertEqual(pages[0]['receipt']['provider']['requests'][-1]['endpoint'],
                         '/blocks/' + gen_fixture.block(2170)['hash'] + '/next?count=100&page=1')

    def test_incomplete_suffix_on_a_replaced_base_refuses_with_its_own_code(self):
        dsn, store = self.prepare()
        self.crash_before_suffix_store(store, dsn, chain_routes())
        store = self.restart(store, dsn)
        pending = copy.deepcopy(store.pending()); before = copy.deepcopy(store.sync_job('provider-active'))
        outcome = self.attempt(store, dsn, deep_fork_routes())
        self.assertEqual(outcome['error'], 'pending_suffix_base_changed', outcome)
        self.assertEqual((outcome['last_failure']['code'], outcome['last_failure']['stage']), ('pending_suffix_base_changed', 'anchors'), outcome)
        self.assertEqual(outcome['last_failure']['block_height'], PARENT_WITNESS)
        self.assertEqual((outcome['ready'], outcome['pending'], outcome['job_id']), (0, 1, before['job_id']))
        self.assertEqual(store.pending(), pending)
        self.assertFalse(store.state()['revoked'])

    def test_provider_chain_short_of_witness_during_suffix_fetch_waits_without_pending(self):
        dsn, store = self.prepare()
        short = fork_routes()
        base = gen_fixture.block(2170); top = gen_fixture.block(2172)
        short['/blocks/' + base['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(2171), top]
        short['/blocks/' + top['hash'] + '/next?count=100&page=1'] = []
        now = [time.time()]; clock = lambda: now[0]
        outcome = self.attempt(store, dsn, short, clock=clock)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), ('provider_unavailable', 0, None), outcome)
        self.assertEqual((outcome['last_failure']['code'], outcome['last_failure']['stage']), ('provider_unavailable', 'metadata'))
        job = store.sync_job('provider-active')
        self.assertEqual((job['state'], job['target'], job['error_code']), ('active', TARGET, 'provider_unavailable'))
        # An orphaned base (HTTP 404 on /next) is the same transient wait.
        now[0] = job['retry_at'] + 1
        gone = fork_routes(); gone['/blocks/' + base['hash'] + '/next?count=100&page=1'] = missing
        outcome = self.attempt(store, dsn, gone, clock=clock)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), ('provider_unavailable', 0, None), outcome)
        job = store.sync_job('provider-active')
        now[0] = job['retry_at'] + 1
        outcome = self.attempt(store, dsn, fork_routes(), clock=clock)
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id']), (1, job['job_id']))
        self.assertGreater(outcome['attempts'], job['attempts'])
        self.assert_published_stored_witness(store, dsn, initial=self.X)

    def test_superseded_witness_history_full_verifier_and_native_restore_agree(self):
        from db_sync_alt.backup import backup, restore, file_hash
        from db_sync_alt.verify.store import physical_inventory
        dsn, store = self.prepare()
        self.assertIsNone(self.attempt(store, dsn, late_fork_routes())['error'])
        full = verify_store(store)
        counts = lambda g: store.generation(g)['manifest']['integrity']['semantic']['counts']
        self.assertEqual({k: v['count'] for k, v in full['digests'].items()}, counts(1))
        self.assertIsNone(self.attempt(store, dsn, chain_routes(TOP + 1))['error'])
        full = verify_store(store)
        self.assertEqual(full['digests'], store.digests(2))
        self.assertEqual({k: v['count'] for k, v in full['digests'].items()}, counts(2))
        self.assertEqual(store.db.execute('SELECT count(*) FROM chain.blocks WHERE height=%s', (WITNESS,)).fetchone()[0], 2)
        store = self.restart(store, dsn)
        per_generation = {g: verify_store(store, g)['digests'] for g in (1, 2)}
        source = physical_inventory(store); store.close()
        archive = Path(self.tmp.name) / 'tip-fork.dump'; manifest = archive.with_suffix('.json')
        backup(archive, manifest, conninfo=dsn)
        destination = self.database()
        restore(archive, manifest, conninfo=destination, expected_manifest_sha256=file_hash(manifest)['sha256'])
        with PgStore(conninfo=destination) as restored:
            restored.certify_ready_full()
            self.assertEqual(verify_store(restored)['digests'], full['digests'])
            for generation in (1, 2):
                self.assertEqual(verify_store(restored, generation)['digests'], per_generation[generation])
            copied = physical_inventory(restored)
            self.assertEqual({k: v for k, v in copied.items() if k != 'metadata'}, {k: v for k, v in source.items() if k != 'metadata'})
            self.assertEqual(restored.block_at(2, WITNESS)['hash'].hex(), self.Y)
            self.assertEqual(restored.block_at(1, WITNESS)['hash'].hex(), self.X)
        self.assertEqual(self.served_hash(destination, WITNESS), self.Y)

    def test_reader_sees_replacement_tip_and_unchanged_stable_blocks(self):
        """The node's queries, as the reader straight from PostgreSQL on prepared statements, across a
        superseded-witness publication."""
        import uuid
        from psycopg import sql
        from psycopg.conninfo import make_conninfo
        from db_sync_alt.store.reader import grant_reader
        from conformance.client import TypedClient
        from conformance.statements import by_name, parameter_types
        dsn, store = self.prepare()
        role = 'tip_fork_reader_' + uuid.uuid4().hex[:10]
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT').format(sql.Identifier(role)))
        self.addCleanup(self.admin.execute, sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
        grant_reader(store.db, role)
        stable_hash = store.block_at(0, START_HEIGHT)['hash']
        client = TypedClient(make_conninfo(dsn, user=role), timeout=20).connect(); self.addCleanup(client.close)
        entries = {'latest': by_name('get_latest_block_info'), 'by_hash': by_name('get_block_by_hash', part='primitives/'),
                   'by_numbers': by_name('get_blocks_by_numbers'), 'stable_epoch': by_name('get_latest_stable_epoch')}

        def query(key, *args):
            entry = entries[key]
            return client.query(entry['sql'], parameter_types(entry), list(args)).rows

        def hex_of(value):
            return bytes(value).hex()

        def view():
            latest = query('latest')[0]
            return {'latest': (latest['block_no'], hex_of(latest['hash'])),
                    'stable': query('by_numbers', START_HEIGHT, TARGET),
                    'stable_by_hash': query('by_hash', stable_hash),
                    'stable_epoch': query('stable_epoch', 2160)}
        before = view()
        self.assertEqual(before['latest'][0], PARENT_WITNESS)
        self.assertIsNone(self.attempt(store, dsn, late_fork_routes())['error'])
        first = view()
        self.assertEqual(first['latest'], (WITNESS, self.X))
        self.assertTrue(query('by_hash', bytes.fromhex(self.X)))
        self.assertIsNone(self.attempt(store, dsn, chain_routes(TOP + 1))['error'])
        second = view()
        self.assertEqual(second['latest'], (WITNESS + 1, gen_fixture.block(TOP + 1)['hash']))
        tip = query('by_numbers', WITNESS - 1, WITNESS + 1)
        self.assertEqual([(r['block_no'], hex_of(r['hash'])) for r in tip],
                         [(WITNESS - 1, gen_fixture.block(TOP - 1)['hash']), (WITNESS, self.Y), (WITNESS + 1, gen_fixture.block(TOP + 1)['hash'])])
        self.assertEqual(query('by_hash', bytes.fromhex(self.X)), [])
        self.assertEqual(len(query('by_hash', bytes.fromhex(self.Y))), 1)
        # Served height never drops below a published witness; stable answers are unchanged.
        heights = [before['latest'][0], first['latest'][0], second['latest'][0]]
        self.assertEqual(heights, sorted(heights)); self.assertGreaterEqual(heights[-1], WITNESS)
        for key in ('stable', 'stable_by_hash', 'stable_epoch'):
            self.assertTrue(before[key], key)
            self.assertEqual(first[key], before[key], key); self.assertEqual(second[key], before[key], key)

    def test_provider_head_below_witness_on_retry_waits_then_publishes(self):
        dsn, store = self.prepare()
        short = fork_routes()
        base = gen_fixture.block(2170); top = gen_fixture.block(2172)
        short['/blocks/' + base['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(2171), top]
        short['/blocks/' + top['hash'] + '/next?count=100&page=1'] = []
        now = [time.time()]; clock = lambda: now[0]
        outcome = self.attempt(store, dsn, short, clock=clock)
        self.assertEqual((outcome['error'], outcome['pending']), ('provider_unavailable', None), outcome)
        # The provider's tip rolled back below W: its head is W-1 on the next poll.
        now[0] = store.sync_job('provider-active')['retry_at'] + 1
        lower = chain_routes(TOP - 1)
        outcome = self.attempt(store, dsn, lower, clock=clock)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), ('provider_unavailable', 0, None), outcome)
        self.assertEqual((outcome['last_failure']['code'], outcome['last_failure']['stage']), ('provider_unavailable', 'anchors'))
        job = store.sync_job('provider-active')
        self.assertEqual((job['state'], job['target'], job['failure_count']), ('active', TARGET, 2))
        now[0] = job['retry_at'] + 1
        outcome = self.attempt(store, dsn, chain_routes(), clock=clock)
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id']), (1, job['job_id']))
        self.assert_published_stored_witness(store, dsn, initial=self.Y)

    def test_stop_during_suffix_fetch_leaves_no_pending_generation(self):
        dsn, store = self.prepare()
        routes = chain_routes(); stop = threading.Event()
        page = '/blocks/' + gen_fixture.block(2170)['hash'] + '/next?count=100&page=1'; rows = routes[page]
        def stopping():
            stop.set(); return copy.deepcopy(rows)
        routes[page] = stopping
        outcome = self.attempt(store, dsn, routes, stop=stop)
        self.assertEqual((outcome['error'], outcome['ready'], outcome['pending']), (None, 0, None), outcome)
        job = store.sync_job('provider-active')
        outcome = self.attempt(store, dsn, chain_routes())
        self.assertIsNone(outcome['error'], outcome)
        self.assertEqual((outcome['ready'], outcome['job_id']), (1, job['job_id']))
        self.assert_published_stored_witness(store, dsn, initial=self.Y)


if __name__ == '__main__':
    unittest.main(verbosity=2)
