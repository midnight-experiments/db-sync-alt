"""Integrity checkpoints against actual PostgreSQL: schema 4 and its upgrade, the checkpoint's guards,
verification from the integrity base, and the checkpoints written at startup and between publications.

No live provider: the fixture chain of `fixtures.blockfrost_generation`, extended so that several
incremental generations can follow generation 0 (targets OFFSET+11 to OFFSET+16).
"""
import os
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from db_sync_alt.contract import Refusal
from db_sync_alt.service.runtime import PostgresRuntime
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.sync import PgHooks, SyncDriver
from db_sync_alt.store import incremental
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.reader import grant_reader, verify_reader
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
import fixtures.blockfrost_generation as gen_fixture
import fixtures.database as real
from fixtures.networks import PREPROD
from fixtures.schema import to_schema3

OFFSET = gen_fixture.OFFSET
TOP = 2176  # fixture index of the provider's newest block: confirmed height OFFSET+16


def chain_routes(top=TOP):
    routes, _ = gen_fixture.fixtures()
    for h in range(8, top + 1):
        b = gen_fixture.block(h)
        routes['/blocks/' + str(OFFSET + h)] = b
        routes['/blocks/' + b['hash']] = b
        routes['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(n) for n in range(h + 1, min(h + 101, top + 1))]
    routes['/blocks/latest'] = gen_fixture.block(top)
    return routes


class Walk:
    """Progress callback: the generations whose transitions a verification recomputes."""
    def __init__(self):
        self.generations = set()

    def __call__(self, phase, **fields):
        if phase == 'delta_family_complete':
            self.generations.add(fields['generation'])


class CheckpointPGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
    @classmethod
    def tearDownClass(cls): cls.admin.close()
    database = real.SyncFixture.database
    make = real.SyncFixture.make
    state = real.SyncFixture.state

    def setUp(self):
        real.SyncFixture.setUp(self)
        self.roles = []

    def tearDown(self):
        real.SyncFixture.tearDown(self)
        for role in self.roles:
            self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))

    def open(self, dsn):
        store = PgStore(conninfo=dsn)
        self.addCleanup(store.close)
        return store

    def follow(self, store, dsn, target):
        """One step of the sync driver, publishing one incremental generation up to `target`."""
        routes = chain_routes(); health = self.state(); stop = threading.Event(); client = real.AccountedFake(routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=self.profile, fixed_target=target)
            driver._poller = poller
            driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        self.assertEqual(store.generation()['target_height'], target)

    def history(self, generations, *, schema=4):
        """Generation 0 plus `generations` incremental generations, at `schema` (3 or 4)."""
        dsn = self.make(); store = self.open(dsn)
        if schema == 3:
            to_schema3(store)
        else:
            upgrade_store(store)
        store.certify_ready_full()
        for n in range(generations):
            self.follow(store, dsn, OFFSET + 11 + n)
        return dsn, store

    def startup(self, dsn, previous=None):
        """The service's startup verification in a new writer session; returns the store and the
        generations whose transitions it walked."""
        if previous is not None:
            previous.close()
        store = self.open(dsn); walk = Walk(); store.progress_callback = walk
        PostgresRuntime(SimpleNamespace(network='preprod')).verify(store)
        store.progress_callback = None
        return store, walk.generations

    def checkpoints(self, store):
        return store.db.execute('SELECT generation,previous_base FROM chain.integrity_checkpoints ORDER BY generation').fetchall()

    def reader(self, dsn):
        role = 'node_reader_' + os.urandom(5).hex(); self.roles.append(role)
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT CONNECTION LIMIT 40').format(sql.Identifier(role)))
        return role

    def node(self, dsn, role):
        db = psycopg.connect(make_conninfo(dsn, user=role), autocommit=True, options='-c statement_timeout=5000')
        self.addCleanup(db.close)
        return db

    def test_upgrade_from_schema3_checkpoints_canonical_while_the_node_reads(self):
        dsn, store = self.history(2, schema=3)
        self.assertEqual((store.state()['schema_version'], store.generation()['id']), (3, 2))
        walk = Walk(); store.progress_callback = walk
        before = verify_store(store)
        self.assertEqual(walk.generations, {1, 2})  # schema 3: every transition since the base
        store.progress_callback = None
        role = self.reader(dsn)
        with patch('db_sync_alt.store.reader.SCHEMA_VERSION', 3):
            grant_reader(store.db, role)
        node = self.node(dsn, role); second = self.node(dsn, role)
        query = 'SELECT count(*),max(block_no),(SELECT count(*) FROM tx_in),(SELECT count(*) FROM epoch_param) FROM block'
        published = node.execute(query).fetchone()
        inside, release, outcome = threading.Event(), threading.Event(), {}
        original = incremental.insert_checkpoint

        def paused(*args, **kwargs):
            written = original(*args, **kwargs)
            inside.set(); release.wait(60)
            return written

        def upgrade():
            try:
                outcome['result'] = upgrade_store(store)
            except BaseException as error:
                outcome['error'] = error

        with patch('db_sync_alt.store.incremental.insert_checkpoint', paused):
            worker = threading.Thread(target=upgrade); worker.start()
            try:
                self.assertTrue(inside.wait(120), outcome)
                # The upgrade transaction is open: schema 4, the gate and the checkpoint are not committed yet.
                during = [node.execute(query).fetchone(), second.execute(query).fetchone()]
                fresh = self.node(dsn, role)
                during.append(fresh.execute(query).fetchone())
            finally:
                release.set(); worker.join(120)
        self.assertNotIn('error', outcome)
        self.assertEqual(during, [published] * 3)
        result = outcome['result']
        self.assertEqual((result['schema_version'], result['checkpoint_generation'], result['base_generation']), (4, 2, 2))
        self.assertEqual(result['full_verification'], before)
        self.assertEqual(node.execute(query).fetchone(), published)
        self.assertEqual(self.node(dsn, role).execute(query).fetchone(), published)
        self.assertEqual(store.state()['schema_version'], 4)
        self.assertEqual(self.checkpoints(store), [(2, 0)])
        row = store.db.execute('SELECT root,digests,service_version FROM chain.integrity_checkpoints').fetchone()
        self.assertEqual(row[:2], (incremental.integrity_reference(store.generation(2))['sha256'], before['digests']))
        self.assertEqual(store.integrity()['base'], 2)
        # The node's reader has exactly its relations: the checkpoints are not among them.
        self.assertEqual(verify_reader(store.db, role)['privileges'], 'node_relations_only')
        with self.assertRaises(psycopg.errors.InsufficientPrivilege):
            node.execute('SELECT * FROM chain.integrity_checkpoints')
        self.assertTrue(store.db.execute("SELECT has_table_privilege(%s,'chain.integrity_checkpoints','SELECT')", (role,)).fetchone() == (False,))
        # A second upgrade changes nothing; verification now starts at the checkpoint.
        self.assertEqual(upgrade_store(store), {'schema_version': 4, 'changed': False})
        store, walked = self.startup(dsn, store)
        self.assertEqual(walked, set())

    def test_checkpoint_guards(self):
        dsn, store = self.history(1)
        store, _ = self.startup(dsn, store)
        self.assertEqual(self.checkpoints(store), [(1, 0)])
        row = store.db.execute('SELECT generation,previous_base,root,digests,service_version FROM chain.integrity_checkpoints').fetchone()
        insert = 'INSERT INTO chain.integrity_checkpoints(generation,previous_base,root,digests,service_version) VALUES(%s,%s,%s,%s,%s)'
        values = (0, 0, row[2], psycopg.types.json.Jsonb(row[3]), 'x')
        attempts = [
            ('no gate', [], insert, values, 'immutable_projection_or_pointer'),
            ('flag without serving lock', ["SET LOCAL db_sync_alt.projection_write='on'"], insert, values, 'immutable_projection_or_pointer'),
            ('serving lock without flag', ['SELECT pg_advisory_xact_lock(36,2)'], insert, values, 'immutable_projection_or_pointer'),
            ('update under the gate', ["SET LOCAL db_sync_alt.projection_write='on'", 'SELECT pg_advisory_xact_lock(36,2)'],
             "UPDATE chain.integrity_checkpoints SET root=repeat('0',64)", (), 'immutable_integrity_checkpoint'),
            ('delete under the gate', ["SET LOCAL db_sync_alt.projection_write='on'", 'SELECT pg_advisory_xact_lock(36,2)'],
             'DELETE FROM chain.integrity_checkpoints', (), 'immutable_integrity_checkpoint'),
            ('truncate', [], 'TRUNCATE chain.integrity_checkpoints', (), 'immutable_storage'),
        ]
        for name, setup, statement, args, code in attempts:
            with self.subTest(name), self.assertRaisesRegex(psycopg.errors.CheckViolation, code):
                with store.atomic():
                    for line in setup:
                        store.db.execute(line)
                    store.db.execute(statement, args)
        self.assertEqual(self.checkpoints(store), [(1, 0)])
        self.assertEqual(store.integrity()['base'], 1)

    def test_write_checkpoint_needs_a_current_capability(self):
        dsn, store = self.history(1)
        cap = store.certify_checkpoint()
        self.assertEqual((cap.generation, store.verified_checkpoint()), (1, cap))
        with self.assertRaisesRegex(Refusal, 'checkpoint_uncertified'):
            store.write_checkpoint(None)
        forged = incremental.VerifiedCheckpoint(store, cap._session, cap.generation, cap.revision, cap.root, cap.report)
        with self.assertRaisesRegex(Refusal, 'checkpoint_uncertified'):
            store.write_checkpoint(forged)
        # A publication after the verification makes it stale.
        self.follow(store, dsn, OFFSET + 12)
        with self.assertRaisesRegex(Refusal, 'checkpoint_uncertified'):
            store.write_checkpoint(cap)
        self.assertEqual((store.integrity()['base'], self.checkpoints(store)), (0, []))
        # A current capability writes once; the same generation again is not after the base.
        cap = store.certify_checkpoint()
        self.assertEqual(store.write_checkpoint(cap), {'generation': 2, 'previous_base': 0})
        with self.assertRaisesRegex(Refusal, 'checkpoint_not_after_base'):
            store.write_checkpoint(cap)
        self.follow(store, dsn, OFFSET + 13)
        cap = store.certify_checkpoint()
        store.mark_conflict()
        with self.assertRaisesRegex(Refusal, 'checkpoint_database_revoked'):
            store.write_checkpoint(cap)
        self.assertEqual(self.checkpoints(store), [(2, 0)])

    def test_checkpoint_and_base_move_commit_together(self):
        dsn, store = self.history(1)
        cap = store.certify_checkpoint()
        admin = psycopg.connect(dsn, autocommit=True); self.addCleanup(admin.close)
        admin.execute('''CREATE FUNCTION public.fail_base_move() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.base_generation<>OLD.base_generation THEN RAISE EXCEPTION 'injected_base_move_failure'; END IF; RETURN NEW; END $$''')
        admin.execute('CREATE TRIGGER fail_base_move BEFORE UPDATE ON chain.integrity_state FOR EACH ROW EXECUTE FUNCTION public.fail_base_move()')
        with self.assertRaisesRegex(psycopg.errors.RaiseException, 'injected_base_move_failure'):
            store.write_checkpoint(cap)
        self.assertEqual((store.integrity()['base'], self.checkpoints(store)), (0, []))
        admin.execute('DROP TRIGGER fail_base_move ON chain.integrity_state')
        admin.execute('DROP FUNCTION public.fail_base_move()')
        self.assertEqual(store.write_checkpoint(cap), {'generation': 1, 'previous_base': 0})
        self.assertEqual((store.integrity()['base'], self.checkpoints(store)), (1, [(1, 0)]))

    def test_startup_walks_only_the_generations_after_the_checkpoint(self):
        dsn, store = self.history(1)
        self.assertEqual(store.integrity(), {'base': 0, 'checkpointed_at': None})
        # A base without a checkpoint verifies from generation 0's own digests and walks every transition.
        walk = Walk(); store.progress_callback = walk
        report = verify_store(store)
        store.progress_callback = None
        self.assertEqual((walk.generations, report['digests']), ({1}, store.digests(1)))
        store, walked = self.startup(dsn, store)
        self.assertEqual(walked, {1})
        self.assertEqual(self.checkpoints(store), [(1, 0)])
        integrity = store.integrity()
        self.assertEqual(integrity['base'], 1)
        self.assertLess(abs(integrity['checkpointed_at'] - time.time()), 120)
        store, walked = self.startup(dsn, store)
        self.assertEqual(walked, set())
        self.assertEqual(self.checkpoints(store), [(1, 0)])
        for n in range(3):
            self.follow(store, dsn, OFFSET + 12 + n)
        store, walked = self.startup(dsn, store)
        self.assertEqual(walked, {2, 3, 4})
        self.assertEqual(self.checkpoints(store), [(1, 0), (4, 1)])
        self.assertEqual(store.integrity()['base'], 4)
        self.assertEqual(verify_store(store)['digests'], store.digests(4))
        for generation in (0, 1, 3):
            with self.subTest(generation), self.assertRaisesRegex(Refusal, 'generation_before_integrity_base'):
                verify_store(store, generation)

    def test_changes_at_or_below_the_base_are_refused_at_startup(self):
        dsn, store = self.history(2)
        store, _ = self.startup(dsn, store)
        self.assertEqual(self.checkpoints(store), [(2, 0)])
        store.close()
        admin = psycopg.connect(dsn, autocommit=True); self.addCleanup(admin.close)
        key = admin.execute('SELECT tx_hash,output_index,address FROM chain.outputs WHERE first_generation=0 ORDER BY tx_hash LIMIT 1').fetchone()
        digests, root = admin.execute('SELECT digests,root FROM chain.integrity_checkpoints WHERE generation=2').fetchone()
        cases = [
            ('fact below the base', 'checkpoint_digest_mismatch',
             ("UPDATE chain.outputs SET address='changed' WHERE tx_hash=%s AND output_index=%s", key[:2]),
             ('UPDATE chain.outputs SET address=%s WHERE tx_hash=%s AND output_index=%s', (key[2], *key[:2]))),
            ('checkpoint digests', 'checkpoint_digest_mismatch',
             ("UPDATE chain.integrity_checkpoints SET digests=jsonb_set(digests,'{outputs,count}','999')", ()),
             ('UPDATE chain.integrity_checkpoints SET digests=%s', (psycopg.types.json.Jsonb(digests),))),
            ('checkpoint root', 'checkpoint_root_mismatch',
             ("UPDATE chain.integrity_checkpoints SET root=repeat('0',64)", ()),
             ('UPDATE chain.integrity_checkpoints SET root=%s', (root,))),
            ('checkpoint previous base', 'checkpoint_chain_mismatch',
             ('UPDATE chain.integrity_checkpoints SET previous_base=1', ()),
             ('UPDATE chain.integrity_checkpoints SET previous_base=0', ())),
        ]
        for name, code, change, restore in cases:
            with self.subTest(name):
                with admin.transaction():
                    admin.execute("SET LOCAL session_replication_role='replica'")
                    admin.execute(*change)
                try:
                    with self.assertRaisesRegex(Refusal, code):
                        self.startup(dsn)
                finally:
                    with admin.transaction():
                        admin.execute("SET LOCAL session_replication_role='replica'")
                        admin.execute(*restore)
                    for pid, in admin.execute("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()").fetchall():
                        admin.execute('SELECT pg_terminate_backend(%s)', (pid,))
        store, walked = self.startup(dsn)
        self.assertEqual((walked, self.checkpoints(store)), (set(), [(2, 0)]))

    def driver(self, store, dsn, clock):
        """A sync driver kept across steps, with the minimum checkpoint interval and a test clock."""
        routes = chain_routes(); health = self.state(); client = real.AccountedFake(routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn); self.addCleanup(pollstore.close)
        poller = HeadPoller(lambda: pollstore, lambda: client, health, threading.Event(), network=PREPROD)
        driver = SyncDriver(SimpleNamespace(sync={'integrity_checkpoint_interval_seconds': 600}), client=client,
                            profile=self.profile, clock=clock)
        driver._poller = poller

        def step(target=None, stop=None):
            poller.tick(pollstore, client)
            driver.fixed_target = target
            return driver.run_once(store, health, stop or threading.Event())
        return driver, health, step

    def test_periodic_checkpoint_between_publications(self):
        dsn, store = self.history(0)
        store, _ = self.startup(dsn, store)  # base 0 is generation 0 itself: no checkpoint row
        now = [time.time()]
        driver, health, step = self.driver(store, dsn, lambda: now[0])
        step(OFFSET + 11)
        self.assertEqual((store.generation()['id'], store.integrity()['base']), (1, 0))
        now[0] += 601
        seen = []
        store.progress_callback = lambda phase, **fields: seen.append(health.snapshot()['sync'])
        self.assertIs(step(), False)
        store.progress_callback = None
        self.assertIn('checkpointing', seen)
        self.assertEqual(self.checkpoints(store), [(1, 0)])
        self.assertEqual(health.snapshot()['integrity'], store.integrity())
        self.assertEqual(health.snapshot()['sync'], 'syncing')
        # Publication resumes; the next checkpoint is due an interval after this one.
        step(OFFSET + 12)
        self.assertEqual((store.generation()['id'], self.checkpoints(store)), (2, [(1, 0)]))
        # A pending generation is finished first: no checkpoint while one is pending.
        original = PgHooks.before_publish
        stop = threading.Event()

        def interrupt(hooks):
            stop.set(); original(hooks)
        with patch.object(PgHooks, 'before_publish', interrupt):
            step(OFFSET + 13, stop)
        self.assertIsNotNone(store.pending())
        now[0] += 10000
        step(OFFSET + 13)
        self.assertEqual((store.generation()['id'], self.checkpoints(store)), (3, [(1, 0)]))
        step()
        self.assertEqual(self.checkpoints(store), [(1, 0), (3, 1)])
        # Nothing new since the last checkpoint: nothing to verify.
        now[0] += 10000
        with patch('db_sync_alt.verify.store.verify_store', side_effect=AssertionError('verified without a new generation')):
            self.assertIs(driver._checkpoint(store, health), False)
        # A revoked database writes no checkpoint.
        step(OFFSET + 14)
        now[0] += 10000
        store.mark_conflict()
        self.assertIs(driver._checkpoint(store, health), False)
        self.assertEqual(self.checkpoints(store), [(1, 0), (3, 1)])


if __name__ == '__main__':
    unittest.main(verbosity=2)
