"""Stores with incremental generations for the integrity-checkpoint tests (test support only).

The fixture chain of `fixtures.blockfrost_generation`, extended so that several incremental
generations can follow generation 0 (targets OFFSET+11 to OFFSET+16). No live provider.
"""
import contextlib
import os
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from db_sync_alt.service.runtime import PostgresRuntime
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.incremental import KIND_V1, KIND_V2, incremental
from db_sync_alt.store.upgrade import upgrade_store
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


class CheckpointFixture(unittest.TestCase):
    """PostgreSQL databases with incremental histories; no test methods of its own."""
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

    def follow(self, store, dsn, target, *, kind=KIND_V2):
        """One step of the sync driver, publishing one incremental generation up to `target`.

        The generation has integrity kind `kind`: the profile bound by its digest (v2, the form the
        writer builds) or embedded in the manifest (v1)."""
        routes = chain_routes(); health = self.state(); stop = threading.Event(); client = real.AccountedFake(routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        begin = store.begin_continuation

        def embedded(**kw):
            m = dict(kw['manifest']); del m['identity_sha256']; m['identity'] = self.profile.identity
            return begin(**{**kw, 'manifest': m})
        try:
            with patch.object(store, 'begin_continuation', embedded) if kind == KIND_V1 else contextlib.nullcontext():
                poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
                poller.tick(pollstore, client)
                driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=self.profile, fixed_target=target)
                driver._poller = poller
                driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        self.assertEqual(store.generation()['target_height'], target)
        self.assertEqual(self.written_kind(store), kind)

    def written_kind(self, store):
        """The integrity kind of the canonical generation (None for a full generation)."""
        g = store.generation()
        return g['manifest']['integrity']['kind'] if incremental(g) else None

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
            result = driver.run_once(store, health, stop or threading.Event())
            self.assertIn(self.written_kind(store), (None, KIND_V2))
            return result
        return driver, health, step
