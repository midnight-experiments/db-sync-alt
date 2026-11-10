# SPDX-License-Identifier: Apache-2.0
"""Payments to the registered-candidates address never stop ingest, and change no committee data.

Node 2.1.101 selects committees without registrations, and db-sync-alt serves none: the address is not a
profile address, so an output there is kept only for a profile asset. Block h=12 of the Blockfrost fixture
carries three payments to it (`fixtures.candidates`); the block is acquired by the full update and followed
by the service, through db-sync-alt's own Blockfrost client and through the shared adapter. The node's
committee-selection queries, run through the reader, answer as for the same block without the payments.
"""
import contextlib
import io
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_sync_alt.sources.blockfrost import adapter, provider
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.store.reader import grant_reader
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
import fixtures.blockfrost_generation as base
from fixtures import candidates
from fixtures.blockfrost import CountingProvider, Fake, FixtureHooks
from fixtures.database import SyncFixture
from fixtures.networks import PREPROD
from integration.test_bridge_metadata import PROVIDER_KEY, RawBlockfrost, shared_root

from conformance.client import TypedClient
from conformance.statements import by_name, parameter_types

TARGET = base.OFFSET + 12
CANDIDATES = candidates.CANDIDATES
# The node's committee-selection reads per epoch (blocks, stake distribution, nonce), from the candidates
# data source; its registrations query is `get_utxos_for_address_tx_in_enabled`.
COMMITTEE = ('get_latest_block_for_epoch', 'get_stake_distribution', 'get_epoch_nonce')


class CandidatesAddressTests(SyncFixture):
    def setUp(self):
        super().setUp()
        self.roles, self.clients = [], []

    def tearDown(self):
        for client in self.clients:
            client.close()
        super().tearDown()
        for role in self.roles:
            self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))

    def ready(self, follow=False):
        """Generation 0 at schema 5; returns (dsn, open store)."""
        dsn = self.database()
        store = base.baseline(dsn)
        self.addCleanup(store.close)
        upgrade_store(store)
        if follow:
            store.certify_ready_full()
        return dsn, store

    def portable(self, routes):
        """db-sync-alt's own accounted Blockfrost client over `routes`, with a private ledger and a simulated clock."""
        ledger = self.enterContext(tempfile.TemporaryDirectory())
        now = [time.time()]
        return provider.PortableClient(PROVIDER_KEY, Path(ledger) / 'accounting', network=PREPROD, transport=RawBlockfrost(routes),
                                       clock=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + seconds))

    def shared(self, routes):
        """The shared adapter over an external accounting client answering `routes`."""
        return provider.SharedClient(str(shared_root(routes, self.enterContext(tempfile.TemporaryDirectory()))), 'preprod', network=PREPROD)

    def acquire(self, routes, client=None):
        """The full Blockfrost update to h=12; returns the open store."""
        dsn, store = self.ready()
        with contextlib.redirect_stdout(io.StringIO()):
            adapter.update(store, CountingProvider(500, client or Fake(routes)), base.PROFILE, TARGET, hooks=FixtureHooks())
        self.assertEqual(store.generation()['target_height'], TARGET)
        verify_store(store)
        return dsn, store

    def follow(self, client):
        """The service's sync driver from generation 0 to h=12; returns the open store."""
        dsn, store = self.ready(follow=True)
        health, stop = self.state(), threading.Event()
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=base.PROFILE)
            driver._poller = poller
            driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        self.assertEqual(store.generation()['target_height'], TARGET)
        verify_store(store)
        return store

    def node(self, dsn, store):
        """A connection as a reader role, as midnight-node reads the database."""
        role = 'node_reader_' + uuid.uuid4().hex[:12]
        self.roles.append(role)
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT').format(sql.Identifier(role)))
        grant_reader(store.db, role)
        client = TypedClient(make_conninfo(dsn, user=role), timeout=20).connect()
        self.clients.append(client)
        return client

    def answers(self, client, name, values):
        entry = by_name(name, part='candidates_data_source')
        return client.query(entry['sql'], parameter_types(entry), values).rows

    def committee(self, client):
        """What the node's committee selection reads, for every stored epoch."""
        out = {(name, epoch): self.answers(client, name, [epoch]) for name in COMMITTEE for epoch in range(4)}
        out['get_latest_stable_epoch'] = self.answers(client, 'get_latest_stable_epoch', [2160])
        return out

    def registrations(self, client):
        return self.answers(client, 'get_utxos_for_address_tx_in_enabled', [CANDIDATES, TARGET])

    def at_candidates(self, store):
        return [(bytes(h), i, None if d is None else bytes(d).hex())
                for h, i, d in store.db.execute('SELECT tx_hash,output_index,datum_hash FROM chain.outputs WHERE address=%s ORDER BY 1,2', (CANDIDATES,))]

    def test_acquisition_keeps_only_the_profile_asset_and_changes_no_committee_data(self):
        clean_dsn, clean = self.acquire(candidates.routes(()))
        # 1 ADA, with or without a datum and an outside asset: nothing is stored, every fact digest is the clean block's.
        _, plain = self.acquire(candidates.routes((candidates.PLAIN, candidates.WITH_DATUM)))
        self.assertEqual(plain.digests(plain.generation()['id']), clean.digests(clean.generation()['id']))
        self.assertEqual(self.at_candidates(plain), [])
        # NIGHT with a datum: the output, its transaction and its datum are kept, as at any address; no input of the wallet.
        dsn, store = self.acquire(candidates.routes())
        night = candidates.transaction(candidates.NIGHT)
        self.assertEqual(self.at_candidates(store), [(night, 0, candidates.DATUM_HASH)])
        self.assertEqual(store.db.execute('SELECT value FROM chain.datums WHERE hash=%s', (bytes.fromhex(candidates.DATUM_HASH),)).fetchone()[0],
                         candidates.DATUM_JSON)
        self.assertFalse(store.db.execute('SELECT 1 FROM chain.inputs WHERE tx_hash=%s', (night,)).fetchone())
        for index in (candidates.PLAIN, candidates.WITH_DATUM):
            self.assertFalse(store.db.execute('SELECT 1 FROM chain.transactions WHERE hash=%s', (candidates.transaction(index),)).fetchone())
        # Through the reader: the committee reads equal the clean block's; registrations are only the NIGHT output.
        node, reference = self.node(dsn, store), self.node(clean_dsn, clean)
        self.assertEqual(self.committee(node), self.committee(reference))
        self.assertEqual(self.registrations(reference), [])
        rows = self.registrations(node)
        self.assertEqual([(bytes(r['utxo_id_tx_hash']), r['utxo_id_index']) for r in rows], [(night, 0)])
        # Its transaction spends no stored output: the registrations RPCs cannot decode this (docs/limitations.md).
        self.assertEqual(rows[0]['tx_inputs'], [''])

    def test_acquisition_and_follow_on_both_adapters_store_the_same_facts(self):
        routes = candidates.routes()
        _, expected = self.acquire(routes)
        digests = expected.digests(expected.generation()['id'])
        for name, client in (('built-in', self.portable), ('shared', self.shared)):
            with self.subTest(adapter=name, path='acquisition'):
                _, store = self.acquire(routes, client(routes))
                self.assertEqual(store.digests(store.generation()['id']), digests)
            with self.subTest(adapter=name, path='follow'):
                store = self.follow(client(routes))
                self.assertEqual(store.digests(store.generation()['id']), digests)
                self.assertEqual(len(self.at_candidates(store)), 1)


if __name__ == '__main__':
    unittest.main()
