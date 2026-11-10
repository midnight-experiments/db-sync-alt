# SPDX-License-Identifier: Apache-2.0
"""Bridge readiness for midnight-node 2.1.101 against PostgreSQL, with no live provider.

The transfer scenario (`fixtures.bridge`) is acquired from fake Blockfrost routes; node 2.1.101's
six-bind bridge query, exactly as its builder formats it, runs through the restricted reader and
is compared with the same query on a cardano-db-sync-shaped reference built from the same chain,
in both tx_in modes. Also: request accounting, the coverage start, the checkpoint report, and the
refusals that keep coverage complete.
"""
import contextlib
import io
import json
import os
from pathlib import Path
import threading
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_sync_alt.bridge import parse_checkpoint, recipient
from db_sync_alt.contract import Refusal, digest
from db_sync_alt.sources.blockfrost import adapter, metadata
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.reader import grant_reader
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
from fixtures import bridge
from fixtures.blockfrost import CountingProvider, Fake, FixtureHooks
import fixtures.database as real
from fixtures.networks import PREPROD

from conformance import supplement
from conformance.client import QueryError, TypedClient

TYPES = ['text', 'text', 'bytea', 'bytea', 'int8', 'int4']
SCHEMA = Path(__file__).resolve().parents[1] / 'fixtures' / 'data' / 'dbsync_schema.sql'
OFFSET, TARGET = bridge.OFFSET, bridge.TARGET
START = OFFSET + 11  # coverage start: the block after generation 0's target


def metadata_calls(calls):
    return [c for c in calls if c.startswith('/txs/') and c.endswith('/metadata')]


class BridgeMetadataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        cls.chain = bridge.scenario()
        cls.routes = bridge.routes(cls.chain)
        cls.txs = [t for h in sorted(cls.chain) for t in cls.chain[h]]

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()

    def setUp(self):
        self.names, self.roles, self.clients = [], [], []

    def tearDown(self):
        for client in self.clients:
            client.close()
        for name in self.names:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        for role in self.roles:
            self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))

    def database(self):
        name = 'db_sync_alt_test_' + uuid.uuid4().hex[:12]
        self.names.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        return make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)

    def client(self, dsn, role=None):
        client = TypedClient(make_conninfo(dsn, user=role) if role else dsn, timeout=20).connect()
        self.clients.append(client)
        return client

    def ready(self):
        """Generation 0 at schema 5, before any bridge-covered update; returns (dsn, open store)."""
        dsn = self.database()
        store = bridge.baseline(dsn)
        self.addCleanup(store.close)
        upgrade_store(store)
        return dsn, store

    def acquired(self, fetch=None):
        """The scenario acquired by the full Blockfrost update; returns (dsn, store, provider calls)."""
        dsn, store = self.ready()
        fake = Fake(self.routes)
        with contextlib.ExitStack() as stack:
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            if fetch is not None:
                stack.enter_context(patch.object(metadata, 'fetch', fetch))
            adapter.update(store, CountingProvider(500, fake), bridge.PROFILE, TARGET, hooks=FixtureHooks())
        self.assertEqual(store.generation()['target_height'], TARGET)
        return dsn, store, fake.calls

    def reference(self):
        """The same chain in a cardano-db-sync-shaped database (db-sync 13.8's metadata rendering)."""
        dsn = self.database()
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute(SCHEMA.read_text())
            bridge.load_reference(db, bridge.db_sync_rows(self.chain))
        return dsn

    def bridge_rows(self, client, mode='Enabled', asset=None):
        values = supplement.bridge_values(bridge.PROFILE, TARGET)
        if asset is not None:
            values[2:4] = list(asset)
        return client.query(supplement.bridge_query(mode, checkpoint=START), TYPES, values).rows

    def test_bridge_query_through_the_reader_equals_the_db_sync_reference(self):
        dsn, store, _ = self.acquired()
        verify_store(store)
        role = 'node_reader_' + uuid.uuid4().hex[:12]
        self.roles.append(role)
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT').format(sql.Identifier(role)))
        grant_reader(store.db, role)
        node = self.client(dsn, role)
        reference = self.client(self.reference())

        # Stored facts: one row per transaction with an output at the ICS address, nothing else.
        ics = {t.hash for t in self.txs if t.ics_producing}
        facts = {bytes(h): (present, value) for h, present, value in store.db.execute('SELECT tx_hash,present,json FROM chain.tx_metadata')}
        self.assertEqual(set(facts), ics)
        t11, t12 = self.chain[11], self.chain[12]
        self.assertEqual(facts[t11[3].hash], (0, None))
        self.assertEqual(facts[t11[4].hash], (1, None))
        self.assertEqual(facts[t12[0].hash], (1, 1152921504606847000))
        self.assertEqual(facts[t12[1].hash], (1, None))
        self.assertEqual(facts[t12[2].hash], (0, None))
        self.assertEqual(store.db.execute('SELECT count(*) FROM serve.tx_metadata').fetchone()[0], sum(p for p, _ in facts.values()))
        # Every asset of an ICS output stored by a covered generation, the one outside the profile too.
        self.assertEqual(store.db.execute('SELECT count(*) FROM chain.output_assets WHERE policy=%s AND name=%s', bridge.OTHER).fetchone()[0], 1)

        for asset in (None, bridge.OTHER):
            with self.subTest(asset=asset):
                served = self.bridge_rows(node, asset=asset)
                expected = self.bridge_rows(reference, asset=asset)
                # The node selects tx_in mode on both; db-sync answers the same in consumed mode.
                self.assertEqual(self.bridge_rows(reference, 'Consumed', asset), expected)
                self.assertEqual([r['tx_hash'] for r in served], [r['tx_hash'] for r in expected])
                self.assertTrue(served)
                for got, want in zip(served, expected):
                    h = bytes(got['tx_hash'])
                    same = {k: v for k, v in got.items() if k != 'c2m_metadata'}
                    self.assertEqual(same, {k: v for k, v in want.items() if k != 'c2m_metadata'}, h.hex())
                    # The node reads only the recipient: identical everywhere.
                    self.assertEqual(recipient(got['c2m_metadata']), recipient(want['c2m_metadata']), h.hex())
                    if h in (t12[0].hash, t12[1].hash):
                        # Blockfrost's rendering (Q1): 2^60 rounded, a control character NULL.
                        self.assertNotEqual(got['c2m_metadata'], want['c2m_metadata'])
                    else:
                        self.assertEqual(got['c2m_metadata'], want['c2m_metadata'], h.hex())
        rows = {bytes(r['tx_hash']): r for r in self.bridge_rows(node)}
        self.assertEqual(recipient(rows[t11[0].hash]['c2m_metadata']), bridge.RECIPIENTS[0])
        self.assertEqual(recipient(rows[t11[1].hash]['c2m_metadata']), bridge.RECIPIENTS[1][:20])
        self.assertEqual({k: int(rows[t12[3].hash][k]) for k in ('bridge_in', 'bridge_out')}, {'bridge_in': 100, 'bridge_out': 120})
        self.assertEqual({k: int(rows[t12[2].hash][k]) for k in ('bridge_out', 'reserve_in', 'reserve_out')},
                         {'bridge_out': 100, 'reserve_in': 1000, 'reserve_out': 900})
        self.assertNotIn(t11[5].hash, rows)

        # The reader reads tx_metadata and still nothing internal; the node's tx_in probes are unchanged.
        self.assertEqual(len(node.query('SELECT id, key, tx_id, json FROM tx_metadata', [], []).rows), sum(p for p, _ in facts.values()))
        self.assertEqual(node.query("select count(*) from information_schema.tables where table_name = 'tx_in'", [], []).rows, [{'count': 1}])
        self.assertEqual(node.query('SELECT EXISTS (SELECT 1 FROM tx_in)', [], []).rows, [{'exists': True}])
        for denied in ('SELECT * FROM chain.tx_metadata', 'SELECT * FROM serve.tx_metadata', 'SELECT * FROM chain.generations'):
            with self.subTest(denied=denied), self.assertRaises(QueryError) as caught:
                node.query(denied, [], [])
            self.assertEqual(caught.exception.sqlstate, '42501')
        # Like every node-facing relation, the bridge query refuses while the database does not serve.
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            db.execute('UPDATE chain.service_state SET revoked=true WHERE id=1')
        with self.assertRaises(QueryError) as caught:
            self.bridge_rows(node)
        self.assertEqual(caught.exception.sqlstate, '55000')

    def test_one_metadata_request_per_transaction_with_an_ics_output(self):
        _, store, calls = self.acquired()
        ics = [t for t in self.txs if t.ics_producing]
        self.assertEqual(len(ics), 10)
        self.assertEqual(sorted(metadata_calls(calls)), sorted('/txs/' + t.hash.hex() + '/metadata' for t in ics))
        # The same acquisition with the metadata supplied without a request: every other request is unchanged.
        def offline(provider, decoded):
            present, value = metadata.provider_metadatum(self.routes['/txs/' + decoded['hash'].hex() + '/metadata'])
            metadata.cross_check(decoded, present, value)
            return metadata.fact(decoded['hash'], present, value)
        _, other, baseline_calls = self.acquired(fetch=offline)
        self.assertEqual(metadata_calls(baseline_calls), [])
        self.assertEqual([c for c in calls if c not in metadata_calls(calls)], baseline_calls)
        self.assertEqual(len(calls), len(baseline_calls) + len(ics))
        self.assertEqual(other.digests(other.generation()['id']), store.digests(store.generation()['id']))

    def test_follow_stores_the_same_bridge_facts_as_the_full_update(self):
        _, reference, _ = self.acquired()
        dsn, store = self.ready()
        store.certify_ready_full()
        health = real.SyncFixture.state(self); stop = threading.Event(); client = real.AccountedFake(self.routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=bridge.PROFILE)
            driver._poller = poller
            driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        g = store.generation()
        self.assertEqual((g['target_height'], g['manifest']['capabilities']['bridge_metadata_v1']), (TARGET, True))
        self.assertEqual(store.digests(g['id']), reference.digests(reference.generation()['id']))
        self.assertEqual(len(metadata_calls(client.calls)), 10)
        verify_store(store)

    def test_coverage_start_and_checkpoint_report(self):
        dsn, store = self.ready()
        none = {'metadata_key': 6500973, 'coverage_start': START, 'checkpoint': None, 'checkpoint_supported': None, 'reason': None}
        # A store bootstrapped from cardano-db-sync has no metadata: coverage starts after its ready target.
        self.assertEqual(store.status()['bridge'], none)
        dsn, store, _ = self.acquired()
        self.assertEqual(store.status()['bridge'], none)
        t11 = self.chain[11]
        original = bytes.fromhex(bridge.base.fixtures()[1][1])
        cases = [('block:%d' % (START - 1), True, None), ('block:%d' % START, True, None), ('block:%d' % (START - 2), False, 'before_coverage'),
                 ('tx:' + t11[0].hash.hex(), True, None), ('tx:' + original.hex(), False, 'before_coverage'),
                 ('tx:' + 'ab' * 32, False, 'transaction_not_served')]
        for text, supported, reason in cases:
            with self.subTest(checkpoint=text):
                self.assertEqual(store.status(parse_checkpoint(text))['bridge'],
                                 dict(none, checkpoint=parse_checkpoint(text), checkpoint_supported=supported, reason=reason))
        # The status command takes the checkpoint as text.
        from db_sync_alt.bridge import report
        self.assertEqual(report(store, 'block:%d' % (START - 2))['reason'], 'before_coverage')

    def test_coverage_never_ends_and_stays_complete(self):
        dsn, store, _ = self.acquired()
        g = store.generation()
        # A cardano-db-sync acquisition cannot extend a covered lineage.
        manifest = {'source_identity': {'adapter': 'db-sync-direct-pg-v1'}}
        with self.assertRaisesRegex(Refusal, 'bridge_metadata_source_missing'):
            store.begin_generation(source='cardano-db-sync-postgresql', start=bridge.PROFILE.start, target=TARGET + 1,
                                   witness=TARGET + 1 + bridge.PROFILE.security, manifest=manifest, parent=g['id'])
        t11 = self.chain[11]
        # Missing or extra bridge metadata is found by verification.
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            row = db.execute('DELETE FROM chain.tx_metadata WHERE tx_hash=%s RETURNING *', (t11[3].hash,)).fetchone()
        with self.assertRaisesRegex(Refusal, 'bridge_metadata_incomplete'):
            verify_store(store)
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            db.execute('INSERT INTO chain.tx_metadata VALUES(%s,%s,%s,%s)', row)
            db.execute('INSERT INTO chain.tx_metadata VALUES(%s,0,NULL,%s)', (t11[5].hash, g['id']))
        with self.assertRaisesRegex(Refusal, 'bridge_metadata_unexpected'):
            verify_store(store)
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            db.execute('DELETE FROM chain.tx_metadata WHERE tx_hash=%s', (t11[5].hash,))
            # An asset outside the profile is kept only at an ICS or reserve output.
            db.execute('INSERT INTO chain.output_assets(tx_hash,output_index,policy,name,quantity,first_generation,ordinal) VALUES(%s,1,%s,%s,1,%s,0)',
                       (t11[0].hash, *bridge.OTHER, g['id']))
            db.execute("INSERT INTO chain.outputs(tx_hash,output_index,address,value,first_generation) VALUES(%s,1,'addr_test1change',1000000,%s)",
                       (t11[0].hash, g['id']))
        with self.assertRaisesRegex(Refusal, 'selected_asset_outside_profile'):
            verify_store(store)

    def test_the_provider_must_agree_with_the_transaction_cbor(self):
        t = self.chain[11][0]
        for name, answer in (('other recipient', [{'label': '6500973', 'json_metadata': ['0x' + 'cd' * 32]}]),
                             ('label missing', []), ('malformed', {'label': '6500973'})):
            with self.subTest(case=name):
                routes = dict(self.routes)
                routes['/txs/' + t.hash.hex() + '/metadata'] = answer
                dsn, store = self.ready()
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(Refusal, 'provider_metadata_conflict|provider_bad_metadata'):
                    adapter.update(store, CountingProvider(500, Fake(routes)), bridge.PROFILE, TARGET, hooks=FixtureHooks())
                self.assertEqual(store.generation()['id'], 0)
                self.assertFalse(store.db.execute('SELECT 1 FROM chain.tx_metadata').fetchone())


if __name__ == '__main__':
    unittest.main()
