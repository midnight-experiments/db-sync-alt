"""Network profiles on real PostgreSQL: per-network contracts, a Preview build from block 0 with k = 432, mismatch refusals.

The Preview db-sync source is a small synthetic db-sync-shaped database; no provider or live source is used.
"""
import copy
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
import uuid

import psycopg
from psycopg import sql
from db_sync_alt import networks
from db_sync_alt.contract import Profile, Refusal, digest
from db_sync_alt.resources import profile_dir
from db_sync_alt.store.database import PgStore
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource, acquire
from db_sync_alt.verify.completion import validate_completion
from db_sync_alt.verify.store import verify_store

ROOT = Path(__file__).resolve().parents[2]
H = lambda n: hashlib.sha256(str(n).encode()).digest()
ATTEST = {'dbsync_version': '13.7.1.0', 'tx_in_retained': True, 'epoch_stake_retained': True, 'operator_attested': True}
PREVIEW = networks.network_profile('preview')
K = PREVIEW.security
T = 3
W = T + K


class Databases:
    """Owned databases on the test server, reachable as libpq services; dropped with their roles."""

    def __init__(self):
        self.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        self.tmp = tempfile.TemporaryDirectory()
        self.servicefile = Path(self.tmp.name) / 'pg_service.conf'
        self.old = os.environ.get('PGSERVICEFILE')
        os.environ['PGSERVICEFILE'] = str(self.servicefile)
        self.names = []

    def create(self):
        name = 'db_sync_alt_test_net_' + uuid.uuid4().hex[:10]
        self.names.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        dsn = psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)
        with self.servicefile.open('a') as out:
            out.write('[' + name + ']\n' + ''.join(k + '=' + v + '\n' for k, v in psycopg.conninfo.conninfo_to_dict(dsn).items()) + '\n')
        return dsn, name

    def close(self):
        for name in self.names:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        self.admin.close()
        if self.old is None:
            os.environ.pop('PGSERVICEFILE', None)
        else:
            os.environ['PGSERVICEFILE'] = self.old
        self.tmp.cleanup()


def fill_preview(c, profile, network_name='preview'):
    """A Preview-shaped db-sync: blocks 0..W (epoch 0 = blocks 0-1), events from the start cursor 0:0."""
    c.execute((ROOT / 'tests/fixtures/data/dbsync_schema.sql').read_text())
    c.execute("ALTER TABLE block ADD COLUMN previous_id bigint; ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint;"
              "CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);"
              "CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL);CREATE TABLE meta(network_name varchar NOT NULL)")
    c.execute('INSERT INTO meta VALUES(%s)', (network_name,))
    with c.transaction():
        for h in range(0, W + 1):
            c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,%s,%s,'2026-01-01',10,%s)",
                      (h + 1, H(h), h, 0 if h <= 1 else 1, h, h if h else None))
        for n, height, index in [(1, 0, 0), (2, 1, 0), (3, 2, 0), (4, 3, 0)]:
            c.execute('INSERT INTO tx VALUES(%s,%s,%s,%s,true)', (n, H('tx' + str(n)), height + 1, index))
        c.execute('INSERT INTO multi_asset VALUES(1,%s,%s)', (bytes.fromhex(profile.night[:56]), bytes.fromhex(profile.night[56:])))
        c.execute('INSERT INTO datum VALUES(1,%s,%s)', (H('datum'), '{"int":7}'))
        for oid, tx, address, datum in [(1, 1, profile.mapping, H('datum')), (2, 2, 'holder', None), (3, 4, 'other', None)]:
            c.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(%s,%s,0,%s,2000000,%s)', (oid, tx, address, datum))
        c.execute('INSERT INTO ma_tx_out VALUES(1,1000,2,1)')
        c.execute('INSERT INTO tx_in VALUES(1,3,2,0); INSERT INTO ma_tx_mint VALUES(1,1000,2,1)')
        c.execute('INSERT INTO pool_hash VALUES(1,%s)', (b'p' * 28,))
        for e in (0, 1):
            c.execute('INSERT INTO epoch_stake_progress VALUES(%s,true)', (e,))
            c.execute('INSERT INTO epoch_param VALUES(%s,%s,%s)', (e, H('epoch' + str(e)), 12000 + e))
            c.execute('INSERT INTO epoch_stake VALUES(%s,1,123456,%s)', (e + 1, e))


class NetworkStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dbs = Databases()
        cls.preview = Profile(profile_dir('preview'))
        cls.source_dsn, cls.source_service = cls.dbs.create()
        with psycopg.connect(cls.source_dsn, autocommit=True) as c:
            fill_preview(c, cls.preview)
        cls.wrong_dsn, cls.wrong_service = cls.dbs.create()
        with psycopg.connect(cls.wrong_dsn, autocommit=True) as c:
            fill_preview(c, cls.preview, network_name='preprod')

    @classmethod
    def tearDownClass(cls):
        cls.dbs.close()

    def store(self, network, **kwargs):
        dsn, _ = self.dbs.create()
        return dsn, PgStore(conninfo=dsn, create=True, network=network, **kwargs)

    def test_each_network_stores_its_contract_and_reopens_as_that_network(self):
        for name in networks.names():
            with self.subTest(network=name):
                profile = networks.network_profile(name)
                dsn, store = self.store(name)
                with store:
                    self.assertIs(store.network, profile)
                    self.assertEqual(digest(store.meta('contract')), digest(profile.contract))
                    self.assertEqual(store.meta('contract')['security'], profile.security)
                with PgStore(conninfo=dsn, allow_incomplete=True) as reopened:
                    self.assertIs(reopened.network, profile)
                for other in networks.names():
                    if other != name:
                        with self.assertRaisesRegex(Refusal, 'network_mismatch'):
                            PgStore(conninfo=dsn, allow_incomplete=True, network=other)
        dsn, _ = self.dbs.create()
        with self.assertRaisesRegex(Refusal, 'network_required'):
            PgStore(conninfo=dsn, create=True)

    def test_preview_build_from_block_zero_with_k_432(self):
        dsn, store = self.store('preview', allow_incomplete=True)
        with store:
            source = CountedSQLSource(self.source_service, self.preview, copy.deepcopy(ATTEST))
            result = acquire(source, store, self.preview, target_height=T)
            self.assertEqual((result['target'], result['witness']), (T, T + 432))
            g = store.generation()
            self.assertEqual((g['start_height'], g['start_index'], g['target_height'], g['witness_height']), (0, 0, T, T + 432))
            m = g['manifest']
            self.assertEqual(m['required_epochs'], [0, 1])
            self.assertEqual(m['contracts'], digest(PREVIEW.contract))
            self.assertEqual(m['source_identity']['network'], 'preview')
            self.assertEqual(m['identity'], self.preview.identity)
            # No history before block 0: no prerequisites checkpoint and no prerequisites range in the receipt.
            self.assertFalse(store.checkpoint_rows(generation=0, phase='prerequisites'))
            self.assertNotIn('prerequisites', [r[0] for r in m['validation']['independent']['ranges']])
            self.assertEqual(result['digests']['inputs']['count'], 1)
            self.assertEqual(result['digests']['mints']['count'], 1)
            self.assertEqual(result['digests']['epoch_nonces']['count'], 2)
            report = verify_store(store)
            self.assertEqual(report['generation'], 0)
            self.assertIsNotNone(validate_completion(store, g, report['digests'], require_seal=True))
            self.assertEqual(store.block_at(0, 0)['hash'], H(0))
            self.assertEqual(store.block_at(0, W)['hash'], H(W))
            self.assertIsNone(store.block_at(0, W + 1))
        with PgStore(conninfo=dsn, readonly=True, network='preview') as store:
            verify_store(store)

    def test_preview_verifier_refuses_a_2160_witness_and_another_start(self):
        dsn, store = self.store('preview', allow_incomplete=True)
        with store:
            store.set_meta('profile', self.preview.identity)
            manifest = {'identity': self.preview.identity, 'contracts': digest(PREVIEW.contract),
                        'capabilities': {'bridge_qualified': False, 'candidates_empty': True, 'epoch_max_tx_size_v1': True},
                        'source_identity': {'adapter': 'db-sync-direct-pg-v1'}}
            with self.assertRaisesRegex(Refusal, 'invalid_bounds'):
                store.begin_generation(source='cardano-db-sync-postgresql', start=(0, 0), target=T, witness=T + 2160, manifest=manifest)
            gen = store.begin_generation(source='cardano-db-sync-postgresql', start=(0, 0), target=T, witness=T + 432, manifest=manifest)
            # A generation row that bypassed the store's check is still refused by the verifier.
            store.db.execute('SET session_replication_role=replica')
            store.db.execute('UPDATE chain.generations SET witness_height=%s WHERE id=%s', (T + 2160, gen))
            with self.assertRaisesRegex(Refusal, 'invalid_witness'):
                verify_store(store, gen, require_ready=False)
            store.db.execute('UPDATE chain.generations SET witness_height=%s,start_height=1 WHERE id=%s', (T + 432, gen))
            with self.assertRaisesRegex(Refusal, 'invalid_start_cursor'):
                verify_store(store, gen, require_ready=False)
            store.db.execute('SET session_replication_role=origin')

    def test_mismatched_source_profile_and_store_refuse(self):
        _, store = self.store('preview', allow_incomplete=True)
        with store:
            wrong = CountedSQLSource(self.wrong_service, self.preview, copy.deepcopy(ATTEST))
            with self.assertRaisesRegex(Refusal, 'source_network_mismatch'):
                acquire(wrong, store, self.preview, target_height=T)
            preprod = Profile(profile_dir('preprod'))
            with self.assertRaisesRegex(Refusal, 'network_mismatch'):
                acquire(CountedSQLSource(self.source_service, preprod, copy.deepcopy(ATTEST)), store, preprod, target_height=T)
            self.assertIsNone(store.meta('canonical_ready'))
            self.assertIsNone(store.pending())


if __name__ == '__main__':
    unittest.main()
