"""Direct serving to a reader role: the gate raises in every non-serving state, in fresh and cached
plans, the reader sees only the node-facing relations, and the node's catalogue checks hold."""
import datetime
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_sync_alt.backup import backup, restore
from db_sync_alt.contract import Refusal
from db_sync_alt.store import serve, upgrade
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.reader import NODE_RELATIONS, grant_reader
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
from fixtures.generations import PROFILE, START, block, h, make_store

from conformance.client import QueryError, TypedClient
from conformance.statements import parameter_types, statements

MAX = (1 << 63) - 1
NODE_INDEXES = ('idx_multi_asset_policy_name_hex', 'idx_ma_tx_out_ident', 'idx_tx_out_address', 'idx_ma_tx_out_id_ident',
                'idx_multi_asset_policy_name', 'idx_block_block_no', 'idx_tx_block_id', 'idx_tx_out_tx_id', 'idx_tx_out_data_hash',
                'idx_tx_in_tx_in_id', 'idx_tx_in_tx_out_id_tx_out_index')
CATALOG = ('pg_indexes', 'information_schema', 'pg_catalog', 'regtype')
# Statement -> state changes (as the superuser, guards bypassed) that must make every gated query raise.
STATES = {
    'revoked': 'UPDATE chain.service_state SET revoked=true,canonical_ready=NULL',
    'unready': "UPDATE chain.service_state SET migration_state='restoring'",
    'projection': 'UPDATE chain.integrity_state SET projection_generation=1',
    'identity': "UPDATE chain.service_state SET owner='another-owner'",
    'no_canonical': 'UPDATE chain.service_state SET canonical_ready=NULL',
}


def values(types, hit):
    """Arguments of one statement: `hit` spans the fixture's data, otherwise values that match nothing."""
    out, int8 = [], 0
    for kind in types:
        if kind == 'int4':
            out.append(START if hit else 7)
        elif kind == 'int8':
            out.append((MAX if int8 % 2 else 0) if hit else 1)
            int8 += 1
        elif kind == 'int2':
            out.append(0)
        elif kind == 'text':
            out.append('holder' if hit else 'nowhere')
        elif kind == 'bytea':
            out.append(block(START)['hash'] if hit else b'\x00' * 32)
        elif kind == 'bytea[]':
            out.append([b'p' * 28])
        elif kind == 'timestamp':
            out.append(datetime.datetime(2026, 1, 1) if hit else datetime.datetime(2000, 1, 1))
        elif kind == 'oid':
            out.append(23)
        else:
            raise ValueError(kind)
    return out


def gated_statements():
    """Every catalogue SELECT that reads relations (not the PostgreSQL catalogue)."""
    out = []
    for entry in statements().values():
        text = entry['sql'].lstrip().upper()
        if entry['name'] == 'driver' or not text.startswith(('SELECT', 'WITH')) or any(c in entry['sql'] for c in CATALOG):
            continue
        out.append(entry)
    return sorted(out, key=lambda e: (e['name'], e['sql']))


class NativeServingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()

    def setUp(self):
        token = uuid.uuid4().hex[:12]
        self.name, self.reader = 'db_sync_alt_test_' + token, 'node_reader_' + token
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.name)))
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT').format(sql.Identifier(self.reader)))
        self.dsn = make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=self.name)
        make_store(self.dsn)
        with PgStore(conninfo=self.dsn) as store:
            upgrade_store(store)
            self.granted = grant_reader(store.db, self.reader)
        self.reader_dsn = make_conninfo(self.dsn, user=self.reader)
        self.owner = psycopg.connect(self.dsn, autocommit=True)
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.close()
        self.owner.close()
        self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(self.name)))
        self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(self.reader)))

    def client(self, generic=False):
        client = TypedClient(self.reader_dsn, timeout=20).connect()
        if generic:
            client.simple('SET plan_cache_mode = force_generic_plan')
        self.clients.append(client)
        return client

    def outcome(self, client, entry, hit):
        types = parameter_types(entry)
        try:
            return ('rows', len(client.query(entry['sql'], types, values(types, hit)).rows))
        except QueryError as error:
            return ('error', error.sqlstate)

    def change(self, statement):
        """Apply a raw state change as the superuser, past the store's own guards."""
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            db.execute(statement)

    def test_reader_privileges_and_node_catalogue_checks(self):
        self.assertEqual(self.granted['relations'], ['public.' + n for n in NODE_RELATIONS])
        client = self.client()
        for name in NODE_INDEXES:
            rows = client.query('select * from pg_indexes where indexname = $1', ['text'], [name]).rows
            self.assertEqual(len(rows), 1, name)
        self.assertEqual(client.query("select count(*) from information_schema.tables where table_name = 'tx_in'", [], []).rows, [{'count': 1}])
        self.assertEqual(client.query('SELECT EXISTS (SELECT 1 FROM tx_in)', [], []).rows, [{'exists': True}])
        for denied in ('SELECT id, key, tx_id, json FROM tx_metadata LIMIT 1', 'SELECT * FROM chain.outputs', 'SELECT chain.gate()',
                       'SELECT * FROM chain.service_state', "UPDATE multi_asset SET name='\\x78'::bytea"):
            with self.subTest(denied=denied):
                with self.assertRaises(QueryError) as caught:
                    client.query(denied, [], [])
                self.assertEqual(caught.exception.sqlstate, '42501')
        # The gate is the one function the reader may run, and only through the views.
        functions = self.owner.execute("""SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname='chain' AND has_function_privilege(%s, p.oid, 'EXECUTE')""", (self.reader,)).fetchall()
        self.assertEqual(functions, [('chain.gate()',)])
        gate = self.owner.execute("SELECT provolatile, prosecdef, proparallel, proconfig FROM pg_proc WHERE oid='chain.gate()'::regprocedure").fetchone()
        self.assertEqual(gate, ('s', True, 'u', ['search_path=pg_catalog, chain']))
        readable = self.owner.execute("""SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND c.relkind IN ('r','v','m','p','f')
              AND has_table_privilege(%s, c.oid, 'SELECT') ORDER BY 1""", (self.reader,)).fetchall()
        self.assertEqual([r[0] for r in readable], sorted(NODE_RELATIONS))
        self.assertFalse(self.owner.execute("SELECT has_schema_privilege(%s, 'chain', 'USAGE')", (self.reader,)).fetchone()[0])
        # A db-sync-alt database is created without PUBLIC CONNECT: only the owner and granted roles connect.
        self.assertEqual(self.owner.execute("SELECT has_database_privilege('public', current_database(), 'CONNECT'), has_database_privilege(%s, current_database(), 'CONNECT')",
                                            (self.reader,)).fetchone(), (False, True))
        with PgStore(conninfo=self.dsn) as store:
            with self.assertRaises(Refusal) as caught:
                grant_reader(store.db, 'missing_reader_' + uuid.uuid4().hex[:8])
            self.assertEqual(caught.exception.code, 'reader_role_missing')
            with self.assertRaises(Refusal) as caught:
                grant_reader(store.db, store.db.info.user)
            self.assertEqual(caught.exception.code, 'invalid_reader_role')

    def test_every_gated_statement_raises_in_every_non_serving_state(self):
        entries = gated_statements()
        self.assertGreater(len(entries), 25)
        fresh, auto, generic = self.client(), self.client(), self.client(generic=True)
        ready = {}
        for entry in entries:
            for hit in (True, False):
                key = (entry['sql'], hit)
                ready[key] = self.outcome(fresh, entry, hit)
                for _ in range(6):  # past PostgreSQL's five custom plans: auto now reuses a generic plan
                    self.assertEqual(self.outcome(auto, entry, hit), ready[key])
                self.assertEqual(self.outcome(generic, entry, hit), ready[key])
        answering = {key for key, value in ready.items() if value[0] == 'rows'}
        # Only statements of features the node does not run fail while serving (missing relation).
        self.assertEqual({entry['name'] for entry in entries for hit in (True, False) if (entry['sql'], hit) not in answering},
                         {'get_stake_pool_delegations_for_pools'})
        self.assertGreater(sum(ready[key][1] for key in answering if key[1]), 10)
        original = self.owner.execute('SELECT revoked,canonical_ready,migration_state,owner FROM chain.service_state').fetchone()
        projection = self.owner.execute('SELECT projection_generation FROM chain.integrity_state').fetchone()[0]
        restore = ("UPDATE chain.service_state SET revoked=%s,canonical_ready=%s,migration_state='%s',owner='%s'" % (
            str(original[0]).lower(), original[1], original[2], original[3]))
        for state, statement in STATES.items():
            with self.subTest(state=state):
                self.change(statement)
                fresh = self.client()  # planned in the non-serving state
                for key in sorted(answering):
                    entry = next(e for e in entries if e['sql'] == key[0])
                    for label, client in (('fresh', fresh), ('auto-cached', auto), ('generic-cached', generic)):
                        self.assertEqual(self.outcome(client, entry, key[1]), ('error', '55000'), (state, label, entry['name']))
                self.change(restore)
                self.change('UPDATE chain.integrity_state SET projection_generation=%d' % projection)
                # Publication resumes: the same prepared statements answer again without reconnecting.
                for key in sorted(answering):
                    entry = next(e for e in entries if e['sql'] == key[0])
                    self.assertEqual(self.outcome(auto, entry, key[1]), ready[key])
                    self.assertEqual(self.outcome(generic, entry, key[1]), ready[key])

    def test_empty_relations_raise_instead_of_answering_when_not_serving(self):
        client = self.client(generic=True)
        exists = 'SELECT EXISTS (SELECT 1 FROM tx_in)'
        self.assertEqual(client.query(exists, [], []).rows, [{'exists': True}])
        self.change(STATES['revoked'])
        for query in (exists, 'SELECT id FROM block WHERE block_no = $1 LIMIT 1', 'SELECT nonce FROM epoch_param WHERE epoch_no = $1'):
            with self.subTest(query=query):
                with self.assertRaises(QueryError) as caught:
                    client.query(query, ['int4'] if '$1' in query else [], [99999999] if '$1' in query else [])
                self.assertEqual(caught.exception.sqlstate, '55000')


if __name__ == '__main__':
    unittest.main(verbosity=2)


class ServedTablesTests(unittest.TestCase):
    """The served tables: positional IDs, guards, the upgrade of older databases, backup and restore."""
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        cls.tmp = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()
        cls.tmp.cleanup()

    def setUp(self):
        self.names, self.roles = [], []

    def tearDown(self):
        for name in self.names:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        for role in self.roles:
            self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))

    def database(self):
        name = 'db_sync_alt_test_' + uuid.uuid4().hex[:12]
        self.names.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        return make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)

    def reader(self):
        role = 'node_reader_' + uuid.uuid4().hex[:12]
        self.roles.append(role)
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT').format(sql.Identifier(role)))
        return role

    def upgraded(self):
        dsn = self.database()
        make_store(dsn)
        with PgStore(conninfo=dsn) as store:
            result = upgrade_store(store)
        return dsn, result

    def test_positional_ids_and_exact_served_rows(self):
        dsn, result = self.upgraded()
        origin_tx, spend_tx = (START - 10) * 4096, START * 4096 + 4
        with PgStore(conninfo=dsn) as store:
            db = store.db
            self.assertEqual(result['served_rows']['block'], db.execute('SELECT count(*) FROM chain.current_membership').fetchone()[0])
            self.assertEqual(result['ordinals_from_source'], {})
            self.assertEqual(db.execute('SELECT count(*) FROM serve.block WHERE id<>block_no').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT id,block_id,block_index FROM serve.tx ORDER BY id').fetchall(),
                             [(origin_tx, START - 10, 0), (spend_tx, START, 4)])
            self.assertEqual(db.execute('SELECT id,tx_id FROM serve.tx_out ORDER BY id').fetchall(),
                             [(origin_tx * 4096, origin_tx), (spend_tx * 4096, spend_tx)])
            self.assertEqual(db.execute('SELECT id,tx_in_id,tx_out_id,tx_out_index FROM serve.tx_in').fetchall(),
                             [(spend_tx * 4096, spend_tx, origin_tx, 0)])
            self.assertEqual(db.execute('SELECT id,tx_out_id FROM serve.ma_tx_out ORDER BY id').fetchall(),
                             [(origin_tx * 4096 * 4096, origin_tx * 4096), (spend_tx * 4096 * 4096, spend_tx * 4096)])
            self.assertEqual(db.execute('SELECT id FROM serve.multi_asset').fetchall(), db.execute('SELECT id FROM chain.assets').fetchall())
            self.assertEqual(db.execute('SELECT projection_generation,serve_generation FROM chain.integrity_state').fetchone(), (2, 2))
            serve.verify(store, 2)
            # The node's index names each resolve to exactly one index, on the served tables.
            self.assertEqual(db.execute("SELECT count(*),count(*) FILTER (WHERE schemaname='serve') FROM pg_indexes WHERE indexname=ANY(%s)",
                                        (list(serve.NODE_INDEXES),)).fetchone(), (11, 11))
            # A served row that differs from the facts fails verification.
            with db.transaction():
                db.execute('SELECT pg_advisory_xact_lock(36,2)')
                db.execute("SELECT set_config('db_sync_alt.projection_write','on',true)")
                db.execute('UPDATE serve.tx_out SET value=value+1 WHERE id=%s', (spend_tx * 4096,))
                with self.assertRaisesRegex(Refusal, 'serve_rows_mismatch'):
                    serve.verify(store, 2)
                db.execute('ROLLBACK')
            serve.verify(store, 2)

    def test_served_blocks_are_verified_range_by_range(self):
        dsn, _ = self.upgraded()
        with PgStore(conninfo=dsn) as store, patch.object(serve, 'BLOCK_RANGE', 500):
            db = store.db
            low, top = db.execute('SELECT min(id),max(id) FROM serve.block').fetchone()
            self.assertGreater(top - low, 4 * 500)  # several ranges; the changes below are in the last one
            serve.verify(store, 2)
            for label, statement in (('changed', 'UPDATE serve.block SET tx_count=tx_count+1 WHERE id=%d' % top),
                                     ('missing', 'DELETE FROM serve.block WHERE id=%d' % (top - 1)),
                                     ('extra', "INSERT INTO serve.block VALUES(%d,'\\x%s',2,1,%d,now(),0)" % (top + 1, '33' * 32, top + 1))):
                with self.subTest(label=label):
                    with db.transaction():
                        db.execute('SELECT pg_advisory_xact_lock(36,2)')
                        db.execute("SELECT set_config('db_sync_alt.projection_write','on',true)")
                        db.execute(statement)
                        with self.assertRaisesRegex(Refusal, 'serve_rows_mismatch'):
                            serve.verify(store, 2)
                        db.execute('ROLLBACK')
            serve.verify(store, 2)

    def test_served_tables_change_only_inside_a_publication_and_refuse_overflow(self):
        dsn, _ = self.upgraded()
        with PgStore(conninfo=dsn) as store:
            db = store.db
            for statement in ('DELETE FROM serve.block WHERE id=%d' % START, 'UPDATE serve.tx SET valid_contract=false',
                              "INSERT INTO serve.multi_asset VALUES(999,'\\x%s','\\x')" % ('00' * 28), 'TRUNCATE serve.tx_in'):
                with self.subTest(statement=statement):
                    with self.assertRaises(psycopg.errors.CheckViolation):
                        db.execute(statement)
            for statement in ('INSERT INTO serve.tx VALUES(%d,\'\\x%s\',%d,4096,true)' % (START * 4096 + 4096, '11' * 32, START),
                              'INSERT INTO serve.block VALUES(134217728,\'\\x%s\',1,1,134217728,now(),0)' % ('22' * 32),
                              'INSERT INTO serve.tx_in VALUES(%d,%d,0,0)' % (START * 4096 * 4096 + 4096, START * 4096)):
                with self.subTest(statement=statement):
                    with self.assertRaises(psycopg.errors.CheckViolation):
                        with db.transaction():
                            db.execute('SELECT pg_advisory_xact_lock(36,2)')
                            db.execute("SELECT set_config('db_sync_alt.projection_write','on',true)")
                            db.execute(statement)
            old = store.generation()
            gen = store.begin_generation(source='blockfrost-cardano', start=PROFILE.start, target=old['target_height'] + 1,
                                         witness=old['witness_height'] + 1, manifest={}, parent=old['id'])
            row = {'tx_hash': h('spend'), 'origin_hash': h('origin'), 'origin_index': 0}
            for ordinal in (None, -1, 4096, '0'):
                with self.subTest(ordinal=ordinal):
                    with self.assertRaisesRegex(Refusal, 'invalid_ordinal'):
                        store.insert('inputs', dict(row, **({} if ordinal is None else {'ordinal': ordinal})), gen)
            with self.assertRaisesRegex(Refusal, 'immutable_fact_conflict'):
                store.insert('inputs', dict(row, ordinal=1), gen)
            self.assertFalse(store.insert('inputs', dict(row, ordinal=0), gen))

    def test_upgrade_of_a_database_without_ordinals_reads_them_from_a_source(self):
        dsn = self.database()
        make_store(dsn)
        with PgStore(conninfo=dsn) as store:
            upgrade._upgrade_v2(store, store.state())
            # A database written before ledger ordinals existed.
            for table in ('inputs', 'output_assets'):
                store.db.execute('ALTER TABLE chain.%s DROP COLUMN ordinal' % table)
            inputs = [(h('spend'), h('origin'), 0)]
            assets = sorted((key, 0, bytes.fromhex(PROFILE.night[:56]), bytes.fromhex(PROFILE.night[56:])) for key in (h('origin'), h('spend')))
            self.assertEqual(upgrade.missing_ordinals(store.db), {'inputs': inputs, 'output_assets': assets})
            with self.assertRaisesRegex(Refusal, 'ordinal_source_required'):
                upgrade_store(store)

            class Source:
                def __init__(self, found):
                    self.found, self.calls = found, []

                def ordinals(self, inputs, output_assets, anchors):
                    self.calls.append((inputs, output_assets, sorted(anchors)))
                    return self.found
            with self.assertRaisesRegex(Refusal, 'ordinal_source_incomplete'):
                upgrade_store(store, Source({'inputs': {}, 'output_assets': {k: 0 for k in assets}}))
            with self.assertRaisesRegex(Refusal, 'invalid_ordinal'):
                upgrade_store(store, Source({'inputs': {k: 4096 for k in inputs}, 'output_assets': {k: 0 for k in assets}}))
            self.assertEqual(store.state()['schema_version'], 2)
            source = Source({'inputs': {k: 3 for k in inputs}, 'output_assets': {k: 0 for k in assets}})
            result = upgrade_store(store, source)
            self.assertEqual((result['schema_version'], result['ordinals_from_source']), (4, {'inputs': 1, 'output_assets': 2}))
            self.assertEqual(source.calls, [(inputs, assets, ['start', 'target'])])
            self.assertEqual(store.db.execute('SELECT id FROM serve.tx_in').fetchall(), [((START * 4096 + 4) * 4096 + 3,)])
            self.assertEqual(upgrade.missing_ordinals(store.db), {'inputs': [], 'output_assets': []})
            verify_store(store)

    def test_backup_and_restore_carry_the_served_tables_and_grant_the_reader_again(self):
        dsn, _ = self.upgraded()
        folder = Path(self.tmp.name) / uuid.uuid4().hex
        folder.mkdir()
        archive, manifest = folder / 'serve.dump', folder / 'serve.manifest.json'
        backup(archive, manifest, conninfo=dsn)
        body = json.loads(manifest.read_text())
        self.assertEqual((body['format'], body['schema_version']), ('db-sync-alt-postgresql-backup-v3', 4))
        self.assertEqual(set(k for k in body['physical_inventory'] if k.startswith('serve.')), {'serve.' + t for t in serve.TABLES})
        role = self.reader()
        restored = self.database()
        result = restore(archive, manifest, conninfo=restored, reader_role=role)
        self.assertEqual(result['reader']['relations'], ['public.' + n for n in NODE_RELATIONS])
        with psycopg.connect(restored) as db:
            # Planner statistics are gathered before the restored database serves.
            self.assertEqual(db.execute("SELECT count(*) FROM pg_stat_user_tables WHERE schemaname='serve' AND last_analyze IS NOT NULL").fetchone()[0], len(serve.TABLES))
        with psycopg.connect(dsn) as a, psycopg.connect(restored) as b:
            for table in serve.TABLES:
                query = 'SELECT * FROM serve.%s ORDER BY %s' % (table, serve.KEYS[table])
                self.assertEqual(a.execute(query).fetchall(), b.execute(query).fetchall(), table)
        client = TypedClient(make_conninfo(restored, user=role), timeout=20).connect()
        try:
            rows = client.query('SELECT block_no, hash FROM block WHERE block_no = $1', ['int4'], [START]).rows
            self.assertEqual(rows, [{'block_no': START, 'hash': block(START)['hash']}])
        finally:
            client.close()
        # Without a reader role the restore grants nothing: privileges never travel in the archive,
        # and no role but the owner may even connect (PUBLIC has no CONNECT on a restored database).
        other, plain = self.database(), self.reader()
        restore(archive, manifest, conninfo=other)
        with psycopg.connect(other) as db:
            name = db.info.dbname
            self.assertEqual(db.execute("SELECT has_database_privilege('public', %s, 'CONNECT'), has_database_privilege(%s, %s, 'CONNECT')",
                                        (name, plain, name)).fetchone(), (False, False))
        with self.assertRaises(QueryError) as caught:
            TypedClient(make_conninfo(other, user=plain), timeout=20).connect()
        self.assertEqual(caught.exception.kind, 'connection')
        with psycopg.connect(other, autocommit=True) as db:
            grant_reader(db, plain)
        client = TypedClient(make_conninfo(other, user=plain), timeout=20).connect()
        try:
            self.assertEqual(client.query('SELECT count(*) FROM block WHERE block_no = $1', ['int4'], [START]).rows, [{'count': 1}])
        finally:
            client.close()


class PublicationInvariantTests(unittest.TestCase):
    """What the verifier requires before a generation is published, beyond its facts' integrity."""
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)

    @classmethod
    def tearDownClass(cls):
        cls.admin.close()

    def setUp(self):
        self.names = []

    def tearDown(self):
        for name in self.names:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))

    def database(self):
        name = 'db_sync_alt_test_' + uuid.uuid4().hex[:12]
        self.names.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        return make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)

    def test_every_completed_required_epoch_needs_its_end_block(self):
        def drop_end(store, generation):
            if generation == 0:
                store.db.execute("UPDATE chain.checkpoints SET receipt=receipt-'epoch_end' WHERE generation=0 AND phase='epochs' AND unit='1'")
        with self.assertRaisesRegex(Refusal, 'epoch_end_missing'):
            make_store(self.database(), before_seal=drop_end)

    def test_the_registered_candidates_address_stays_empty(self):
        candidate = PROFILE.configs['registered-candidates-addresses']['committee_candidates_address']

        def candidate_output(store, generation):
            if generation == 0:
                store.insert('outputs', {'tx_hash': h('origin'), 'output_index': 1, 'address': candidate, 'value': '1', 'datum_hash': None}, 0)
        with self.assertRaisesRegex(Refusal, 'unsupported_candidate_activity'):
            make_store(self.database(), before_seal=candidate_output)

    def test_a_served_generation_has_at_least_one_spend(self):
        dsn = self.database()
        make_store(dsn)
        with PgStore(conninfo=dsn) as store:
            upgrade_store(store)
            with store.db.transaction():
                store.db.execute('SELECT pg_advisory_xact_lock(36,2)')
                store.db.execute("SELECT set_config('db_sync_alt.projection_write','on',true)")
                store.db.execute('DELETE FROM serve.tx_in')
                with self.assertRaisesRegex(Refusal, 'serve_rows_mismatch'):
                    serve.verify(store, 2)
                with self.assertRaisesRegex(Refusal, 'no_served_spend'):
                    serve.require_spends(store.db)
                with self.assertRaisesRegex(Refusal, 'no_served_spend'):
                    serve.advance(store.db, 2, cutoff=1 << 30, bindings='SELECT height,hash FROM chain.current_membership WHERE false', bindings_args=())
                store.db.execute('ROLLBACK')
            serve.verify(store, 2)
