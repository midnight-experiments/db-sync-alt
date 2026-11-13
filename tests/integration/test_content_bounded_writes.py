"""Served-table writes stay under the store's statement limit whatever the content of a range (issue 00050).

On the real Mainnet store, ranges sized by rows held up to 90 MiB of datum values or the densest blocks, and
one was cancelled by the 30 s session limit on a slow disk. Here the limit is lowered for the build and the
publication of a small serving store that gains large datums and dense blocks: a range or a publication
statement the limit cancels is written as two halves (or in build ranges) and gives the same rows. Built
unsplit, the dense blocks' served transactions or the datums of one build range each take several times the
lowered limit in one statement (`test_the_fixture_needs_the_split`).

The independent pass's lookup in its TEMP table is checked for its bound too: a primary-key lookup whatever
the table's size, without ANALYZE (issue 00050, FR-003).
"""
import logging
import os
import unittest
from unittest import mock
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

from db_sync_alt.sources.dbsync.acquire import EXPECTED_BATCH, EXPECTED_LOOKUP, EXPECTED_TABLE
from db_sync_alt.store import serve
from db_sync_alt.store.database import PROJECTION_WRITE, SERVING_LOCK, PgStore
from db_sync_alt.store.upgrade import upgrade_store
from fixtures.generations import START, make_store

GENERATION = 2
LIMIT = '200ms'
# Dense blocks: 40 served blocks of 4,000 transactions each, in one build range of heights.
DENSE = range(START + 100, START + 140)
TXS = 4000
# Large datums: one build range of ids, about 5.4 KiB of jsonb each.
DATUMS = range(10000, 10000 + serve.BUILD_DATUMS)
FIELDS = 160
# Transactions of an old block in another build range of heights: facts a later generation stores late.
OLD = START - 100000
KEY = {'epoch_param': 'epoch_no'}


def inject(db, generation):
    """Dense blocks, large datums and a few transactions of an old block, first stored by `generation`."""
    db.execute("SET LOCAL session_replication_role='replica'")
    db.execute("INSERT INTO chain.transactions(hash,block_hash,tx_index,valid,first_generation)"
               " SELECT decode(md5(%s||m.height||'.'||i)||md5(%s||i||'.'||m.height),'hex'),m.hash,i,1,%s"
               ' FROM chain.current_membership m CROSS JOIN generate_series(0,%s::int) i WHERE m.height BETWEEN %s AND %s',
               ('dense', 'dense', generation, TXS - 1, DENSE[0], DENSE[-1]))
    db.execute("INSERT INTO chain.blocks(hash,height,parent_hash,epoch,slot,time,tx_count,first_generation)"
               " VALUES(decode(md5('old')||md5('block'),'hex'),%s,NULL,0,%s,'2026-01-01',3,%s)", (OLD, OLD * 20, generation))
    db.execute("INSERT INTO chain.transactions(hash,block_hash,tx_index,valid,first_generation)"
               " SELECT decode(md5('old'||i)||md5('tx'||i),'hex'),decode(md5('old')||md5('block'),'hex'),i,1,%s FROM generate_series(0,2) i",
               (generation,))
    db.execute("INSERT INTO chain.datums(id,hash,value,first_generation)"
               " SELECT n,decode(md5('datum'||n)||md5('hash'||n),'hex'),"
               "jsonb_build_object('constructor',0,'fields',(SELECT jsonb_agg(md5(n||'.'||f)) FROM generate_series(1,%s::int) f)),%s"
               ' FROM generate_series(%s::int,%s::int) n', (FIELDS, generation, DATUMS[0], DATUMS[-1]))
    db.execute("SET LOCAL session_replication_role='origin'")


def rows(db, physical=False):
    """Every served row in key order and, with `physical`, every served table's keys in physical order."""
    out = {}
    order = ",md5(string_agg(x.{key}::text,',' ORDER BY x.ctid))" if physical else ''
    for table in serve.tables(db):
        key = sql.Identifier(KEY.get(table, 'id'))
        out[table] = db.execute(sql.SQL("SELECT count(*),md5(string_agg(x::text,',' ORDER BY x.{key}))" + order + ' FROM serve.{table} x')
                                .format(key=key, table=sql.Identifier(table))).fetchone()
    return out


def limit(db, value):
    db.execute("SELECT set_config('statement_timeout',%s,true)", (value,))


def splits(logs, message):
    return [line for line in logs.output if message in line]


class ContentBoundedWriteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        cls.name = 'db_sync_alt_test_' + uuid.uuid4().hex[:12]
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        cls.dsn = make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=cls.name)
        make_store(cls.dsn)
        with PgStore(conninfo=cls.dsn) as store:
            upgrade_store(store)

    @classmethod
    def tearDownClass(cls):
        cls.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.name)))
        cls.admin.close()

    def setUp(self):
        self.store = PgStore(conninfo=self.dsn)
        self.addCleanup(self.store.close)
        self.db = self.store.db

    def empty(self):
        """Empty the served tables, with their guards bypassed until the transaction ends or `rebuild` restores them."""
        self.db.execute("SET LOCAL session_replication_role='replica'")
        for table in reversed(serve.tables(self.db)):
            self.db.execute(sql.SQL('TRUNCATE serve.{} CASCADE').format(sql.Identifier(table)))

    def rebuild(self):
        """Empty the served tables and build them again, as the upgrade does (rolled back by each test)."""
        self.empty()
        counts = serve.build(self.db, GENERATION)
        self.db.execute("SET LOCAL session_replication_role='origin'")
        return counts

    def publication(self):
        """The publication's own gate, as `PgStore.publish` holds it."""
        self.db.execute('SELECT pg_advisory_xact_lock(%s,%s)', SERVING_LOCK)
        self.db.execute('SELECT set_config(%s,%s,true)', (PROJECTION_WRITE, 'on'))

    def test_the_fixture_needs_the_split(self):
        """Negative control: unsplit, the dense range and the datum range are each cancelled by the lowered limit."""
        with self.db.transaction():
            inject(self.db, 0)
            self.empty()
            limit(self.db, LIMIT)
            for table, bounds in (('tx', (DENSE[0] - DENSE[0] % serve.BUILD_HEIGHTS, DENSE[0] - DENSE[0] % serve.BUILD_HEIGHTS + serve.BUILD_HEIGHTS - 1)),
                                  ('datum', (DATUMS[0], DATUMS[-1]))):
                with self.subTest(table=table), self.assertRaises(psycopg.errors.QueryCanceled):
                    with self.db.transaction():
                        serve._insert(self.db, table, *serve.built(table, GENERATION, bounds, self.db))
            # The build with splitting disabled fails as 0.4.4 did on Mainnet.
            with mock.patch.object(serve, '_fill', lambda db, table, statement, low, high: serve._insert(db, table, *statement(low, high))):
                with self.assertRaises(psycopg.errors.QueryCanceled):
                    with self.db.transaction():
                        serve.build(self.db, GENERATION)
            raise psycopg.Rollback()

    def test_build_splits_cancelled_ranges_into_the_same_rows(self):
        with self.db.transaction():
            inject(self.db, 0)
            unsplit = self.rebuild()
            expected = rows(self.db, physical=True)
            limit(self.db, LIMIT)
            with self.assertLogs('db_sync_alt.serve', logging.WARNING) as logs:
                split = self.rebuild()
            limit(self.db, '30s')
            self.assertEqual(split, unsplit)
            self.assertEqual(split['tx'], expected['tx'][0])
            self.assertGreaterEqual(split['tx'], len(DENSE) * TXS)
            self.assertGreaterEqual(split['datum'], len(DATUMS))
            for table in ('tx', 'datum'):
                self.assertTrue(splits(logs, 'serve_range_split table=' + table + ' '), table)
            self.assertEqual(rows(self.db, physical=True), expected)
            serve.verify(self.store, GENERATION)
            raise psycopg.Rollback()

    def test_a_single_value_still_cancelled_fails_the_build(self):
        """The split stops at one id: a datum no statement can write within the limit fails the build closed."""
        stuck = DATUMS[0] + 1234
        with self.db.transaction():
            self.db.execute("CREATE FUNCTION pg_temp.stall() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(1); RETURN NEW; END $$")
            self.db.execute('CREATE TRIGGER stall BEFORE INSERT ON serve.datum FOR EACH ROW WHEN (NEW.id=%s) EXECUTE FUNCTION pg_temp.stall()' % stuck)
            self.db.execute('ALTER TABLE serve.datum ENABLE ALWAYS TRIGGER stall')
            inject(self.db, 0)
            limit(self.db, LIMIT)
            with self.assertLogs('db_sync_alt.serve', logging.WARNING) as logs, self.assertRaises(psycopg.errors.QueryCanceled):
                with self.db.transaction():
                    self.rebuild()
            narrowest = dict(field.split('=') for field in splits(logs, 'serve_range_split table=datum ')[-1].split() if '=' in field)
            low, high = int(narrowest['low']), int(narrowest['high'])
            self.assertEqual(high - low, 1)
            self.assertTrue(low <= stuck <= high, (low, high))
            raise psycopg.Rollback()

    def test_publication_writes_a_dense_generation_in_ranges(self):
        """`serve.advance` of a generation whose new facts exceed the limit in one statement (a cardano-db-sync
        update onto a serving store): the same rows, from the generation's own build ranges, old ones included."""
        with self.db.transaction():
            inject(self.db, GENERATION)
            self.publication()
            with self.db.transaction():
                reference = serve.advance(self.db, GENERATION, cutoff=1 << 30, bindings='SELECT height,hash FROM chain.current_membership WHERE false', bindings_args=())
                expected = rows(self.db)
                raise psycopg.Rollback()
            limit(self.db, LIMIT)
            with self.subTest(control='one statement'), self.assertRaises(psycopg.errors.QueryCanceled):
                with self.db.transaction():
                    serve._insert(self.db, 'tx', *serve._new('tx', GENERATION, self.db))
            with self.assertLogs('db_sync_alt.serve', logging.WARNING) as logs:
                counts = serve.advance(self.db, GENERATION, cutoff=1 << 30, bindings='SELECT height,hash FROM chain.current_membership WHERE false', bindings_args=())
            limit(self.db, '30s')
            self.assertEqual(counts, reference)
            self.assertEqual(counts['tx'], len(DENSE) * TXS + 3)
            self.assertEqual(counts['datum'], len(DATUMS))
            ranged = {line.split('table=')[1].split()[0] for line in splits(logs, 'serve_advance_ranged')}
            self.assertTrue({'tx', 'datum'} <= ranged, ranged)
            self.assertEqual(rows(self.db), expected)
            serve.verify(self.store, GENERATION)
            raise psycopg.Rollback()

    def test_new_ranges_are_the_build_ranges_holding_new_rows(self):
        with self.db.transaction():
            inject(self.db, GENERATION)
            step = serve.BUILD_HEIGHTS
            self.assertEqual(serve._new_ranges(self.db, 'tx', GENERATION),
                             [(OLD - OLD % step, OLD - OLD % step + step - 1), (DENSE[0] - DENSE[0] % step, DENSE[0] - DENSE[0] % step + step - 1)])
            self.assertEqual(serve._new_ranges(self.db, 'datum', GENERATION), [(DATUMS[0], DATUMS[-1])])
            self.assertEqual(serve._new_ranges(self.db, 'tx_out', GENERATION), [])
            raise psycopg.Rollback()

    def test_ranged_publication_statements_read_only_their_range(self):
        """Without fresh statistics a generation's own facts look rare, and a statement filtering them inside the
        per-block lookup reads every fact of the generation for each block. The ranged statements find a range's
        facts by block or id and select the generation's own afterwards: no plan reads a `delta_` index."""
        with self.db.transaction():
            inject(self.db, GENERATION)
            bounds = {'datum': (DATUMS[0], DATUMS[0] + 99), 'multi_asset': (0, 99), 'epoch_stake': (0, 99), 'epoch_param': (0, 99)}
            # pool_hash keeps its one form: a pool has no generation of its own, it is served when a stake row of
            # the generation names it (a few thousand 28-byte rows per epoch).
            for table in serve.tables(self.db)[1:]:
                if table == 'pool_hash':
                    continue
                with self.subTest(table=table):
                    query, params = serve._new(table, GENERATION, self.db, bounds.get(table, (DENSE[0], DENSE[0])))
                    plan = self.db.execute('EXPLAIN (FORMAT JSON) ' + query, params).fetchone()[0][0]
                    used = {n.get('Index Name') for n in walk(plan['Plan'])} - {None}
                    self.assertFalse({name for name in used if name.startswith('delta_')}, used)
            query, params = serve._new('tx', GENERATION, self.db, (DENSE[0], DENSE[0]))
            self.assertEqual(len(self.db.execute(query, params).fetchall()), TXS)
            raise psycopg.Rollback()

    def test_the_expected_rows_lookup_is_bounded_by_its_batch(self):
        """FR-003: on the un-analyzed TEMP table, custom and generic plans of the shipped lookup use the primary key."""
        rows_per_family = 50000
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute(EXPECTED_TABLE)
            db.execute("INSERT INTO sql_expected SELECT 'outputs',jsonb_build_array(md5(n::text)||md5('x'||n),n%%3),"
                       "jsonb_build_object('tx_hash',md5(n::text)||md5('x'||n),'output_index',n%%3,'address','addr1q'||md5('a'||n)||md5('b'||n),"
                       "'value',(n*7919)::text) FROM generate_series(1,%s::int) n", (rows_per_family,))
            db.execute("INSERT INTO sql_expected SELECT 'datums',jsonb_build_array(md5('h'||n)),jsonb_build_object('hash',md5('h'||n),'value',md5('v'||n))"
                       ' FROM generate_series(1,%s::int) n', (rows_per_family,))
            self.assertEqual(db.execute("SELECT reltuples FROM pg_class WHERE relname='sql_expected'").fetchone()[0], -1, 'never analyzed')
            keys = [Jsonb(k) for k, in db.execute("SELECT jsonb_build_array(md5(n::text)||md5('x'||n),n%%3) FROM generate_series(%s::int,%s::int) n",
                                            (rows_per_family // 2, rows_per_family // 2 + EXPECTED_BATCH - 1))]
            db.execute('PREPARE lookup(text,jsonb[]) AS ' + EXPECTED_LOOKUP % ('$1', '$2'))
            copy = 'CREATE TEMP TABLE unkeyed AS SELECT * FROM sql_expected'
            for mode in ('force_custom_plan', 'force_generic_plan'):
                with self.subTest(plan=mode):
                    db.execute('SET plan_cache_mode=' + mode)
                    # EXPLAIN EXECUTE is a utility statement: its arguments are bound on the client.
                    plan = psycopg.ClientCursor(db).execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE lookup(%s,%s)', ('outputs', keys)).fetchone()[0][0]
                    nodes = list(walk(plan['Plan']))
                    self.assertEqual(plan['Plan']['Actual Rows'], EXPECTED_BATCH)
                    self.assertIn('sql_expected_pkey', {n.get('Index Name') for n in nodes})
                    self.assertNotIn('Seq Scan', {n['Node Type'] for n in nodes})
            # The assertion can fail: the same lookup on a copy without the primary key reads the whole table.
            db.execute(copy)
            db.execute('RESET plan_cache_mode')
            plan = db.execute('EXPLAIN (FORMAT JSON) ' + EXPECTED_LOOKUP.replace('sql_expected', 'unkeyed'), ('outputs', keys)).fetchone()[0][0]
            self.assertIn('Seq Scan', {n['Node Type'] for n in walk(plan['Plan'])})


def walk(node):
    yield node
    for child in node.get('Plans', []):
        yield from walk(child)


if __name__ == '__main__':
    unittest.main()
