"""One selection of the relevant history per pass, not one per window (issue 00040), on real PostgreSQL."""
import threading
import unittest

import psycopg

import db_sync_alt.sources.dbsync.reader as reader_module
from db_sync_alt.contract import TABLES, KEYS, Refusal
from db_sync_alt.sources.dbsync.acquire import acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
import integration.test_dbsync_acquire as base_sql
from integration.test_dbsync_acquire import S, T, H

# Twenty prefix blocks, each in its own 10,000-block window, carrying 5,000 asset outputs each.
PREFIX = [S - 10000 * (k + 1) for k in range(20)]
OUTPUTS = 100000
FIRST = 300000
# SelectionTests: one unspent asset output alone in an earlier prefix window.
OWN = S - 20000


class Fixture:
    database = classmethod(base_sql.SQLTests.database.__func__)
    fill = classmethod(base_sql.SQLTests.fill.__func__)
    tearDownClass = classmethod(base_sql.SQLTests.tearDownClass.__func__)
    source = base_sql.SQLTests.source
    owner = base_sql.SQLTests.owner

    def counted(self, source):
        """Count the per-pass selections and the per-window independent selector, and record the size of every
        id array a statement receives."""
        calls = {'selection': 0, 'independent_select_ids': 0, 'array_sizes': []}
        selection, select_ids, rows = source.selection, source.select_ids, source.rows
        def counted_selection():
            calls['selection'] += 1
            yield from selection()
        def counted_select_ids(lo, hi, *, independent=False):
            calls['independent_select_ids'] += bool(independent)
            return select_ids(lo, hi, independent=independent)
        def counted_rows(query, params=()):
            calls['array_sizes'] += [len(p) for p in params if isinstance(p, list)]
            return rows(query, params)
        source.selection, source.select_ids, source.rows = counted_selection, counted_select_ids, counted_rows
        return calls

    def reference(self, store, ranges):
        """0.4.1's facts: the per-window independent selector over the prefix windows, plus the events."""
        source = self.source(); expected = {t: {} for t in TABLES}
        def keep(table, row):
            body = {k: bytes(row[k]) if isinstance(row[k], memoryview) else row[k] for k in TABLES[table]}
            key = tuple(body[k] for k in KEYS[table])
            self.assertEqual(expected[table].get(key, body), body); expected[table][key] = body
        for lo, hi in ranges:
            with source.snapshot():
                for table, row in source.facts(*source.select_ids(lo, hi, independent=True)): keep(table, row)
        with source.snapshot():
            for row in source.blocks(S, T + self.profile.security): keep('blocks', row)
            for table, row in source.facts(*source.select_ids(S, T, independent=True)): keep(table, row)
            for epoch in (273, 274, 275):
                for table, row in source.epochs(epoch, T): keep(table, row)
        for table in TABLES:
            actual = {tuple(row[k] for k in KEYS[table]): row for row in store.rows(table, 0)}
            self.assertEqual(actual, expected[table], table)


class LargeSelectionTests(Fixture, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_sql.SQLTests.setUpClass.__func__(cls)
        c = cls.source_db
        with c.transaction():
            for h in PREFIX:
                c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,273,%s,'2026-01-01',%s,%s)",
                          (h, H(h), h, h, OUTPUTS // len(PREFIX), h - 1))
            c.execute("INSERT INTO tx(id,hash,block_id,block_index,valid_contract) SELECT n,decode(md5(n::text)||md5(n::text),'hex'),"
                      "(%s::bigint[])[1+(n-%s)%%20],(n-%s)/20,true FROM generate_series(%s,%s) n", (PREFIX, FIRST, FIRST, FIRST, FIRST + OUTPUTS - 1))
            c.execute("INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) SELECT n,n,0,'large-'||n,1,NULL FROM generate_series(%s,%s) n",
                      (FIRST, FIRST + OUTPUTS - 1))
            c.execute('INSERT INTO ma_tx_out(id,quantity,tx_out_id,ident) SELECT n,1,n,1 FROM generate_series(%s,%s) n', (FIRST, FIRST + OUTPUTS - 1))
            # 2,000 of them spent before the start cursor and 500 inside the event range.
            c.execute("INSERT INTO tx(id,hash,block_id,block_index,valid_contract) SELECT n,decode(md5('s'||n)||md5('s'||n),'hex'),"
                      "CASE WHEN n<%s THEN %s ELSE %s END,1000+n-%s,true FROM generate_series(%s,%s) n",
                      (500000 + 2000, S - 1, S + 2, 500000, 500000, 500000 + 2499))
            c.execute('INSERT INTO tx_in(id,tx_in_id,tx_out_id,tx_out_index) SELECT n,n,n-500000+%s,0 FROM generate_series(%s,%s) n',
                      (FIRST, 500000, 500000 + 2499))

    def test_one_selection_per_pass_with_bounded_statements_and_the_same_facts(self):
        with self.owner() as store:
            source = self.source(); calls = self.counted(source)
            result = acquire(source, store, self.profile, target_height=T, window_blocks=10000)
            self.assertEqual(calls['selection'], 2)
            self.assertEqual(calls['independent_select_ids'], 0)
            self.assertTrue(calls['array_sizes'] and max(calls['array_sizes']) <= reader_module.SELECTION_BATCH)
            self.assertEqual(result['digests']['outputs']['count'], OUTPUTS + 3)
            self.assertGreaterEqual(result['digests']['inputs']['count'], 2500)
            verify_store(store)
            windows = sorted({(h - h % 10000, min(S - 1, h - h % 10000 + 9999)) for h in PREFIX + [S - 100, S - 50, S - 1]})
            self.reference(store, windows)


class SelectionTests(Fixture, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_sql.SQLTests.setUpClass.__func__(cls)
        c = cls.source_db
        with c.transaction():
            # An unspent prefix output in its own window, before the window of the other prefix facts.
            c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,273,%s,'2026-01-01',1,%s)",
                      (OWN, H(OWN), OWN, OWN, OWN - 1))
            c.execute('INSERT INTO tx VALUES(8,%s,%s,0,true)', (H('tx8'), OWN))
            c.execute("INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(8,8,0,'prefix-token',2000000,NULL)")
            c.execute('INSERT INTO ma_tx_out VALUES(3,1000,8,1)')
            c.execute('INSERT INTO pool_hash VALUES(2,%s),(3,%s),(4,%s)', (b'b' * 28, b'a' * 28, b'c' * 28))
            c.execute('INSERT INTO epoch_stake VALUES(900,2,1,275),(901,2,2,275),(902,3,99999999999999999999,275),(903,3,1,275),(904,4,0,275),(905,1,7,275)')

    def run_acquire(self, store, source=None, **kw):
        kw.setdefault('window_blocks', 10000)
        return acquire(source or self.source(), store, self.profile, target_height=T, **kw)

    def test_streamed_epoch_stake_sums_equal_the_group_by(self):
        old = ('SELECT s.epoch_no AS epoch,p.hash_raw AS pool_hash,sum(s.amount)::text AS amount FROM epoch_stake s JOIN pool_hash p '
               'ON p.id=s.pool_id WHERE s.epoch_no=%s GROUP BY s.epoch_no,p.hash_raw ORDER BY p.hash_raw')
        source = self.source()
        for epoch in (273, 274, 275):
            with source.snapshot():
                streamed = [row for table, row in source.epochs(epoch, T) if table == 'epoch_stakes']
                grouped = [dict(r, pool_hash=bytes(r['pool_hash'])) for r in source.rows(old, (epoch,))]
            self.assertEqual(streamed, grouped, epoch)
        self.assertEqual([r['amount'] for r in streamed], ['100000000000000000000', '3', '0', '123463'])

    def test_a_selection_omitted_by_the_main_pass_is_refused_by_the_independent_pass(self):
        with self.owner() as store:
            source = self.source(); selection = source.selection; seen = []
            def omitting():
                seen.append(1)
                for row in selection():
                    if len(seen) == 1 and row[0] == 'outputs' and row[2] == OWN:continue
                    yield row
            source.selection = omitting
            with self.assertRaisesRegex(Refusal, 'independent_source_mismatch'):self.run_acquire(store, source)
            self.assertEqual(len(seen), 2); self.assertIsNone(store.meta('canonical_ready'))

    def test_an_extra_prerequisite_fact_is_refused(self):
        with self.owner() as store:
            injected = False
            def extra(phase, **fields):
                nonlocal injected
                if phase == 'prerequisites' and not injected:
                    store.insert('datums', {'hash': H('extra-prestart-datum'), 'value': '{"int":5}'}, fields['generation']); injected = True
            with self.assertRaisesRegex(Refusal, 'independent_source_extra_fact'):self.run_acquire(store, progress=extra)
            self.assertTrue(injected); self.assertIsNone(store.meta('canonical_ready'))

    def test_an_interrupted_prefix_resumes_with_a_fresh_selection_and_the_same_result(self):
        with self.owner() as clean:
            expected = self.run_acquire(clean)['digests']
        with self.owner() as store:
            stop = threading.Event()
            def after(phase, **fields):
                if phase == 'prerequisites' and any(r['receipt'].get('rows', 0) > 0 for r in store.checkpoint_rows(phase='prerequisites')):stop.set()
            with self.assertRaisesRegex(Refusal, 'interrupted'):self.run_acquire(store, stop_event=stop, progress=after)
            dsn = store.db.info.dsn
        with PgStore(conninfo=dsn, allow_incomplete=True) as store:
            self.assertIsNone(store.db.execute("SELECT to_regclass('pg_temp.sql_selection')").fetchone()[0])
            source = self.source(); calls = self.counted(source)
            self.assertEqual(self.run_acquire(store, source)['digests'], expected)
            self.assertEqual(calls['selection'], 2); verify_store(store)

    def test_a_cancelled_resolution_statement_is_retried_in_halves(self):
        source = self.source(); real = source.rows; seen = []
        def cancelling(query, params=()):
            seen.append(len(params[0]))
            if len(params[0]) > 1:raise psycopg.errors.QueryCanceled('fixture cancel')
            return real(query, params)
        with source.snapshot():
            ids = [1, 2, 3]
            expected = [r['id'] for r in real(reader_module.OUTPUT_HEIGHTS, (ids,))]
            source.rows = cancelling
            self.assertEqual(sorted({r['id'] for r in source.bounded_rows(reader_module.OUTPUT_HEIGHTS, ids)}), sorted(set(expected)))
            self.assertEqual(seen, [3, 1, 2, 1, 1])
            def always(query, params=()):raise psycopg.errors.QueryCanceled('fixture cancel')
            source.rows = always
            with self.assertRaises(psycopg.errors.QueryCanceled):list(source.bounded_rows(reader_module.OUTPUT_HEIGHTS, [1]))


if __name__ == '__main__':
    unittest.main()
