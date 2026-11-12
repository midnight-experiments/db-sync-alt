"""The epoch-end anchor on the upstream db-sync 13.8 catalog reads only the epoch's own blocks (issue 00047).

Many blocks follow the first epoch, as on a source whose tip is far past the epochs being read."""
from types import SimpleNamespace
import unittest

import psycopg

from db_sync_alt.sources.dbsync.reader import EPOCH_END, SQLSource
from integration.test_dbsync_versions import apply_upstream
from integration.test_network_profiles import Databases

# The 0.4.3 statement, which PostgreSQL answers by walking idx_block_block_no back from the newest block.
UNFENCED = 'SELECT max(block_no) AS height FROM block WHERE epoch_no=%s'
FIRST, EPOCHS, PER_EPOCH = 600, 40, 5000
LAST = FIRST + EPOCHS - 1
MIDDLE = FIRST + EPOCHS // 2
# An epoch whose only block has no height (a Byron boundary block), and an epoch with no block at all.
BOUNDARY_ONLY, EMPTY = FIRST - 1, LAST + 1


def end_of(epoch):
    return (epoch - FIRST + 1) * PER_EPOCH - 1


class EpochEndAnchorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dbs = Databases()
        dsn, cls.service = cls.dbs.create()
        with psycopg.connect(dsn, autocommit=True) as db:
            apply_upstream(db, '13.8.0.0')
            columns = 'id,hash,epoch_no,slot_no,epoch_slot_no,block_no,previous_id,slot_leader_id,size,time,tx_count,proto_major,proto_minor,vrf_key,op_cert,op_cert_counter'
            with db.transaction():
                # Boundary blocks without a height: one alone in its epoch, one opening the first epoch.
                db.execute('INSERT INTO block(' + columns + ') VALUES'
                           "(1,sha256('\\x01'::bytea),%s,NULL,NULL,NULL,NULL,1,0,'2020-01-01',0,0,0,NULL,NULL,NULL),"
                           "(2,sha256('\\x02'::bytea),%s,NULL,NULL,NULL,1,1,0,'2020-01-01',0,0,0,NULL,NULL,NULL)",
                           (BOUNDARY_ONLY, FIRST))
                db.execute('INSERT INTO block(' + columns + ') '
                           'SELECT n+3,sha256(int8send(n)),%s+n/%s,n*20,(n%%%s)*20,n,n+2,1,1000,'
                           "timestamp '2020-01-01'+n*interval '20 seconds',3,10,0,'vrf_vk1'||md5(n::text)||md5((n+1)::text),"
                           'sha256(int8send(-n-1)),n FROM generate_series(0,%s) n',
                           (FIRST, PER_EPOCH, PER_EPOCH, EPOCHS * PER_EPOCH - 1))
            db.execute('ANALYZE block')
            cls.indexes = {name: definition for name, definition in db.execute(
                "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='block' AND indexname IN ('idx_block_block_no','idx_block_epoch_no')")}

    @classmethod
    def tearDownClass(cls):
        cls.dbs.close()

    def run_in_source(self, work):
        """`work(source)` inside the reader's own read-only snapshot and session settings."""
        source = SQLSource(self.service, SimpleNamespace(units=[], governance=[], addresses=[]))
        with source.snapshot():
            return work(source)

    def plan(self, statement, epoch):
        return '\n'.join(r['QUERY PLAN'] for r in self.run_in_source(lambda s: s.rows('EXPLAIN ' + statement, (epoch,))))

    def buffers(self, statement, epoch):
        def explain(source):
            plan = source.one('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + statement, (epoch,))['QUERY PLAN'][0]['Plan']
            return plan.get('Shared Hit Blocks', 0) + plan.get('Shared Read Blocks', 0)
        return self.run_in_source(explain)

    def test_fixture_has_the_upstream_block_indexes(self):
        self.assertEqual(self.indexes, {'idx_block_block_no': 'CREATE INDEX idx_block_block_no ON public.block USING btree (block_no)',
                                        'idx_block_epoch_no': 'CREATE INDEX idx_block_epoch_no ON public.block USING btree (epoch_no)'})

    def test_shipped_statement_reads_the_epoch_index_without_walking_back_from_the_tip(self):
        for epoch in (FIRST, MIDDLE, LAST):
            with self.subTest(epoch=epoch):
                plan = self.plan(EPOCH_END, epoch)
                self.assertIn('idx_block_epoch_no', plan)
                self.assertNotIn('idx_block_block_no', plan)
                self.assertNotIn('Backward', plan)

    def test_unfenced_statement_walks_back_from_the_tip(self):
        """Negative control: on this fixture the 0.4.3 statement is planned as the walk the fix removes."""
        self.assertIn('Index Scan Backward using idx_block_block_no', self.plan(UNFENCED, FIRST))

    def test_shipped_cost_does_not_grow_with_the_distance_from_the_tip(self):
        near, far = self.buffers(EPOCH_END, LAST), self.buffers(EPOCH_END, FIRST)
        walk = self.buffers(UNFENCED, FIRST)
        self.assertLessEqual(far, 2 * near)
        self.assertGreater(walk, 10 * far)

    def test_both_statements_give_the_same_end(self):
        for epoch, expected in ((FIRST, end_of(FIRST)), (MIDDLE, end_of(MIDDLE)), (LAST, end_of(LAST)), (BOUNDARY_ONLY, None), (EMPTY, None)):
            with self.subTest(epoch=epoch):
                heights = self.run_in_source(lambda s: [s.one(q, (epoch,))['height'] for q in (EPOCH_END, UNFENCED)])
                self.assertEqual(heights, [expected, expected])


if __name__ == '__main__':
    unittest.main()
