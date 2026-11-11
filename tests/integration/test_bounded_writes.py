"""Root SQL acquisition and upgrade: no destination statement grows with the history (issue 00042).

The store's session limit is lowered for the test to 200 ms, on a store larger than one served-table build
range and one selection COPY. Statements under a documented limit of their own (verify/limits.py: bulk DDL,
verification, ANALYZE) keep it; every other statement must stay small however long the chain is. Built
unchunked, the served tx_out rows of this store alone take about 0.6 s in one statement.
"""
import unittest
from unittest import mock

import db_sync_alt.sources.dbsync.acquire as acquire_module
import db_sync_alt.store.database as database
from db_sync_alt.sources.dbsync.acquire import acquire
from db_sync_alt.store import serve
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
import integration.test_dbsync_acquire as base_sql
from integration.test_dbsync_acquire import S, T, H
from integration.test_global_selection import Fixture

# Eighty prefix blocks, each in its own build range and acquisition window, with 2,500 asset outputs each
# (a block's transaction index must stay below the served 4,096).
PREFIX = [S - 10000 * (k + 1) for k in range(80)]
PER_BLOCK = 2500
FIRST = 300000


class BoundedWriteTests(Fixture, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_sql.SQLTests.setUpClass.__func__(cls)
        c = cls.source_db; n = PER_BLOCK * len(PREFIX)
        with c.transaction():
            for h in PREFIX:
                c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,273,%s,'2026-01-01',%s,%s)",
                          (h, H(h), h, h, PER_BLOCK, h - 1))
            c.execute("INSERT INTO tx(id,hash,block_id,block_index,valid_contract) SELECT n,decode(md5(n::text)||md5(n::text),'hex'),"
                      "(%s::bigint[])[1+(n-%s)/%s],(n-%s)%%%s,true FROM generate_series(%s,%s) n", (PREFIX, FIRST, PER_BLOCK, FIRST, PER_BLOCK, FIRST, FIRST + n - 1))
            c.execute("INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) SELECT n,n,0,'bounded-'||n,1,NULL FROM generate_series(%s,%s) n", (FIRST, FIRST + n - 1))
            c.execute('INSERT INTO ma_tx_out(id,quantity,tx_out_id,ident) SELECT n,1,n,1 FROM generate_series(%s,%s) n', (FIRST, FIRST + n - 1))
            # 2,500 of them spent before the start cursor.
            c.execute("INSERT INTO tx(id,hash,block_id,block_index,valid_contract) SELECT n,decode(md5('s'||n)||md5('s'||n),'hex'),%s,1000+n-%s,true"
                      ' FROM generate_series(%s,%s) n', (S - 1, 900000, 900000, 900000 + 2499))
            c.execute('INSERT INTO tx_in(id,tx_in_id,tx_out_id,tx_out_index) SELECT n,n,n-900000+%s,0 FROM generate_series(%s,%s) n', (FIRST, 900000, 900000 + 2499))
            c.execute('UPDATE block SET tx_count=4000 WHERE block_no=%s', (S - 1,))

    def test_root_acquire_and_upgrade_keep_every_session_statement_small(self):
        with mock.patch.object(database, 'WRITER_TIMEOUT', '200ms'), mock.patch.object(acquire_module, 'SELECTION_COPY_ROWS', 10000):
            with self.owner() as store:
                self.assertEqual(store.db.execute('SHOW statement_timeout').fetchone()[0], '200ms')
                acquire(self.source(), store, self.profile, target_height=T, window_blocks=10000)
                self.assertGreater(len(serve.ranges(store.db, 'tx')), len(PREFIX))
                result = upgrade_store(store)
                self.assertEqual(result['schema_version'], 5)
                self.assertEqual(result['served_rows']['tx_out'], PER_BLOCK * len(PREFIX) + 3)
                self.assertEqual(result['served_rows']['ma_tx_out'], PER_BLOCK * len(PREFIX) + 2)
                self.assertGreaterEqual(result['served_rows']['tx_in'], 2500)
                verify_store(store)


if __name__ == '__main__':
    unittest.main()
