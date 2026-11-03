# SPDX-License-Identifier: Apache-2.0
"""Actual PG continuation with captured/synthetic provider routes, zero live calls."""
import copy
import unittest
from unittest.mock import patch
from db_sync_alt.store.database import PgStore
from db_sync_alt.errors import TransientSyncFailure
import fixtures.blockfrost_generation as gen_fixture
from fixtures.cbor import tx
from fixtures.database import SyncFixture


class RealSyncTests(SyncFixture):
    def test_selected_spend_mint_and_epoch_transition_match_all_ten_families(self):
        dsn=self.make(transition=True);routes,_=gen_fixture.fixtures(transition=True)
        with PgStore(conninfo=dsn) as store:
            old=store.digests(0);self.run_driver(store,routes)
            self.compare_reference(routes,store,transition=True)
            self.assertEqual(store.digests(0),old)
            self.assertEqual(store.generation()['target_height'],gen_fixture.OFFSET+12)
            self.assertEqual(store.sync_job('provider-active')['state'],'completed')
            receipt=store.checkpoint_rows(generation=1,phase='epochs',unit='2')[0]['receipt']
            self.assertEqual(receipt['epoch_end']['height'],gen_fixture.OFFSET+11)
    def test_late_pre_start_origin_insertion_matches_reference_and_keeps_old_ids(self):
        original_block=gen_fixture.block
        def with_origin_count(height,*a,**kw):
            result=original_block(height,*a,**kw)
            if height==9:result['tx_count']=1
            return result
        with patch.object(gen_fixture,'block',side_effect=with_origin_count):
            dsn=self.make();routes,_=gen_fixture.fixtures()
        origin=tx({0:[],1:[[b'a',100]],2:4})
        spend=tx({0:[[bytes.fromhex(origin[1]),0]],1:[],2:3})
        for height,(raw,hash_) in ((9,origin),(11,spend)):
            block=with_origin_count(height)
            routes['/blocks/'+block['hash']+'/txs/cbor?count=100&page=1&order=asc']=[{'tx_hash':hash_,'cbor':raw}]
            routes['/txs/'+hash_+'/cbor']={'cbor':raw}
            routes['/txs/'+hash_]={'hash':hash_,'block':block['hash'],'block_height':gen_fixture.OFFSET+height,'index':0,'valid_contract':True}
            routes['/txs/'+hash_+'/utxos']={'hash':hash_,'inputs':[] if height==9 else [dict(gen_fixture.out(),tx_hash=origin[1],reference=False,collateral=False)],'outputs':[gen_fixture.out()] if height==9 else []}
        with PgStore(conninfo=dsn) as store:
            old_ids=store.db.execute('SELECT hash,id FROM chain.transactions ORDER BY id').fetchall()
            self.run_driver(store,routes)
            with patch.object(gen_fixture,'block',side_effect=with_origin_count):self.compare_reference(routes,store)
            self.assertEqual(store.db.execute('SELECT hash,id FROM chain.transactions WHERE first_generation=0 ORDER BY id').fetchall(),old_ids)
            self.assertIsNotNone(store.fact('outputs',(bytes.fromhex(origin[1]),0)))
            self.assertEqual(store.fact('transactions',(bytes.fromhex(origin[1]),))['block_hash'],bytes.fromhex(with_origin_count(9)['hash']))
    def test_failed_request_resume_on_new_writer_keeps_budget_and_pending_target(self):
        dsn=self.make();routes,_=gen_fixture.fixtures();broken=copy.deepcopy(routes)
        endpoint='/blocks/'+gen_fixture.block(12)['hash']+'/txs/cbor?count=100&page=1&order=asc'
        def unavailable():raise RuntimeError('HTTP 503; request counted, no automatic retry')
        broken[endpoint]=unavailable
        now=[1700000000.0]
        with PgStore(conninfo=dsn) as store:
            before=store.digests(store.generation()['id'])
            with self.assertRaises(TransientSyncFailure):self.run_driver(store,broken,clock=lambda:now[0])
            saved=store.sync_job('provider-active');self.assertEqual(store.generation()['id'],0)
            self.assertEqual(store.digests(store.generation()['id']),before);self.assertTrue(store.completed(1,'events',gen_fixture.OFFSET+11))
        now[0]+=86400
        with PgStore(conninfo=dsn) as store:
            self.run_driver(store,routes,clock=lambda:now[0]);finished=store.sync_job('provider-active')
            self.assertEqual(finished['job_id'],saved['job_id']);self.assertEqual(finished['budget'],saved['budget'])
            self.assertGreater(finished['attempts'],saved['attempts']);self.compare_reference(routes,store)


if __name__=='__main__':unittest.main(verbosity=2)
