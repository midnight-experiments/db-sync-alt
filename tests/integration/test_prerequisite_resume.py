"""Real PostgreSQL coverage corruption, explicit legacy and boundary behavior."""
import copy
import importlib.util
import threading
from pathlib import Path
from psycopg.types.json import Jsonb
from db_sync_alt.contract import Refusal,Profile,TABLES,canonical,digest
from db_sync_alt.sources.dbsync.acquire import acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.prerequisites import prerequisite_coverage,chunk_receipt,seal_receipt
from db_sync_alt.verify.store import verify_store
from integration.test_prerequisites import SQLFixture,baseline_acquire,S,T
import integration.test_dbsync_acquire as base_sql
from integration.test_dbsync_acquire import H
import unittest
from unittest.mock import patch
import psycopg

class ResumeTests(SQLFixture,unittest.TestCase):
 setUpClass=classmethod(base_sql.SQLTests.setUpClass.__func__)
 def test_oversized_fresh_window_refuses_before_generation_or_fact_work(self):
  with self.owner() as store:
   source=self.source()
   with patch.object(source,'select_ids',side_effect=AssertionError('fact query before size admission')):
    with self.assertRaisesRegex(Refusal,'^sql_completion_receipt_too_large$'):
     acquire(source,store,self.profile,target_height=T,window_blocks=1)
   self.assertEqual(store.db.execute('SELECT count(*) FROM chain.generations').fetchone()[0],0)
   self.assertFalse(store.checkpoint_rows());self.assertIsNone(store.meta('canonical_ready'))
   self.assertIsNone(store.meta('profile'))
 def test_oversized_pending_window_cannot_be_overridden_on_resume(self):
  with self.owner() as store:
   # Emulate an earlier v2 writer without the new gate, interrupted after its
   # real generation commit but before range allocation. Do not construct
   # millions of obsolete intervals merely to set up this resume regression.
   with patch('db_sync_alt.sources.dbsync.acquire.admit_receipt_size'), patch(
       'db_sync_alt.sources.dbsync.acquire.prerequisite_coverage',
       side_effect=Refusal('fixture_after_generation_commit')):
    with self.assertRaisesRegex(Refusal,'^fixture_after_generation_commit$'):
     acquire(self.source(),store,self.profile,target_height=T,window_blocks=1)
   saved=copy.deepcopy(store.pending());checkpoints=store.checkpoint_rows();dsn=store.db.info.dsn
   self.assertEqual(saved['manifest']['acquisition'],{'version':2,'window_blocks':1})
   expected=[{'generation':saved['id'],'phase':'sql-acquisition','unit':'identity',
              'receipt':{'version':1,'initial_manifest_sha256':digest(saved['manifest'])}}]
   self.assertEqual(canonical(checkpoints),canonical(expected))
   self.assertFalse(store.checkpoint_rows(phase='prerequisites'))
   for table in TABLES:self.assertIsNone(store.db.execute('SELECT 1 FROM chain.'+table+' LIMIT 1').fetchone())
   pending_bytes=canonical(saved).encode();checkpoint_bytes=canonical(checkpoints).encode()
  with PgStore(conninfo=dsn,allow_incomplete=True) as store:
   source=self.source()
   with patch.object(source,'select_ids',side_effect=AssertionError('fact query before resume admission')):
    with self.assertRaisesRegex(Refusal,'^sql_completion_receipt_too_large$'):
     acquire(source,store,self.profile,target_height=T,window_blocks=10000)
   self.assertEqual(store.pending(),saved);self.assertEqual(store.checkpoint_rows(),checkpoints)
   self.assertEqual(canonical(store.pending()).encode(),pending_bytes)
   self.assertEqual(canonical(store.checkpoint_rows()).encode(),checkpoint_bytes)
   self.assertIsNone(store.meta('canonical_ready'))
 def interrupted(self,store,count=3):
  stop=threading.Event();seen=[]
  def after(phase,**kw):
   if phase=='prerequisites':seen.append(kw['height'])
   if len(seen)>=count:stop.set()
  with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop,progress=after)
  self.assertIsNone(store.meta('canonical_ready'));return store.pending()
 def test_corrupt_chunks_and_false_completion_never_resume(self):
  with self.owner() as store:
   g=self.interrupted(store);saved=store.checkpoint_rows(phase='prerequisites');self.assertEqual(len(saved),3)
   mutations=[{'lo':True},{'hi':True},{'rows':False},{'binding':'f'*64},{'read_only':False},{'source_kind':'foreign'},{'hi':10000}]
   for mutation in mutations:
    with self.subTest(mutation=mutation),store.db.transaction(force_rollback=True):
     store.db.execute("UPDATE chain.checkpoints SET receipt=%s WHERE generation=0 AND phase='prerequisites' AND unit=%s",(Jsonb({**saved[0]['receipt'],**mutation}),saved[0]['unit']))
     with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.run_acquire(store)
   with store.db.transaction(force_rollback=True):
    store.db.execute("DELETE FROM chain.checkpoints WHERE generation=0 AND phase='prerequisites' AND unit=%s",(saved[0]['unit'],))
    with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.run_acquire(store)
   with store.db.transaction(force_rollback=True):
    store.db.execute("INSERT INTO chain.checkpoints VALUES(0,'prerequisites','1:9999',%s)",(Jsonb(chunk_receipt(g,1,9999,0)),))
    with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.run_acquire(store)
   with store.db.transaction(force_rollback=True):
    store.db.execute("INSERT INTO chain.checkpoints VALUES(0,'prerequisites','complete',%s)",(Jsonb(seal_receipt(g,prerequisite_coverage(store,g,require_complete=False)['chunks'])),))
    with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.run_acquire(store)
   self.assertEqual(store.checkpoint_rows(phase='prerequisites'),saved);self.assertIsNone(store.meta('canonical_ready'))
 def test_legacy_incomplete_refuses_without_relabeling(self):
  with self.owner() as store:
   source=self.source();old=source.select_ids
   def stopped(lo,hi,**kw):
    if hi<S:raise Refusal('fixture_before_prerequisite')
    return old(lo,hi,**kw)
   source.select_ids=stopped
   with self.assertRaisesRegex(Refusal,'fixture_before_prerequisite'):baseline_acquire()(source,store,self.profile,target_height=T,window_blocks=10000)
   saved=copy.deepcopy(store.pending());self.assertEqual(saved['manifest']['acquisition']['version'],1)
   with self.assertRaisesRegex(Refusal,'prerequisites_v1_pending_requires_recovery'):self.run_acquire(store)
   self.assertEqual(store.pending(),saved);self.assertFalse(store.checkpoint_rows(phase='prerequisites'))
 def test_legacy_complete_resume_and_malformed_whole_refuse(self):
  with self.owner() as store:
   stop=threading.Event()
   with self.assertRaisesRegex(Refusal,'interrupted'):baseline_acquire()(self.source(),store,self.profile,target_height=S,window_blocks=10000,stop_event=stop,progress=lambda phase,**kw:stop.set() if phase=='metadata' else None)
   old=store.checkpoint_rows(phase='prerequisites')[0];saved=copy.deepcopy(store.pending()['manifest']);self.assertEqual(saved['acquisition']['version'],1)
   for mutation in [{'lo':True},{'hi':S-2},{'rows':False},{'read_only':False},{'extra':'forged'},{'source_kind':'foreign'},{'snapshot':'earlier-writer-snapshot'}]:
    with self.subTest(mutation=mutation),store.db.transaction(force_rollback=True):
     store.db.execute("UPDATE chain.checkpoints SET receipt=%s WHERE generation=0 AND phase='prerequisites'",(Jsonb({**old['receipt'],**mutation}),))
     with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.run_acquire(store,target=S)
   self.run_acquire(store,target=S);self.assertEqual(store.generation()['manifest']['acquisition'],saved['acquisition']);self.assertEqual(store.checkpoint_rows(phase='prerequisites'),[old]);verify_store(store)
   self.run_acquire(store,target=T);self.assertEqual(store.generation()['id'],1);self.assertEqual(store.generation()['manifest']['acquisition']['version'],2);self.assertFalse(store.checkpoint_rows(generation=1,phase='prerequisites'));self.assertEqual(store.checkpoint_rows(generation=0,phase='prerequisites'),[old]);verify_store(store)
 def test_source_profile_and_anchor_changes_refuse(self):
  with self.owner() as store:
   self.interrupted(store)
   wrong=self.source();wrong.service=self.twin_service
   with self.assertRaisesRegex(Refusal,'resume_namespace_mismatch'):acquire(wrong,store,self.profile,target_height=T,window_blocks=10000)
   wrong_profile=Profile(Path(__file__).resolve().parents[2]/'profiles/preview-node-1.0.0')
   with self.assertRaisesRegex(Refusal,'network_mismatch'):acquire(self.source(),store,wrong_profile,target_height=T)
   self.source_db.execute('UPDATE block SET hash=%s WHERE block_no=%s',(H('changed-target'),T))
   try:
    with self.assertRaisesRegex(Refusal,'source_anchor_conflict'):self.run_acquire(store)
   finally:self.source_db.execute('UPDATE block SET hash=%s WHERE block_no=%s',(H(T),T))
   self.assertIsNone(store.meta('canonical_ready'))
 def test_prestart_spends_ordinals_inside_cursor_and_old_reader_refusal(self):
  c=self.source_db
  with c.transaction():
   c.execute("INSERT INTO tx VALUES(8,%s,%s,0,true),(9,%s,%s,1,true),(10,%s,%s,2,true)",(H('extra8'),S,H('extra9'),S-100,H('extra10'),S-100))
   c.execute("INSERT INTO tx_out(id,tx_id,index,address,value) VALUES(8,8,0,%s,1),(9,9,0,'irrelevant',1),(10,10,0,%s,1)",(self.profile.mapping,self.profile.mapping))
   c.execute('INSERT INTO tx_in VALUES(0,4,9,0),(4,2,10,0),(5,5,8,0)')
   c.execute("INSERT INTO multi_asset VALUES(2,%s,'')",(b'x'*28,));c.execute('INSERT INTO ma_tx_out VALUES(0,1,1,2)')
  try:
   with self.owner() as store:
    result=self.run_acquire(store);verify_store(store);self.assertTrue(prerequisite_coverage(store,store.generation())['complete'])
    self.assertEqual(store.ordinal('inputs',(H('tx4'),H('tx1'),0)),1)
    self.assertEqual(store.ordinal('output_assets',(H('tx1'),0,bytes.fromhex(self.profile.night[:56]),bytes.fromhex(self.profile.night[56:]))),1)
    self.assertIsNotNone(store.fact('outputs',{'tx_hash':H('extra8'),'output_index':0}))
    source=self.source();inputs=[];assets=[]
    for r in store.rows('inputs',0):inputs.append((r['tx_hash'],r['origin_hash'],r['origin_index']))
    for r in store.rows('output_assets',0):assets.append((r['tx_hash'],r['output_index'],r['policy'],r['name']))
    observed=source.ordinals(inputs,assets,store.generation()['manifest']['anchors'])
    for key,n in observed['inputs'].items():self.assertEqual(store.ordinal('inputs',key),n)
    for key,n in observed['output_assets'].items():self.assertEqual(store.ordinal('output_assets',key),n)
    path=Path(__file__).resolve().parents[1]/'fixtures/sql_completion_v1.py';spec=importlib.util.spec_from_file_location('old_completion',path);old=importlib.util.module_from_spec(spec);spec.loader.exec_module(old)
    g=store.generation();receipt=store.checkpoint_rows(phase='final-anchors',unit='complete')[0]['receipt']
    with self.assertRaisesRegex(Refusal,'completion_invalid'):old.validate_sql_receipt(store,g,g['manifest'],receipt)
    self.assertEqual(result['generation'],0)
  finally:
   with c.transaction():
    c.execute('DELETE FROM tx_in WHERE id IN (0,4,5);DELETE FROM ma_tx_out WHERE id=0;DELETE FROM multi_asset WHERE id=2;DELETE FROM tx_out WHERE id IN (8,9,10);DELETE FROM tx WHERE id IN (8,9,10)')

 def test_empty_prerequisite_windows_are_sealed(self):
  dsn,alias=self.database()
  with psycopg.connect(dsn,autocommit=True) as db:
   self.fill(db)
   db.execute('DELETE FROM tx_in WHERE tx_out_id IN(1,2);DELETE FROM ma_tx_out WHERE tx_out_id=1;DELETE FROM tx_out WHERE id IN(1,2);DELETE FROM tx WHERE id IN(1,2)')
  source=self.source();source.service=alias
  with self.owner() as store:
   acquire(source,store,self.profile,target_height=T,window_blocks=10000);state=prerequisite_coverage(store,store.generation());self.assertTrue(state['complete']);self.assertEqual(state['position'],S);self.assertTrue(state['chunks']);self.assertTrue(all(c['receipt']['rows']==0 for c in state['chunks']));verify_store(store)
 def test_complete_looking_forged_coverage_still_needs_independent_source(self):
  c=self.source_db
  c.execute('INSERT INTO tx VALUES(9,%s,%s,2,true)',(H('unspent-pre9'),S-1));c.execute('INSERT INTO tx_out(id,tx_id,index,address,value) VALUES(9,9,0,%s,1)',(self.profile.mapping,))
  try:
   with self.owner() as store:
    g=self.interrupted(store);store.db.execute("DELETE FROM chain.checkpoints WHERE generation=0 AND phase='prerequisites'")
    with store.atomic():
     for lo in range(0,S,10000):
      hi=min(S-1,lo+9999);store.checkpoint(0,'prerequisites',f'{lo}:{hi}',chunk_receipt(g,lo,hi,0))
     chunks=prerequisite_coverage(store,g,require_complete=False)['chunks'];store.checkpoint(0,'prerequisites','complete',seal_receipt(g,chunks))
    self.assertTrue(prerequisite_coverage(store,g)['complete'])
    with self.assertRaisesRegex(Refusal,'independent_source_mismatch'):self.run_acquire(store)
    self.assertIsNone(store.meta('canonical_ready'));self.assertNotIn('completion',store.pending()['manifest'])
  finally:c.execute('DELETE FROM tx_out WHERE id=9;DELETE FROM tx WHERE id=9')
