"""Real PostgreSQL >100k prerequisite density and atomic coverage/resume fixtures."""
import json
import time
import unittest
import importlib.util
import hashlib
import threading
from pathlib import Path
from db_sync_alt.contract import TABLES,KEYS
from db_sync_alt.store.database import PgStore
from db_sync_alt.sources.dbsync.acquire import acquire
from db_sync_alt.verify.store import verify_store
from db_sync_alt.verify.prerequisites import prerequisite_coverage
from db_sync_alt.contract import Refusal
import integration.test_dbsync_acquire as base_sql
from integration.test_dbsync_acquire import S,T

def baseline_acquire():
 path=Path(__file__).resolve().parents[1]/'fixtures/qualified_00041_acquire.py'
 assert hashlib.sha256(path.read_bytes()).hexdigest()=='95385711f0b82f44ee2aa493375cf7c5f6de947f26880a27ac6f67eb137265f8'
 spec=importlib.util.spec_from_file_location('qualified_baseline_acquire',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module.acquire

class SQLFixture:
 database=classmethod(base_sql.SQLTests.database.__func__)
 fill=classmethod(base_sql.SQLTests.fill.__func__)
 tearDownClass=classmethod(base_sql.SQLTests.tearDownClass.__func__)
 source=base_sql.SQLTests.source
 owner=base_sql.SQLTests.owner
 def run_acquire(self,store,target=T,**kw):kw.setdefault('window_blocks',10000);return acquire(self.source(),store,self.profile,target_height=target,**kw)

class DensityTests(SQLFixture,unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  base_sql.SQLTests.setUpClass.__func__(cls)
  c=cls.source_db
  with c.transaction():
   c.execute('UPDATE block SET tx_count=40000 WHERE block_no<%s',(S,))
   c.execute("INSERT INTO tx(id,hash,block_id,block_index,valid_contract) SELECT n,decode(md5(n::text)||md5(n::text),'hex'),CASE n%%3 WHEN 0 THEN %s WHEN 1 THEN %s ELSE %s END,(n/3)+10,true FROM generate_series(1000,101000)n",(S-100,S-50,S-1))
   c.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) SELECT n,n,0,%s,1,NULL FROM generate_series(1000,101000)n',(cls.profile.mapping,))
 @classmethod
 def tearDownClass(cls):base_sql.SQLTests.tearDownClass.__func__(cls)
 def test_baseline_unsplittable_density(self):
  with self.owner() as store:
   started=time.monotonic()
   with self.assertRaisesRegex(Refusal,'work_unit_too_dense'):baseline_acquire()(self.source(),store,self.profile,target_height=T,window_blocks=10000)
   self.assertIsNone(store.meta('canonical_ready'));self.assertFalse(store.checkpoint_rows(phase='prerequisites'))
   print('BASELINE_DENSITY '+json.dumps({'new_selected_prestart_outputs':100001,'existing_selected':2,'result':'work_unit_too_dense','prerequisite_receipts':0,'published':False,'seconds':time.monotonic()-started}),flush=True)

 def test_density_interruptions_resume_and_independent_reference(self):
  started=time.monotonic();stop=threading.Event()
  with self.owner() as store:
   def during(phase,**kw):
    if phase=='sql_facts':stop.set()
   store.progress_callback=during
   with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop)
   self.assertEqual(store.db.execute('SELECT count(*) FROM chain.outputs').fetchone()[0],0)
   self.assertFalse(prerequisite_coverage(store,store.pending(),require_complete=False)['complete']);dsn=store.db.info.dsn
  with PgStore(conninfo=dsn,allow_incomplete=True) as store:
   stop=threading.Event()
   def after(phase,**kw):
    if phase=='prerequisites' and any(r['receipt'].get('rows',0)>0 for r in store.checkpoint_rows(phase='prerequisites')):stop.set()
   with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop,progress=after)
   self.assertGreater(store.db.execute('SELECT count(*) FROM chain.outputs').fetchone()[0],0);prefix=prerequisite_coverage(store,store.pending(),require_complete=False);self.assertFalse(prefix['complete']);before=store.checkpoint_rows(phase='prerequisites')
  with PgStore(conninfo=dsn,allow_incomplete=True) as store:
   result=self.run_acquire(store);self.assertEqual(store.generation()['manifest']['acquisition']['version'],2);self.assertTrue(prerequisite_coverage(store,store.generation())['complete']);self.assertTrue(all(r in store.checkpoint_rows(phase='prerequisites') for r in before));verify_store(store)
   expected={t:{} for t in TABLES};source=self.source()
   def keep(table,row):
    body={k:bytes(row[k]) if isinstance(row[k],memoryview) else row[k] for k in TABLES[table]};key=tuple(body[k] for k in KEYS[table]);self.assertEqual(expected[table].get(key,body),body);expected[table][key]=body
   for height in (S-100,S-50,S-1):
    with source.snapshot():
     for table,row in source.facts(*source.select_ids(height,height,independent=True)):keep(table,row)
   with source.snapshot():
    for row in source.blocks(S,T+self.profile.security):keep('blocks',row)
    for table,row in source.facts(*source.select_ids(S,T,independent=True)):keep(table,row)
    for epoch in (273,274,275):
     for table,row in source.epochs(epoch,T):keep(table,row)
   for table in TABLES:
    actual={tuple(row[k] for k in KEYS[table]):row for row in store.rows(table,0)};self.assertEqual(actual,expected[table],table)
   saved=result['digests'];self.assertEqual(saved['outputs']['count'],100004)
  with self.owner() as uninterrupted:
   result=self.run_acquire(uninterrupted);self.assertEqual(result['digests'],saved);verify_store(uninterrupted)
  print('DENSITY_CANDIDATE '+json.dumps({'selected_prestart_outputs':100003,'outputs':saved['outputs']['count'],'facts':sum(v['count'] for v in saved.values()),'before_commit_rollback':True,'after_commit_resume':True,'independent_source_reference':True,'uninterrupted_match':True,'seconds':time.monotonic()-started}),flush=True)
 def test_irreducible_one_block_refuses(self):
  c=self.source_db;c.execute('UPDATE tx SET block_id=%s,block_index=id+200000 WHERE id>=1000',(S-100,))
  source=self.source();old=source.select_ids;calls=[]
  def measured(lo,hi,**kw):calls.append((lo,hi));return old(lo,hi,**kw)
  source.select_ids=measured
  try:
   with self.owner() as store:
    with self.assertRaisesRegex(Refusal,'work_unit_too_dense'):acquire(source,store,self.profile,target_height=T,window_blocks=10000)
    self.assertIn((S-100,S-100),calls);self.assertIsNone(store.meta('canonical_ready'));self.assertFalse(store.completed(0,'prerequisites','complete'))
  finally:c.execute('UPDATE tx SET block_id=CASE id%%3 WHEN 0 THEN %s WHEN 1 THEN %s ELSE %s END,block_index=(id/3)+10 WHERE id>=1000',(S-100,S-50,S-1))
