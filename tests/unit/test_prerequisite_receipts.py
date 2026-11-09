import copy
import unittest
from db_sync_alt.contract import Refusal
from db_sync_alt.verify.prerequisites import chunk_receipt,seal_receipt,prerequisite_coverage

class Receipts(unittest.TestCase):
 def setUp(self):
  self.g={'id':0,'parent':None,'source':'cardano-db-sync-postgresql','start_height':10,'start_index':3,'target_height':11,'witness_height':2171,'manifest':{'acquisition':{'version':2,'window_blocks':6},'source_identity':{'adapter':'db-sync-direct-pg-v1','binding':'a'*64},'identity':{'profile':'test-profile'},'anchors':{'start':{'height':10,'hash':'b'*64}}}}
  self.rows=[]
 def checkpoint_rows(self,**kw):return self.rows
 def completed(self,*a):return any(r['unit']=='complete' for r in self.rows)
 def chunk(self,lo,hi):
  r={'generation':0,'phase':'prerequisites','unit':f'{lo}:{hi}','receipt':chunk_receipt(self.g,lo,hi,0)};self.rows.append(r);return r
 def state(self,required=True):return prerequisite_coverage(self,self.g,require_complete=required)
 def seal(self):
  chunks=self.state(False)['chunks'];self.rows.append({'generation':0,'phase':'prerequisites','unit':'complete','receipt':seal_receipt(self.g,chunks)})
 def test_partial_and_full_chunks_need_distinct_complete_seal(self):
  self.chunk(0,3);self.assertEqual(self.state(False)['position'],4)
  with self.assertRaisesRegex(Refusal,'prerequisites_incomplete'):self.state()
  self.chunk(4,9)
  with self.assertRaisesRegex(Refusal,'prerequisites_incomplete'):self.state()
  self.seal();self.assertTrue(self.state()['complete'])
 def test_bounds_units_gaps_overlaps_types_and_bindings(self):
  for change in [{'lo':True},{'hi':True},{'lo':-1},{'hi':10},{'hi':6},{'rows':False},{'read_only':1},{'version':True},{'binding':'f'*64},{'source_kind':'foreign'},{'kind':'complete'},{'extra':'field'}]:
   with self.subTest(change=change):
    self.rows=[];r=self.chunk(0,5);r['receipt'].update(change)
    with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.state(False)
  for ranges in [[(1,5)],[(0,3),(3,5)],[(0,3),(5,9)]]:
   self.rows=[]
   for lo,hi in ranges:self.chunk(lo,hi)
   with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.state(False)
  self.rows=[];r=self.chunk(0,3);r['unit']='00:3'
  with self.assertRaises(Refusal):self.state(False)
 def test_false_or_stale_completion_and_changed_generation_identity_refuse(self):
  self.chunk(0,3);self.seal()
  with self.assertRaisesRegex(Refusal,'prerequisites_receipt_invalid'):self.state()
  self.rows=[];self.chunk(0,3);self.chunk(4,9);self.seal();self.rows[-1]['receipt']['chunk_count']=True
  with self.assertRaises(Refusal):self.state()
  for field in ['identity','anchors','source_identity','acquisition']:
   self.rows=[];self.chunk(0,3);old=copy.deepcopy(self.g['manifest'][field]);self.g['manifest'][field]={**old,'changed':True}
   with self.assertRaises(Refusal):self.state(False)
   self.g['manifest'][field]=old
 def test_legacy_complete_and_malformed_whole_receipts(self):
  self.g['manifest']['acquisition']['version']=1;self.assertFalse(self.state(False)['complete'])
  good={'lo':0,'hi':9,'rows':0,'read_only':True,'source_kind':self.g['source']}
  self.rows=[{'generation':0,'phase':'prerequisites','unit':'complete','receipt':good}];self.assertTrue(self.state()['complete'])
  for change in [{'lo':True},{'hi':8},{'read_only':False},{'rows':True},{'source_kind':'foreign'},{'version':2},{'snapshot':'earlier-writer-snapshot'},{'snapshot':{'read_only':True}}]:
   self.rows[0]['receipt']={**good,**change}
   with self.assertRaises(Refusal):self.state()
 def test_block_zero_and_children_have_no_new_prerequisites(self):
  for start,parent in [(0,None),(10,0)]:
   self.g.update(start_height=start,parent=parent);self.rows=[];self.assertTrue(self.state()['complete']);self.chunk(0,0)
   with self.assertRaises(Refusal):self.state()
if __name__=='__main__':unittest.main()
