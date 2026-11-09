"""Fresh pending admission must retain every original mutation fence on real PostgreSQL."""
import json
import os
from pathlib import Path
import unittest
import uuid
from unittest.mock import patch
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from db_sync_alt.contract import Refusal
from db_sync_alt.store.database import PgStore
from fixtures.generations import PROFILE,block,h


def original_pending(store,generation):
    # Full-row reference check, exercised against the same PostgreSQL schema.
    store.require_writer()
    if store.generation(generation)['status']!='pending' or store.meta('source_conflict'):raise Refusal('immutable_generation')


class RecordingConnection:
    def __init__(self,db):self.db=db;self.reads=[]
    def __getattr__(self,key):return getattr(self.db,key)
    def execute(self,query,*args,**kwargs):
        cur=self.db.execute(query,*args,**kwargs)
        if isinstance(query,str) and query.startswith('SELECT'):
            self.reads.append({'query':query,'columns':tuple(c.name for c in cur.description),'bytes':sum(len(cur.pgresult.get_value(r,c) or b'') for r in range(cur.pgresult.ntuples) for c in range(cur.pgresult.nfields))})
        return cur


class PendingCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
    @classmethod
    def tearDownClass(cls):cls.admin.close()
    def setUp(self):self.databases=[]
    def tearDown(self):
        for name in self.databases:self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
    def owner(self,manifest=None):
        name='pending_check_'+uuid.uuid4().hex[:10];self.databases.append(name);self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        dsn=psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name);s=PgStore(conninfo=dsn,create=True,network='preprod')
        s.begin_generation(source='blockfrost-cardano',start=PROFILE.start,target=PROFILE.start[0],witness=PROFILE.start[0]+PROFILE.security,manifest={} if manifest is None else manifest)
        return s
    def result(self,fn):
        try:return ('ok',fn())
        except Refusal as error:return ('refusal',error.code)
    def compare(self,scenario):
        outcomes=[]
        for reference in (True,False):
            with self.owner() as s:
                if reference:s.require_pending=lambda generation:original_pending(s,generation)
                outcomes.append(scenario(s))
        self.assertEqual(*outcomes);return outcomes[0]
    def test_none_zero_missing_ready_and_short_circuit_order(self):
        def scenario(s):
            out=[];out.append(self.result(lambda:s.require_pending(None)));out.append(self.result(lambda:s.require_pending(0)));out.append(self.result(lambda:s.require_pending(99)))
            s.db.execute('UPDATE chain.service_state SET canonical_ready=0 WHERE id=1');out.append(self.result(lambda:s.require_pending(None)))
            s.db.execute("UPDATE chain.generations SET status='ready' WHERE id=0")
            original=s.meta
            def meta(key):
                if key=='source_conflict':raise AssertionError('must short circuit before revocation')
                return original(key)
            with patch.object(s,'meta',side_effect=meta):
                out.append(self.result(lambda:s.require_pending(0)));out.append(self.result(lambda:s.require_pending(None)));out.append(self.result(lambda:s.require_pending(99)))
            return out
        out=self.compare(scenario);self.assertEqual(out,[('refusal','no_ready_generation'),('ok',None),('refusal','generation_missing'),('ok',None),('refusal','immutable_generation'),('refusal','immutable_generation'),('refusal','generation_missing')])
    def test_revocation_and_status_are_fresh_inside_and_outside_atomic(self):
        def scenario(s):
            out=[]
            with psycopg.connect(s.db.info.dsn,autocommit=True) as external:
                for atomic in (False,True):
                    def operations():
                        out.append(self.result(lambda:s.require_pending(0)));external.execute('UPDATE chain.service_state SET revoked=true WHERE id=1');out.append(self.result(lambda:s.require_pending(0)));external.execute('UPDATE chain.service_state SET revoked=false WHERE id=1')
                        external.execute("UPDATE chain.generations SET status='ready' WHERE id=0");out.append(self.result(lambda:s.require_pending(0)));external.execute("UPDATE chain.generations SET status='pending' WHERE id=0");out.append(self.result(lambda:s.require_pending(0)))
                    if atomic:
                        with s.atomic():operations()
                    else:operations()
            return out
        out=self.compare(scenario);self.assertEqual(out,[('ok',None),('refusal','immutable_generation'),('refusal','immutable_generation'),('ok',None)]*2)
    def test_writer_closed_readonly_and_lost_ownership_precede_queries(self):
        for reference in (True,False):
            with self.owner() as s:
                fn=lambda generation:original_pending(s,generation) if reference else PgStore.require_pending(s,generation)
                s._writer=False
                self.assertEqual(self.result(lambda:fn(99)),('refusal','writer_ownership_required'))
                s._writer=True
                with PgStore(conninfo=s.db.info.dsn,readonly=True,allow_incomplete=True) as reader:
                    self.assertEqual(self.result(lambda:original_pending(reader,0) if reference else reader.require_pending(0)),('refusal','writer_ownership_required'))
                s.close();s._writer=True
                self.assertEqual(self.result(lambda:fn(0)),('refusal','writer_ownership_required'))
    def test_projection_constant_bytes_and_generation_full_api_unchanged(self):
        manifest=json.loads((Path(__file__).parents[1]/'fixtures/data/completion-g3-manifest.json').read_text())['generation']['manifest']
        with self.owner(manifest) as s:
            real=s.db;rec=RecordingConnection(real);s.db=rec
            for body in ({},manifest,{'unrelated_manifest_fields':['malformed',None,123]},None):
                real.execute('UPDATE chain.generations SET manifest=%s WHERE id=0',(Jsonb(body),));before=len(rec.reads);s.require_pending(0);reads=rec.reads[before:];generation=[r for r in reads if 'FROM chain.generations' in r['query']];self.assertEqual([(r['columns'],r['bytes']) for r in generation],[(('status',),7)])
                self.assertEqual([r['query'] for r in reads],['SELECT status FROM chain.generations WHERE id=%s',"SELECT to_regclass('chain.service_state')",'SELECT * FROM chain.service_state WHERE id=1'])
                self.assertEqual(s.generation(0)['manifest'],body);self.assertIn('manifest',rec.reads[-1]['columns'])
            s.db=real
    def test_duplicates_ordinals_conflicts_and_atomic_rollback_match(self):
        def scenario(s):
            b=block(PROFILE.start[0]);s.insert('blocks',b,0);s.bind_block(0,b['height'],b['hash']);origin=h('pending-origin');spend=h('pending-spend')
            for key,index in [(origin,0),(spend,1)]:s.insert('transactions',{'hash':key,'block_hash':b['hash'],'tx_index':index,'valid':1},0)
            out={'tx_hash':origin,'output_index':0,'address':'fixture','value':'1','datum_hash':None};s.insert('outputs',out,0)
            policy=bytes.fromhex(PROFILE.night[:56]);asset={'policy':policy,'name':bytes.fromhex(PROFILE.night[56:])};s.insert('assets',asset,0)
            ia={'tx_hash':spend,'origin_hash':origin,'origin_index':0,'ordinal':0};oa={'tx_hash':origin,'output_index':0,**asset,'quantity':'1','ordinal':0};s.insert('inputs',ia,0);s.insert('output_assets',oa,0)
            before=s.digests(0);results=[s.insert('outputs',out,0),s.insert('inputs',ia,0),s.insert('output_assets',oa,0)]
            for table,row in [('outputs',dict(out,value='2')),('inputs',dict(ia,ordinal=1)),('output_assets',dict(oa,quantity='2')),('inputs',dict(ia,ordinal=True))]:results.append(self.result(lambda:s.insert(table,row,0)))
            try:
                with s.atomic():s.insert('outputs',dict(out,output_index=1),0);s.insert('outputs',dict(out,value='2'),0)
            except Refusal as error:results.append(('rollback',error.code))
            self.assertIsNone(s.fact('outputs',(origin,1)));self.assertEqual(s.digests(0),before);return results,before,s.ordinal('inputs',ia),s.ordinal('output_assets',oa)
        result=self.compare(scenario);self.assertEqual(result[0][:3],[False]*3);self.assertEqual(result[2:],(0,0))
