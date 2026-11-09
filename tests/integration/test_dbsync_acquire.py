"""Owned real db-sync-shaped PG -> owned canonical PG; no live source calls."""
import copy
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from db_sync_alt.contract import Profile, Refusal
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.dbsync.acquire import CountedSQLSource,acquire
from db_sync_alt.verify.completion import validate_completion

ROOT=Path(__file__).resolve().parents[2]
S=4500197;T=S+3;W=T+2160
H=lambda n:hashlib.sha256(str(n).encode()).digest()
ATTEST={'dbsync_version':'13.7.1.0','tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}

class SQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        cls.profile=Profile(ROOT/'profiles/preprod-node-1.0.400')
        cls.tmp=tempfile.TemporaryDirectory();cls.servicefile=Path(cls.tmp.name)/'pg_service.conf'
        cls.old=os.environ.get('PGSERVICEFILE');os.environ['PGSERVICEFILE']=str(cls.servicefile)
        cls.dbs=[];cls.roles=[]
        cls.source_dsn,cls.source_service=cls.database()
        cls.source_db=psycopg.connect(cls.source_dsn,autocommit=True)
        cls.fill(cls.source_db)
        # A second db-sync database with identical content: a different source.
        cls.twin_dsn,cls.twin_service=cls.database()
        with psycopg.connect(cls.twin_dsn,autocommit=True) as twin:cls.fill(twin)
    @classmethod
    def fill(cls,c):
        c.execute((ROOT/'tests/fixtures/data/dbsync_schema.sql').read_text())
        c.execute("ALTER TABLE block ADD COLUMN previous_id bigint; ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint; CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL);CREATE TABLE meta(network_name varchar NOT NULL);INSERT INTO meta VALUES('preprod')")
        with c.transaction():
            for h,e in [(S-100,273),(S-50,274),(S-1,275)]+[(h,275) for h in range(S,W+1)]:
                c.execute("INSERT INTO block(id,hash,block_no,epoch_no,slot_no,time,tx_count,previous_id) VALUES(%s,%s,%s,%s,%s,'2026-01-01',10,%s)",(h,H(h),h,e,h,h-1))
            for n,h,i in [(1,S-1,0),(2,S-1,1),(3,S,4),(4,S+1,0),(5,S+2,0),(6,S+3,0),(7,S+4,0)]:c.execute('INSERT INTO tx VALUES(%s,%s,%s,%s,true)',(n,H('tx'+str(n)),h,i))
            p=cls.profile;c.execute('INSERT INTO multi_asset VALUES(1,%s,%s)',(bytes.fromhex(p.night[:56]),bytes.fromhex(p.night[56:])))
            c.execute('INSERT INTO datum VALUES(1,%s,%s)',(H('datum'),'{"int":7}'))
            for oid,tx,address,datum in [(1,1,'old-token',None),(2,2,p.mapping,H('datum')),(3,3,'new-token',None)]:c.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(%s,%s,0,%s,2000000,%s)',(oid,tx,address,datum))
            for n,o in [(1,1),(2,3)]:c.execute('INSERT INTO ma_tx_out VALUES(%s,1000,%s,1)',(n,o))
            c.execute('INSERT INTO tx_in VALUES(1,4,1,0),(2,5,2,0),(3,7,3,0); INSERT INTO ma_tx_mint VALUES(1,-7,6,1)')
            c.execute('INSERT INTO pool_hash VALUES(1,%s)',(b'p'*28,))
            for e in (273,274,275):
                c.execute('INSERT INTO epoch_stake_progress VALUES(%s,true)',(e,));c.execute('INSERT INTO epoch_param VALUES(%s,%s)',(e,H('epoch'+str(e))));c.execute('INSERT INTO epoch_stake VALUES(%s,1,123456,%s)',(e,e))
    @classmethod
    def database(cls):
        name='db_sync_alt_test_sql_'+uuid.uuid4().hex[:10];cls.dbs.append(name)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        dsn=psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
        with cls.servicefile.open('a') as out:
            out.write('['+name+']\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(dsn).items())+'\n')
        return dsn,name
    @classmethod
    def tearDownClass(cls):
        cls.source_db.close()
        for name in cls.dbs:cls.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        for name in cls.roles:cls.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(name)))
        cls.admin.close()
        if cls.old is None:os.environ.pop('PGSERVICEFILE',None)
        else:os.environ['PGSERVICEFILE']=cls.old
        cls.tmp.cleanup()
    def source(self):return CountedSQLSource(self.source_service,self.profile,copy.deepcopy(ATTEST))
    def owner(self):
        dsn,alias=self.database();return PgStore(conninfo=dsn,create=True,network='preprod')
    def run_acquire(self,store,target=T,**kw):return acquire(self.source(),store,self.profile,target_height=target,**kw)
    def test_fresh_and_increment_preserve_ids_and_certify_nonempty_families(self):
        with self.owner() as store:
            started=time.monotonic();self.run_acquire(store,S)
            print('ACQUISITION_TIMING '+json.dumps({'phase':'fresh','metadata_blocks':2161,'seconds':time.monotonic()-started}),flush=True)
            ids=store.db.execute('SELECT hash,id FROM chain.transactions ORDER BY id').fetchall();before=store.digests(0)
            # Replace only the provisional tail; committed S and all prior IDs stay fixed.
            self.source_db.execute("UPDATE block SET hash=decode(md5(block_no::text)||md5(block_no::text),'hex') WHERE block_no>%s",(S,))
            try:
                started=time.monotonic();result=self.run_acquire(store,T)
            finally:
                with self.source_db.cursor() as cur:cur.executemany('UPDATE block SET hash=%s WHERE block_no=%s',[(H(h),h) for h in range(S+1,W+1)])
            self.assertNotEqual(store.block_at(1,S+1)['hash'],store.block_at(0,S+1)['hash'])
            print('ACQUISITION_TIMING '+json.dumps({'phase':'increment','metadata_blocks':2163,'seconds':time.monotonic()-started}),flush=True)
            self.assertEqual(result['generation'],1);self.assertEqual(result['digests']['inputs']['count'],2);self.assertEqual(result['digests']['mints']['count'],1)
            self.assertEqual(store.db.execute('SELECT hash,id FROM chain.transactions WHERE first_generation=0 ORDER BY id').fetchall(),ids)
            self.assertEqual(store.digests(0),before);self.assertEqual(store.state()['publication_serial'],2)
            self.assertEqual(store.generation()['source'],'cardano-db-sync-postgresql');verify_store(store)
            seal=validate_completion(store,store.generation(),result['digests'],require_seal=True);self.assertIsNotNone(seal)
            self.assertLess(result['queries'],300)
    def test_interrupted_checkpoint_and_sealed_resume_preserve_manifest(self):
        with self.owner() as store:
            stop=threading.Event()
            def checkpoint(phase,**fields):
                if phase=='metadata':stop.set()
            with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop,progress=checkpoint)
            self.assertIsNone(store.meta('canonical_ready'));self.assertTrue(store.checkpoint_rows(phase='metadata'))
            dsn=store.db.info.dsn
        with PgStore(conninfo=dsn,allow_incomplete=True) as store:
            stop=threading.Event()
            def sealed(phase,**fields):
                if phase=='sealed':stop.set()
            with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop,progress=sealed)
            saved=store.pending()['manifest'];original=store.checkpoint_rows(phase='final-anchors')
            self.assertIn('completion',saved);self.assertIsNone(store.meta('canonical_ready'))
        with PgStore(conninfo=dsn,allow_incomplete=True) as store:
            self.run_acquire(store);self.assertEqual(store.generation()['manifest'],saved);self.assertEqual(store.checkpoint_rows(phase='final-anchors'),original)
            self.assertEqual(len(store.checkpoint_rows(phase='completion-revalidation')),1)
    def test_direct_publication_cannot_remove_required_completion(self):
        with self.owner() as store:
            stop=threading.Event()
            with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop,progress=lambda phase,**kw:stop.set() if phase=='sealed' else None)
            m=copy.deepcopy(store.pending()['manifest']);m.pop('completion')
            with self.assertRaisesRegex(Refusal,'completion_mismatch'):store.publish(0,m)
            store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=0',(Jsonb(m),))
            store.db.execute("DELETE FROM chain.checkpoints WHERE generation=0 AND phase='final-anchors'")
            with self.assertRaisesRegex(Refusal,'completion_invalid'):store.publish(0,m)
            self.assertIsNone(store.meta('canonical_ready'))
    def test_independent_selector_catches_omitted_normal_inventory(self):
        with self.owner() as store:
            source=self.source();old=source.select_ids
            def omitted(lo,hi,*,independent=False):
                a,b,c=old(lo,hi,independent=independent)
                return (a,b,c) if independent else ([],b,c)
            source.select_ids=omitted
            with self.assertRaisesRegex(Refusal,'independent_source_mismatch'):acquire(source,store,self.profile,target_height=T)
            self.assertIsNone(store.meta('canonical_ready'))
    def test_wrong_target_namespace_source_and_unselected_extra_refuse(self):
        with self.owner() as store:
            stop=threading.Event()
            with self.assertRaisesRegex(Refusal,'interrupted'):self.run_acquire(store,stop_event=stop,progress=lambda *a,**k:stop.set())
            with self.assertRaisesRegex(Refusal,'resume_target_mismatch'):self.run_acquire(store,T-1)
            # The same content in another db-sync database is a different source.
            twin=CountedSQLSource(self.twin_service,self.profile,copy.deepcopy(ATTEST))
            with self.assertRaisesRegex(Refusal,'resume_namespace_mismatch'):acquire(twin,store,self.profile,target_height=T)
            from db_sync_alt.cli.acquire import blockfrost_once
            with self.assertRaisesRegex(Refusal,'no_ready_generation'):blockfrost_once(store,self.profile,target_height=T)
            self.assertIsNone(store.meta('canonical_ready'))
    def test_source_writable_wrongnetwork_and_same_database_refuse(self):
        source=self.source()
        with self.assertRaisesRegex(Refusal,'source_query_failed'):
            with source.snapshot():source.conn.execute("UPDATE meta SET network_name='mainnet'")
        with self.owner() as store:
            self.source_db.execute("UPDATE meta SET network_name='mainnet'")
            try:
                with self.assertRaisesRegex(Refusal,'source_network_mismatch'):self.run_acquire(store)
            finally:self.source_db.execute("UPDATE meta SET network_name='preprod'")
            same=self.source();same.service=store.db.info.dbname
            with self.assertRaisesRegex(Refusal,'source_equals_destination'):acquire(same,store,self.profile,target_height=T)
    def test_cli_create_without_fixture_imports(self):
        dsn,alias=self.database();att=Path(self.tmp.name)/'att.json';att.write_text(json.dumps(ATTEST))
        code="""import sys,importlib.abc
class NoFixtures(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname.split('.')[0]=='fixtures':raise RuntimeError('test fixture import forbidden')
sys.meta_path.insert(0,NoFixtures())
import db_sync_alt.cli.acquire as pg_acquire
raise SystemExit(pg_acquire.main(sys.argv[1:]))
"""
        argv=[sys.executable,'-c',code,'--source','db-sync','--network','preprod','--destination-service',alias,'--create','--profile-dir',str(ROOT/'profiles/preprod-node-1.0.400'),'--source-service',self.source_service,'--source-attestation',str(att),'--target-height',str(T),'--reserve','/tmp:1']
        result=subprocess.run(argv,capture_output=True,text=True,timeout=60)
        self.assertEqual(result.returncode,0,result.stderr[-1500:]);self.assertEqual(json.loads(result.stdout)['target'],T)
        with PgStore(conninfo=dsn,readonly=True) as store:verify_store(store)

    def test_sealed_generation_does_not_reveal_the_source(self):
        from db_sync_alt.backup import backup
        from db_sync_alt.sources.dbsync.acquire import physical_identity
        with self.source_db.cursor() as cur:
            system=physical_identity(cur)['system_identifier']
        with self.owner() as store:
            self.run_acquire(store)
            m=store.generation()['manifest']
            self.assertEqual(set(m['source_identity']),{'adapter','network','schema','dbsync_version','retention','binding'})
            self.assertEqual({k:v for k,v in m['source_identity'].items() if k!='binding'},
                             {'adapter':'db-sync-direct-pg-v1','network':'preprod','schema':[15,48,6],'dbsync_version':'13.7.1.0','retention':{'tx_in':True,'epoch_stake':True}})
            self.assertRegex(m['source_identity']['binding'],'^[0-9a-f]{64}$')
            self.assertEqual(m['completion']['semantic']['source_identity'],m['source_identity'])
            for anchors in (m['anchors'],m['completion']['semantic']['anchors']):
                for anchor in anchors.values():self.assertNotIn('source_id',anchor)
            receipts=store.checkpoint_rows(generation=0)
            for row in receipts:
                self.assertNotIn('snapshot',row['receipt'])
                if row['phase'] in ('prerequisites','metadata','events','epochs'):self.assertIs(row['receipt']['read_only'],True)
            dsn=store.db.info.dsn
        folder=Path(self.tmp.name)/uuid.uuid4().hex;folder.mkdir()
        backup(folder/'state.dump',folder/'state.json',conninfo=dsn)
        dump=subprocess.run(['pg_restore','-f','-',str(folder/'state.dump')],capture_output=True,text=True,check=True).stdout
        for text in (dump,(folder/'state.json').read_text()):
            self.assertNotIn(system,text)
            self.assertNotIn('txid',text);self.assertNotIn('"snapshot"',text);self.assertNotIn('namespace_witnesses',text)
            self.assertIsNone(re.search(r'"\d+:\d+:[\d,]*"',text))
    def test_floor_loss_and_direct_uncontracted_creation_refuse(self):
        with self.owner() as store:
            with self.assertRaisesRegex(Refusal,'sql_acquisition_contract_required'):
                store.begin_generation(source='cardano-db-sync-postgresql',start=self.profile.start,target=T,witness=W,manifest={})
            self.assertIsNone(store.pending())
            self.run_acquire(store)
            before=store.generation()['manifest']
            for action in ('DELETE FROM chain.metadata WHERE key=\'sql_completion_required_from\'', "UPDATE chain.metadata SET value='99' WHERE key='sql_completion_required_from'"):
                with store.db.transaction(force_rollback=True):
                    store.db.execute(action)
                    with self.assertRaisesRegex(Refusal,'completion_invalid'):verify_store(store)
            self.assertEqual(store.generation()['manifest'],before)

    def test_independent_inventory_rejects_extra_natural_fact(self):
        with self.owner() as store:
            injected=False
            def extra(phase,**fields):
                nonlocal injected
                if phase=='events' and not injected:
                    store.insert('datums',{'hash':H('extra-datum'),'value':'{"int":123}'},fields['generation']);injected=True
            with self.assertRaisesRegex(Refusal,'independent_source_extra_fact'):self.run_acquire(store,progress=extra)
            self.assertIsNone(store.meta('canonical_ready'))

    def test_independent_rejects_extra_binding_of_old_physical_prestart_header(self):
        with self.owner() as store:
            self.run_acquire(store,S)
            before=store.digests(0)
            # Fixture-only retained physical header, outside the parent's
            # certified membership and all independently selected dependencies.
            extra=H('retained-unrelated-prestart')
            store.db.execute("INSERT INTO chain.blocks(hash,height,parent_hash,epoch,slot,time,tx_count,first_generation) VALUES(%s,%s,NULL,274,%s,'2026-01-01',0,0)",(extra,S-25,S-25))
            self.assertEqual(store.digests(0),before)
            def bind(phase,**fields):
                if phase=='events':store.bind_block(fields['generation'],S-25,extra)
            with self.assertRaisesRegex(Refusal,'independent_membership_extra'):
                self.run_acquire(store,T,progress=bind)
            self.assertEqual(store.meta('canonical_ready'),0)
            self.assertEqual(store.digests(0),before)
            self.assertEqual(store.fact('blocks',{'hash':extra})['height'],S-25)

    def test_malformed_independent_receipts_refuse_before_finalization(self):
        with self.owner() as store:
            original=store.finalize_pending;attempts=[]
            def checked(generation,manifest,receipt,computed):
                before=store.pending();checkpoints=store.checkpoint_rows()
                variants=[]
                for field,value in [('version',True),('scope','fixture'),('parent_digest','f'*64),('ranges',[]),('epochs',[]),('inventory',{})]:
                    m=copy.deepcopy(manifest);r=copy.deepcopy(receipt)
                    m['validation']['independent'][field]=value;r['independent'][field]=value;variants.append((m,r))
                m=copy.deepcopy(manifest);r=copy.deepcopy(receipt)
                m['validation'].pop('independent');r.pop('independent');variants.append((m,r))
                m=copy.deepcopy(manifest);r=copy.deepcopy(receipt)
                m['validation']['independent']['inventory']['blocks']['count']=True;r['independent']=copy.deepcopy(m['validation']['independent']);variants.append((m,r))
                for m,r in variants:
                    with self.assertRaisesRegex(Refusal,'completion_invalid'):original(generation,m,r,computed)
                    self.assertEqual(store.pending(),before);self.assertEqual(store.checkpoint_rows(),checkpoints)
                    attempts.append(1)
                return original(generation,manifest,receipt,computed)
            store.finalize_pending=checked
            self.run_acquire(store)
            self.assertEqual(len(attempts),8);verify_store(store)

    def test_source_and_independent_windows_split_without_overlap(self):
        with self.owner() as store:
            source=self.source();old=source.blocks;splits=[]
            def bounded(lo,hi):
                if hi-lo+1>600:
                    splits.append((lo,hi));raise Refusal('source_timeout')
                yield from old(lo,hi)
            source.blocks=bounded
            acquire(source,store,self.profile,target_height=T,window_blocks=1000)
            self.assertGreaterEqual(len(splits),4)
            receipts=store.checkpoint_rows(generation=0,phase='metadata')
            self.assertTrue(all(r['receipt']['hi']-r['receipt']['lo']+1<=600 for r in receipts))
            verify_store(store)

if __name__=='__main__':unittest.main(verbosity=2)

# The db-sync source below is built from a fixture store by test code; each
# acquisition itself runs only the service's PostgreSQL code.
class InterchangeTests(unittest.TestCase):
    incremental = False
    @classmethod
    def setUpClass(cls):
        import fixtures.database as integration
        cls.integration=integration;integration.SyncFixture.setUpClass()
    @classmethod
    def tearDownClass(cls):cls.integration.SyncFixture.tearDownClass()
    def test_sql_blockfrost_sql_epoch_transition_and_old_namespace_ids(self):
        it=self.integration;fixture=it.SyncFixture();fixture.setUp()
        old=os.environ.get('PGSERVICEFILE');tmp=tempfile.TemporaryDirectory()
        try:
            seed=fixture.make(transition=True);routes,_=it.gen_fixture.fixtures(transition=True)
            with PgStore(conninfo=seed) as prepared:
                fixture.run_driver(prepared,routes)
                source_dsn=fixture.database();dest=fixture.database()
                with psycopg.connect(source_dsn,autocommit=True) as db:
                    db.execute((ROOT/'tests/fixtures/data/dbsync_schema.sql').read_text())
                    db.execute("ALTER TABLE block ADD COLUMN previous_id bigint;ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint;CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);CREATE TABLE meta(network_name varchar NOT NULL);INSERT INTO meta VALUES('preprod');CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL)")
                    # Distinct source IDs demonstrate namespace independence.
                    blocks=list(prepared.rows('blocks',1));block_ids={r['hash']:r['height']+123 for r in blocks}
                    original_blocks=copy.deepcopy(blocks)
                    # Full db-sync retains predecessors of sparse selected headers.
                    missing={r['parent_hash'] for r in blocks if r['parent_hash'] and r['parent_hash'] not in block_ids}
                    self.assertEqual(missing,{bytes.fromhex(it.gen_fixture.block(7)['hash'])})
                    predecessor=it.gen_fixture.block_row(it.gen_fixture.block(7,transition=True))
                    blocks.append(predecessor);block_ids[predecessor['hash']]=predecessor['height']+123
                    txs=list(prepared.rows('transactions',1));tx_ids={r['hash']:i+77 for i,r in enumerate(txs)}
                    assets=list(prepared.rows('assets',1));asset_ids={(r['policy'],r['name']):i+500 for i,r in enumerate(assets)}
                    outs=list(prepared.rows('outputs',1));out_ids={(r['tx_hash'],r['output_index']):i+800 for i,r in enumerate(outs)}
                    with db.transaction():
                        for r in blocks:db.execute('INSERT INTO block(id,hash,epoch_no,slot_no,block_no,time,tx_count,previous_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)',(block_ids[r['hash']],r['hash'],r['epoch'],r['slot'],r['height'],r['time'],r['tx_count'],block_ids.get(r['parent_hash'])))
                        for r in txs:db.execute('INSERT INTO tx VALUES(%s,%s,%s,%s,true)',(tx_ids[r['hash']],r['hash'],block_ids[r['block_hash']],r['tx_index']))
                        for i,r in enumerate(prepared.rows('datums',1)):db.execute('INSERT INTO datum VALUES(%s,%s,%s)',(i+1,r['hash'],Jsonb(json.loads(r['value']))))
                        for r in outs:db.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(%s,%s,%s,%s,%s,%s)',(out_ids[(r['tx_hash'],r['output_index'])],tx_ids[r['tx_hash']],r['output_index'],r['address'],r['value'],r['datum_hash']))
                        for r in assets:db.execute('INSERT INTO multi_asset VALUES(%s,%s,%s)',(asset_ids[(r['policy'],r['name'])],r['policy'],r['name']))
                        for i,r in enumerate(prepared.rows('output_assets',1)):db.execute('INSERT INTO ma_tx_out VALUES(%s,%s,%s,%s)',(i+1,r['quantity'],out_ids[(r['tx_hash'],r['output_index'])],asset_ids[(r['policy'],r['name'])]))
                        for i,r in enumerate(prepared.rows('inputs',1)):db.execute('INSERT INTO tx_in VALUES(%s,%s,%s,%s)',(i+1,tx_ids[r['tx_hash']],tx_ids[r['origin_hash']],r['origin_index']))
                        for i,r in enumerate(prepared.rows('mints',1)):db.execute('INSERT INTO ma_tx_mint VALUES(%s,%s,%s,%s)',(i+1,r['quantity'],tx_ids[r['tx_hash']],asset_ids[(r['policy'],r['name'])]))
                        pools={r['pool_hash'] for r in prepared.rows('epoch_stakes',1)};pool_ids={p:i+1 for i,p in enumerate(sorted(pools))}
                        for pool,pid in pool_ids.items():db.execute('INSERT INTO pool_hash VALUES(%s,%s)',(pid,pool))
                        for r in prepared.rows('epoch_nonces',1):db.execute('INSERT INTO epoch_param VALUES(%s,%s)',(r['epoch'],r['nonce']));db.execute('INSERT INTO epoch_stake_progress VALUES(%s,true)',(r['epoch'],))
                        for i,r in enumerate(prepared.rows('epoch_stakes',1)):db.execute('INSERT INTO epoch_stake VALUES(%s,%s,%s,%s)',(i+1,pool_ids[r['pool_hash']],r['amount'],r['epoch']))
                expected=prepared.digests(1)
            cfg=Path(tmp.name)/'pg_service.conf';cfg.write_text('[source]\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(source_dsn).items()));os.environ['PGSERVICEFILE']=str(cfg)
            source=lambda:CountedSQLSource('source',fixture.profile,ATTEST)
            from db_sync_alt.cli.acquire import blockfrost_once
            check=source()
            with check.snapshot():
                observed={r['hash']:r for r in check.blocks(min(r['height'] for r in original_blocks),max(r['height'] for r in original_blocks))}
            self.assertEqual({r['hash']:r for r in original_blocks},observed)
            with PgStore(conninfo=dest,create=True,network='preprod') as store:
                acquire(source(),store,fixture.profile,target_height=it.gen_fixture.OFFSET+10)
                if self.incremental:
                    from db_sync_alt.store.upgrade import upgrade_store
                    upgrade_store(store);store.certify_ready_full()
                old_ids=store.db.execute('SELECT hash,id FROM chain.transactions ORDER BY id').fetchall();old_digest=store.digests(0)
                # Too-high fixed target must not create lower acquisition job.
                with self.assertRaisesRegex(Exception,'unconfirmed_target'):
                    blockfrost_once(store,fixture.profile,target_height=it.gen_fixture.OFFSET+13,client=it.AccountedFake(routes))
                self.assertIsNone(store.sync_job('provider-active'));self.assertIsNone(store.pending());self.assertEqual(store.meta('canonical_ready'),0)
                blockfrost_once(store,fixture.profile,target_height=it.gen_fixture.OFFSET+11,client=it.AccountedFake(routes))
                self.assertEqual(store.generation()['source'],'blockfrost-cardano')
                acquire(source(),store,fixture.profile,target_height=it.gen_fixture.OFFSET+12)
                self.assertEqual(store.digests(2),expected);self.assertEqual(store.digests(0),old_digest)
                self.assertEqual(store.db.execute('SELECT hash,id FROM chain.transactions WHERE first_generation=0 ORDER BY id').fetchall(),old_ids)
                self.assertEqual(store.generation()['manifest']['required_epochs'],[0,1,2,3])
                self.assertTrue(any(r['receipt'].get('epoch_end',{}).get('height')==it.gen_fixture.OFFSET+11 for r in store.checkpoint_rows(phase='epochs',unit='2') if r['receipt'].get('epoch_end')))
                verify_store(store)
                if self.incremental:
                    # Both publication paths kept the served tables exact; db-sync reports the stored ordinals.
                    from db_sync_alt.store import serve
                    serve.verify(store,store.generation()['id'])
                    inputs={tuple(bytes(v) if isinstance(v,memoryview) else v for v in r[:3]):r[3] for r in store.db.execute('SELECT tx_hash,origin_hash,origin_index,ordinal FROM chain.inputs')}
                    assets={tuple(bytes(v) if isinstance(v,memoryview) else v for v in r[:4]):r[4] for r in store.db.execute('SELECT tx_hash,output_index,policy,name,ordinal FROM chain.output_assets')}
                    self.assertTrue(inputs and assets)
                    anchors={k:store.generation()['manifest']['anchors'][k] for k in ('start','target')}
                    self.assertEqual(source().ordinals(sorted(inputs),sorted(assets),anchors),{'inputs':inputs,'output_assets':assets})
                    with self.assertRaisesRegex(Exception,'source_anchor_conflict'):
                        source().ordinals([],[],{'start':dict(anchors['start'],hash='00'*32),'target':anchors['target']})
        finally:
            if old is None:os.environ.pop('PGSERVICEFILE',None)
            else:os.environ['PGSERVICEFILE']=old
            tmp.cleanup();fixture.tearDown()

class IncrementalInterchangeTests(InterchangeTests):
    incremental = True
