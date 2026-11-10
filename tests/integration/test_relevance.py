import copy
import unittest
from unittest.mock import patch

from db_sync_alt.contract import Refusal
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.relevance import qualified_parent
from fixtures import relevance as f
from fixtures.blockfrost import FixtureHooks

class RelevancePGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):f.SQLFixture.setup()
    @classmethod
    def tearDownClass(cls):f.SQLFixture.teardown()
    def prepare(self,rows):
        store,routes=f.SQLFixture.prepare(rows);self.addCleanup(store.close);return store,routes
    def test_supported_v1_and_v2_sql_roots_keep_filtering_facts_and_ordinals_equal(self):
        created=f.make_tx(19,relevant=True,mint=3)
        spent=f.make_tx(20,inputs=[(bytes.fromhex(created['tx_hash']),0)],mint=-1)
        irrelevant=f.make_tx(21);rows=[[created,spent,irrelevant]];digests=[];ordinals=[]
        for version in (1,2):
            with self.subTest(root_version=version):
                store,routes=f.SQLFixture.prepare(rows,root_version=version)
                self.addCleanup(store.close)
                self.assertEqual(store.generation()['manifest']['acquisition']['version'],version)
                self.assertIsNotNone(qualified_parent(store,f.PROFILE))
                _,calls=f.SQLFixture.follow(store,routes,f.S+1)
                verify_store(store);digests.append(store.digests(1))
                self.assertNotIn('/txs/'+irrelevant['tx_hash']+'/utxos',calls)
                self.assertEqual(store.generation()['manifest']['relevance_coverage']['root']['generation'],0)
                values=(store.ordinal('inputs',(bytes.fromhex(spent['tx_hash']),bytes.fromhex(created['tx_hash']),0)),
                        store.ordinal('output_assets',(bytes.fromhex(created['tx_hash']),0,f.P,f.N)))
                self.assertEqual(values,(0,0));ordinals.append(values)
        self.assertEqual(digests[0],digests[1]);self.assertEqual(ordinals[0],ordinals[1])
    def test_irrelevant_transactions_never_fetch_utxos_and_are_independently_enumerated(self):
        rows=[f.make_tx(i+20) for i in range(51)];store,routes=self.prepare([rows])
        self.assertIsNotNone(qualified_parent(store,f.PROFILE));result,calls=f.SQLFixture.follow(store,routes,f.S+1)
        self.assertTrue(result);self.assertEqual(sum('/txs/cbor?' in p for p in calls),2)
        for row in rows:self.assertNotIn('/txs/'+row['tx_hash']+'/utxos',calls)
        self.assertIn('relevance_coverage',store.generation()['manifest']);verify_store(store)
    def test_new_outputs_same_block_cross_block_and_pre_start_spends(self):
        created=f.make_tx(21,relevant=True);same=f.make_tx(22,inputs=[(bytes.fromhex(created['tx_hash']),0)],relevant=True)
        cross=f.make_tx(23,inputs=[(bytes.fromhex(same['tx_hash']),0)])
        pre=f.make_tx(24,inputs=[(bytes.fromhex(f.PRE_SPEND['tx_hash']),0)],mint=-1)
        root=f.make_tx(25,inputs=[(bytes.fromhex(f.ROOT['tx_hash']),0)])
        store,routes=self.prepare([[created,same],[cross,root,pre]])
        f.SQLFixture.follow(store,routes,f.S+2);verify_store(store)
        for row in (same,cross,root,pre):self.assertIsNotNone(store.fact('inputs',(bytes.fromhex(row['tx_hash']),row['body'][0][0][0],0)))
        self.assertEqual(store.generation()['target_height'],f.S+2)
    def test_gate_omission_cannot_hide_from_independent_discovery(self):
        row=f.make_tx(30,relevant=True);store,routes=self.prepare([[row]])
        from db_sync_alt.sources.blockfrost.relevance import Discovery
        original=Discovery.select
        with patch.object(Discovery,'select',lambda self,d:original(self,d) if self.independent else False):
            with self.assertRaisesRegex(Refusal,'independent_provider_mismatch'):f.SQLFixture.follow(store,routes,f.S+1)
        self.assertEqual(store.generation()['target_height'],f.S)
    def test_provider_output_omission_is_detected_against_cbor(self):
        row=f.make_tx(31,relevant=True);store,routes=self.prepare([[row]])
        routes['/txs/'+row['tx_hash']+'/utxos']['outputs'][0]['amount'].pop()
        with self.assertRaisesRegex(Refusal,'output_cbor_mismatch'):f.SQLFixture.follow(store,routes,f.S+1)
    def test_fresh_independent_provider_response_change_refuses(self):
        row=f.make_tx(32,relevant=True);store,routes=self.prepare([[row]])
        path='/txs/'+row['tx_hash']+'/utxos';first=copy.deepcopy(routes[path]);bad=copy.deepcopy(first);bad['outputs'][0]['amount'][0]['quantity']='99';values=[first,bad]
        routes[path]=lambda:values.pop(0)
        with self.assertRaisesRegex(Refusal,'output_cbor_mismatch'):f.SQLFixture.follow(store,routes,f.S+1)
    def test_resume_replays_completed_pending_inventory(self):
        first=f.make_tx(40,relevant=True);second=f.make_tx(41,inputs=[(bytes.fromhex(first['tx_hash']),0)])
        store,routes=self.prepare([[first],[second]])
        class Stop(FixtureHooks):
            def __init__(self):super().__init__();self.done=False
            def sample(self,store,**fields):
                if fields.get('phase')=='events':self.done=True
            def stopped(self):return self.done
        with self.assertRaisesRegex(Refusal,'interrupted'):f.SQLFixture.follow(store,routes,f.S+2,Stop())
        _,calls=f.SQLFixture.follow(store,routes,f.S+2);self.assertEqual(sum('/blocks/'+f.H(f.S+1).hex()+'/txs/cbor?' in p for p in calls),2);verify_store(store)
    def test_full_scan_reference_semantics_and_ordinals_match(self):
        from db_sync_alt.sources.blockfrost.adapter import update
        from fixtures.blockfrost import Fake
        created=f.make_tx(50,relevant=True,mint=3);spend=f.make_tx(51,inputs=[(bytes.fromhex(created['tx_hash']),0)],mint=-2)
        irrelevant=[f.make_tx(52+i) for i in range(8)];rows=[[created,*irrelevant],[spend]]
        store,routes=self.prepare(rows);f.SQLFixture.follow(store,routes,f.S+2)
        reference,_=self.prepare(rows);update(reference,f.Provider(50000,Fake(routes)),f.PROFILE,f.S+2,hooks=FixtureHooks())
        self.assertEqual(store.digests(store.generation()['id']),reference.digests(reference.generation()['id']))
        self.assertEqual(store.ordinal('inputs',(bytes.fromhex(spend['tx_hash']),bytes.fromhex(created['tx_hash']),0)),0)
    def test_cross_generation_and_fresh_session_keep_certified_coverage(self):
        from db_sync_alt.store.database import PgStore
        created=f.make_tx(70,relevant=True);spend=f.make_tx(71,inputs=[(bytes.fromhex(created['tx_hash']),0)])
        store,routes=self.prepare([[created],[spend]]);f.SQLFixture.follow(store,routes,f.S+1)
        dsn=store.db.info.dsn;store.close()
        with PgStore(conninfo=dsn) as restored:
            restored.certify_ready_full();self.assertIsNotNone(qualified_parent(restored,f.PROFILE));f.SQLFixture.follow(restored,routes,f.S+2);verify_store(restored)
            self.assertIsNotNone(restored.fact('inputs',(bytes.fromhex(spend['tx_hash']),bytes.fromhex(created['tx_hash']),0)))
    def test_candidate_omission_and_corruption_do_not_publish(self):
        for mode in ('omit','quantity'):
            row=f.make_tx(80,relevant=True);store,routes=self.prepare([[row]]);original=store.insert
            def insert(table,value,generation):
                if mode=='omit' and table in ('outputs','output_assets') and value['tx_hash']==bytes.fromhex(row['tx_hash']):return False
                if mode=='quantity' and table=='output_assets' and value['tx_hash']==bytes.fromhex(row['tx_hash']):value={**value,'quantity':'2'}
                return original(table,value,generation)
            with patch.object(store,'insert',side_effect=insert):
                with self.assertRaisesRegex(Refusal,'independent_provider_mismatch'):f.SQLFixture.follow(store,routes,f.S+1)
            self.assertEqual(store.generation()['target_height'],f.S)
    def test_invalid_collateral_return_and_collateral_spend_refuse(self):
        for row in (f.make_tx(90,valid=False,returned=[f.ADDRESS,[100,{f.P:{f.N:3}}]]),f.make_tx(91,valid=False,collateral=[(bytes.fromhex(f.ROOT['tx_hash']),0)])):
            store,routes=self.prepare([[row]])
            with self.assertRaisesRegex(Refusal,'unsupported_relevant_invalid'):f.SQLFixture.follow(store,routes,f.S+1)
    def test_reference_and_collateral_overlap_preserve_ordinary_input(self):
        key=(bytes.fromhex(f.ROOT['tx_hash']),0)
        row=f.make_tx(92,inputs=[key],collateral=[key]);store,routes=self.prepare([[row]])
        f.SQLFixture.follow(store,routes,f.S+1);self.assertEqual(store.ordinal('inputs',(bytes.fromhex(row['tx_hash']),*key)),0)
        only=f.make_tx(93,reference=[key]);other,paths=self.prepare([[only]])
        _,calls=f.SQLFixture.follow(other,paths,f.S+1);self.assertNotIn('/txs/'+only['tx_hash']+'/utxos',calls)
    def test_fresh_inventory_change_and_duplicates_refuse(self):
        for mode in ('reorder','duplicate','malformed'):
            first,second=f.make_tx(100),f.make_tx(101);store,routes=self.prepare([[first,second]])
            path='/blocks/'+f.H(f.S+1).hex()+'/txs/cbor?count=100&page=1&order=asc';valid=copy.deepcopy(routes[path])
            if mode=='reorder':
                values=[valid,list(reversed(valid))];routes[path]=lambda:values.pop(0)
            elif mode=='duplicate':routes[path]=[valid[0],valid[0]]
            else:routes[path][0]['cbor']='00'
            with self.assertRaises(Refusal):f.SQLFixture.follow(store,routes,f.S+1)
    def test_ready_parent_mutation_is_fenced_and_corrupt_certificate_refuses(self):
        import psycopg
        from psycopg.types.json import Jsonb
        row=f.make_tx(120,relevant=True);store,routes=self.prepare([[row],[]])
        with self.assertRaises(psycopg.errors.CheckViolation):store.db.execute('DELETE FROM chain.outputs WHERE tx_hash=%s',(bytes.fromhex(f.ROOT['tx_hash']),))
        f.SQLFixture.follow(store,routes,f.S+1)
        # Deliberate administrative corruption, outside the writer API: even
        # before a restarted full audit, the qualified proof must not fall back.
        bad=copy.deepcopy(store.checkpoint_rows(generation=1,phase='relevance',unit='coverage')[0]['receipt']);bad['profile']='00'*32
        store.db.execute('ALTER TABLE chain.checkpoints DISABLE TRIGGER USER')
        try:store.db.execute("UPDATE chain.checkpoints SET receipt=%s WHERE generation=1 AND phase='relevance'",(Jsonb(bad),))
        finally:store.db.execute('ALTER TABLE chain.checkpoints ENABLE TRIGGER USER')
        with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):f.SQLFixture.follow(store,routes,f.S+2)
    def test_unknown_output_address_uses_accounted_full_fallback(self):
        from fixtures.cbor import tx
        from db_sync_alt.sources.blockfrost.relevance import _encode_address
        pointer=b'\x41'+b'p'*28+b'\x01\x02\x03'
        row=f.make_tx(130);row['body'][1][0][0]=pointer
        row['cbor'],row['tx_hash']=tx(row['body']);store,routes=self.prepare([[row]])
        routes['/txs/'+row['tx_hash']+'/utxos']['outputs'][0]['address']=_encode_address(pointer,'mainnet')
        _,calls=f.SQLFixture.follow(store,routes,f.S+1)
        self.assertEqual(calls.count('/txs/'+row['tx_hash']+'/utxos'),2);verify_store(store)
    def test_provisional_fork_reuses_only_certified_committed_prefix(self):
        row=f.make_tx(140,relevant=True);store,routes=self.prepare([[row]])
        mapping={f.H(h).hex():f.H('fork'+str(h)).hex() for h in range(f.S+1,f.S+f.K+f.FUTURE+1)}
        def transform(value):
            if isinstance(value,list):return [transform(v) for v in value]
            if isinstance(value,dict):return {k:transform(v) for k,v in value.items()}
            if isinstance(value,str):return mapping.get(value,value)
            return value
        fork={}
        for path,value in routes.items():
            parts=path.split('/')
            if len(parts)>2 and parts[1]=='blocks':parts[2]=mapping.get(parts[2],parts[2])
            fork['/'.join(parts)]=transform(value)
        f.SQLFixture.follow(store,fork,f.S+1);verify_store(store)
        self.assertEqual(store.block_at(1,f.S)['hash'],store.block_at(0,f.S)['hash'])
        self.assertNotEqual(store.block_at(1,f.S+1)['hash'],store.block_at(0,f.S+1)['hash'])
    def test_extra_candidate_facts_are_not_source_completeness(self):
        row=f.make_tx(150);store,routes=self.prepare([[row]])
        class Extra(FixtureHooks):
            def sample(self,store,**fields):
                if fields.get('phase')=='events':
                    generation=store.pending()['id'];key=bytes.fromhex(row['tx_hash'])
                    with store.atomic():
                        store.insert('transactions',{'hash':key,'block_hash':f.H(f.S+1),'tx_index':0,'valid':1},generation)
                        store.insert('outputs',{'tx_hash':key,'output_index':0,'address':f.PROFILE.mapping,'value':'100','datum_hash':None},generation)
        with self.assertRaisesRegex(Refusal,'independent_provider_extra_fact'):f.SQLFixture.follow(store,routes,f.S+1,Extra())
    def test_unsealed_legacy_root_retains_counted_full_scan(self):
        from fixtures import blockfrost_generation as legacy
        from fixtures.blockfrost import CountingProvider,Fake
        from fixtures.cbor import tx
        from db_sync_alt.store.upgrade import upgrade_store
        from db_sync_alt.sources.blockfrost.follow import update_follow
        dsn,_=f.SQLFixture.database();store=legacy.baseline(dsn);self.addCleanup(store.close);upgrade_store(store);store.certify_ready_full()
        self.assertIsNone(qualified_parent(store,legacy.PROFILE))
        routes,_=legacy.fixtures();raw,key=tx({0:[],1:[],2:555});block=legacy.block(11)
        routes['/blocks/'+block['hash']+'/txs/cbor?count=100&page=1&order=asc']=[{'cbor':raw,'tx_hash':key}]
        routes['/txs/'+key+'/utxos']={'hash':key,'inputs':[],'outputs':[]}
        class Provider(CountingProvider):
            def get_optional(self,path):return self.get(path)
        client=Fake(routes);update_follow(store,Provider(10000,client),legacy.PROFILE,legacy.OFFSET+11,hooks=FixtureHooks())
        self.assertEqual(client.calls.count('/txs/'+key+'/utxos'),1);self.assertNotIn('relevance_coverage',store.generation()['manifest'])
