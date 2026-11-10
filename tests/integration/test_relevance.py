import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import time
import unittest
from unittest.mock import patch

from db_sync_alt.contract import Refusal
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.relevance import qualified_parent
from fixtures import relevance as f
from fixtures.blockfrost import FixtureHooks


def key(transaction,index=0):return (bytes.fromhex(transaction['tx_hash']),index)


def sweep_cases():
    """The ledger-valid transactions of issues 00031 and 00032, each alone, as (name, blocks, named).

    The pointer address has redundant leading zero groups in its slot and transaction index, which the
    ledger accepts from protocol version 9 and re-serialises minimally."""
    pointer=bytes([0x40|(1 if f.PROFILE.network.network=='mainnet' else 0)])+b'p'*28+b'\x80\x81\x00\x80\x02\x03'
    wallet=f.make_tx(160,outputs=[[f.OTHER,[200,{f.P:{f.N:5}}]]]);plain=f.make_tx(162)
    # Its body would spend the wallet's cNIGHT, pay cNIGHT to the mapping address and mint: none of it took effect.
    unrelated=f.make_tx(163,valid=False,inputs=[key(wallet)],relevant=True,mint=4,collateral=[key(plain)],returned=[f.OTHER,50])
    invalid=f.make_tx(164,valid=False,mint=2,collateral=[key(wallet)],returned=[f.OTHER,[150,{f.P:{f.N:5}}]])
    spend=f.make_tx(165,inputs=[key(invalid,1)])
    returned=f.make_tx(166,relevant=True,collateral=[key(wallet)],returned=[f.OTHER,[190,{f.P:{f.N:5}}]])
    pointed=f.make_tx(167,outputs=[[pointer,[100,{f.P:{f.N:1}}]]])
    return [('unrelated invalid',[[wallet,plain,unrelated]],dict(wallet=wallet,plain=plain,unrelated=unrelated)),
            ('relevant invalid',[[wallet,invalid],[spend]],dict(wallet=wallet,invalid=invalid,spend=spend)),
            ('collateral return with an asset',[[wallet,returned]],dict(wallet=wallet,returned=returned)),
            ('non-minimal pointer',[[pointed]],dict(pointed=pointed))]


def mainnet_shared_root(routes,directory):
    """An external accounting client (tests/fixtures/shared_client) for the relevance fixture's network."""
    root=Path(directory)
    shutil.copy(Path(__file__).resolve().parents[1]/'fixtures'/'shared_client'/'client.py',root/'client.py')
    (root/'profiles.json').write_text(json.dumps({f.PROFILE.network.network:{'base_url':f.PROFILE.network.blockfrost_base}}))
    (root/'answers').mkdir()
    for path,value in routes.items():(root/'answers'/hashlib.sha256(path.encode()).hexdigest()).write_bytes(json.dumps(value).encode())
    return root


class Transport:
    """HTTP transport of db-sync-alt's own client: every route answered as JSON bytes."""
    def __init__(self,routes):self.routes=routes
    def __call__(self,path):
        if path not in self.routes:return 404,{},b'{"status_code":404}'
        return 200,{},json.dumps(self.routes[path]).encode()

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
    def full(self,rows,client=None):
        from db_sync_alt.sources.blockfrost.adapter import update
        from fixtures.blockfrost import Fake
        store,routes=self.prepare(rows)
        update(store,f.Provider(50000,client(routes) if client else Fake(routes)),f.PROFILE,f.S+len(rows),hooks=FixtureHooks())
        return store
    def relevance(self,rows,client=None):
        from db_sync_alt.sources.blockfrost.follow import update_follow
        store,routes=self.prepare(rows)
        if client is None:f.SQLFixture.follow(store,routes,f.S+len(rows))
        else:update_follow(store,f.Provider(50000,client(routes)),f.PROFILE,f.S+len(rows),hooks=FixtureHooks(),relevance_enabled=True)
        return store
    def facts(self,store):
        """The facts the follow stored, in the shape cardano-db-sync has them."""
        rows=lambda query:store.db.execute(query).fetchall()
        return {'transactions':{bytes(h):v for h,v in rows('SELECT hash,valid FROM chain.transactions WHERE first_generation>0')},
                'outputs':{(bytes(h),i):(a,str(v)) for h,i,a,v in rows('SELECT tx_hash,output_index,address,value FROM chain.outputs WHERE first_generation>0')},
                'output_assets':{(bytes(h),i,bytes(p),bytes(n)):str(q) for h,i,p,n,q in rows('SELECT tx_hash,output_index,policy,name,quantity FROM chain.output_assets WHERE first_generation>0')},
                'inputs':{(bytes(h),bytes(o),i):n for h,o,i,n in rows('SELECT tx_hash,origin_hash,origin_index,ordinal FROM chain.inputs WHERE first_generation>0')},
                'mints':rows('SELECT tx_hash FROM chain.mints WHERE first_generation>0'),
                'served':{bytes(h):v for h,v in rows('SELECT hash,valid_contract FROM serve.tx WHERE block_id>%d'%f.S)}}
    def expected(self,named):
        """cardano-db-sync 13.8's rows for the sweep cases: a phase-2-invalid transaction holds only its collateral
        inputs (tx_in) and its collateral return (tx_out at the number of body outputs) with valid_contract false;
        a valid transaction's collateral return is never created; a pointer address is the ledger's canonical one."""
        from db_sync_alt.sources.blockfrost.relevance import address_text,_encode_address
        network=f.PROFILE.network.network;other=address_text(f.OTHER,network);h=lambda name:key(named[name])[0]
        out={'transactions':{},'outputs':{},'output_assets':{},'inputs':{},'mints':[],'served':{}}
        def tx(name,valid):out['transactions'][h(name)]=valid;out['served'][h(name)]=valid==1
        def output(name,index,address,value,quantity):
            out['outputs'][(h(name),index)]=(address,str(value));out['output_assets'][(h(name),index,f.P,f.N)]=str(quantity)
        if 'wallet' in named:tx('wallet',1);output('wallet',0,other,200,5)
        if 'invalid' in named:
            tx('invalid',0);output('invalid',1,other,150,5);out['inputs'][(h('invalid'),*key(named['wallet']))]=0
            tx('spend',1);out['inputs'][(h('spend'),*key(named['invalid'],1))]=0
        if 'returned' in named:tx('returned',1);output('returned',0,f.PROFILE.mapping,100,3)
        if 'pointed' in named:
            tx('pointed',1);output('pointed',0,_encode_address(bytes([named['pointed']['body'][1][0][0][0]])+b'p'*28+b'\x81\x00\x02\x03',network),100,1)
        return out
    def test_sweep_cases_ingest_on_both_adapters_in_full_and_relevance_mode_as_cardano_db_sync_stores_them(self):
        """Issues 00031 and 00032, all cases in one range: full and relevance mode store cardano-db-sync's rows and
        serve valid_contract as it does, through the in-process provider, db-sync-alt's own client (full mode) and
        the shared adapter's external client (relevance mode)."""
        from db_sync_alt.sources.blockfrost import provider
        cases=sweep_cases();named={}
        for _,_,more in cases:named.update(more)
        rows=[[*cases[0][1][0],cases[2][1][0][1],cases[1][1][0][1],cases[3][1][0][0]],cases[1][1][1]]
        def portable(routes):
            ledger=self.enterContext(tempfile.TemporaryDirectory());now=[time.time()]
            return provider.PortableClient(f.PROFILE.network.key_prefix+'synthetic0000000000000000',Path(ledger)/'accounting',network=f.PROFILE.network,
                                           transport=Transport(routes),clock=lambda:now[0],sleep=lambda seconds:now.__setitem__(0,now[0]+seconds))
        def shared(routes):
            return provider.SharedClient(str(mainnet_shared_root(routes,self.enterContext(tempfile.TemporaryDirectory()))),network=f.PROFILE.network)
        relevance=self.relevance(rows)
        self.assertEqual(self.facts(relevance),self.expected(named))
        verify_store(relevance);digests=relevance.digests(relevance.generation()['id'])
        for name,store in (('full',self.full(rows)),('full, own client',self.full(rows,portable)),('relevance, shared adapter',self.relevance(rows,shared))):
            with self.subTest(run=name):self.assertEqual(store.digests(store.generation()['id']),digests)
    def test_invalid_transaction_validity_must_agree_with_its_cbor(self):
        _,rows,named=sweep_cases()[1];store,routes=self.prepare(rows)
        routes['/txs/'+named['invalid']['tx_hash']]['valid_contract']=True
        with self.assertRaisesRegex(Refusal,'provider_transaction_mismatch'):f.SQLFixture.follow(store,routes,f.S+2)
        self.assertEqual(store.generation()['target_height'],f.S)
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


class InvalidTransactionSQLTests(unittest.TestCase):
    """A phase-2-invalid transaction in the cardano-db-sync source, as cardano-db-sync 13.8 stores it: tx with
    valid_contract false, its collateral input as tx_in, its collateral return as tx_out at the number of body
    outputs. It spends the relevant output of the root's PRE_SPEND and returns cNIGHT to a key address."""
    @classmethod
    def setUpClass(cls):
        from db_sync_alt.sources.blockfrost.relevance import address_text
        f.configure('mainnet');f.SQLFixture.setup()
        cls.invalid=f.make_tx(170,valid=False,collateral=[key(f.PRE_SPEND)],returned=[f.OTHER,[60,{f.P:{f.N:3}}]])
        cls.other=address_text(f.OTHER,'mainnet')
        c=f.SQLFixture.source
        with c.transaction():
            c.execute('INSERT INTO tx VALUES(10,%s,%s,2,false)',(key(cls.invalid)[0],f.S-1))
            c.execute('INSERT INTO tx_in VALUES(10,10,2,0)')
            c.execute('INSERT INTO tx_out(id,tx_id,index,address,value,data_hash) VALUES(10,10,1,%s,60,NULL)',(cls.other,))
            c.execute('INSERT INTO ma_tx_out VALUES(10,3,10,1)')
    @classmethod
    def tearDownClass(cls):f.SQLFixture.teardown()
    def prepare(self,rows):
        store,routes=f.SQLFixture.prepare(rows,history=[(f.S-1,2,self.invalid)]);self.addCleanup(store.close);return store,routes
    def test_sql_source_stores_and_serves_it_and_blockfrost_continues_from_it(self):
        spend=f.make_tx(171,inputs=[key(self.invalid,1)]);invalid=key(self.invalid)[0]
        store,routes=self.prepare([[spend]])
        self.assertEqual(store.fact('transactions',(invalid,))['valid'],0)
        self.assertEqual(store.ordinal('inputs',(invalid,*key(f.PRE_SPEND))),0)
        self.assertIsNone(store.fact('outputs',(invalid,0)))
        self.assertEqual(store.fact('outputs',(invalid,1))['address'],self.other)
        self.assertEqual([(bytes(r['policy']),bytes(r['name']),str(r['quantity'])) for r in store.output_assets(invalid,1)],[(f.P,f.N,'3')])
        self.assertEqual(store.db.execute('SELECT valid_contract FROM serve.tx WHERE hash=%s',(invalid,)).fetchone(),(False,))
        verify_store(store)
        # The hand-off check reads the stored invalid transaction back from Blockfrost; the spend of its return follows.
        _,calls=f.SQLFixture.follow(store,routes,f.S+1)
        self.assertIn('/txs/'+self.invalid['tx_hash']+'/utxos',calls)
        self.assertEqual(store.ordinal('inputs',(key(spend)[0],invalid,1)),0)
        verify_store(store)
        from db_sync_alt.sources.blockfrost.adapter import update
        from fixtures.blockfrost import Fake
        reference,paths=self.prepare([[spend]]);update(reference,f.Provider(50000,Fake(paths)),f.PROFILE,f.S+1,hooks=FixtureHooks())
        self.assertEqual(reference.digests(reference.generation()['id']),store.digests(store.generation()['id']))
