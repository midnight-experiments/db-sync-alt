import bisect
import copy
import json
import unittest
from unittest.mock import patch
from db_sync_alt.sources.blockfrost.cbor import decode_transaction
from db_sync_alt.sources.blockfrost.adapter import amounts, classify_inputs, epoch_facts, inventory, output_value, pool_hash, ordinary_outputs
from db_sync_alt.contract import Refusal
from fixtures.cbor import F, body, tx
from fixtures.blockfrost import CountingProvider as ProviderClient, Fake, POOL, pool_id

def out(h='61'*32,i=0):return {'tx_hash':h,'output_index':i,'address':'addr_test1fixture','amount':[{'unit':'lovelace','quantity':'3'}],'data_hash':None}
class ProviderTests(unittest.TestCase):
    def test_real_overlap_roles(self):
        r=json.loads((F/'block-cbor.json').read_text())[0];d=decode_transaction(r['cbor'],r['tx_hash']);u=json.loads((F/'utxos.json').read_text())
        self.assertEqual(len(ordinary_outputs(u['outputs'],d)),d['output_count']);self.assertTrue(any(r.get('collateral') for r in u['outputs']))
        ordinary=classify_inputs(u['inputs'],d);self.assertEqual(set(ordinary),d['ordinary']);self.assertTrue(set(ordinary)&d['collateral'])
        bad=copy.deepcopy(u['inputs']);bad.pop()
        with self.assertRaises(Refusal):classify_inputs(bad,d)
    def test_optional_reference_and_duplicate_roles(self):
        b=body();b[18]=[[b'b'*32,4]];d=decode_transaction(*tx(b));rows=[out(),out('62'*32,4)]
        self.assertEqual(set(classify_inputs(rows,d)),d['ordinary'])
        with self.assertRaises(Refusal):classify_inputs(rows+[rows[0]],d)
        rows[1]['reference']=False
        with self.assertRaises(Refusal):classify_inputs(rows,d)
    def test_input_only_pre_s(self):
        d=decode_transaction(*tx(body()));r=out();r['amount'].append({'unit':'70'*28,'quantity':'1'})
        self.assertEqual(set(classify_inputs([r],d)),{(b'a'*32,0)})
    def test_complete_inventory(self):
        raw,h=tx(body());row={'cbor':raw,'tx_hash':h};block={'hash':b'c'*32,'tx_count':1};path='/blocks/'+block['hash'].hex()+'/txs/cbor?count=100&page=1&order=asc'
        self.assertEqual(len(inventory(ProviderClient(5,Fake({'/metrics':[],path:[row]})),block)),1)
        for data in ([],[row,row]):
            with self.assertRaises(Refusal):inventory(ProviderClient(5,Fake({'/metrics':[],path:data})),block)
    def test_large_transaction_scan_never_writes_stdout(self):
        from types import SimpleNamespace
        import db_sync_alt.sources.blockfrost.adapter as module
        decoded=[{'hash':i.to_bytes(32,'big'),'ordinary':set(),'collateral':set(),'reference':set(),'valid':True,'mints':[],'output_count':0,'collateral_return':False} for i in range(51)]
        routes={'/txs/'+d['hash'].hex()+'/utxos':{'hash':d['hash'].hex(),'inputs':[],'outputs':[]} for d in decoded}
        source=module.BlockfrostSource(Fake(routes),SimpleNamespace(),SimpleNamespace(generation=lambda _: {'target_height':100,'manifest':{}}),1,lambda:False)
        with patch.object(module,'inventory',return_value=decoded),patch('builtins.print',side_effect=AssertionError('worker stdout write')):
            facts,stats=source.block_facts({'height':99})
        self.assertEqual(facts,[]);self.assertEqual(stats['transactions_scanned'],51)
    def test_amount_and_inline_integrity(self):
        r=out();r['amount'].append(r['amount'][0])
        with self.assertRaises(Refusal):amounts(r)
        r=out();r['inline_datum']='00';r['data_hash']='00'*32
        with self.assertRaises(Refusal):output_value(r)
    def test_pool_checksum(self):
        self.assertEqual(len(pool_hash(POOL)),28)
        with self.assertRaises(Refusal):pool_hash(POOL[:-1]+'q')
    def epoch_routes(self):
        block={'hash':'aa'*32,'height':99,'previous_block':'bb'*32,'epoch':4,'slot':500,'time':1660000000,'tx_count':1}
        return {'/metrics':[], '/epochs/4':{'epoch':4,'active_stake':'19'},'/epochs/4/parameters':{'epoch':4,'nonce':'cc'*32,'max_tx_size':12000},'/epochs/4/blocks?count=1&page=1&order=desc':['aa'*32],'/blocks/'+'aa'*32:block,
                '/epochs/latest':{'epoch':4},'/pools/extended?count=100&page=1&order=asc':[{'pool_id':POOL,'hex':'00'*28,'active_stake':'19'}],'/pools/retired?count=100&page=1&order=desc':[]}
    def test_epoch_complete_twice(self):
        p=ProviderClient(30,Fake(self.epoch_routes()));nonce,stakes,anchor,receipt=epoch_facts(p,4,stake_prefix='stake_test1');self.assertEqual(stakes,[{'epoch':4,'pool_hash':pool_hash(POOL),'amount':'19'}]);self.assertEqual(nonce['nonce'],b'\xcc'*32)
        self.assertEqual((receipt['stake_source'],receipt['stake_pools']),('pools-current',1))
        self.assertEqual(sum(x.startswith('/pools/extended?') for x in p.client.calls),2)
        self.assertFalse(any('/stakes' in x for x in p.client.calls))
        # The size comes from the two parameter reads epoch_facts already makes: no extra request.
        self.assertEqual(nonce,{'epoch':4,'nonce':b'\xcc'*32,'max_tx_size':12000});self.assertEqual(receipt['max_tx_size'],12000)
        self.assertEqual(sum(x=='/epochs/4/parameters' for x in p.client.calls),2)
        self.assertEqual(len(p.client.calls),15)
    def test_epoch_max_tx_size_domain(self):
        for value in (0,1,16384,2**31-1):
            routes=self.epoch_routes();routes['/epochs/4/parameters']['max_tx_size']=value
            self.assertEqual(epoch_facts(ProviderClient(30,Fake(routes)),4,stake_prefix='stake_test1')[0]['max_tx_size'],value)
        for value in (None,True,False,16384.0,'16384',-1,2**31):
            routes=self.epoch_routes();routes['/epochs/4/parameters']['max_tx_size']=value
            with self.assertRaisesRegex(Refusal,'invalid_epoch_max_tx_size'):epoch_facts(ProviderClient(30,Fake(routes)),4,stake_prefix='stake_test1')
        routes=self.epoch_routes();del routes['/epochs/4/parameters']['max_tx_size']
        with self.assertRaisesRegex(Refusal,'invalid_epoch_max_tx_size'):epoch_facts(ProviderClient(30,Fake(routes)),4,stake_prefix='stake_test1')
    def test_epoch_max_tx_size_changed_between_reads(self):
        routes=self.epoch_routes();first=routes['/epochs/4/parameters'];values=[first,dict(first,max_tx_size=12001)]
        routes['/epochs/4/parameters']=lambda:values.pop(0)
        with self.assertRaisesRegex(Refusal,'provider_epoch_changed'):epoch_facts(ProviderClient(30,Fake(routes)),4,stake_prefix='stake_test1')
    def test_epoch_null_duplicate_missing_changed(self):
        key='/pools/extended?count=100&page=1&order=asc'
        for mutate in ('null','duplicate','missing','changed','short','latest-ahead'):
            routes=self.epoch_routes();rows=routes[key]
            if mutate=='null':routes['/epochs/4']['active_stake']=None
            elif mutate=='duplicate':rows.append(dict(rows[0],active_stake='0'))
            elif mutate=='missing':rows.pop()
            elif mutate=='short':rows[0]['active_stake']='18'
            elif mutate=='latest-ahead':routes['/epochs/latest']={'epoch':3}
            else:
                other=pool_id(b'\x01'*28);values=[[dict(rows[0],active_stake='10'),{'pool_id':other,'active_stake':'9'}],[dict(rows[0],active_stake='9'),{'pool_id':other,'active_stake':'10'}]];routes[key]=lambda:values.pop(0)
            with self.subTest(mutate=mutate),self.assertRaises(Refusal) as caught:epoch_facts(ProviderClient(30,Fake(routes)),4,stake_prefix='stake_test1')
            expected={'null':'provider_epoch_not_ready','duplicate':'provider_duplicate_or_invalid_stake','missing':'provider_epoch_total_mismatch','short':'provider_epoch_total_mismatch','latest-ahead':'provider_epoch_not_ready','changed':'provider_epoch_changed'}
            self.assertEqual(caught.exception.code,expected[mutate])

class SyntheticEpoch:
    """Blockfrost's answers for one epoch of `addresses` delegated stake addresses over `pools` registered pools,
    rendered from one model as Blockfrost renders them: per address (/epochs/{E}/stakes), per pool for the current
    epoch (/pools/extended, /pools/{id}) and for a completed one (/pools/{id}/history), and the epoch total.

    `zero` registered pools have no delegators. Two pools retired inside the window (E - 1, E) still hold stake;
    one retired long before holds none. One pool's only delegators hold 0 lovelace (a row with a zero sum)."""
    def __init__(self,addresses,pools,*,current,zero=600,epoch=500):
        self.E,self.current=epoch,current
        raw=lambda i:i.to_bytes(28,'big')
        self.registered=[pool_id(raw(i)) for i in range(pools)]
        self.retired=[(pool_id(raw(10**6+1)),epoch),(pool_id(raw(10**6+2)),epoch-1),(pool_id(raw(10**6+3)),epoch-5)]
        staked=self.registered[:pools-zero-1]+[self.retired[0][0],self.retired[1][0]]
        self.zero_rows=self.registered[pools-zero-1]
        self.count={p:addresses//len(staked)+(1 if k<addresses%len(staked) else 0) for k,p in enumerate(staked)}
        self.count[self.zero_rows]=3
        self.amount={p:1000000+k%13 for k,p in enumerate(staked)};self.amount[self.zero_rows]=0
        self.sums={p:self.count[p]*self.amount[p] for p in self.count}
        self.order=list(self.count);self.total=sum(self.sums.values())
        self.first=[0]
        for p in self.order:self.first.append(self.first[-1]+self.count[p])
        self.calls=[]
    def expected(self):
        return [{'epoch':self.E,'pool_hash':pool_hash(p),'amount':str(v)} for p,v in sorted(self.sums.items(),key=lambda kv:pool_hash(kv[0]))]
    def page(self,rows,path):
        query=dict(x.split('=') for x in path.split('?',1)[1].split('&'));count=int(query.get('count',100));page=int(query.get('page',1))
        return rows[(page-1)*count:page*count]
    def get(self,path,purpose=None):
        self.calls.append(path);E=str(self.E);base=path.split('?')[0]
        block={'hash':'aa'*32,'height':99,'previous_block':'bb'*32,'epoch':self.E,'slot':500,'time':1660000000,'tx_count':1}
        if path=='/metrics':return []
        if path=='/epochs/latest':return {'epoch':self.current}
        if path=='/epochs/'+E:return {'epoch':self.E,'active_stake':str(self.total)}
        if path=='/epochs/'+E+'/parameters':return {'epoch':self.E,'nonce':'cc'*32,'max_tx_size':16384}
        if path=='/epochs/'+E+'/blocks?count=1&page=1&order=desc':return ['aa'*32]
        if path=='/blocks/'+'aa'*32:return block
        if base=='/epochs/'+E+'/stakes':  # the per-address listing the per-pool read never needs
            query=dict(x.split('=') for x in path.split('?',1)[1].split('&'));start=(int(query['page'])-1)*100;rows=[]
            at=bisect.bisect_right(self.first,start)-1
            for k in range(start,min(start+100,self.first[-1])):
                while k>=self.first[at+1]:at+=1
                p=self.order[at];rows.append({'stake_address':'stake_test1u%053d'%k,'pool_id':p,'amount':str(self.amount[p])})
            return rows
        if base.startswith('/epochs/'+E+'/stakes/'):
            p=base.rsplit('/',1)[1];return [{'stake_address':'stake_test1u%053d'%0,'amount':str(self.amount[p])}] if p in self.count else []
        if base=='/pools/extended':
            assert self.current==self.E
            return self.page([{'pool_id':p,'hex':pool_hash(p).hex(),'active_stake':str(self.sums.get(p,0))} for p in self.registered],path)
        if base=='/pools':return self.page(list(self.registered),path)
        if base=='/pools/retired':return self.page([{'pool_id':p,'epoch':e} for p,e in sorted(self.retired,key=lambda r:-r[1])],path)
        if base.endswith('/history'):
            assert self.current>self.E
            p=base.split('/')[2];rows=[{'epoch':self.E-1,'blocks':0,'active_stake':'5','active_size':0.0,'delegators_count':1,'rewards':'0','fees':'0'}]
            if p in self.count:rows.insert(0,{'epoch':self.E,'blocks':0,'active_stake':str(self.sums[p]),'active_size':0.0,'delegators_count':self.count[p],'rewards':'0','fees':'0'})
            return self.page(rows,path)
        if base.startswith('/pools/'):
            p=base.split('/')[2];return {'pool_id':p,'active_stake':str(self.sums.get(p,0))}
        raise AssertionError(path)


class StakeDistributionTests(unittest.TestCase):
    """An epoch's stake is read per pool: requests never grow with delegated addresses (Mainnet has ~1.3 M)."""
    def read(self,model):
        provider=ProviderClient(10000,model)
        nonce,stakes,anchor,receipt=epoch_facts(provider,model.E,stake_prefix='stake_test1')
        self.assertFalse([c for c in model.calls if c.startswith('/epochs/%d/stakes?'%model.E)])
        return stakes,receipt,provider.attempts
    def test_current_epoch_of_a_mainnet_sized_distribution_within_budget(self):
        model=SyntheticEpoch(1500000,3000,current=500)
        stakes,receipt,attempts=self.read(model)
        self.assertEqual(stakes,model.expected());self.assertEqual(receipt['stake_source'],'pools-current')
        self.assertEqual(receipt['stake_pools'],2402)
        # 2 x (31 list pages, the last one empty + 1 retired page + 2 retired pools + 601 zero checks)
        # + 2 x 5 header reads + 2 /metrics.
        self.assertEqual(attempts,2*(31+1+2+601)+10+2)
        self.assertEqual(self.read(SyntheticEpoch(3000000,3000,current=500))[2],attempts)
    def test_completed_epoch_of_a_mainnet_sized_distribution_within_budget(self):
        model=SyntheticEpoch(1500000,3000,current=501)
        stakes,receipt,attempts=self.read(model)
        self.assertEqual(stakes,model.expected());self.assertEqual(receipt['stake_source'],'pools-history')
        # 2 x (31 pool pages + 1 retired page + 3,002 histories) + 2 x 5 header reads + 7 /metrics.
        self.assertEqual(attempts,2*(31+1+3002)+10+7)
        self.assertLess(attempts,10000)
        self.assertEqual(self.read(SyntheticEpoch(3000000,3000,current=501))[2],attempts)
    def test_retired_then_registered_again_is_read_once(self):
        model=SyntheticEpoch(1000,30,current=500,zero=5);model.retired.append((model.registered[0],499))
        self.assertEqual(self.read(model)[0],model.expected())
    def test_history_row_beyond_the_first_page(self):
        model=SyntheticEpoch(1000,30,current=650,zero=5);get=model.get
        def history(path,purpose=None):
            if path.split('?')[0].endswith('/history'):
                model.calls.append(path);p=path.split('/')[2];rows=[{'epoch':e,'active_stake':'5','delegators_count':1} for e in range(649,500,-1)]
                if p in model.count:rows.append({'epoch':500,'active_stake':str(model.sums[p]),'delegators_count':model.count[p]})
                rows.append({'epoch':499,'active_stake':'5','delegators_count':1})
                return model.page(rows,path)
            return get(path,purpose)
        model.get=history
        stakes,receipt,_=self.read(model)
        self.assertEqual((stakes,receipt['stake_source']),(model.expected(),'pools-history'))
        self.assertTrue(any('/history?count=100&page=2&order=desc' in c for c in model.calls))
    def test_an_epoch_boundary_during_the_reads_waits(self):
        model=SyntheticEpoch(1000,30,current=500,zero=5);get=model.get;latest=[{'epoch':500},{'epoch':501}]
        model.get=lambda path,purpose=None:latest.pop(0) if path=='/epochs/latest' else get(path,purpose)
        with self.assertRaisesRegex(Refusal,'provider_epoch_changed'):self.read(model)
    def test_a_malformed_zero_stake_row_refuses(self):
        model=SyntheticEpoch(1000,30,current=500,zero=5);get=model.get
        model.get=lambda path,purpose=None:[{'stake_address':'stake1wrong','amount':'0'}] if model.zero_rows in path and '/stakes/' in path else get(path,purpose)
        with self.assertRaisesRegex(Refusal,'provider_duplicate_or_invalid_stake'):self.read(model)

if __name__=='__main__':unittest.main()

class CurrentEpochTests(unittest.TestCase):
    def test_current_epoch_tip_can_advance_above_target(self):
        routes=ProviderTests().epoch_routes();anchor=routes['/blocks/'+'aa'*32];second=dict(anchor,hash='dd'*32,height=100,previous_block='aa'*32)
        ends=[['aa'*32],['dd'*32]];routes['/epochs/4/blocks?count=1&page=1&order=desc']=lambda:ends.pop(0)
        routes['/blocks/'+'dd'*32]=second;routes['/blocks/98']=dict(anchor,hash='ee'*32,height=98)
        result=epoch_facts(ProviderClient(30,Fake(routes)),4,98,stake_prefix='stake_test1')
        self.assertEqual(result[2]['height'],98)


class CollateralOmissionTests(unittest.TestCase):
    def fixtures(self):
        c=json.loads((F/'collateral-omission-cbor.json').read_text());u=json.loads((F/'collateral-omission-utxos.json').read_text());origin=json.loads((F/'collateral-omission-origin.json').read_text());d=decode_transaction(c['cbor'],c['tx_hash'])
        def resolve(h,index):
            self.assertEqual(h.hex(),origin['hash']);return next(r for r in origin['outputs'] if r['output_index']==index)
        return u,d,resolve
    def test_actual_omission_requires_exact_original_output(self):
        u,d,resolve=self.fixtures()
        with self.assertRaisesRegex(Refusal,'conflicting_input'):classify_inputs(u['inputs'],d)
        ordinary=classify_inputs(u['inputs'],d,resolve)
        overlap=d['ordinary']&d['collateral'];self.assertTrue(overlap)
        for k in overlap:self.assertEqual(len(output_value(ordinary[k])[1]),4)
    def test_changed_asset_or_base_fields_still_refused(self):
        for kind in ('partial-native','changed-native','lovelace','address','missing-role'):
            u,d,resolve=self.fixtures();col=next(r for r in u['inputs'] if r['collateral']);normal=next(r for r in u['inputs'] if not r['collateral'] and r['tx_hash']==col['tx_hash'] and r['output_index']==col['output_index'])
            if kind=='partial-native':col['amount'].append(copy.deepcopy(normal['amount'][1]))
            elif kind=='changed-native':normal['amount'][1]['quantity']='1'
            elif kind=='lovelace':col['amount'][0]['quantity']='1'
            elif kind=='address':col['address']='addr_test1wrong'
            else:u['inputs'].remove(col)
            with self.assertRaises(Refusal):classify_inputs(u['inputs'],d,resolve)


class LedgerOrdinalTests(unittest.TestCase):
    def test_ordinals_follow_bytewise_ledger_order(self):
        from db_sync_alt.sources.blockfrost.adapter import ledger_ordinals
        inputs = [(bytes.fromhex('ff' * 32), 0), (bytes.fromhex('01' * 32), 7), (bytes.fromhex('01' * 32), 2), (bytes.fromhex('02' + '00' * 31), 0)]
        self.assertEqual(ledger_ordinals(inputs), {inputs[2]: 0, inputs[1]: 1, inputs[3]: 2, inputs[0]: 3})
        # Asset names compare bytewise, not by length.
        assets = [(b'p' * 28, b'b'), (b'p' * 28, b'ab'), (b'a' * 28, b'zz')]
        self.assertEqual(ledger_ordinals(assets), {assets[2]: 0, assets[1]: 1, assets[0]: 2})

    def test_stored_output_assets_carry_their_position_among_all_assets_of_the_output(self):
        from db_sync_alt.contract import Profile
        from db_sync_alt.resources import profile_dir
        from db_sync_alt.sources.blockfrost.adapter import BlockfrostSource
        profile = Profile(profile_dir('preprod'))
        units = ['00' * 28 + '01', profile.night, 'ff' * 28]
        for address, bridge, kept in (('addr_test1fixture', True, [profile.night]), (profile.ics, False, [profile.night]),
                                      (profile.ics, True, sorted(units))):
            source = BlockfrostSource.__new__(BlockfrostSource)
            source.facts, source.profile, source.bridge = [], profile, bridge
            row = {'output_index': 3, 'address': address, 'data_hash': None,
                   'amount': [{'unit': 'lovelace', 'quantity': '5'}] + [{'unit': u, 'quantity': '1'} for u in reversed(units)]}
            source.output(b'\x01' * 32, row)
            stored = sorted((r['policy'].hex() + r['name'].hex(), r['ordinal']) for t, r in source.facts if t == 'output_assets')
            # A bridge-covered generation keeps every asset of an ICS (or reserve) output; positions are among all assets.
            with self.subTest(address=address, bridge=bridge):
                self.assertEqual(stored, [(u, sorted(units).index(u)) for u in kept])
