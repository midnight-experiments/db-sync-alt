import copy
import hashlib
import json
import unittest
from unittest.mock import patch
from db_sync_alt.sources.blockfrost.cbor import decode_transaction
from db_sync_alt.sources.blockfrost.adapter import amounts, classify_inputs, epoch_facts, inventory, output_value, pool_hash, ordinary_outputs
from db_sync_alt.contract import Refusal, canonical
from fixtures.cbor import F, body, tx
from fixtures.blockfrost import CountingProvider as ProviderClient, Fake, POOL

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
        return {'/metrics':[], '/epochs/4':{'epoch':4,'active_stake':'19'},'/epochs/4/parameters':{'epoch':4,'nonce':'cc'*32,'max_tx_size':12000},'/epochs/4/blocks?count=1&page=1&order=desc':['aa'*32],'/blocks/'+'aa'*32:block,'/epochs/4/stakes?count=100&page=1&order=asc':[{'stake_address':'stake_test1a','pool_id':POOL,'amount':'10'},{'stake_address':'stake_test1b','pool_id':POOL,'amount':'9'}]}
    def test_epoch_complete_twice(self):
        p=ProviderClient(30,Fake(self.epoch_routes()));nonce,stakes,anchor,receipt=epoch_facts(p,4,stake_prefix='stake_test1');self.assertEqual(stakes[0]['amount'],'19');self.assertEqual(receipt['stake_addresses'],2);self.assertEqual(nonce['nonce'],b'\xcc'*32)
        self.assertEqual(sum('/stakes?' in x for x in p.client.calls),2)
        # The size comes from the two parameter reads epoch_facts already makes: no extra request.
        self.assertEqual(nonce,{'epoch':4,'nonce':b'\xcc'*32,'max_tx_size':12000});self.assertEqual(receipt['max_tx_size'],12000)
        self.assertEqual(sum(x=='/epochs/4/parameters' for x in p.client.calls),2)
        self.assertEqual(len(p.client.calls),11)
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
        for mutate in ('null','duplicate','missing','changed'):
            routes=self.epoch_routes();key='/epochs/4/stakes?count=100&page=1&order=asc'
            if mutate=='null':routes['/epochs/4']['active_stake']=None
            elif mutate=='duplicate':routes[key].append(routes[key][0])
            elif mutate=='missing':routes[key].pop()
            else:
                values=[routes[key],[dict(routes[key][0],stake_address='stake_test1changed'),routes[key][1]]];routes[key]=lambda:values.pop(0)
            with self.assertRaises(Refusal):epoch_facts(ProviderClient(30,Fake(routes)),4,stake_prefix='stake_test1')


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

class DatumTests(unittest.TestCase):
    """Datums of relevant outputs are stored as cardano-db-sync 13.8 stores them; none refuses a ledger-valid block."""
    def source(self, routes, stored=None):
        from types import SimpleNamespace
        from db_sync_alt.sources.blockfrost.adapter import BlockfrostSource
        source = BlockfrostSource.__new__(BlockfrostSource)
        stored = stored or {}
        source.facts, source.provider = [], ProviderClient(50, Fake({'/metrics': [], **routes}))
        source.store = SimpleNamespace(fact=lambda table, key: stored.get((table, key)), output_assets=lambda h, i: [],
                                       output_generation=lambda h, i: 1, bridge_covered=lambda generation: False)
        source.profile = SimpleNamespace(required=lambda p, n: False, kept_assets=lambda address, assets, covered: {})
        return source

    def datums(self, source):
        return {r['hash']: r['value'] for t, r in source.facts if t == 'datums'}

    def requests(self, source):
        return [p for p in source.provider.client.calls if p != '/metrics']

    def test_hash_without_preimage_on_chain_stores_nothing(self):
        from fixtures.blockfrost import NOT_FOUND
        h = hashlib.blake2b(b'\x43abc', digest_size=32).digest()
        source = self.source({'/scripts/datum/' + h.hex() + '/cbor': NOT_FOUND})
        source.datum(h)
        self.assertEqual(self.datums(source), {})
        self.assertEqual(self.requests(source), ['/scripts/datum/' + h.hex() + '/cbor'])

    def test_value_past_the_nesting_limit_is_null(self):
        raw = b'\x81' * 1000 + b'\x00'
        h = hashlib.blake2b(raw, digest_size=32).digest()
        source = self.source({})
        source.datum(h, raw.hex())
        self.assertEqual(self.datums(source), {h: None})
        self.assertEqual(self.requests(source), [])

    def test_value_comes_from_the_cbor_never_from_blockfrost_json(self):
        """Blockfrost rounds integers beyond 2^53 and serves NULL as {}: its JSON is never asked for."""
        cases = [(bytes.fromhex('1b0020000000000001'), {'int': 2**53 + 1}),
                 (bytes.fromhex('d866821b002000000000000180'), {'constructor': 2**53 + 1, 'fields': []}),
                 (b'\xc2\x4d' + (123456789012345678901234567890).to_bytes(13, 'big'), {'int': 123456789012345678901234567890}),
                 (b'\xa2\x01\x00\x01\x00', {'map': [{'k': {'int': 1}, 'v': {'int': 0}}] * 2})]
        for raw, value in cases:
            with self.subTest(raw=raw.hex()):
                h = hashlib.blake2b(raw, digest_size=32).digest()
                lossy = json.loads(json.dumps(value).replace(str(2**53 + 1), str(2**53)))
                source = self.source({'/scripts/datum/' + h.hex() + '/cbor': {'cbor': raw.hex()}, '/scripts/datum/' + h.hex(): {'json_value': lossy}})
                source.datum(h)
                self.assertEqual(self.datums(source), {h: canonical(value)})
                self.assertEqual(self.requests(source), ['/scripts/datum/' + h.hex() + '/cbor'])

    def test_malformed_or_mismatched_datum_still_refuses(self):
        raw = b'\x82\x00'
        h = hashlib.blake2b(raw, digest_size=32).digest()
        with self.assertRaisesRegex(Refusal, 'cbor_truncated'):
            self.source({}).datum(h, raw.hex())
        with self.assertRaisesRegex(Refusal, 'provider_datum_hash_mismatch'):
            self.source({}).datum(b'\x00' * 32, raw.hex())

    def test_a_spend_stores_a_datum_first_seen_with_it(self):
        raw = b'\xd8\x79\x80'
        h = hashlib.blake2b(raw, digest_size=32).digest()
        old = {'tx_hash': b'\x01' * 32, 'output_index': 0, 'address': 'addr_test1fixture', 'value': '5', 'datum_hash': h}
        routes = {'/scripts/datum/' + h.hex() + '/cbor': {'cbor': raw.hex()}}
        source = self.source(routes, {('outputs', (b'\x01' * 32, 0)): old})
        observed = {'address': 'addr_test1fixture', 'amount': [{'unit': 'lovelace', 'quantity': '5'}], 'data_hash': h.hex(), 'inline_datum': None}
        source.origin(b'\x01' * 32, 0, observed)
        self.assertEqual(self.datums(source), {h: canonical({'constructor': 0, 'fields': []})})
        # Already stored: no request.
        source = self.source(routes, {('outputs', (b'\x01' * 32, 0)): old, ('datums', (h,)): {'hash': h, 'value': '{}'}})
        source.origin(b'\x01' * 32, 0, observed)
        self.assertEqual((self.datums(source), self.requests(source)), ({}, []))


if __name__=='__main__':unittest.main()
