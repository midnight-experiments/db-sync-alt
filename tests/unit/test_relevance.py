import copy
import json
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from db_sync_alt.contract import Profile,Refusal
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.cbor import decode_transaction,Tag
from db_sync_alt.sources.blockfrost.relevance import Discovery,address_bytes,address_text,canonical_address,compare_outputs,inventory
from fixtures.cbor import tx

class RelevanceTests(unittest.TestCase):
    def setUp(self):
        self.profile=Profile(profile_dir('mainnet'));self.raw=address_bytes(self.profile.mapping,'mainnet')
        self.policy=bytes.fromhex(self.profile.night[:56]);self.name=bytes.fromhex(self.profile.night[56:])
        self.retained=set();self.queries=[]
        def execute(query,params):
            self.assertIn('first_generation<=',query);self.assertEqual(params[-1],7);self.queries.append(params)
            return SimpleNamespace(fetchone=lambda:(1,) if params[:2] in self.retained else None)
        self.store=SimpleNamespace(db=SimpleNamespace(execute=execute))
    def decode(self,outputs=None,inputs=None,mint=None,valid=True,extra=None):
        body={0:inputs or [],1:outputs or [],2:3}
        if mint is not None:body[9]=mint
        body.update(extra or {})
        return decode_transaction(*tx(body,valid))
    def discovery(self,independent=False):return Discovery(self.store,self.profile,7,independent=independent)
    def row(self,unit=None):return {'output_index':0,'address':self.profile.mapping,'amount':[{'unit':'lovelace','quantity':'100'}]+([{'unit':unit,'quantity':'3'}] if unit else []),'data_hash':None}
    def test_all_packaged_address_checksums_roundtrip(self):
        for network in ('mainnet','preprod'):
            profile=Profile(profile_dir(network))
            for address in profile.addresses:self.assertEqual(address_text(address_bytes(address,network),network),address)
        bad=self.profile.mapping[:-1]+('q' if self.profile.mapping[-1]!='q' else 'p')
        with self.assertRaisesRegex(Refusal,'profile_address'):address_bytes(bad,'mainnet')
        with self.assertRaisesRegex(Refusal,'network_mismatch'):address_text(b'\x60'+b'a'*28,'mainnet')
    def test_exact_outputs_assets_and_inline_datum(self):
        raw,h=tx({0:[],1:[{0:self.raw,1:[100,{self.policy:{self.name:3}}],2:[1,Tag(24,b'\x01')]}],2:3})
        decoded=decode_transaction(raw,h);self.assertEqual(decoded['outputs'][0]['assets'],((self.policy,self.name,3),));self.assertTrue(self.discovery().select(decoded))
        self.assertIsNotNone(decoded['outputs'][0]['datum_hash'])
    def test_known_irrelevant_and_exact_mint_burn(self):
        other=b'\x61'+b'a'*28
        self.assertFalse(self.discovery().select(self.decode([[other,100]])))
        for quantity in (1,-1):self.assertTrue(self.discovery().select(self.decode(mint={self.policy:{self.name:quantity}})))
        self.assertFalse(self.discovery().select(self.decode(mint={b'x'*28:{b'other':1}})))
    def test_unknown_forms_never_negative(self):
        for output in ([[b'a',100]],[[self.raw,[100,{self.policy:{self.name:0}}]]],[[self.raw,True]]):self.assertTrue(self.discovery().select(self.decode(output)))
        decoded=self.decode(extra={16:[b'a',100]});self.assertTrue(self.discovery().select(decoded))
        other=b'\x61'+b'a'*28
        for output in ({0:other,1:100,2:[1,Tag(24,b'')]},{0:other,1:100,3:Tag(24,b'unknown')}):self.assertTrue(self.discovery().select(self.decode([output])))
    def test_parent_ordinary_spend_only_and_roles(self):
        key=(b'p'*32,0);self.retained.add(key)
        self.assertTrue(self.discovery().select(self.decode(inputs=[[key[0],key[1]]])))
        self.assertFalse(self.discovery().select(self.decode(extra={18:[[key[0],0]],13:[[key[0],0]]})))
        self.assertTrue(self.discovery().select(self.decode(valid=False,extra={13:[[key[0],0]]})))
    def test_same_and_cross_block_discovery_is_independent(self):
        normal,audit=self.discovery(),self.discovery(True)
        created=self.decode([[self.raw,100]])
        normal.advance(created,[self.row()]);audit.advance(created,[self.row()])
        spend=self.decode(inputs=[[created['hash'],0]])
        self.assertTrue(normal.select(spend));self.assertTrue(audit.select(spend))
        normal.begin();audit.begin();self.assertTrue(normal.select(spend));self.assertTrue(audit.select(spend))
        fresh=self.discovery(True);self.assertFalse(fresh.select(spend))
    def test_invalid_collateral_return_is_selected(self):
        decoded=self.decode(valid=False,extra={16:[self.raw,100]});self.assertTrue(self.discovery().select(decoded))
    def test_provider_cannot_omit_or_change_relevance(self):
        decoded=self.decode([[self.raw,[100,{self.policy:{self.name:3}}]]]);row=self.row(self.profile.night)
        compare_outputs(decoded,[row],'mainnet')
        for field in ('address','asset','quantity','datum'):
            bad=copy.deepcopy(row)
            if field=='address':bad['address']=address_text(b'\x61'+b'x'*28,'mainnet')
            elif field=='asset':bad['amount'].pop()
            elif field=='quantity':bad['amount'][0]['quantity']='99'
            else:bad['data_hash']='ff'*32
            with self.assertRaisesRegex(Refusal,'output_cbor_mismatch'):compare_outputs(decoded,[bad],'mainnet')
    def test_pointer_addresses_compare_in_the_ledger_canonical_form(self):
        """Issue 00032 (S10): from protocol version 9 the ledger accepts redundant leading zero groups in a pointer
        and keeps it re-serialised minimally, which cardano-db-sync renders."""
        head=b'\x41'+b'p'*28
        for suffix,canonical in ((b'\x01\x02\x03',b'\x01\x02\x03'),(b'\x80\x81\x00\x80\x02\x03',b'\x81\x00\x02\x03'),
                                 (b'\x80\x80\x80\x80\x01\x80\x80\x00\x80\x80\x07',b'\x01\x00\x07'),(b'\x8f\xff\xff\xff\x7f\x83\xff\x7f\x00',b'\x8f\xff\xff\xff\x7f\x83\xff\x7f\x00')):
            self.assertEqual(canonical_address(head+suffix),head+canonical)
        # Not a pointer the ledger accepts (slot or index too wide, too many groups, truncated, trailing bytes): unchanged.
        for suffix in (b'\x90\x80\x80\x80\x00\x00\x00',b'\x00\x84\x80\x00\x00',b'\x80\x80\x80\x80\x80\x01\x00\x00',b'\x01\x02',b'\x01\x02\x03\x04'):
            self.assertEqual(canonical_address(head+suffix),head+suffix)
        for raw in (self.raw,b'\x61'+b'a'*28,b'\x82\x00'):self.assertEqual(canonical_address(raw),raw)
        decoded=self.decode([[head+b'\x80\x81\x00\x80\x02\x03',[100,{self.policy:{self.name:3}}]]])
        from db_sync_alt.sources.blockfrost.relevance import _encode_address
        row=dict(self.row(self.profile.night),address=_encode_address(head+b'\x81\x00\x02\x03','mainnet'));compare_outputs(decoded,[row],'mainnet')
        row['address']=_encode_address(head+b'\x80\x81\x00\x80\x02\x03','mainnet')
        with self.assertRaisesRegex(Refusal,'output_cbor_mismatch'):compare_outputs(decoded,[row],'mainnet')
    def test_collateral_return_assets_are_not_listed_by_blockfrost(self):
        """Issue 00032 (S9), with a real Preprod answer (transaction 6fa3def7…): a valid transaction's collateral
        return is a `collateral` row with lovelace only. Its CBOR (rebuilt from the answer) returns the tokens of
        the collateral input."""
        from fixtures.cbor import F
        from db_sync_alt.sources.blockfrost.adapter import ledger_effects
        answer=json.loads((F/'collateral-return-assets-utxos.json').read_text())
        def value(amount):
            assets={}
            for a in amount[1:]:assets.setdefault(bytes.fromhex(a['unit'][:56]),{})[bytes.fromhex(a['unit'][56:])]=int(a['quantity'])
            return [int(amount[0]['quantity']),assets] if assets else int(amount[0]['quantity'])
        ordinary=next(r for r in answer['inputs'] if not r['collateral']);outpoint=[bytes.fromhex(ordinary['tx_hash']),ordinary['output_index']]
        address=lambda row:address_bytes(row['address'],'preprod',require_supported=False)
        body={0:[outpoint],1:[[address(r),value(r['amount'])] for r in answer['outputs'][:3]],2:281110,13:[outpoint],
              16:[address(answer['outputs'][3]),[int(answer['outputs'][3]['amount'][0]['quantity']),value(ordinary['amount'])[1]]]}
        decoded=decode_transaction(*tx(body))
        self.assertTrue(decoded['collateral_output']['assets'])
        compare_outputs(decoded,answer['outputs'],'preprod')
        spent,created,_=ledger_effects(answer,decoded,lambda h,index:ordinary)
        self.assertEqual((set(spent),[r['output_index'] for r in created]),({tuple(outpoint)},[0,1,2]))
        # Listing the return's tokens would still be the same output; other values are not.
        listed=copy.deepcopy(answer['outputs']);listed[3]['amount']=[listed[3]['amount'][0]]+copy.deepcopy(ordinary['amount'][1:]);compare_outputs(decoded,listed,'preprod')
        for change in ('lovelace','asset'):
            bad=copy.deepcopy(answer['outputs'])
            if change=='lovelace':bad[3]['amount'][0]['quantity']='1'
            else:bad[3]['amount'].append(dict(ordinary['amount'][1],quantity='1'))
            with self.assertRaisesRegex(Refusal,'output_cbor_mismatch'):compare_outputs(decoded,bad,'preprod')
    def test_bounded_new_stream_state(self):
        discovery=self.discovery()
        with patch('db_sync_alt.sources.blockfrost.relevance.MAX_KEYS',1):
            first=self.decode([[self.raw,100]]);discovery.advance(first,[self.row()])
            second=self.decode([[self.raw,101]])
            with self.assertRaisesRegex(Refusal,'work_unit_limit'):discovery.advance(second,[self.row()])
    def test_inventory_includes_ledger_ordinals_and_deduplicates(self):
        row={'tx_hash':b'a'*32,'origin_hash':b'b'*32,'origin_index':0,'ordinal':2}
        first=inventory([('inputs',row),('inputs',row)])
        self.assertEqual(first['inputs']['count'],1)
        self.assertNotEqual(first,inventory([('inputs',{**row,'ordinal':1})]))
    def test_mainnet_batch_fragment_preserves_defaults_and_security(self):
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        fragment=json.loads((Path(__file__).resolve().parents[2]/'deploy/mainnet-relevance-sync.json').read_text())
        configured=SyncDriver(SimpleNamespace(sync=fragment['sync']),profile=self.profile)
        self.assertEqual(configured.options['follow_min_blocks'],3);self.assertEqual(configured.options['follow_max_wait_seconds'],60)
        unchanged=SyncDriver(SimpleNamespace(sync={}),profile=self.profile)
        self.assertEqual(unchanged.options['follow_min_blocks'],1);self.assertEqual(unchanged.options['follow_max_wait_seconds'],40)
        self.assertEqual(self.profile.security,2160);self.assertEqual(self.profile.network.serving_lag_max_blocks,9)
