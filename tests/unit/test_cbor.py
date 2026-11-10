import hashlib
import json
import random
import unittest
from db_sync_alt.sources.blockfrost.cbor import Decoder, Tag, decode_datum, decode_transaction
from db_sync_alt.contract import Refusal
from fixtures.cbor import F, body, enc, tx



class DecoderTests(unittest.TestCase):
    def test_real_original_hash(self):
        records=json.loads((F/'block-cbor.json').read_text())
        for r in records:self.assertEqual(decode_transaction(r['cbor'],r['tx_hash'])['hash'].hex(),r['tx_hash'])
        d=decode_transaction(records[0]['cbor'],records[0]['tx_hash']);self.assertTrue(d['ordinary']&d['collateral']);self.assertEqual(d['mints'][0][2],1000)
    def test_tagged_roles_and_signed_mint(self):
        b=body();b.update({0:Tag(258,b[0]),13:[[b'a'*32,0]],18:[[b'b'*32,4]],9:{b'p'*28:{b'new':8,b'burn':-3}}})
        d=decode_transaction(*tx(b));self.assertEqual(d['ordinary'],d['collateral']);self.assertEqual(len(d['reference']),1);self.assertEqual(len(d['mints']),2)
    def test_hashes_exact_indefinite_body(self):
        raw=b'\xbf'+enc(0)+enc([[b'a'*32,0]])+enc(1)+enc([])+enc(2)+enc(3)+b'\xff'
        encoded=b'\x84'+raw+b'\xa0\xf5\xf6';h=hashlib.blake2b(raw,digest_size=32).hexdigest()
        self.assertEqual(decode_transaction(encoded.hex(),h)['body_bytes'],len(raw))
        with self.assertRaises(Refusal):decode_transaction(encoded.hex(),tx(body())[1])
    def test_duplicate_keys_rejected(self):
        for raw in (b'\xa2\x00\x01\x00\x02',b'\xbf\x00\x01\x00\x02\xff'):
            with self.assertRaisesRegex(Refusal,'duplicate'):Decoder(raw).item()
    def test_bounds_malformed_and_identity(self):
        for raw in (b'\x1c',b'\xff',b'\x5f\x5f\xff\xff',b'\x81',b'\xfa\x00\x00\x00\x00'):
            with self.assertRaises(Refusal):Decoder(raw).item()
        with self.assertRaisesRegex(Refusal,'size'):Decoder(b'12345',max_bytes=4)
        with self.assertRaisesRegex(Refusal,'depth'):Decoder(b'\x81'*8+b'\x00',max_depth=4).item()
        with self.assertRaisesRegex(Refusal,'item'):Decoder(b'\x98\x20',max_items=5).item()
        raw,h=tx(body())
        with self.assertRaisesRegex(Refusal,'trailing'):decode_transaction(raw+'00',h)
        with self.assertRaisesRegex(Refusal,'hash_mismatch'):decode_transaction(raw,'00'*32)
        with self.assertRaises(Refusal):decode_transaction(raw.upper(),h)
    def test_invalid_body_and_inputs(self):
        for b in ({0:[],1:[]},{**body(),23:0},{**body(),0:[[b'a',0]]},{**body(),0:[[b'a'*32,0],[b'a'*32,0]]},{**body(),9:{b'p'*28:{b'x':0}}}):
            with self.assertRaises(Refusal):decode_transaction(*tx(b))
    def test_datum_semantics(self):
        self.assertEqual(decode_datum(enc(Tag(121,[4,b'abc']))),{'constructor':0,'fields':[{'int':4},{'bytes':'616263'}]})
        self.assertEqual(decode_datum(enc({b'a':3})),{'map':[{'k':{'bytes':'61'},'v':{'int':3}}]})
        self.assertEqual(decode_datum(enc(Tag(3,b'\x01\x00'))),{'int':-257})
        with self.assertRaises(Refusal):decode_datum(enc('bad'))

class AuxiliaryDataTests(unittest.TestCase):
    """Anyone can put well-formed auxiliary data of any shape on chain; only malformed bytes refuse."""
    def test_any_nesting_and_repeated_keys_are_skipped(self):
        label=b'\xa1\x19\x02\xa2'  # {674: ...}
        cases={'1000 nested arrays':label+b'\x81'*1000+b'\x00','1000 nested maps':label+b'\xa1\x00'*1000+b'\x00',
               '1000 nested indefinite arrays':label+b'\x9f'*1000+b'\x00'+b'\xff'*1000,'repeated key':label+b'\xa2\x61k\x01\x61k\x02',
               'tagged map, indefinite repeated key, native script':b'\xd9\x01\x03\xa2\x00'+label+b'\xbf\x61k\x01\x61k\x02\xff\x01\x81\x82\x00\x58\x1c'+b's'*28,
               'indefinite strings':label+b'\x82\x5f\x41a\x41b\xff\x7f\x61a\x61b\xff'}
        for name,auxiliary in cases.items():
            with self.subTest(name):
                b={**body(),7:hashlib.blake2b(auxiliary,digest_size=32).digest()}
                raw,h=tx(b,auxiliary=auxiliary);d=decode_transaction(raw,h);start,end=d.pop('auxiliary_data_span')
                self.assertEqual(bytes.fromhex(raw)[start:end],auxiliary)
                plain=decode_transaction(*tx(b));plain.pop('auxiliary_data_span')
                self.assertEqual(d,plain)
    def test_malformed_auxiliary_data_refuses_with_existing_codes(self):
        for auxiliary,code in [(b'\x82\x00','cbor_truncated'),(b'\x9f\x00','cbor_truncated'),(b'\x5b'+b'\xff'*8,'cbor_truncated'),
                               (b'\x9b\x00\x00\x00\x01\x00\x00\x00\x00','cbor_item_limit'),(b'\x1c','cbor_reserved_encoding'),
                               (b'\xff','cbor_unsupported_simple'),(b'\xbf\x00\xff','cbor_unsupported_simple'),(b'\xfb'+b'\x00'*8,'cbor_unsupported_simple'),
                               (b'\x3f','cbor_invalid_integer'),(b'\x5f\x61a\xff','cbor_invalid_chunks'),(b'\x61\xff','cbor_invalid_utf8'),
                               (b'\xdf\x00','cbor_invalid_tag'),(b'\xf6\x00','cbor_trailing_bytes')]:
            with self.subTest(code=code,auxiliary=auxiliary.hex()):
                with self.assertRaisesRegex(Refusal,'^'+code+'$'):decode_transaction(*tx(body(),auxiliary=auxiliary))
    def test_body_limits_unchanged(self):
        deep=0
        for _ in range(100):deep=[deep]
        with self.assertRaisesRegex(Refusal,'^cbor_depth_limit$'):decode_transaction(*tx({**body(),1:deep}))
        repeated=b'\xa4'+enc(0)+enc([[b'a'*32,0]])+enc(1)+enc([])+enc(2)+enc(3)+enc(2)+enc(3)
        with self.assertRaisesRegex(Refusal,'^cbor_duplicate_map_key$'):decode_transaction((b'\x84'+repeated+b'\xa0\xf5\xf6').hex(),'00'*32)
    def test_skip_reads_exactly_what_item_reads(self):
        """Same bytes, item budget and reason codes as `item`, except nesting depth and map keys."""
        rng=random.Random(19)
        def gen(depth=0):
            kind=rng.randrange(9 if depth<5 else 5);n=rng.randrange(4)
            items=lambda count:b''.join(gen(depth+1) for _ in range(count))
            if kind==0:return bytes([rng.randrange(24)])
            if kind==1:return b'\x38'+bytes([rng.randrange(256)])
            if kind==2:return b'\x43abc'
            if kind==3:return b'\x7f\x61a\x61b\xff'
            if kind==4:return bytes([rng.choice((0xf4,0xf5,0xf6))])
            if kind==5:return bytes([0x80+n])+items(n)
            if kind==6:return bytes([0xa0+n])+items(2*n)
            if kind==7:return (b'\x9f'+items(n) if rng.random()<.5 else b'\xbf'+items(2*n))+b'\xff'
            return b'\xd8\x79'+gen(depth+1)
        for _ in range(3000):
            data=bytearray(gen())
            if rng.random()<.5:
                at=rng.randrange(len(data));op=rng.randrange(3)
                if op==0:data[at]=rng.randrange(256)
                elif op==1:del data[at:]
                else:data.insert(at,rng.randrange(256))
            limits={'max_depth':rng.choice((64,2)),'max_items':rng.choice((200000,6))};outcomes=[]
            for read in (Decoder.item,Decoder.skip):
                d=Decoder(bytes(data),**limits)
                try:read(d);outcomes.append((d.pos,d.remaining))
                except Refusal as e:outcomes.append(str(e))
            if outcomes[0] not in ('cbor_depth_limit','cbor_duplicate_map_key','cbor_unsupported_map_key'):
                self.assertEqual(outcomes[1],outcomes[0],bytes(data).hex())

class WitnessSetTests(unittest.TestCase):
    """Plutus data in a witness set may nest to any depth and repeat map keys; only the map head is checked."""
    def test_any_plutus_data_is_skipped(self):
        cases={'datum, 1000 nested lists':b'\xa1\x04\x81'+b'\x81'*1000+b'\x00',
               'tagged datum set, indefinite map with a repeated key':b'\xa1\x04\xd9\x01\x02\x81\xbf\x01\x00\x01\x00\xff',
               'redeemer map, 1000 nested constructors':b'\xa1\x05\xa1\x82\x00\x00\x82'+b'\xd8\x79\x81'*1000+b'\xd8\x79\x80\x82\x01\x01',
               'redeemer list, repeated key':b'\xa1\x05\x81\x84\x00\x00\xa2\x01\x00\x01\x00\x82\x01\x01',
               'indefinite witness map':b'\xbf\x04\x81\x00\xff'}
        plain=decode_transaction(*tx(body()));plain.pop('auxiliary_data_span')
        for name,witness in cases.items():
            with self.subTest(name):
                d=decode_transaction(*tx(body(),witness=witness));d.pop('auxiliary_data_span')
                self.assertEqual(d,plain)
    def test_malformed_or_non_map_witness_set_refuses_with_existing_codes(self):
        for witness,code in [(b'\x80','cbor_unsupported_envelope'),(b'\x00','cbor_unsupported_envelope'),(b'\xf6','cbor_unsupported_envelope'),
                             (b'\xa1\x04','cbor_truncated'),(b'\xa1\x04\x81\x5b'+b'\xff'*8,'cbor_truncated'),(b'\xbb'+b'\xff'*8,'cbor_item_limit'),
                             (b'\xbc','cbor_reserved_encoding'),(b'\xff','cbor_unsupported_simple'),(b'\xbf\x04\xff','cbor_unsupported_simple')]:
            with self.subTest(code=code,witness=witness.hex()):
                with self.assertRaisesRegex(Refusal,'^'+code+'$'):decode_transaction(*tx(body(),witness=witness))

def dbsync_nesting_exceeded(text):
    """cardano-db-sync 13.8 `jsonbNestingExceeded`: more than 2000 levels, one per '{' or '[' outside strings."""
    level=0;inside=escaped=False
    for ch in text:
        if inside:
            if escaped:escaped=False
            elif ch=='\\':escaped=True
            elif ch=='"':inside=False
        elif ch=='"':inside=True
        elif ch in '{[':
            level+=1
            if level>2000:return True
        elif ch in '}]':level-=1
    return False

class DatumTests(unittest.TestCase):
    """`decode_datum` gives cardano-db-sync 13.8's `datum.value`: its JSON, or None (NULL) past 2000 nesting levels."""
    def test_any_depth_follows_dbsync_nesting_rule(self):
        def lists(n):
            v={'int':0}
            for _ in range(n):v={'list':[v]}
            return b'\x81'*n+b'\x00',v
        def constructors(n):
            v={'int':0}
            for _ in range(n):v={'constructor':0,'fields':[v]}
            return b'\xd8\x79\x81'*n+b'\x00',v
        def maps(n):
            v={'int':0}
            for _ in range(n):v={'map':[{'k':{'int':0},'v':v}]}
            return b'\xa1\x00'*n+b'\x00',v
        def indefinite(n):
            v={'int':0}
            for _ in range(n):v={'list':[v]}
            return b'\x9f'*n+b'\x00'+b'\xff'*n,v
        for name,make,edge in [('lists',lists,999),('constructors',constructors,999),('maps',maps,666),('indefinite lists',indefinite,999)]:
            for n in (64,65,edge-1,edge,edge+1,edge+2,3000):
                with self.subTest(name,levels=n):
                    raw,reference=make(n);null=dbsync_nesting_exceeded(json.dumps(reference))
                    self.assertEqual(decode_datum(raw),None if null else reference)
                    self.assertEqual(null,n>edge)
    def test_repeated_map_keys_keep_every_pair(self):
        self.assertEqual(decode_datum(b'\xa2\x01\x00\x01\x02'),{'map':[{'k':{'int':1},'v':{'int':0}},{'k':{'int':1},'v':{'int':2}}]})
        self.assertEqual(decode_datum(b'\xbf\x01\x00\x01\x00\xff'),{'map':[{'k':{'int':1},'v':{'int':0}},{'k':{'int':1},'v':{'int':0}}]})
    def test_malformed_datums_refuse_with_existing_codes(self):
        for raw,code in [(b'\x82\x00','cbor_truncated'),(b'\x1c','cbor_reserved_encoding'),(b'\x00\x00','cbor_datum_trailing_bytes'),
                         (b'\x61a','cbor_datum_shape'),(b'\xf5','cbor_datum_shape'),(b'\xfb'+b'\x00'*8,'cbor_unsupported_simple'),
                         (b'\xd8\x64\x80','cbor_datum_tag'),(b'\xd8\x79\x00','cbor_datum_constructor'),(b'\xdf\x80','cbor_invalid_tag'),
                         (b'\x9b\x00\x00\x00\x01\x00\x00\x00\x00','cbor_item_limit'),(b'\xbf\x00\xff','cbor_unsupported_simple'),
                         (b'\x81'*1000+b'\x61a','cbor_datum_shape')]:
            with self.subTest(code=code,raw=raw[:8].hex()):
                with self.assertRaisesRegex(Refusal,'^'+code+'$'):decode_datum(raw)
    def test_value_builds_what_item_builds(self):
        """`Decoder.value` gives `item`'s result (maps as pairs) and its reason codes, except nesting depth and map keys."""
        rng=random.Random(23)
        def gen(depth=0):
            kind=rng.randrange(9 if depth<5 else 5);n=rng.randrange(4)
            items=lambda count:b''.join(gen(depth+1) for _ in range(count))
            if kind==0:return bytes([rng.randrange(24)])
            if kind==1:return b'\x38'+bytes([rng.randrange(256)])
            if kind==2:return b'\x43abc'
            if kind==3:return b'\x5f\x41a\x41b\xff'
            if kind==4:return bytes([rng.choice((0xf4,0xf5,0xf6))])
            if kind==5:return bytes([0x80+n])+items(n)
            if kind==6:return bytes([0xa0+n])+items(2*n)
            if kind==7:return (b'\x9f'+items(n) if rng.random()<.5 else b'\xbf'+items(2*n))+b'\xff'
            return b'\xd8\x79'+gen(depth+1)
        for _ in range(3000):
            data=bytearray(gen())
            if rng.random()<.5:
                at=rng.randrange(len(data));op=rng.randrange(3)
                if op==0:data[at]=rng.randrange(256)
                elif op==1:del data[at:]
                else:data.insert(at,rng.randrange(256))
            limits={'max_depth':rng.choice((64,2)),'max_items':rng.choice((200000,6))};outcomes=[]
            for read in (lambda d:d.item(),lambda d:d.value()):
                d=Decoder(bytes(data),maps_as_pairs=True,**limits)
                try:outcomes.append((read(d),d.pos,d.remaining))
                except Refusal as e:outcomes.append(str(e))
            if outcomes[0] not in ('cbor_depth_limit','cbor_duplicate_map_key','cbor_unsupported_map_key'):
                self.assertEqual(outcomes[1],outcomes[0],bytes(data).hex())

if __name__=='__main__':unittest.main()
