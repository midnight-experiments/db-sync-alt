import hashlib
import json
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

if __name__=='__main__':unittest.main()
