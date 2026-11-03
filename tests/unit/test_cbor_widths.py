"""Legal argument widths never change transaction identity or validation limits."""
import hashlib
import json
import unittest
from fixtures.cbor import F, body, tx
from db_sync_alt.sources.blockfrost.cbor import Decoder, Tag, decode_datum, decode_transaction
from db_sync_alt.contract import Refusal


def argument(major,value,width):
    ai={1:24,2:25,4:26,8:27}[width]
    return bytes([(major<<5)|ai])+value.to_bytes(width,'big')


class ArgumentWidthTests(unittest.TestCase):
    def test_confirmed_public_inventory_original_hashes(self):
        path=F/'nonpreferred-block-5357253.json'
        rows=json.loads(path.read_text());self.assertEqual(len(rows),19)
        for row in rows:
            with self.subTest(tx=row['tx_hash']):
                self.assertEqual(decode_transaction(row['cbor'],row['tx_hash'])['hash'].hex(),row['tx_hash'])
        self.assertEqual(rows[15]['tx_hash'],'b9100a5c785c65dd28c46df95ce7f6925e150a91726b8f4f756aa668683de5cc')
        self.assertEqual(bytes.fromhex(rows[15]['cbor'])[102:111],bytes.fromhex('1b00000000001b7740'))

    def test_unsigned_and_negative_argument_boundaries(self):
        values=[0,1,23,24,255,256,65535,65536,2**32-1,2**32,2**64-1]
        for major in (0,1):
            for value in values:
                for width in (1,2,4,8):
                    if value>=2**(8*width):continue
                    with self.subTest(major=major,value=value,width=width):
                        self.assertEqual(Decoder(argument(major,value,width)).item(),value if major==0 else -1-value)

    def test_lengths_and_tags_use_argument_widths(self):
        for width in (1,2,4,8):
            for major,payload,expected in [(2,b'abc',b'abc'),(3,b'abc','abc'),(4,b'\x00\x01\x02',[0,1,2]),(5,b'\x00\x01',{0:1})]:
                count=1 if major==5 else 3
                self.assertEqual(Decoder(argument(major,count,width)+payload).item(),expected)
                self.assertEqual(Decoder(argument(major,0,width)).item(),{2:b'',3:'',4:[],5:{}}[major])
            self.assertEqual(Decoder(argument(6,1,width)+b'\x00').item(),Tag(1,0))
        self.assertEqual(decode_datum(argument(6,121,8)+argument(4,0,8)),{'constructor':0,'fields':[]})

    def test_original_body_hash_not_normalized(self):
        minimal=bytes.fromhex('a3008001800200')
        wide=bytes.fromhex('a300800180021b0000000000000000')
        hashes=[]
        for raw in (minimal,wide):
            expected=hashlib.blake2b(raw,digest_size=32).hexdigest();hashes.append(expected)
            self.assertEqual(decode_transaction((b'\x84'+raw+b'\xa0\xf5\xf6').hex(),expected)['hash'].hex(),expected)
        self.assertNotEqual(*hashes)
        with self.assertRaisesRegex(Refusal,'body_hash_mismatch'):
            decode_transaction((b'\x84'+wide+b'\xa0\xf5\xf6').hex(),hashes[0])

    def test_duplicate_semantic_keys_with_different_widths(self):
        for major in (0,1):
            raw=b'\xa2'+bytes([major<<5])+b'\x01'+argument(major,0,8)+b'\x02'
            with self.assertRaisesRegex(Refusal,'duplicate_map_key'):Decoder(raw).item()

    def test_truncated_and_reserved_arguments(self):
        for major in range(8):
            for width in (1,2,4,8):
                encoded=argument(major,0,width)
                for cutoff in range(1,len(encoded)):
                    with self.assertRaisesRegex(Refusal,'truncated'):Decoder(encoded[:cutoff]).item()
            for ai in (28,29,30):
                with self.assertRaisesRegex(Refusal,'reserved'):Decoder(bytes([(major<<5)|ai])).item()

    def test_extended_simple_and_float_payloads_cannot_alias_bool_null(self):
        for width in (1,2,4,8):
            for value in (0,20,21,22,32):
                with self.assertRaisesRegex(Refusal,'unsupported_simple'):Decoder(argument(7,value,width)).item()
        self.assertIs(Decoder(b'\xf4').item(),False)
        self.assertIs(Decoder(b'\xf5').item(),True)
        self.assertIsNone(Decoder(b'\xf6').item())

    def test_nonpreferred_arguments_still_obey_limits(self):
        with self.assertRaisesRegex(Refusal,'size_limit'):Decoder(argument(0,1,8),max_bytes=8)
        with self.assertRaisesRegex(Refusal,'item_limit'):Decoder(argument(4,10,8),max_items=4).item()
        with self.assertRaisesRegex(Refusal,'depth_limit'):Decoder(argument(4,1,8)*5+b'\x00',max_depth=3).item()
        with self.assertRaisesRegex(Refusal,'truncated'):Decoder(argument(2,2**64-1,8)).item()
        b=body();b[0]=[[b'a'*32,65536]]
        with self.assertRaisesRegex(Refusal,'input_shape'):decode_transaction(*tx(b))
        raw,h=tx(body())
        with self.assertRaisesRegex(Refusal,'unsupported_envelope'):decode_transaction('00'+raw[2:],h)


if __name__=='__main__':unittest.main()
