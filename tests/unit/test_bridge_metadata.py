# SPDX-License-Identifier: Apache-2.0
"""Bridge metadata: the auxiliary data label reader, the stored normalization and the provider cross-check."""
import hashlib
import unittest

from db_sync_alt.bridge import parse_checkpoint, recipient
from db_sync_alt.contract import Refusal
from db_sync_alt.sources.blockfrost.cbor import ABSENT, PRESENT, UNKNOWN, Tag, bridge_metadatum, decode_transaction
from db_sync_alt.sources.blockfrost.metadata import cross_check, fact, normalize, provider_metadatum
from fixtures.cbor import enc

KEY = 6500973
ADDRESS = bytes(range(32))


def nested(depth, leaf=b'\x80'):
    """`depth` nested one-element CBOR arrays around `leaf` (an empty array by default)."""
    return b'\x81' * depth + leaf


def bound(aux):
    """The decoded auxiliary data fields of a transaction whose auxiliary data is `aux` (CBOR bytes or None)."""
    return {'auxiliary_data': aux, 'auxiliary_data_hash': None if aux is None else hashlib.blake2b(aux, digest_size=32).digest()}


def transaction(aux):
    """Hex CBOR, hash and decoded facts of a transaction whose auxiliary data is `aux` (CBOR bytes or None)."""
    body = {0: [[b'a' * 32, 0]], 1: [], 2: 3}
    if aux is not None:
        body[7] = hashlib.blake2b(aux, digest_size=32).digest()
    raw = enc(body)
    tail = aux if aux is not None else enc(None)
    hexed = (b'\x84' + raw + enc({}) + enc(True) + tail).hex()
    h = hashlib.blake2b(raw, digest_size=32).hexdigest()
    return decode_transaction(hexed, h)


class LabelReaderTests(unittest.TestCase):
    def test_every_auxiliary_data_era_carries_the_label(self):
        metadata = {KEY: [ADDRESS], 674: {'msg': ['hello']}}
        for name, aux in (('shelley', enc(metadata)), ('allegra', enc([metadata, []])),
                          ('alonzo', enc(Tag(259, {0: metadata, 1: []})))):
            with self.subTest(era=name):
                self.assertEqual(bridge_metadatum(aux, KEY), (PRESENT, ADDRESS))
        self.assertEqual(bridge_metadatum(None, KEY), (ABSENT, None))
        self.assertEqual(bridge_metadatum(enc({674: 'x'}), KEY), (ABSENT, None))
        self.assertEqual(bridge_metadatum(enc(Tag(259, {1: []})), KEY), (ABSENT, None))

    def test_recipient_shapes_follow_the_node(self):
        cases = [
            ([ADDRESS], ADDRESS),                      # bytes: db-sync renders 0x + hex
            (['0x' + ADDRESS.hex()[:40]], ADDRESS[:20]),
            (['0xABcd'], b'\xab\xcd'),                  # the node decodes either case
            (['0x'], b''),
            (['0xabc'], None), (['0xzz'], None), (['abcd'], None), (['0X' + 'ab'], None),
            ([ADDRESS, ADDRESS], None), ([], None), ([1], None), ('0x' + ADDRESS.hex()[:40], None),
            ({1: ADDRESS}, None), ([[ADDRESS]], None), (7, None),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(bridge_metadatum(enc({KEY: value}), KEY), (PRESENT, expected))

    def test_chunked_bytes_are_one_recipient(self):
        chunked = b'\x81\x5f' + enc(ADDRESS[:10]) + enc(ADDRESS[10:]) + b'\xff'
        aux = b'\xa1' + enc(KEY) + chunked
        self.assertEqual(bridge_metadatum(aux, KEY), (PRESENT, ADDRESS))

    def test_hostile_values_are_never_a_refusal(self):
        deep_label = b'\xa1' + enc(KEY) + nested(5000)
        self.assertEqual(bridge_metadatum(deep_label, KEY), (PRESENT, None))
        # A deep sibling label and repeated sibling keys are only skipped.
        aux = b'\xa3' + enc(1) + nested(5000) + enc(1) + enc('again') + enc(KEY) + enc([ADDRESS])
        self.assertEqual(bridge_metadatum(aux, KEY), (PRESENT, ADDRESS))
        same = b'\xa2' + enc(KEY) + enc([ADDRESS]) + enc(KEY) + enc([ADDRESS])
        self.assertEqual(bridge_metadatum(same, KEY), (PRESENT, ADDRESS))
        different = b'\xa2' + enc(KEY) + enc([ADDRESS]) + enc(KEY) + enc(['0xab'])
        self.assertEqual(bridge_metadatum(different, KEY), (UNKNOWN, None))
        for aux in (enc(7), enc({KEY: [ADDRESS]})[:-3], b'\xa1\xff', enc(Tag(259, [1])), enc({b'k': 1})):
            with self.subTest(aux=aux.hex()):
                self.assertEqual(bridge_metadatum(aux, KEY)[0], UNKNOWN)

    def test_decoded_transactions_carry_their_auxiliary_data(self):
        aux = enc({KEY: [ADDRESS]})
        decoded = transaction(aux)
        self.assertEqual((decoded['auxiliary_data'], decoded['auxiliary_data_hash']), (aux, hashlib.blake2b(aux, digest_size=32).digest()))
        self.assertEqual((transaction(None)['auxiliary_data'], transaction(None)['auxiliary_data_hash']), (None, None))


class NormalizationTests(unittest.TestCase):
    def test_numbers_and_shapes(self):
        self.assertEqual(normalize(['0x' + ADDRESS.hex()]), ['0x' + ADDRESS.hex()])
        # Blockfrost's JavaScript round trip: 2^60 arrives rounded, 10^21 as a float.
        self.assertEqual(normalize({'a': [1152921504606847000, 1e21, 2.5]}), {'a': [1152921504606847000, 10**21, 2.5]})
        self.assertIs(type(normalize(1e21)), int)
        self.assertEqual(normalize(None), None)
        for bad in (float('nan'), [float('inf')], {'x': object()}):
            with self.subTest(value=repr(bad)), self.assertRaisesRegex(Refusal, 'provider_bad_metadata'):
                normalize(bad)

    def test_answers_the_provider_client_cut_are_null(self):
        from db_sync_alt.sources.blockfrost.provider import TooDeep
        self.assertIsNone(normalize(TooDeep()))
        self.assertIsNone(normalize([[TooDeep()]]))
        self.assertEqual(provider_metadatum([{'label': '6500973', 'json_metadata': [TooDeep()]}]), (True, None))

    def test_values_cardano_db_sync_stores_as_null(self):
        deep = []
        for _ in range(2000):
            deep = [deep]
        self.assertEqual(normalize(deep[0]), deep[0])
        self.assertIsNone(normalize(deep))
        for value in (['a\x00b'], {'k\x00': 1}, ['\ud800'], {'\ud800': 1}):
            with self.subTest(value=repr(value)):
                self.assertIsNone(normalize(value))
        self.assertEqual(normalize(['\x01']), ['\x01'])

    def test_provider_response(self):
        self.assertEqual(provider_metadatum([]), (False, None))
        self.assertEqual(provider_metadatum([{'label': '674', 'json_metadata': {'msg': ['x']}}]), (False, None))
        self.assertEqual(provider_metadatum([{'label': '6500973', 'json_metadata': None}]), (True, None))
        self.assertEqual(provider_metadatum([{'label': '674', 'json_metadata': 'x'}, {'label': '6500973', 'json_metadata': ['0xab']}]), (True, ['0xab']))
        for bad in ({}, [1], [{'label': 6500973, 'json_metadata': None}], [{'label': '6500973'}],
                    [{'label': '6500973', 'json_metadata': 1}, {'label': '6500973', 'json_metadata': 1}]):
            with self.subTest(response=bad), self.assertRaisesRegex(Refusal, 'provider_bad_metadata'):
                provider_metadatum(bad)

    def test_fact(self):
        self.assertEqual(fact(b'h', True, {'b': [1], 'a': 'é'}), {'tx_hash': b'h', 'present': 1, 'json': '{"a":"é","b":[1]}'})
        self.assertEqual(fact(b'h', True, None), {'tx_hash': b'h', 'present': 1, 'json': None})
        self.assertEqual(fact(b'h', False, None), {'tx_hash': b'h', 'present': 0, 'json': None})


class CrossCheckTests(unittest.TestCase):
    def test_agreement_and_representational_differences(self):
        valid = bound(enc({KEY: [ADDRESS]}))
        cross_check(valid, True, ['0x' + ADDRESS.hex()])
        cross_check(valid, True, ['0x' + ADDRESS.hex().upper()])
        cross_check(bound(None), False, None)
        cross_check(bound(enc({674: 'x'})), False, None)
        # D1/D2: big integers rounded by the provider; never a recipient either way.
        cross_check(bound(enc({KEY: 2**60})), True, 1152921504606847000)
        cross_check(bound(enc({KEY: [2**64 - 1]})), True, [18446744073709552000])
        # D3: a control character a 13.7-based provider returns as NULL.
        cross_check(bound(enc({KEY: ['\x01']})), True, None)
        cross_check(bound(enc({KEY: ['\x01']})), True, ['\x01'])
        # D4: nesting cardano-db-sync 13.8 stores as NULL.
        cross_check(bound(b'\xa1' + enc(KEY) + nested(2100)), True, None)
        # Undecidable auxiliary data is accepted as the provider has it.
        cross_check(bound(b'\xa2' + enc(KEY) + enc([ADDRESS]) + enc(KEY) + enc(['0xab'])), True, ['0xab'])

    def test_disagreement_is_refused(self):
        other = bytes(32)
        cases = [
            (bound(enc({KEY: [ADDRESS]})), True, ['0x' + other.hex()]),
            (bound(enc({KEY: [ADDRESS]})), True, None),
            (bound(enc({KEY: [ADDRESS]})), True, 'x'),
            (bound(enc({KEY: [ADDRESS]})), False, None),
            (bound(enc({KEY: ['abc']})), True, ['0x' + other.hex()]),
            (bound(enc({674: 'x'})), True, ['0x' + other.hex()]),
            (bound(None), True, None),
        ]
        for decoded, present, value in cases:
            with self.subTest(present=present, value=value), self.assertRaisesRegex(Refusal, 'provider_metadata_conflict'):
                cross_check(decoded, present, value)
        unbound = dict(bound(enc({KEY: [ADDRESS]})), auxiliary_data=enc({KEY: [other]}))
        with self.assertRaisesRegex(Refusal, 'provider_metadata_conflict'):
            cross_check(unbound, True, ['0x' + other.hex()])
        with self.assertRaisesRegex(Refusal, 'provider_metadata_conflict'):
            cross_check(dict(bound(None), auxiliary_data_hash=bytes(32)), False, None)


class CheckpointTests(unittest.TestCase):
    def test_stated_checkpoints(self):
        self.assertEqual(parse_checkpoint('block:0'), {'kind': 'block', 'height': 0})
        self.assertEqual(parse_checkpoint('block:4500300'), {'kind': 'block', 'height': 4500300})
        self.assertEqual(parse_checkpoint('tx:' + 'ab' * 32), {'kind': 'tx', 'hash': 'ab' * 32})
        for bad in ('block:-1', 'block:01', 'block:2147483648', 'tx:' + 'AB' * 32, 'tx:ab', 'Block(1)', '', None, 5):
            with self.subTest(text=bad), self.assertRaisesRegex(Refusal, 'invalid_bridge_checkpoint'):
                parse_checkpoint(bad)

    def test_node_recipient_reading(self):
        self.assertEqual(recipient(['0x' + ADDRESS.hex()]), ADDRESS)
        for value in (None, [], ['0x0'], ['0xg0'], ['x0ab'], [['0xab']], ['0xab', '0xab'], '0xab', {'0': '0xab'}, ['0x ab']):
            with self.subTest(value=value):
                self.assertIsNone(recipient(value))


if __name__ == '__main__':
    unittest.main()
