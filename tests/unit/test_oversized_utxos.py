"""A `/txs/{h}/utxos` answer above 32 MiB (issue: about 300 reference inputs to outputs of 1,304 assets each) is
read with its input rows cut to what facts read, identically on both adapters; nothing else changes."""
import hashlib
import json
import tempfile
import threading
import unittest
import unittest.mock

from db_sync_alt.contract import Refusal, canonical
from db_sync_alt.errors import LifecycleFailure
from db_sync_alt.sources.blockfrost import provider
from db_sync_alt.sources.blockfrost.adapter import amounts, bounded_answer, input_cut, output_value
from db_sync_alt.sources.blockfrost.provider import MAX_BYTES, CutAnswer, PortableClient, cut_utxos
from db_sync_alt.sources.blockfrost.sync import AccountedProvider
from fixtures.networks import PREPROD, PREPROD_PROFILE
from fixtures.sync import Clock, Store, health

PROFILE = PREPROD_PROFILE
JUNK = bytes(range(28))
# 1,304 one-policy assets fill one output value of at most 5,000 bytes (maxValueSize).
NAMES = ([b''] + [bytes([i]) for i in range(256)] + [bytes([i, j]) for i in range(256) for j in range(256)])[:1304]
OTHER = 'addr_test1vz' + 'q' * 51
HASH = 'ab' * 32


def row(i, *, address=OTHER, extra=(), reference=True):
    amount = [{'unit': 'lovelace', 'quantity': '23000000'}] + [{'unit': unit, 'quantity': q} for unit, q in extra]
    amount += [{'unit': (JUNK + n).hex(), 'quantity': '1'} for n in NAMES]
    return {'address': address, 'amount': amount, 'tx_hash': hashlib.blake2b(i.to_bytes(4, 'big'), digest_size=32).hexdigest(),
            'output_index': 0, 'data_hash': None, 'inline_datum': None, 'reference_script_hash': None,
            'collateral': False, 'reference': reference}


def answer(count=300):
    return {'hash': HASH, 'inputs': [row(i) for i in range(count)] + [row(10**6, reference=False, extra=[(PROFILE.night, '7')])],
            'outputs': [{'address': OTHER, 'amount': [{'unit': 'lovelace', 'quantity': '2000000'}], 'output_index': 0,
                         'data_hash': None, 'inline_datum': None, 'collateral': False, 'reference_script_hash': None}]}


class CutRuleTests(unittest.TestCase):
    def test_cut_keeps_everything_a_fact_or_relevance_decision_reads(self):
        cut = input_cut(PROFILE)
        ics = sorted(PROFILE.bridge_addresses)[0]
        for name, whole in (('junk only', row(1)), ('cNIGHT among junk', row(2, extra=[(PROFILE.night, '5')])),
                            ('ICS address keeps every asset', row(3, address=ics)),
                            ('profile address', row(4, address=PROFILE.mapping))):
            with self.subTest(case=name):
                value, assets = output_value(whole)
                cut_value, cut_assets = output_value(cut(whole))
                self.assertEqual(cut_value, value)
                self.assertEqual(PROFILE.relevant(value['address'], cut_assets), PROFILE.relevant(value['address'], assets))
                for covered in (False, True):
                    self.assertEqual(PROFILE.kept_assets(value['address'], cut_assets, covered=covered),
                                     PROFILE.kept_assets(value['address'], assets, covered=covered))
                self.assertEqual({k: v for k, v in cut(whole).items() if k != 'amount'}, {k: v for k, v in whole.items() if k != 'amount'})
        self.assertEqual(len(cut(row(5))['amount']), 1)
        self.assertEqual(cut(row(6, address=ics))['amount'], row(6, address=ics)['amount'])

    def test_malformed_rows_stay_whole_and_refuse_as_before(self):
        cut = input_cut(PROFILE)
        duplicate = row(1)
        duplicate['amount'].append(dict(duplicate['amount'][1]))
        for bad in (duplicate, {'address': OTHER, 'amount': 'x'}, ['not', 'a', 'row'], {**row(2), 'amount': [{'unit': 'lovelace'}]}):
            with self.subTest(row=str(bad)[:40]):
                self.assertIs(cut(bad), bad)
        with self.assertRaisesRegex(Refusal, 'provider_duplicate_asset'):
            amounts(cut(duplicate))

    def test_answers_up_to_the_limit_are_untouched(self):
        small = answer(2)
        result, encoded = bounded_answer(small, input_cut(PROFILE))
        self.assertIs(result, small)
        self.assertEqual(encoded, canonical(small).encode())
        self.assertNotIsInstance(result, CutAnswer)


class OversizedAnswerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = json.dumps(answer(), separators=(',', ':')).encode()
        assert len(cls.raw) > MAX_BYTES and len(canonical(json.loads(cls.raw)).encode()) > MAX_BYTES

    def accounted(self, client, profile=PROFILE):
        store, clock = Store(security=PREPROD.security), Clock()
        job = dict(job_id='j', attempts=0, budget=100, state='active', kind='acquire', parent=0, target=101,
                   witness=101 + PREPROD.security, created_at=clock(), retry_at=0, failure_count=0)
        store.save_sync_job('provider-active', job)
        return AccountedProvider(store, 'provider-active', job, client, health(), threading.Event(), network=PREPROD,
                                 clock=clock, profile=profile)

    def portable(self, body):
        root = tempfile.mkdtemp()
        self.addCleanup(__import__('shutil').rmtree, root, True)
        transport = lambda path: (200, {}, b'[]' if path == '/metrics' else body)
        return PortableClient('preprod' + 'A' * 32, root, network=PREPROD, transport=transport, sleep=lambda s: None)

    def test_both_adapters_read_the_same_cut_answer_and_receipt(self):
        path = '/txs/' + HASH + '/utxos'
        raw = self.raw

        class Parsed:  # the shared adapter: its external client hands over the whole parsed answer
            def status(self): return {'budget_used': 0, 'daily_limit': 50000, 'cooldown_until': 0}
            def get(self, path, purpose=None): return [] if path == '/metrics' else json.loads(raw)
        built_in, shared = self.accounted(self.portable(raw)), self.accounted(Parsed())
        first, second = built_in.get(path), shared.get(path)
        self.assertIsInstance(first, CutAnswer)
        self.assertIsInstance(second, CutAnswer)
        self.assertEqual(first, second)
        self.assertEqual(first, cut_utxos(raw, input_cut(PROFILE)))
        self.assertEqual(built_in.receipts[-1]['sha256'], shared.receipts[-1]['sha256'])
        self.assertLess(built_in.receipts[-1]['bytes'], 1024 * 1024)
        self.assertEqual(first['outputs'], json.loads(raw)['outputs'])
        whole = json.loads(raw)['inputs']
        self.assertEqual([{k: v for k, v in r.items() if k != 'amount'} for r in first['inputs']],
                         [{k: v for k, v in r.items() if k != 'amount'} for r in whole])
        self.assertEqual(first['inputs'][-1]['amount'], [whole[-1]['amount'][0], whole[-1]['amount'][1]])

    def test_without_a_profile_or_on_another_path_it_refuses_as_before(self):
        raw = self.raw
        with self.assertRaisesRegex(LifecycleFailure, 'provider_request_rejected'):
            self.accounted(self.portable(raw), profile=None).get('/txs/' + HASH + '/utxos')
        with self.assertRaisesRegex(LifecycleFailure, 'provider_request_rejected'):
            self.accounted(self.portable(raw)).get('/txs/' + HASH)

        class Parsed:
            def status(self): return {'budget_used': 0, 'daily_limit': 50000, 'cooldown_until': 0}
            def get(self, path, purpose=None): return [] if path == '/metrics' else json.loads(raw)
        with self.assertRaisesRegex(Refusal, 'provider_response_size_limit'):
            self.accounted(Parsed()).get('/txs/' + HASH)

    def test_the_read_bound_and_the_key_check_still_hold(self):
        path = '/txs/' + HASH + '/utxos'
        with unittest.mock.patch.object(provider, 'UTXOS_MAX_BYTES', len(self.raw) - 1):
            with self.assertRaisesRegex(LifecycleFailure, 'provider_request_rejected'):
                self.accounted(self.portable(self.raw)).get(path)
        leaked = self.raw[:-1] + b',"note":"preprod' + b'A' * 32 + b'"}'
        with self.assertRaisesRegex(LifecycleFailure, 'provider_request_rejected'):
            self.accounted(self.portable(leaked)).get(path)


if __name__ == '__main__':
    unittest.main(verbosity=2)
