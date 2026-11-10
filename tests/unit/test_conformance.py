"""Conformance harness pieces that need no database: typing, encodings, normalization, corpus, coverage."""
import datetime
import decimal
import gzip
import hashlib
import re
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

from db_sync_alt.contract import Profile, Refusal, canonical
from db_sync_alt.node_queries import CAT, REFERENCE
from db_sync_alt.resources import profile_dir

from conformance import corpus as corpus_file, harness, supplement
from conformance.client import QueryError, encode
from conformance.run import coverage, judge, run_supplement
from conformance.statements import (BRIDGE_DYNAMIC_BINDINGS, BRIDGE_FRAGMENT_SHA256, BRIDGE_SOURCE, HELPERS,
                                    bridge_fragment, by_name, emitted_statements, parameter_types, placeholders, sha, statements)

CORPUS_FAMILIES = ('get_asset_creates', 'get_asset_spends', 'get_block_by_hash', 'get_blocks_by_numbers', 'get_deregistrations',
                   'get_epoch_for_block_hash', 'get_epoch_nonce', 'get_governance_body_utxo', 'get_highest_block',
                   'get_latest_block_for_epoch', 'get_latest_block_info', 'get_latest_stable_epoch', 'get_max_tx_size', 'get_registrations',
                   'get_stake_distribution', 'get_token_utxo_for_epoch', 'get_utxos_for_address_tx_in_enabled')


def synthetic_corpus(directory, profile, *, cases=None, header_changes=None):
    """A small corpus in the real file format: one case per node statement of the corpus families."""
    entries = [e for e in CAT.values() if e['name'] in CORPUS_FAMILIES and not (e['name'] == 'get_token_utxo_for_epoch' and placeholders(e['sql']) == 3)]
    items = cases or []
    if cases is None:
        for i, entry in enumerate(sorted(entries, key=lambda e: sha(e['sql']))):
            case = {'name': entry['name'], 'query_sha256': sha(entry['sql']), 'args': [277] if entry['name'] in ('get_token_utxo_for_epoch', 'get_stake_distribution', 'get_epoch_nonce') else []}
            if entry['name'] == 'get_token_utxo_for_epoch':
                case['args'] = [{'asset': profile.permissioned}, 277]
            elif entry['name'] == 'get_max_tx_size':
                case['args'] = [4500300]
            items.append({'case': case, 'expected': harness.digest([]), 'id': i})
    families = {}
    for item in items:
        families[item['case']['name']] = families.get(item['case']['name'], 0) + 1
    header = {'complete': True, 'cases': len(items), 'families': families, 'identity': profile.identity, 'target': 4500300,
              'witness': 4502460, 'anchors': {}, **(header_changes or {})}
    lines = [canonical({'corpus': corpus_file.FORMAT, 'header': header, 'origin': {}})] + [canonical(i) for i in items]
    path = Path(directory) / 'corpus.jsonl.gz'
    path.write_bytes(gzip.compress(''.join(line + '\n' for line in lines).encode(), mtime=0))
    return path


class StatementTests(unittest.TestCase):
    def test_every_emitted_catalogue_statement_has_sqlx_parameter_types(self):
        self.assertEqual(len(CAT), 53)
        emitted = emitted_statements()
        self.assertEqual(len(emitted), 47)
        fragments = {sha(entry['sql']) for entry in CAT.values() if bridge_fragment(entry)}
        self.assertEqual(fragments, BRIDGE_FRAGMENT_SHA256)
        self.assertEqual(set(statements()), set(emitted) | fragments)
        for entry in emitted.values():
            with self.subTest(statement=entry['name'], sha=sha(entry['sql'])[:12]):
                self.assertEqual(len(parameter_types(entry)), placeholders(entry['sql']))
        for fingerprint in fragments:
            with self.subTest(fragment=fingerprint), self.assertRaisesRegex(ValueError, 'non-emitted bridge'):
                parameter_types(statements()[fingerprint])

    def test_bridge_classification_refuses_changed_sql_and_provenance(self):
        original = statements()[min(BRIDGE_FRAGMENT_SHA256)]
        for changed in (
            {**original, 'sql': original['sql'] + ' AND true'},
            {**original, 'name': 'get_asset_creates'},
            {**original, 'file': 'unknown.rs'},
            {**original, 'sources': [{'name': 'get_bridge_txs', 'file': 'unknown.rs'}]},
        ):
            with self.subTest(changed=changed), self.assertRaisesRegex(ValueError, 'unclassified bridge'):
                bridge_fragment(changed)

    def test_bridge_final_bind_order_matches_exact_released_builder(self):
        source = (REFERENCE / BRIDGE_SOURCE).read_bytes()
        self.assertEqual(hashlib.sha256(source).hexdigest(),
                         'a016a1a87356905083c5f817fd8d4ac1b79e639ddd315b792689c51b223a813d')
        body = source.decode().split('pub(crate) async fn get_bridge_txs(', 1)[1]
        binds = body.split('let query = query_builder', 1)[1].split('Ok(query.fetch_all', 1)[0]
        # One bind per line; the metadata-key bind itself contains nested calls.
        expressions = re.findall(r'^\s*\.bind\((.*)\)[;]?$', binds, re.MULTILINE)
        self.assertEqual(expressions, [expression for expression, _ in BRIDGE_DYNAMIC_BINDINGS])
        self.assertEqual([kind for _, kind in BRIDGE_DYNAMIC_BINDINGS],
                         ['text', 'text', 'bytea', 'bytea', 'int8', 'int4'])
        self.assertEqual(sorted({int(n) for n in re.findall(r'\$(\d+)', body)}), [1, 2, 3, 4, 5, 6])

    def test_event_and_bound_statements_bind_the_node_types(self):
        self.assertEqual(parameter_types(by_name('get_asset_creates')), ['int8'] + ['int4'] * 6 + ['int8'] * 6)
        self.assertEqual(parameter_types(by_name('get_registrations')), ['text', 'int8'] + ['int4'] * 6 + ['int8'] * 6)
        self.assertEqual(parameter_types(by_name('get_high_bounds')), ['int4'])
        self.assertEqual(parameter_types(by_name('get_highest_block')), ['int4', 'int8', 'int8', 'timestamp', 'timestamp'])
        self.assertEqual(parameter_types(by_name('get_token_utxo_for_epoch', parameters=2)), ['int8', 'int4'])
        self.assertEqual(parameter_types(by_name('get_token_utxo_for_epoch', parameters=3)), ['bytea', 'bytea', 'int4'])


class EncodingTests(unittest.TestCase):
    def test_binary_parameters_match_sqlx_encodings(self):
        self.assertEqual(encode('int2', -2), b'\xff\xfe')
        self.assertEqual(encode('int4', 5356455), struct.pack('!i', 5356455))
        self.assertEqual(encode('int8', (1 << 63) - 1), b'\x7f' + b'\xff' * 7)
        self.assertEqual(encode('oid', 23), b'\x00\x00\x00\x17')
        self.assertEqual(encode('text', 'addr_test1é'), 'addr_test1é'.encode())
        self.assertEqual(encode('bytea', b'\x00\x01'), b'\x00\x01')
        self.assertEqual(encode('timestamp', datetime.datetime(2000, 1, 1)), struct.pack('!q', 0))
        self.assertEqual(encode('timestamp', datetime.datetime(2026, 3, 12, 1, 11, 43, 5)),
                         struct.pack('!q', int((datetime.datetime(2026, 3, 12, 1, 11, 43) - datetime.datetime(2000, 1, 1)).total_seconds()) * 1_000_000 + 5))
        array = encode('bytea[]', [b'\xaa' * 28])
        self.assertEqual(struct.unpack_from('!iiIii', array), (1, 0, 17, 1, 1))
        self.assertEqual(array[20:24], struct.pack('!i', 28))
        with self.assertRaises(ValueError):
            encode('timestamp', datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc))


class NormalizationTests(unittest.TestCase):
    def test_rows_normalize_like_the_captured_answers(self):
        row = {'time': datetime.datetime(2026, 3, 12, 1, 11, 43), 'stake': decimal.Decimal('1200'), 'hash': b'\x01\xab',
               'datum': {'b': [1, {'z': True}], 'a': 'x'}, 'inputs': ['ab#0'], 'none': None}
        self.assertEqual(harness.canonical_row(row), {'time': '2026-03-12T01:11:43', 'stake': '1200', 'hash': '01ab',
                                                      'datum': {'b': [1, {'z': True}], 'a': 'x'}, 'inputs': ['ab#0'], 'none': None})
        with self.assertRaises(Refusal):
            harness.canonical_row(decimal.Decimal('1.5'))

    def test_unordered_answers_sort_before_hashing(self):
        rows = [{'a': 2}, {'a': 1}]
        self.assertEqual(harness.digest(rows, ordered=False), harness.digest(rows[::-1], ordered=False))
        self.assertNotEqual(harness.digest(rows), harness.digest(rows[::-1]))
        self.assertEqual(harness.digest([]), {'rows': 0, 'sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'})

    def test_id_columns_are_recognized(self):
        for name in ('id', 'tx_id!', 'consuming_tx_id', 'native_token_id', 'ma_tx_out_id!'):
            self.assertTrue(harness.id_column(name), name)
        for name in ('utxo_id_index', 'utxo_id_tx_hash', 'block_number!: _', 'tx_index_in_block: _', 'pool_hash'):
            self.assertFalse(harness.id_column(name), name)


class CorpusTests(unittest.TestCase):
    def setUp(self):
        self.profile = Profile(profile_dir('preprod'))
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.directory.cleanup()

    def test_valid_corpus_loads_with_its_own_inventory(self):
        path = synthetic_corpus(self.directory.name, self.profile)
        corpus = corpus_file.load(path, profile=self.profile)
        self.assertEqual(len(corpus.cases), 19)
        self.assertEqual(corpus.families['get_max_tx_size'], 1)
        self.assertEqual(sum(corpus.families.values()), corpus.header['cases'])
        self.assertEqual(corpus_file.load(path, expect_sha256=corpus.sha256).sha256, corpus.sha256)

    def test_corrupt_or_foreign_corpora_are_refused(self):
        good = synthetic_corpus(self.directory.name, self.profile)
        with self.assertRaises(Refusal) as caught:
            corpus_file.load(good, expect_sha256='0' * 64)
        self.assertEqual(caught.exception.code, 'corpus_sha256_mismatch')
        cases = [{'case': {'name': 'get_epoch_nonce', 'query_sha256': sha(by_name('get_epoch_nonce')['sql']), 'args': [1]}, 'expected': harness.digest([]), 'id': 0}]
        for changes, code in (({'cases': 2}, 'corpus_inventory'), ({'complete': False}, 'corpus_header'),
                              ({'identity': {'network': 'mainnet'}}, 'corpus_profile_mismatch')):
            with tempfile.TemporaryDirectory() as directory, self.subTest(code=code):
                path = synthetic_corpus(directory, self.profile, cases=cases, header_changes=changes)
                with self.assertRaises(Refusal) as caught:
                    corpus_file.load(path, profile=self.profile)
                self.assertEqual(caught.exception.code, code)
        for case, code in (({**cases[0], 'id': 1}, 'corpus_case_sequence'),
                           ({**cases[0], 'case': {**cases[0]['case'], 'name': 'get_stake_distribution'}}, 'corpus_query_fingerprint'),
                           ({**cases[0], 'expected': {'rows': 0}}, 'corpus_answer_shape')):
            with tempfile.TemporaryDirectory() as directory, self.subTest(code=code):
                path = synthetic_corpus(directory, self.profile, cases=[case])
                with self.assertRaises(Refusal) as caught:
                    corpus_file.load(path)
                self.assertEqual(caught.exception.code, code)


class SupplementTests(unittest.TestCase):
    def test_corpus_helpers_and_supplement_cover_every_catalogue_statement(self):
        profile = Profile(profile_dir('preprod'))
        with tempfile.TemporaryDirectory() as directory:
            corpus = corpus_file.load(synthetic_corpus(directory, profile), profile=profile)
        cases = supplement.cases(profile, corpus.header, corpus.cases)
        records = [{'statement_sha256': sha(c.statement['sql']) if c.statement else None, 'label': c.label} for c in cases]

        class Calls:
            calls = {name: 1 for name in HELPERS}
        table, missing = coverage(corpus, Calls(), records)
        self.assertEqual(missing, [])
        self.assertEqual(len(table), len(statements()))
        for case in cases:
            with self.subTest(case=case.label):
                self.assertTrue(supplement.reach(case))
                if case.expect == 'component':
                    self.assertTrue(bridge_fragment(case.statement))
                    self.assertTrue(case.guard)
                    self.assertTrue(all(g.startswith('get_bridge_txs:formatted-') for g in case.guard))
                elif case.statement is not None and not case.simple:
                    self.assertEqual(len(supplement.statement_types(case)), placeholders(case.sql or case.statement['sql']))

    def test_bridge_query_is_formatted_from_the_reference_source(self):
        for mode in ('Enabled', 'Consumed'):
            with self.subTest(mode=mode):
                query = supplement.bridge_query(mode)
                self.assertNotIn('{', query)
                self.assertEqual(placeholders(query), 6)
                self.assertIn('LEFT JOIN tx_metadata ON tx_metadata.tx_id = tx.id AND tx_metadata.key = $5', query)
                self.assertIn('WHERE block.block_no <= $6', query)
                self.assertIn('LIMIT null', query)
                if mode == 'Enabled':
                    self.assertIn('ON tx_out.tx_id = tx_in.tx_out_id AND tx_out.index = tx_in.tx_out_index', query)
                else:
                    self.assertIn('tx_out.consumed_by_tx_id IS NOT NULL', query)
        with self.assertRaisesRegex(ValueError, 'unknown bridge'):
            supplement.bridge_query('unknown')

    def test_bridge_values_follow_public_ICS_reserve_and_native_token_configuration(self):
        profile = Profile(profile_dir('preprod'))
        values = supplement.bridge_values(profile, 4500300)
        ics, reserve = profile.configs['ics-config'], profile.configs['reserve-config']
        self.assertEqual(values, [ics['illiquid_circulation_supply_validator_address'], reserve['reserve_validator_address'],
                                 bytes.fromhex(ics['asset']['policy_id'].removeprefix('0x')), ics['asset']['asset_name'].encode(),
                                 supplement.TOKEN_TRANSFER_METADATUM_KEY, 4500300])
        self.assertNotEqual(values[0], profile.mapping)

    def test_components_are_not_executed_and_need_final_query_refusal(self):
        entry = statements()[min(BRIDGE_FRAGMENT_SHA256)]
        component = supplement.Case('get_bridge_txs:component-test', entry, 'component',
                                    guard=['get_bridge_txs:formatted-Enabled'])
        final = supplement.Case('get_bridge_txs:formatted-Enabled', None, 'error', sql=supplement.bridge_query(),
                                types=['text', 'text', 'bytea', 'bytea', 'int8', 'int4'], values=['ics', 'reserve', b'p', b'n', 1, 2])
        endpoint = mock.Mock()
        endpoint.calls = {'formatted': 0}
        endpoint.client.query.side_effect = QueryError('statement', '42501')
        corpus = mock.Mock(header={}, cases=[])
        with mock.patch.object(supplement, 'cases', return_value=[component, final]):
            results = run_supplement(endpoint, None, corpus, {})
        self.assertEqual(endpoint.client.query.call_count, 1)
        self.assertEqual(results[0]['outcome'], {'executed': False, 'kind': 'non_emitted_component'})
        self.assertEqual((results[0]['verdict'], results[0]['guard_verdict']), ('component', 'pass'))
        endpoint.client.query.reset_mock()
        with mock.patch.object(supplement, 'cases', return_value=[component]):
            results = run_supplement(endpoint, None, corpus, {})
        endpoint.client.query.assert_not_called()
        self.assertEqual(results[0]['guard_verdict'], 'fail')

    def test_expectations_judge_refusals_and_answers(self):
        Case = supplement.Case
        encoded = harness.encoded_rows([{'a': 1}])
        self.assertEqual(judge(Case('x', None, 'error'), None, None), 'pass')
        self.assertEqual(judge(Case('x', None, 'error'), [], None), 'fail')
        self.assertEqual(judge(Case('x', None, ('rows', [{'a': 1}])), encoded, None), 'pass')
        self.assertEqual(judge(Case('x', None, ('rows', [{'a': 1}])), [], None), 'fail')
        self.assertEqual(judge(Case('x', None, 'error_or_rows'), [], [{'a': 1}]), 'fail')
        self.assertEqual(judge(Case('x', None, 'error_or_rows'), None, [{'a': 1}]), 'pass')
        self.assertEqual(judge(Case('x', None, 'error_or_digest'), encoded, harness.digest([{'a': 1}])), 'pass')
        self.assertEqual(judge(Case('x', None, 'error_or_digest'), [], harness.digest([{'a': 1}])), 'fail')
        self.assertEqual(judge(Case('x', None, 'error_or_complete'), [], None), 'pass')
        self.assertEqual(judge(Case('x', None, 'error_or_complete'), encoded, None), 'fail')
        self.assertEqual(judge(Case('x', None, 'observed'), encoded, None), 'observed')


class DrillTests(unittest.TestCase):
    def test_drills_run_in_dependency_order(self):
        from conformance.drills import DRILLS, Runner
        self.assertEqual(DRILLS[0], 'ready')
        self.assertLess(DRILLS.index('updater_stopped'), DRILLS.index('publication_during_sequence'))
        self.assertEqual(DRILLS[-1], 'revoked')
        for name in DRILLS:
            if name != 'updater_stopped':
                self.assertTrue(callable(getattr(Runner, 'drill_' + name)))


if __name__ == '__main__':
    unittest.main(verbosity=2)


class TieOrderTests(unittest.TestCase):
    def test_only_rows_with_equal_order_keys_may_change_places(self):
        from conformance.harness import encoded_rows, tie_order
        row = lambda block, tx, utxo, origin: {'block_number!: _': block, 'tx_index_in_block: _': tx, 'utxo_index: _': utxo, 'utxo_tx_hash: _': origin}
        reference = encoded_rows([row(1, 0, 0, 'aa'), row(1, 0, 0, 'bb'), row(2, 1, 0, 'cc')])
        swapped = encoded_rows([row(1, 0, 0, 'bb'), row(1, 0, 0, 'aa'), row(2, 1, 0, 'cc')])
        self.assertTrue(tie_order('get_asset_spends', swapped, reference))
        # A row moved past a different key, a different row, or no reference page: not tie order.
        moved = encoded_rows([row(2, 1, 0, 'cc'), row(1, 0, 0, 'aa'), row(1, 0, 0, 'bb')])
        other = encoded_rows([row(1, 0, 0, 'bb'), row(1, 0, 0, 'dd'), row(2, 1, 0, 'cc')])
        self.assertFalse(tie_order('get_asset_spends', moved, reference))
        self.assertFalse(tie_order('get_asset_spends', other, reference))
        self.assertFalse(tie_order('get_asset_spends', swapped, None))
        self.assertFalse(tie_order('get_epoch_nonce', swapped, reference))


class WindowTieOrderTests(unittest.TestCase):
    def test_pages_of_a_window_paged_across_tied_rows(self):
        from conformance.harness import encoded_rows, reference_windows, window_tie_order
        row = lambda block, tx, utxo, origin: {'block_number!: _': block, 'tx_index_in_block: _': tx, 'utxo_index: _': utxo, 'utxo_tx_hash: _': origin}
        captured = encoded_rows([row(1, 0, 0, 'aa'), row(1, 0, 0, 'bb'), row(2, 1, 0, 'cc')])
        native = encoded_rows([row(1, 0, 0, 'bb'), row(1, 0, 0, 'aa'), row(2, 1, 0, 'cc')])
        base = {'name': 'get_asset_spends', 'start': [1, 0], 'end': [3, 0]}

        class Corpus:
            cases = [{'id': i, 'case': dict(base, limit=1, offset=i)} for i in range(4)]
        baseline = {i: captured[i:i + 1] for i in range(3)}
        baseline[3] = []
        references = reference_windows(Corpus, baseline)
        self.assertEqual(list(references.values()), [captured])
        reference = next(iter(references.values()))
        for offset in range(3):
            self.assertTrue(window_tie_order('get_asset_spends', dict(base, limit=1, offset=offset), native[offset:offset + 1], reference, native))
        # A row from another key position, a row outside the window, or a different native window: refused.
        self.assertFalse(window_tie_order('get_asset_spends', dict(base, limit=1, offset=0), captured[2:3], reference, native))
        other = encoded_rows([row(1, 0, 0, 'dd')])
        self.assertFalse(window_tie_order('get_asset_spends', dict(base, limit=1, offset=0), other, reference, native))
        self.assertFalse(window_tie_order('get_asset_spends', dict(base, limit=1, offset=0), native[0:1], reference, native[:2]))
        # Pages that disagree at a position leave the window undetermined.

        class Overlapping:
            cases = Corpus.cases + [{'id': 9, 'case': dict(base, limit=2, offset=0)}]
        self.assertEqual(reference_windows(Overlapping, {**baseline, 9: [captured[1], captured[0]]}), {})
