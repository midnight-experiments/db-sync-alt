"""Analytical size admission; no SQL, range lists or candidate inventories."""
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace
import contextlib
from pathlib import Path
from db_sync_alt.contract import TABLES, Refusal, canonical
from db_sync_alt.verify.sql_receipt import receipt_size_bound, admit_receipt_size


class ReceiptSizeTests(unittest.TestCase):
    def manifest(self, start, target, witness, parent=None):
        return {'anchors': {key: {'height': height, 'hash': 'a'*64, 'epoch': 8,
                                  'tx_count': 2**63-1}
                            for key, height in [('start', start), ('target', target), ('witness', witness)]},
                'required_epochs': [6, 7, 8], 'parent_digest': parent}

    def bounds(self, start=100, target=103, witness=110, window=10, root=True, version=2, range_start=None):
        return dict(start=start, range_start=start if range_start is None else range_start,
                    target=target, witness=witness, window=window, root=root, version=version)

    def test_conservative_against_full_encoded_receipt_for_roots_children_and_v1(self):
        for bounds in [self.bounds(), self.bounds(window=1), self.bounds(start=0),
                       self.bounds(version=1, window=1), self.bounds(root=False, range_start=102),
                       self.bounds(start=9999, target=10002, witness=10020, window=17)]:
            with self.subTest(bounds=bounds):
                m = self.manifest(bounds['start'], bounds['target'], bounds['witness'], None if bounds['root'] else 'f'*64)
                ranges = []
                if bounds['root'] and bounds['start']:
                    w = bounds['window'] if bounds['version'] == 2 else bounds['start']
                    ranges.extend(('prerequisites', lo, min(bounds['start']-1, lo+w-1)) for lo in range(0, bounds['start'], w))
                for phase, end in [('metadata', bounds['witness']), ('events', bounds['target'])]:
                    ranges.extend((phase, lo, min(end, lo+bounds['window']-1)) for lo in range(bounds['range_start'], end+1, bounds['window']))
                receipt = {'source_kind': 'cardano-db-sync-postgresql', 'anchors': m['anchors'], 'independent': {
                    'version': 1, 'scope': 'independent-new-facts-and-membership-with-certified-parent',
                    'parent_digest': m['parent_digest'], 'ranges': ranges, 'epochs': m['required_epochs'],
                    'inventory': {t: {'count': 2**63-1, 'sha256': '0'*64} for t in TABLES}}}
                self.assertGreaterEqual(receipt_size_bound(m, **bounds), len(canonical(receipt).encode()))

    def test_exact_cap_boundary_and_supported_small_window(self):
        b = self.bounds(window=1); m = self.manifest(b['start'], b['target'], b['witness'])
        size = receipt_size_bound(m, **b)
        with patch('db_sync_alt.verify.sql_receipt.MAX_RECEIPT_BYTES', size):
            self.assertEqual(admit_receipt_size(m, **b), size)
        with patch('db_sync_alt.verify.sql_receipt.MAX_RECEIPT_BYTES', size-1):
            with self.assertRaisesRegex(Refusal, '^sql_completion_receipt_too_large$'):
                admit_receipt_size(m, **b)
        admit_receipt_size(m, **b)

    def test_large_history_refuses_without_materializing_any_range(self):
        b = self.bounds(start=2**31-4000, target=2**31-3000, witness=2**31-1, window=1)
        m = self.manifest(b['start'], b['target'], b['witness'])
        with patch('builtins.range', side_effect=AssertionError('ranges must not be allocated')):
            with self.assertRaisesRegex(Refusal, '^sql_completion_receipt_too_large$'):
                admit_receipt_size(m, **b)

    def test_epoch_and_parent_sizes_are_reserved_without_candidate_counts(self):
        b = self.bounds(); m = self.manifest(b['start'], b['target'], b['witness'])
        before = receipt_size_bound(m, **b)
        m['required_epochs'] += [999999]; m['parent_digest'] = 'f'*64
        self.assertGreater(receipt_size_bound(m, **b), before)

    def test_window_domain_is_unchanged(self):
        m = self.manifest(100, 103, 110)
        for window in (0, True, 10001, '1'):
            with self.subTest(window=window), self.assertRaisesRegex(Refusal, '^invalid_window$'):
                receipt_size_bound(m, **self.bounds(window=window))

    def test_no_progress_does_not_skip_existing_parent_verification(self):
        from db_sync_alt.sources.dbsync.acquire import acquire
        for parent_error in (None, Refusal('fixture_invalid_parent')):
            with self.subTest(parent_error=parent_error):
                profile = SimpleNamespace(network='fixture', identity={'fixture': True}, start=(100, 3))
                store = Mock(network=profile.network)
                store.pending.return_value = None
                store.meta.side_effect = lambda key: 0 if key == 'canonical_ready' else profile.identity
                store.generation.return_value = {'id': 0, 'target_height': 103,
                                                 'manifest': {'anchors': {'start': {}, 'target': {}}}}
                source = Mock(); source.snapshot.side_effect = contextlib.nullcontext
                m = self.manifest(100, 103, 110); m.update(identity=profile.identity)
                with patch('db_sync_alt.sources.dbsync.acquire._source_manifest', return_value=m), patch(
                    'db_sync_alt.store.incremental.integrity_reference', return_value={'sha256': 'f'*64}), patch(
                    'db_sync_alt.sources.dbsync.acquire.verify_store', side_effect=parent_error) as verify:
                    with self.assertRaisesRegex(Refusal, '^'+('fixture_invalid_parent' if parent_error else 'no_confirmed_progress')+'$'):
                        acquire(source, store, profile)
                verify.assert_called_once_with(store)
                self.assertEqual(source.check_anchors.call_count, 0 if parent_error else 1)
                store.begin_generation.assert_not_called()

    def test_actual_packaged_profiles_reach_generation_with_height_only_size_bounds(self):
        from db_sync_alt.contract import Profile
        from db_sync_alt.sources.dbsync.acquire import acquire
        profiles = Path(__file__).resolve().parents[2]/'profiles'
        for directory in sorted(profiles.iterdir()):
            if not directory.is_dir():
                continue
            with self.subTest(profile=directory.name):
                profile = Profile(directory)
                self.assertIsInstance(profile.start, tuple)
                start = profile.start[0]
                m = self.manifest(start, start+3, start+3+profile.security)
                m['identity'] = profile.identity
                store = Mock(network=profile.network)
                store.pending.return_value = None; store.meta.return_value = None
                store.atomic.side_effect = contextlib.nullcontext
                store.begin_generation.side_effect = Refusal('fixture_generation_boundary')
                with patch('db_sync_alt.sources.dbsync.acquire._source_manifest', return_value=m):
                    with self.assertRaisesRegex(Refusal, '^fixture_generation_boundary$'):
                        acquire(Mock(), store, profile, window_blocks=10000)
                self.assertEqual(store.begin_generation.call_args.kwargs['start'], profile.start)
