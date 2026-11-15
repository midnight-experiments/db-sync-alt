# SPDX-License-Identifier: Apache-2.0
"""How many blocks one relevance generation holds: the longest run whose per-block key bounds fit MAX_KEYS."""
import unittest
from unittest.mock import patch

from db_sync_alt.sources.blockfrost import relevance
from db_sync_alt.sources.blockfrost.relevance import MAX_KEYS, generation_blocks
from db_sync_alt.sources.blockfrost.sync import job_target
from fixtures.sync import Store


class Lazy:
    """Bounds handed out one at a time, counting how many were taken."""
    def __init__(self, bounds):
        self.bounds, self.taken = bounds, 0

    def __iter__(self):
        for bound in self.bounds:
            self.taken += 1
            yield bound


class GenerationSizingTests(unittest.TestCase):
    def test_a_sum_of_exactly_the_key_limit_is_included_and_one_key_more_cuts(self):
        half = MAX_KEYS // 2
        self.assertEqual(generation_blocks([half, MAX_KEYS - half]), 2)
        self.assertEqual(generation_blocks([half, MAX_KEYS - half + 1]), 1)
        self.assertEqual(generation_blocks([half, MAX_KEYS - half - 1, 1, 0, 0]), 5)
        self.assertEqual(generation_blocks([half, MAX_KEYS - half - 1, 2, 0, 0]), 2)

    def test_the_first_block_is_always_included(self):
        self.assertEqual(generation_blocks([MAX_KEYS + 1]), 1)
        self.assertEqual(generation_blocks([MAX_KEYS + 1, 0, 0]), 1)
        self.assertEqual(generation_blocks([MAX_KEYS, 1]), 1)
        self.assertEqual(generation_blocks([0, MAX_KEYS + 1]), 1)

    def test_empty_blocks_add_nothing_and_the_limit_ends_the_generation(self):
        self.assertEqual(generation_blocks([0] * 100), 100)
        self.assertEqual(generation_blocks([0] * 50 + [MAX_KEYS] + [0] * 49), 100)
        self.assertEqual(generation_blocks([1] + [0] * 49 + [MAX_KEYS] + [0] * 49), 50)
        self.assertEqual(generation_blocks([7] * 3), 3)

    def test_sizing_reads_at_most_one_block_past_the_generation(self):
        bounds = Lazy([90000, 90000, 90000, 90000])
        self.assertEqual(generation_blocks(bounds), 2)
        self.assertEqual(bounds.taken, 3)
        bounds = Lazy([1, 2, 3])
        self.assertEqual(generation_blocks(bounds), 3)
        self.assertEqual(bounds.taken, 3)

    def test_the_key_limit_is_read_when_sizing(self):
        with patch.object(relevance, 'MAX_KEYS', 20000):
            self.assertEqual(generation_blocks([10000, 10000, 1]), 2)
        self.assertEqual(generation_blocks([10000, 10000, 1]), 3)

    def test_the_request_budget_limit_cuts_before_the_keys(self):
        # Blocks of 2,000 transactions: the job's request estimate holds one of them, whatever their key bounds.
        store = Store()
        ready = store.generation()
        top = ready['target_height']
        store.tx_counts.update({h: 2000 for h in range(top + 1, top + 101)})
        limit = job_target(store, ready, top + 100, 10000, relevance=True)
        self.assertEqual(limit, top + 1)
        self.assertEqual(top + generation_blocks([0] * (limit - top)), top + 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
