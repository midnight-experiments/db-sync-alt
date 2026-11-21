"""Small offline preparation controls; not a PostgreSQL/product qualification."""
import math
import unittest
from restore_pair import PREDECESSOR, configuration, io_delta, query_timings, snapshot_description, wal_delta


class PreparationControls(unittest.TestCase):
    def test_candidate_must_be_exact_and_distinct(self):
        for candidate in ['main', 'a' * 39, PREDECESSOR, 'a' * 40 + '; echo unsafe']:
            with self.assertRaises(AssertionError): configuration(candidate)
        self.assertEqual(configuration('a' * 40)['target'], 'restore_pair')

    def test_query_durations_and_unmeasured_short_query_scope(self):
        log = ('LOG:  duration: 1234.500 ms  execute s12: SELECT 1\n'
               'STATEMENT: this has no duration\n'
               'LOG:  duration: 250.001 ms  statement: CHECKPOINT\n')
        items = query_timings(log)
        self.assertEqual(len(items), 2)
        self.assertTrue(math.isclose(items[0]['seconds'], 1.2345))
        self.assertTrue(math.isclose(items[1]['seconds'], .250001))
        self.assertEqual(items[0]['first_sql_line'], 'SELECT 1')

    def test_setting_pair_keeps_exact_predecessor_code(self):
        self.assertEqual(configuration(PREDECESSOR, 'owner_work_mem_pair')['source'], PREDECESSOR)
        with self.assertRaises(AssertionError): configuration('a' * 40, 'owner_work_mem_pair')
        with self.assertRaises(AssertionError): configuration(PREDECESSOR, 'unbounded')

    def test_stacked_devices_are_not_added(self):
        before = {'252:0': {'wbytes': 10}, '259:3': {'wbytes': 10}}
        after = {'252:0': {'wbytes': 30}, '259:3': {'wbytes': 30}}
        self.assertEqual(io_delta(before, after), {'252:0': {'wbytes': 20}, '259:3': {'wbytes': 20}})

    def test_wal_epoch_and_lsn_carry(self):
        before = {'insert_lsn': '0/FFFFFFF0', 'flush_lsn': '0/FFFFFFF0',
                  'wal': {'wal_bytes': 20, 'stats_reset': 'epoch-a'}}
        after = {'insert_lsn': '1/10', 'flush_lsn': '1/0',
                 'wal': {'wal_bytes': 52, 'stats_reset': 'epoch-a'}}
        self.assertEqual(wal_delta(before, after)['insert_lsn_bytes'], 32)
        self.assertEqual(wal_delta(before, after)['flush_lsn_bytes'], 16)
        after['wal']['stats_reset'] = 'epoch-b'
        with self.assertRaises(AssertionError): wal_delta(before, after)
        after['wal'] = {'wal_bytes': 10, 'stats_reset': 'epoch-a'}
        with self.assertRaises(AssertionError): wal_delta(before, after)

    def test_invalid_descriptor_is_rejected(self):
        with self.assertRaises(AssertionError): snapshot_description(b'not a snapshot')


if __name__ == '__main__': unittest.main()
