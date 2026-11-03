"""tools/memory.py parsing and summaries (no cgroup or Docker access needed)."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('memory_tool', Path(__file__).resolve().parents[2] / 'tools' / 'memory.py')
memory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(memory)


class MemoryToolTests(unittest.TestCase):
    def test_cgroup_memory_stat_is_read_in_bytes(self):
        text = 'anon 28483584\nfile 1016623104\nkernel 25665536\nkernel_stack 1\nshmem 148074496\nfile_mapped 190205952\n'
        self.assertEqual(memory.parse_stat(text), {'anon': 28483584, 'file': 1016623104, 'kernel': 25665536,
                                                   'shmem': 148074496, 'file_mapped': 190205952})

    def test_smaps_rollup_kilobytes_become_bytes(self):
        text = ('55d0c0000000-7ffd00000000 ---p 00000000 00:00 0  [rollup]\nRss:  20000 kB\nPss:  12000 kB\n'
                'Pss_Dirty: 1 kB\nPss_Anon:  3000 kB\nPss_File:  1000 kB\nPss_Shmem:  8000 kB\nSwapPss:  12 kB\n')
        self.assertEqual(memory.parse_smaps_rollup(text), {'pss': 12000 * 1024, 'pss_anon': 3000 * 1024, 'pss_file': 1000 * 1024,
                                                           'pss_shmem': 8000 * 1024, 'swap_pss': 12 * 1024})

    def test_postgresql_process_kinds_come_from_process_titles(self):
        self.assertEqual(memory.process_kind('postgres\0-c\0config_file=x\0'), 'postmaster')
        self.assertEqual(memory.process_kind('postgres: checkpointer '), 'checkpointer')
        self.assertEqual(memory.process_kind('postgres: autovacuum launcher '), 'autovacuum launcher')
        self.assertEqual(memory.process_kind('postgres: node_reader cardano 127.0.0.1(5432) idle'), 'client backend')
        self.assertIsNone(memory.process_kind('python /p3tools/measure.py'))

    def test_postgresql_totals_and_sample_summary(self):
        totals = memory.pg_totals([('postmaster', {'pss': 10, 'pss_shmem': 4}), ('client backend', {'pss': 7, 'pss_anon': 3}),
                                   ('client backend', {'pss': 5})])
        self.assertEqual((totals['pss'], totals['pss_anon'], totals['pss_shmem'], totals['processes'], totals['backends']), (22, 3, 4, 3, 2))
        self.assertEqual(totals['pss_by_kind'], {'postmaster': 10, 'client backend': 12})
        header = '\t'.join(memory.COLUMNS)
        rows = ['1.0\tpg\tcontainer\t100\t150\t0\t40\t60\t1\t2\t3\t\t\t\t\t\t\t',
                '3.0\tpg\tcontainer\t120\t160\t5\t50\t70\t1\t2\t3\t90\t30\t10\t50\t0\t7\t1']
        summary = memory.summarize([header, *rows])['pg']
        self.assertEqual(summary['samples'], 2)
        self.assertEqual(summary['max']['current'], 120)
        self.assertEqual(summary['max']['swap_current'], 5)
        self.assertEqual(summary['last']['pg_pss'], 90)
        self.assertEqual(summary['last_ts'], 3.0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
