"""The epochs phase reads an epoch's last block through the epoch's own blocks (issue 00047)."""
from types import SimpleNamespace
import unittest

from db_sync_alt.contract import Refusal
from db_sync_alt.sources.dbsync import reader
from db_sync_alt.sources.dbsync.reader import SQLSource

POOL = b'p' * 28


class RecordingSource(SQLSource):
    """Answers the epochs phase's statements from memory and records each one."""

    def __init__(self, end):
        super().__init__('fixture', SimpleNamespace(units=[], governance=[], addresses=[]))
        self.end = end
        self.statements = []

    def one(self, query, params=()):
        self.statements.append((query, params))
        if 'epoch_stake_progress' in query:
            return {'completed': True}
        if 'FROM epoch_param' in query:
            return {'n': 1, 'nonces': 1, 'distinct_nonces': 1, 'nonce': '07' * 32, 'max_tx_size': 16384, 'invalid': False}
        if query == reader.EPOCH_END:
            return {'height': self.end}
        raise AssertionError('unexpected statement: ' + query)

    def stream(self, query, params=(), batch=500):
        self.statements.append((query, params))
        if 'FROM epoch_stake' in query:
            yield {'pool_id': 1, 'amount': '5'}
        elif 'FROM pool_hash' in query:
            yield {'id': 1, 'hash_raw': POOL}
        else:
            raise AssertionError('unexpected statement: ' + query)


class EpochEndAnchorTests(unittest.TestCase):
    def test_statement_is_fenced_to_the_epochs_blocks(self):
        self.assertEqual(reader.EPOCH_END,
                         'SELECT max(block_no) AS height FROM (SELECT block_no FROM block WHERE epoch_no=%s OFFSET 0) s')

    def test_epochs_reads_the_end_with_the_fenced_statement(self):
        source = RecordingSource(end=900)
        rows = list(source.epochs(617, target=800))
        self.assertEqual([table for table, _ in rows], ['epoch_nonces', 'epoch_stakes'])
        anchors = [(query, params) for query, params in source.statements if 'FROM block' in query]
        self.assertEqual(anchors, [(reader.EPOCH_END, (617,))])

    def test_epoch_without_blocks_is_refused(self):
        with self.assertRaisesRegex(Refusal, 'epoch_blocks_unavailable'):
            list(RecordingSource(end=None).epochs(617, target=800))


if __name__ == '__main__':
    unittest.main()
