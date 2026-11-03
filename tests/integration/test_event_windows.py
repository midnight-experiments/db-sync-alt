"""cNIGHT event windows read by the node's reader role: the node's own statements answer every window edge.

The node bounds every event window with "(b > $s OR (b = $s AND i >= $si))" and
"(b < $e OR (b = $e AND i < $ei))" on a block number b and a transaction index i, both never NULL.
These tests run the node's unmodified statements as the reader on the served tables and compare
them, at every edge of a window, with the same statements written as row comparisons
"(b, i) >= ($s, $si)" / "(b, i) < ($e, $ei)", which state the window's meaning directly.
"""
import json
import os
import re
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_sync_alt.store.database import PgStore
from db_sync_alt.store.reader import grant_reader
from db_sync_alt.store.upgrade import upgrade_store

from conformance.statements import by_name, parameter_types
from fixtures.generations import PROFILE, START, make_store

EVENTS = ('get_registrations', 'get_deregistrations', 'get_asset_creates', 'get_asset_spends')
MAX = (1 << 63) - 1
WINDOW = re.compile(r'\((\w+)\.block_no ([<>]) \$(\d+) OR \(\1\.block_no = \$\3 AND (\w+)\.block_index (>=|<) \$(\d+)\)\)')


def row_comparisons(text):
    """The node's window bounds written as row comparisons; exactly two bounds per statement."""
    def row(match):
        block, op, b, tx, iop, i = match.groups()
        assert (op, iop) in (('>', '>='), ('<', '<')), match.group(0)
        return '(%s.block_no, %s.block_index) %s ($%s, $%s)' % (block, tx, '>=' if op == '>' else '<', b, i)
    out, count = WINDOW.subn(row, text)
    assert count == 2, count
    return out


def window_arguments(name, asset, start, end, limit=128, offset=0):
    if name == 'get_registrations':
        return [PROFILE.mapping, asset, *start, *end, limit, offset] + [0, MAX] * 3
    if name == 'get_asset_creates':
        return [asset, *start, *end, limit, offset] + [0, MAX] * 3
    if name == 'get_asset_spends':
        return [asset, *start, *end, limit, offset] + [0, MAX] * 2
    return [PROFILE.mapping, *start, *end, limit, offset] + [0, MAX] * 2


class EventWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        token = uuid.uuid4().hex[:10]
        cls.name, cls.reader = 'db_sync_alt_test_' + token, 'node_reader_' + token
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        cls.admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT').format(sql.Identifier(cls.reader)))
        cls.dsn = make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=cls.name)
        make_store(cls.dsn)
        with PgStore(conninfo=cls.dsn) as store:
            upgrade_store(store)
            grant_reader(store.db, cls.reader)
            cls.night = store.db.execute('SELECT id FROM serve.multi_asset WHERE policy=%s AND name=%s',
                                         (bytes.fromhex(PROFILE.night[:56]), bytes.fromhex(PROFILE.night[56:]))).fetchone()[0]

    @classmethod
    def tearDownClass(cls):
        cls.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(cls.name)))
        cls.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(cls.reader)))
        cls.admin.close()

    def run_statement(self, text, entry, values):
        """As the reader, in a read-only snapshot, with a custom plan for the bound values."""
        with psycopg.connect(make_conninfo(self.dsn, user=self.reader), autocommit=True, prepare_threshold=None) as conn:
            cursor = psycopg.ClientCursor(conn)
            cursor.execute('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cursor.execute('SET LOCAL plan_cache_mode = force_custom_plan')
            cursor.execute('PREPARE q(' + ', '.join(parameter_types(entry)) + ') AS ' + text)
            return cursor.execute('EXECUTE q(' + ', '.join(['%s'] * len(values)) + ')', values).fetchall()

    def test_node_windows_answer_like_row_comparisons_at_every_window_edge(self):
        windows = [([START - 10, 0], [START, 4]), ([START - 10, 0], [START, 5]), ([START - 10, 1], [START + 1, 0]),
                   ([START, 4], [START + 1, 0]), ([START, 5], [START + 1, 0]), ([START, 3], [START, 4]),
                   ([START - 11, 0], [START - 10, 0]), ([START - 11, 0], [START - 10, 1]), ([START + 1, 0], [START + 2160, 0])]
        answered = 0
        for name in EVENTS:
            entry = by_name(name)
            for start, end in windows:
                with self.subTest(name=name, start=start, end=end):
                    values = window_arguments(name, self.night, start, end)
                    expected = self.run_statement(row_comparisons(entry['sql']), entry, values)
                    actual = self.run_statement(entry['sql'], entry, values)
                    key = lambda rows: sorted(json.dumps(r, default=str) for r in rows)
                    self.assertEqual(key(actual), key(expected))
                    answered += len(actual)
        # The fixture's NIGHT outputs at START-10:0 and START:4 (and the spend at START:4) fall inside some windows.
        self.assertGreater(answered, 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)
