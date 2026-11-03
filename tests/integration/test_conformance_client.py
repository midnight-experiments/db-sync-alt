"""The conformance client against PostgreSQL 17: typed binds, errors, deadlines."""
import datetime
import decimal
import os
import unittest

from conformance.client import QueryError, TypedClient
from conformance.statements import by_text

DSN = os.environ.get('DB_SYNC_ALT_TEST_DSN')


class PostgreSQLClientTests(unittest.TestCase):
    def setUp(self):
        self.client = TypedClient(DSN, timeout=5).connect()

    def tearDown(self):
        self.client.close()

    def test_binary_parameters_round_trip_with_the_node_types(self):
        when = datetime.datetime(2026, 3, 12, 1, 11, 43, 123456)
        result = self.client.query('SELECT $1::int2 AS a, $2::int4 AS b, $3::int8 AS c, $4::text AS d, $5::bytea AS e, $6::timestamp AS f, $7::bytea[] AS g',
                                   ['int2', 'int4', 'int8', 'text', 'bytea', 'timestamp', 'bytea[]'],
                                   [-3, 5356455, (1 << 63) - 1, 'addr_test1', b'\x00\xff', when, [b'\x01' * 28, b'\x02' * 28]])
        self.assertEqual(result.rows, [{'a': -3, 'b': 5356455, 'c': (1 << 63) - 1, 'd': 'addr_test1', 'e': b'\x00\xff', 'f': when,
                                        'g': [b'\x01' * 28, b'\x02' * 28]}])
        typed = self.client.query("SELECT 12::numeric AS n, '{\"b\":1}'::jsonb AS j, ARRAY['x#1']::text[] AS t", [], [])
        self.assertEqual(typed.rows, [{'n': decimal.Decimal(12), 'j': {'b': 1}, 't': ['x#1']}])

    def test_sqlx_driver_statements_answer_catalogue_facts(self):
        self.assertEqual(self.client.query(by_text('SELECT $1::regtype::oid')['sql'], ['text'], ['INT4']).rows, [{'oid': 23}])
        row = self.client.query(by_text('SELECT typname, typtype, typelem, typrelid, typbasetype FROM pg_catalog.pg_type WHERE oid = $1')['sql'], ['oid'], [23]).rows
        self.assertEqual(row, [{'typname': 'int4', 'typtype': 'b', 'typelem': 0, 'typrelid': 0, 'typbasetype': 0}])
        self.assertEqual(self.client.simple("SET extra_float_digits = 3; SET application_name = 'sqlx'").command, 'SET')
        self.assertEqual(self.client.simple('SELECT 1').rows, [{'?column?': 1}])

    def test_statement_errors_keep_the_connection_and_report_sqlstate(self):
        with self.assertRaises(QueryError) as caught:
            self.client.query('SELECT * FROM conformance_missing_relation', [], [])
        self.assertEqual((caught.exception.kind, caught.exception.sqlstate), ('statement', '42P01'))
        self.assertTrue(self.client.connected)
        self.assertEqual(self.client.query('SELECT $1::int4 AS v', ['int4'], [7]).rows, [{'v': 7}])

    def test_a_stalled_query_becomes_a_timeout_and_the_client_reconnects(self):
        client = TypedClient(DSN, timeout=0.5).connect()
        with self.assertRaises(QueryError) as caught:
            client.query('SELECT pg_sleep(3)', [], [])
        self.assertEqual(caught.exception.kind, 'timeout')
        self.assertFalse(client.connected)
        self.assertEqual(client.query('SELECT $1::int4 AS v', ['int4'], [1]).rows, [{'v': 1}])
        client.close()

if __name__ == '__main__':
    unittest.main(verbosity=2)
