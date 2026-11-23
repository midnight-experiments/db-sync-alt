"""The reference check must refuse node SQL that is neither vendored nor listed."""
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location('reference_source',
                                              Path(__file__).with_name('check_reference_source.py'))
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)

QUERY = 'pub async fn f(pool: &PgPool) {\n    sqlx::query("SELECT id FROM block WHERE hash = $1");\n}\n'


def listed(file, text, line):
    (found,) = [entry for entry in checker.statements(text) if entry[0] == line]
    return {'file': file, 'line': found[0], 'sha256': found[1], 'class': 'unsupported',
            'subcommands': ['example'], 'reason': 'test'}


class ReferenceSqlChecks(unittest.TestCase):
    def test_unlisted_sql_in_a_node_file_fails(self):
        errors = checker.scan({'node/src/new.rs': QUERY}, set(), {'statements': []})
        self.assertEqual(errors, ['node_sql_outside_reference: node/src/new.rs:2'])

    def test_vendored_listed_and_test_files_pass(self):
        sources = {'vendored.rs': QUERY, 'node/src/listed.rs': QUERY,
                   'node/src/tests.rs': QUERY, 'node/tests/it.rs': QUERY,
                   'node/src/db_tests.rs': QUERY}
        outside = {'statements': [listed('node/src/listed.rs', QUERY, 2)]}
        self.assertEqual(checker.scan(sources, {'vendored.rs'}, outside), [])

    def test_a_new_statement_in_a_listed_file_fails_and_a_removed_one_is_stale(self):
        changed = QUERY + 'fn g() { let q = r#"\nWITH\n  x AS (SELECT 1)\nSELECT * FROM x"#; }\n'
        outside = {'statements': [listed('node/src/listed.rs', QUERY, 2)]}
        self.assertEqual(checker.scan({'node/src/listed.rs': changed}, set(), outside),
                         ['node_sql_outside_reference: node/src/listed.rs:4'])
        self.assertEqual(checker.scan({'node/src/listed.rs': 'fn f() {}\n'}, set(), outside),
                         ['stale_sql_exclusion: node/src/listed.rs:2'])
        self.assertEqual(checker.scan({}, set(), outside),
                         ['stale_sql_exclusion: node/src/listed.rs'])

    def test_sql_looking_rule(self):
        for value in ['SELECT 1', 'select count(*) from information_schema.tables',
                      '\n\t\tWITH\n\t\t\ta AS (SELECT 1) SELECT * FROM a;',
                      'with a as (select 1) select * from a', 'INSERT INTO t VALUES (1)',
                      'CREATE INDEX i ON t(c)']:
            self.assertTrue(checker.sql_looking(value), value)
        for value in ['Insert key into keystore (unsafe)', 'Select the genesis UTXO',
                      'update the chain spec', 'selected', '', 'Delete from list']:
            self.assertFalse(checker.sql_looking(value), value)

    def test_literals_skip_comments_and_character_literals(self):
        text = ('// "SELECT a FROM b"\n/* "SELECT c FROM d" /* nested */ */\n'
                "let q = '\"'; let l: &'static str = \"SELECT e FROM f\";\n"
                'let r = br##"SELECT "g" FROM h"##; let s = "a\\\n    b\\x41\\u{42}";\n')
        self.assertEqual([value for _, value in checker.literals(text)],
                         ['SELECT e FROM f', 'SELECT "g" FROM h', 'abAB'])


if __name__ == '__main__':
    unittest.main()
