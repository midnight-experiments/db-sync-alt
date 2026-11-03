"""The db-sync source binding stored in sealed manifests reveals neither database."""
import unittest

from db_sync_alt.sources.dbsync.acquire import source_binding

SOURCE = {'system_identifier': '7685462014501982247', 'database_oid': 16384}
DESTINATION = {'system_identifier': '7300000000000000001', 'database_oid': 16385}
NAMESPACE = {'start': 4500199, 'target': 5364047}


class BindingTests(unittest.TestCase):
    def test_deterministic_opaque_and_sensitive_to_every_input(self):
        value = source_binding(SOURCE, DESTINATION, NAMESPACE)
        self.assertRegex(value, '^[0-9a-f]{64}$')
        self.assertEqual(value, source_binding(dict(SOURCE), dict(DESTINATION), dict(NAMESPACE)))
        for text in ('7685462014501982247', '16384', '4500199', '5364047', '7300000000000000001'):
            self.assertNotIn(text, value)
        changed = [source_binding(dict(SOURCE, database_oid=16386), DESTINATION, NAMESPACE),
                   source_binding(dict(SOURCE, system_identifier='1'), DESTINATION, NAMESPACE),
                   source_binding(SOURCE, dict(DESTINATION, system_identifier='2'), NAMESPACE),
                   source_binding(SOURCE, DESTINATION, dict(NAMESPACE, target=5364048))]
        self.assertEqual(len({value, *changed}), 5)


if __name__ == '__main__':
    unittest.main()
