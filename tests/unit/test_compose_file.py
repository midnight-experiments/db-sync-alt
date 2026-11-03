"""The Compose deployment (deploy/compose.yaml, read as text) and its operator provisioning."""
from pathlib import Path
import re
import unittest

COMPOSE = Path(__file__).resolve().parents[2] / 'deploy' / 'compose.yaml'


class ComposeFileTests(unittest.TestCase):
    def test_postgres_health_check_uses_tcp(self):
        # The official image first initialises a new data directory with a temporary server that
        # listens on the Unix socket only; a socket check reports it ready, and the bootstrap's TCP
        # connection is then refused. Only a TCP check waits for the real server.
        text = COMPOSE.read_text()
        checks = re.findall(r'test: \[CMD-SHELL, "(pg_isready[^"]*)"\]', text)
        self.assertEqual(len(checks), 1, checks)
        self.assertIn('-h 127.0.0.1', checks[0])
        self.assertIn('condition: service_healthy', text)

    def test_postgres_reaches_the_node_network_only(self):
        text = COMPOSE.read_text()
        postgres = text.split('\n  service:')[0]
        self.assertNotIn('ports:', postgres)
        self.assertIn('networks: [backend, node]', postgres)
        self.assertRegex(text, r'\n  node: \{internal: true\}')
        # The service publishes its HTTP status on loopback only; nothing else.
        self.assertEqual(re.findall(r'- "(127\.0\.0\.1:[^"]+)"', text), ['127.0.0.1:${HTTP_PORT:-18080}:18080'])

    def test_operator_provisioning_creates_plain_logins_and_a_closed_database(self):
        from db_sync_alt.cli import compose
        text = [str(s.as_string(None)) for s in compose.provisioning_statements('owner-secret', 'reader-secret')]
        self.assertEqual(len(text), 8)
        for statement in text[:2]:
            self.assertIn('LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS', statement)
        self.assertIn('CONNECTION LIMIT 40', text[1])
        self.assertEqual([t.split(' SET ')[1] for t in text[2:5]],
                         ['"statement_timeout" = \'60s\'', '"idle_in_transaction_session_timeout" = \'60s\'', '"search_path" = \'public\''])
        self.assertEqual(text[5:], ['CREATE DATABASE "db_sync_alt" OWNER "db_sync_alt_owner" ALLOW_CONNECTIONS false',
                                    'REVOKE ALL ON DATABASE "db_sync_alt" FROM PUBLIC', 'ALTER DATABASE "db_sync_alt" ALLOW_CONNECTIONS true'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
