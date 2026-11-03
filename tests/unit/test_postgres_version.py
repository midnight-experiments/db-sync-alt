"""PostgreSQL 17 is the only accepted major version: server, client tools, backup manifests and the launcher."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from db_sync_alt import backup
from db_sync_alt.cli import run as launcher
from db_sync_alt.contract import Profile, Refusal
from db_sync_alt.resources import profile_dir
from db_sync_alt.store import database

OTHER_SERVERS = (150015, 160010, 180001)


class FakeConnection:
    def __init__(self, server_version):
        self.info = type('Info', (), {'server_version': server_version})()
        self.closed = False

    def close(self):
        self.closed = True


def tool_output(text):
    return subprocess.CompletedProcess(['pg_dump', '--version'], 0, stdout=text, stderr='')


class ServerVersionTests(unittest.TestCase):
    def test_only_major_17_connects(self):
        self.assertEqual(database.POSTGRES_MAJOR, 17)
        accepted = FakeConnection(170011)
        with patch.object(database.psycopg, 'connect', return_value=accepted):
            self.assertIs(database.connect(conninfo='host=127.0.0.1'), accepted)
        self.assertFalse(accepted.closed)
        for version in OTHER_SERVERS:
            refused = FakeConnection(version)
            with self.subTest(server_version=version), patch.object(database.psycopg, 'connect', return_value=refused):
                with self.assertRaisesRegex(Refusal, '^unsupported_postgres_version$'):
                    database.connect(conninfo='host=127.0.0.1')
                self.assertTrue(refused.closed)


class ClientToolTests(unittest.TestCase):
    def test_tool_and_server_must_both_be_17(self):
        cases = [
            ('pg_dump (PostgreSQL) 17.11 (Debian 17.11-1.pgdg12+2)\n', 170011, 17),
            ('pg_restore (PostgreSQL) 15.15\n', 170011, None),
            ('pg_restore (PostgreSQL) 18.1\n', 170011, None),
            ('pg_dump (PostgreSQL) 15.15\n', 150015, None),
            ('pg_dump (PostgreSQL) 18.1\n', 180001, None),
        ]
        for text, server, expected in cases:
            with self.subTest(tool=text.strip(), server=server), \
                    patch.object(backup.subprocess, 'run', return_value=tool_output(text)):
                db = FakeConnection(server)
                if expected is None:
                    with self.assertRaisesRegex(Refusal, '^postgres_backup_tool_version_mismatch$'):
                        backup.check_tool('pg_dump', db)
                else:
                    self.assertEqual(backup.check_tool('pg_dump', db), expected)


class ManifestTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.archive = self.root / 'state.dump'
        self.archive.write_bytes(b'synthetic archive')

    def manifest(self, major, name):
        body = {'format': backup.BACKUP_FORMAT, 'postgres_major': major, 'schema_version': database.SCHEMA_VERSION,
                'owner': database.OWNER, 'profile': Profile(profile_dir('preprod')).identity,
                'archive': backup.file_hash(self.archive)}
        path = self.root / name
        path.write_text(json.dumps(body))
        return path

    def test_restore_refuses_other_majors_before_connecting(self):
        with patch.object(backup, 'connect', side_effect=AssertionError('restore connected')):
            for major in (15, 16, 18, '17', None):
                with self.subTest(postgres_major=major):
                    with self.assertRaisesRegex(Refusal, '^unsupported_backup_identity$'):
                        backup.restore(self.archive, self.manifest(major, 'm-' + str(major) + '.json'))
        # A 17 manifest passes the identity check and reaches the destination.
        with patch.object(backup, 'connect', side_effect=Refusal('destination_reached')):
            with self.assertRaisesRegex(Refusal, '^destination_reached$'):
                backup.restore(self.archive, self.manifest(17, 'm-17.json'))

    def test_launcher_accepts_only_17_backups(self):
        args = argparse.Namespace(network='preprod', archive=self.archive, manifest=self.manifest(17, 'accepted.json'))
        self.assertEqual(launcher.accepted_backup(args), (self.archive.absolute(), args.manifest.absolute()))
        for major in (15, 16, 18):
            with self.subTest(postgres_major=major):
                args = argparse.Namespace(network='preprod', archive=self.archive, manifest=self.manifest(major, 'refused-' + str(major) + '.json'))
                with self.assertRaisesRegex(launcher.LaunchError, '^invalid_accepted_backup$'):
                    launcher.accepted_backup(args)
