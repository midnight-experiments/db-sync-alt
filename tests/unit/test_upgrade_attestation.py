"""CLI refusal protocol, explicit source options and documented JSON examples."""
import contextlib
import io
import json
from pathlib import Path
import re
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_sync_alt.networks import network_profile
from db_sync_alt.store import upgrade

ROOT = Path(__file__).resolve().parents[2]
ATTESTATION = {'dbsync_version':'13.8.0.0','tx_in_retained':True,
               'epoch_stake_retained':True,'operator_attested':True}


class UpgradeAttestationTests(unittest.TestCase):
    def invoke(self, extra=(), *, network='preprod', legacy=False):
        output, error = io.StringIO(), io.StringIO()
        store = SimpleNamespace(network=network_profile(network), legacy_format=legacy)
        with patch('db_sync_alt.store.database.PgStore') as opened, patch.object(upgrade, 'upgrade_store', return_value={'schema_version':4,'changed':False}) as migrate, contextlib.redirect_stdout(output), contextlib.redirect_stderr(error):
            opened.return_value.__enter__.return_value = store
            try:
                upgrade.main(['--service','destination',*extra]); code = 0
            except SystemExit as stopped:
                code = stopped.code
        text = error.getvalue() or output.getvalue()
        self.assertNotIn('Traceback', text)
        return code, json.loads(text), opened.called, migrate

    def test_pair_required_returns_JSON_before_database_access(self):
        for args in (['--source-service','source'], ['--source-attestation','attestation.json']):
            with self.subTest(args=args):
                code, body, opened, migrate = self.invoke(args)
                self.assertEqual((code,body), (2,{'error':'source_attestation_pair_required'}))
                self.assertFalse(opened); migrate.assert_not_called()

    def test_missing_malformed_and_unreadable_files_refuse_without_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); malformed = root / 'malformed.json'; malformed.write_text('{')
            denied = root / 'denied.json'; denied.write_text('{}')
            original = Path.read_text
            def read(path, *args, **kwargs):
                if path == denied: raise PermissionError('private file detail')
                return original(path, *args, **kwargs)
            for path in (root / 'missing.json', malformed, denied):
                with self.subTest(path=path.name), patch.object(Path, 'read_text', read):
                    code, body, _, migrate = self.invoke(['--source-service','source','--source-attestation',str(path)])
                    self.assertEqual((code,body), (2,{'error':'schema_upgrade_failed'}))
                    migrate.assert_not_called()

    def test_ordinary_local_upgrade_keeps_no_source_path(self):
        code, body, opened, migrate = self.invoke()
        self.assertEqual((code,body), (0,{'schema_version':4,'changed':False}))
        self.assertTrue(opened)
        self.assertIsNone(migrate.call_args.args[1])
        # A store that already has epoch parameters needs no epoch source.
        self.assertIsNone(migrate.call_args.args[2])

    def test_nonce_only_store_reads_epoch_parameters_from_the_given_source(self):
        from db_sync_alt.store.epoch_parameters import SQLEpochSource
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'source.json'; path.write_text(json.dumps(ATTESTATION))
            code, _, _, migrate = self.invoke(['--source-service','source','--source-attestation',str(path)], legacy=True)
        self.assertEqual(code, 0)
        epochs = migrate.call_args.args[2]
        self.assertIsInstance(epochs, SQLEpochSource)
        self.assertIs(epochs.source, migrate.call_args.args[1])

    def test_explicit_pair_is_network_neutral_and_passes_exact_attestation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'source.json'; path.write_text(json.dumps(ATTESTATION))
            for network in ('mainnet','preprod','preview'):
                with self.subTest(network=network):
                    code, _, _, migrate = self.invoke(['--source-service','source','--source-attestation',str(path)], network=network)
                    self.assertEqual(code, 0)
                    source = migrate.call_args.args[1]
                    self.assertEqual(source.profile.network.network, network)
                    self.assertEqual(source.attestation, ATTESTATION)

    def test_help_explains_pair_and_operations_JSON_fences_parse(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output), self.assertRaises(SystemExit) as stopped:
            upgrade.main(['--help'])
        self.assertEqual(stopped.exception.code, 0)
        help_text = ' '.join(output.getvalue().split())
        self.assertIn('requires --source-attestation', help_text)
        self.assertIn('requires --source-service', help_text)
        blocks = re.findall(r'^\s*```json\s*\n(.*?)^\s*```\s*$',
                            (ROOT / 'docs/operations.md').read_text(), re.M | re.S)
        self.assertTrue(blocks)
        for block in blocks: json.loads(block)
        self.assertLessEqual(sum(line.startswith('## Unreleased') for line in (ROOT / 'CHANGELOG.md').read_text().splitlines()), 1)
