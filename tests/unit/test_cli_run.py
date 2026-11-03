"""Native setup safety and actual foreground PostgreSQL 17 restore/restart (synthetic provider)."""
import contextlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import db_sync_alt.cli.run as launcher
KEY='preprod'+'synthetic0000000000000000'


class LauncherArguments(unittest.TestCase):
    def args(self,*values):return launcher.parser().parse_args(['--network','preprod',*values])
    def test_defaults_and_explicit_precedence(self):
        with patch.dict(os.environ,{'DBSYNC_DATABASE_PASSWORD':''}):
            d=launcher.database(self.args())
            self.assertEqual((d['host'],d['port'],d['dbname'],d['user'],d['password']),('127.0.0.1',5432,'db_sync_alt','db_sync_alt_owner',''))
        with patch.dict(os.environ,{'DBSYNC_DATABASE_PASSWORD':'environment'}):
            d=launcher.database(self.args('--database-url','postgres://urluser:urlpassword@127.0.0.1:5432/test','--database-user','explicit','--database-password','flag'))
            self.assertEqual((d['user'],d['password']),('explicit','flag'))
            self.assertEqual(launcher.database(self.args('--database-url','postgres://u:p@127.0.0.1/test'))['password'],'environment')
    def test_network_and_credential_errors_are_redacted_before_state(self):
        for argv in (['--network',KEY],['--network','preprod','--unknown',KEY],['--network','preprod','--blockfrost-key','wrong-secret']):
            output=io.StringIO()
            with contextlib.redirect_stderr(output):self.assertEqual(launcher.main(argv),2)
            self.assertNotIn(KEY,output.getvalue());self.assertNotIn('wrong-secret',output.getvalue())
    def test_url_rejects_remote_unix_options_tls_and_injection(self):
        for url in ('postgres://localhost/test','postgres://192.0.2.1/test','postgres:///test','postgres://127.0.0.1,127.0.0.2/test',
                    'postgres://127.0.0.1/test?sslmode=require','postgres://127.0.0.1/test?sslmode=disable','postgres://127.0.0.1/test#x',
                    'postgres://127.0.0.1/test%0ahost=remote','postgres://127.0.0.1/test%','postgres://127.0.0.1/test?','postgres://127.0.0.1:0/test'):
            with self.subTest(url=url),self.assertRaises(launcher.LaunchError):launcher.database(self.args('--database-url',url))
        self.assertEqual(launcher.database(self.args('--database-url','postgresql://[::1]:55432/db'))['host'],'::1')
    def test_password_file_prompt_and_permissions(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'secret with spaces';p.write_text('private-value\n');p.chmod(0o600)
            self.assertEqual(launcher.credential(None,p,'UNSET'),'private-value')
            link=Path(tmp)/'symlink';link.symlink_to(p)
            with self.assertRaises(launcher.LaunchError):launcher.credential(None,link,'UNSET')
            p.chmod(0o644)
            with self.assertRaises(launcher.LaunchError):launcher.credential(None,p,'UNSET')
        with patch('sys.stdin.isatty',return_value=True),patch.object(launcher.getpass,'getpass',return_value='hidden'):
            self.assertEqual(launcher.credential(launcher.PROMPT,None,'UNSET'),'hidden')
        with patch('sys.stdin.isatty',return_value=False),self.assertRaises(launcher.LaunchError):launcher.credential(launcher.PROMPT,None,'UNSET')
    def test_reader_login_is_named_and_never_the_owner(self):
        params=launcher.database(self.args())
        with patch.dict(os.environ,{'DBSYNC_READER_PASSWORD':'reader-secret'}):
            self.assertEqual(launcher.reader(self.args(),params),('db_sync_alt_reader','reader-secret'))
            self.assertEqual(launcher.reader(self.args('--reader-role','node_reader'),params)[0],'node_reader')
            for role in ('db_sync_alt_owner','bad name','x;drop'):
                with self.subTest(role=role),self.assertRaisesRegex(launcher.LaunchError,'invalid_reader_role'):
                    launcher.reader(self.args('--reader-role',role),params)
    def test_shared_credential_mismatch_before_role_or_ledger(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        db=Mock();args=self.args();params=launcher.database(args)
        with patch.dict(os.environ,{'DBSYNC_PROVIDER_ADAPTER':'shared','DBSYNC_SHARED_ROOT':'/synthetic/shared','DBSYNC_SHARED_PROFILE':'preprod'}),patch.object(launcher,'db_identity',return_value={}),patch('db_sync_alt.sources.blockfrost.provider.SharedClient',return_value=SimpleNamespace(client=SimpleNamespace(key='different'))):
            with self.assertRaisesRegex(launcher.LaunchError,'shared_provider_credential_mismatch'):
                launcher.configure(args,params,KEY,Path('/unused'),db,False)
        db.execute.assert_not_called()

    def test_libpq_environment_is_controlled_then_restored(self):
        with patch.dict(os.environ,{'PGPASSWORD':'wrong','PGHOST':'remote','PGOPTIONS':'-c search_path=bad','DBSYNC_PROVIDER_ADAPTER':'shared','DBSYNC_SHARED_ROOT':'/synthetic/shared'}):
            with launcher.controlled_environment():
                self.assertFalse(any(k.startswith('PG') for k in os.environ))
                self.assertEqual(os.environ['DBSYNC_PROVIDER_ADAPTER'],'shared')
            self.assertEqual(os.environ['PGPASSWORD'],'wrong')
    def test_existing_incomplete_or_symlink_state_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            state=Path(tmp)/'state';state.mkdir(mode=0o700)
            args=self.args('--blockfrost-key',KEY,'--state-dir',str(state))
            with self.assertRaisesRegex(launcher.LaunchError,'incomplete'):launcher.launch(args)
            link=Path(tmp)/'link';link.symlink_to(state)
            args.state_dir=link
            with self.assertRaisesRegex(launcher.LaunchError,'unsafe_state'):launcher.launch(args)
    def test_distinct_ports_rejected_before_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            state=Path(tmp)/'never-created'
            args=self.args('--blockfrost-key',KEY,'--state-dir',str(state),'--http-port','5432')
            with self.assertRaisesRegex(launcher.LaunchError,'ports'):launcher.launch(args)
            self.assertFalse(state.exists())
    def test_actual_help_and_bad_argument_process_redaction(self):
        help_run=subprocess.run([sys.executable,'-m','db_sync_alt','run','--help'],capture_output=True,text=True)
        self.assertEqual(help_run.returncode,0);self.assertIn('--database-url',help_run.stdout)
        bad=subprocess.run([sys.executable,'-m','db_sync_alt','run','--network',KEY],capture_output=True,text=True)
        self.assertEqual(bad.returncode,2);self.assertNotIn(KEY,bad.stdout+bad.stderr)


