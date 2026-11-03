"""Native setup safety and actual foreground PostgreSQL 17 restore/restart (synthetic provider)."""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
import uuid

ROOT=Path(__file__).resolve().parents[2]
KEY='preprod'+'synthetic0000000000000000'




class NativeForeground(unittest.TestCase):
    def test_fresh_restore_ready_restart_stop_and_refusals(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import conninfo_to_dict,make_conninfo
        from fixtures.generations import make_store
        from db_sync_alt.store.upgrade import upgrade_store
        from db_sync_alt.backup import backup
        from db_sync_alt.store.database import PgStore
        from db_sync_alt.node_queries import named
        config=conninfo_to_dict(os.environ['DB_SYNC_ALT_TEST_DSN'])
        source_name='native_seed_'+uuid.uuid4().hex[:12];target_name='native_target_'+uuid.uuid4().hex[:12]
        admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        processes=[];reader='native_reader_'+uuid.uuid4().hex[:10];created_roles=[reader]
        def dsn(name):return make_conninfo(**dict(config,dbname=name))
        def port():
            with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
        try:
            for name in (source_name,target_name):admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
            # The operator's reader login; the launcher never creates or alters a role.
            admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT CONNECTION LIMIT 40').format(sql.Identifier(reader)))
            for setting,value in (('statement_timeout','60s'),('idle_in_transaction_session_timeout','60s'),('search_path','public')):
                admin.execute(sql.SQL('ALTER ROLE {} SET {} = {}').format(sql.Identifier(reader),sql.Identifier(setting),sql.Literal(value)))
            with tempfile.TemporaryDirectory(prefix='native launcher ') as tmp:
                temp=Path(tmp);state=temp/'private state'
                make_store(dsn(source_name))
                with PgStore(conninfo=dsn(source_name)) as store:upgrade_store(store)
                archive=temp/'accepted archive.dump';manifest=temp/'accepted manifest.json'
                backup(archive,manifest,conninfo=dsn(source_name))
                http=port()
                url='postgresql://127.0.0.1:'+config.get('port','5432')+'/'+target_name
                common=['--network','preprod','--database-url',url,'--database-user',config.get('user','postgres'),
                        '--reader-role',reader,'--state-dir',str(state),'--http-port',str(http)]
                env=dict(os.environ,DBSYNC_BLOCKFROST_KEY=KEY,DBSYNC_DATABASE_PASSWORD=config.get('password',''),DBSYNC_READER_PASSWORD='',
                         PGPASSWORD='must-not-be-used',PGHOST='192.0.2.1',PGOPTIONS='-c search_path=wrong')
                # A failure before main must not echo any supplied credential or URL password.
                def command(*extra):return [sys.executable,str(ROOT/'tests/fixtures/native_launcher.py'),*common,*extra]
                def start(*extra):
                    p=subprocess.Popen(command(*extra),env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);processes.append(p)
                    end=time.monotonic()+75
                    while time.monotonic()<end:
                        if p.poll() is not None:
                            out,err=p.communicate();self.fail('launcher stopped: '+out+err)
                        try:
                            with urllib.request.urlopen('http://127.0.0.1:'+str(http)+'/health',timeout=1) as response:
                                if response.status==200:return p
                        except (OSError,urllib.error.HTTPError):pass
                        time.sleep(.1)
                    self.fail('native readiness timed out')
                def stop(p):
                    p.send_signal(signal.SIGTERM);out,err=p.communicate(timeout=15)
                    self.assertEqual(p.returncode,0,out+err);self.assertNotIn(KEY,out+err)
                preflight_failures=[(['--database-url','postgresql://127.0.0.1:'+config.get('port','5432')+'/missing_'+uuid.uuid4().hex],env),([],env)]
                if config.get('password'):
                    preflight_failures.insert(0,([],dict(env,DBSYNC_DATABASE_PASSWORD='wrong-owner-secret')))
                for bad_options,bad_env in preflight_failures:
                    failed=subprocess.run(command(*bad_options),env=bad_env,capture_output=True,text=True,timeout=10)
                    self.assertEqual(failed.returncode,2);self.assertFalse(state.exists(),failed.stderr)
                    self.assertNotIn('wrong-owner-secret',failed.stderr)
                p=start('--archive',str(archive),'--manifest',str(manifest))
                receipt=json.loads((state/'receipt.json').read_text())
                self.assertEqual((receipt['version'],receipt['identity']['reader']),(2,reader))
                self.assertEqual(json.loads((state/'service.json').read_text())['reader'],{'role':reader,'pg_service':'db_sync_alt_reader'})
                status=json.loads(urllib.request.urlopen('http://127.0.0.1:'+str(http)+'/status',timeout=2).read())
                self.assertEqual((status['reader']['role'],status['reader']['warnings']),(reader,[]))
                self.assertEqual(stat.S_IMODE(state.stat().st_mode),0o700)
                for child in state.iterdir():
                    if child.is_file():self.assertEqual(stat.S_IMODE(child.stat().st_mode),0o600)
                tracked={name:(state/name).read_bytes() for name in receipt['files']}
                # midnight-node's connection: the reader, straight to the database.
                with psycopg.connect(dsn(target_name),user=reader,autocommit=True,prepare_threshold=0) as node:
                    self.assertIsNotNone(node.execute(named('get_epoch_nonce','candidates_data_source').replace('$1','%s'),(psycopg.types.numeric.Int4(2),)).fetchone())
                    with self.assertRaises(psycopg.errors.InsufficientPrivilege):node.execute('SELECT * FROM chain.service_state')
                blocked=subprocess.run(command(),env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(blocked.returncode,2);self.assertIn('launcher_already_running',blocked.stderr)
                stop(p)
                self.assertTrue(receipt['identity']['cluster_system_identifier'])
                self.assertEqual(receipt['identity']['owner_password_sha256'],hashlib.sha256(config.get('password','').encode()).hexdigest())
                ledger_before=json.loads((state/'accounting/usage.json').read_text())
                p=start();stop(p)
                self.assertEqual(json.loads((state/'receipt.json').read_text()),receipt)
                self.assertEqual({name:(state/name).read_bytes() for name in tracked},tracked)
                ledger_after=json.loads((state/'accounting/usage.json').read_text())
                finger=hashlib.sha256(KEY.encode()).hexdigest()
                self.assertGreaterEqual(sum(ledger_after['keys'][finger]['days'].values()),sum(ledger_before['keys'][finger]['days'].values()))
                mismatch=subprocess.run(command('--http-port',str(port())),env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(mismatch.returncode,2);self.assertIn('identity_mismatch',mismatch.stderr)
                saved_receipt=(state/'receipt.json').read_text()
                corrupt=dict(receipt,files={})
                (state/'receipt.json').write_text(json.dumps(corrupt))
                omitted=subprocess.run(command(),env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(omitted.returncode,2);self.assertIn('invalid_state_receipt',omitted.stderr)
                corrupt=dict(receipt,identity=dict(receipt['identity'],cluster_system_identifier='wrong-cluster'))
                (state/'receipt.json').write_text(json.dumps(corrupt))
                cluster=subprocess.run(command(),env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(cluster.returncode,2);self.assertIn('identity_mismatch',cluster.stderr)
                (state/'receipt.json').write_text(saved_receipt)
                admin.execute(sql.SQL('ALTER ROLE {} CREATEDB').format(sql.Identifier(reader)))
                rejected=subprocess.run(command(),env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(rejected.returncode,2);self.assertIn('reader_role_identity_mismatch',rejected.stderr)
                admin.execute(sql.SQL('ALTER ROLE {} NOCREATEDB').format(sql.Identifier(reader)))
                (state/'accounting/usage.json').unlink()
                missing=subprocess.run(command(),env=env,capture_output=True,text=True,timeout=10)
                self.assertEqual(missing.returncode,2);self.assertFalse((state/'accounting/usage.json').exists())
                self.assertEqual({name:(state/name).read_bytes() for name in tracked},tracked)
                print('Native foreground restore, readiness, direct reader access, lock, graceful stop/restart, ledger and identity guards passed.')
        finally:
            for p in processes:
                if p.poll() is None:p.kill();p.communicate(timeout=10)
            for name in (source_name,target_name):admin.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(name)))
            for role in created_roles:
                admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
            admin.close()
