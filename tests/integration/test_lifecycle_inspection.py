"""Lifecycle-only PG projection, refusal parity with the diagnostic inspection, and full verification."""
import os
from pathlib import Path
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from db_sync_alt.store import database as d
from db_sync_alt.service.runtime import PostgresRuntime, failure_code
from db_sync_alt.cli import run as launcher
from db_sync_alt.contract import Refusal
from db_sync_alt.store.upgrade import upgrade_store
from fixtures.generations import PROFILE, make_store


class InspectionTests(unittest.TestCase):
    def setUp(self):
        self.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.databases=[];self.roles=[];self.old=os.environ.get('PGSERVICEFILE')
        os.environ['PGSERVICEFILE']=str(self.root/'pg_service.conf')
    def tearDown(self):
        for db in self.databases:self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(db)))
        for role in self.roles:self.admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
        self.admin.close();self.tmp.cleanup()
        if self.old is None:os.environ.pop('PGSERVICEFILE',None)
        else:os.environ['PGSERVICEFILE']=self.old
    def service(self,dsn):
        name='inspection_'+uuid.uuid4().hex[:8]
        with (self.root/'pg_service.conf').open('a') as out:out.write('['+name+']\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(dsn).items()))
        return name
    def database(self,owned=True,rows=1):
        name='db_sync_alt_test_inspect_'+uuid.uuid4().hex[:8];self.databases.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        dsn=psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
        if owned:
            with d.PgStore(conninfo=dsn,create=True,network='preprod') as store:
                store.set_meta('profile',PROFILE.identity)
                for n in range(rows):store.db.execute("INSERT INTO chain.generations VALUES(%s,NULL,'ready','cardano-db-sync-postgresql',0,0,1,2161,%s)",(n,Jsonb({'historical_manifest':str(n)})))
                store.db.execute("UPDATE chain.service_state SET migration_state='ready',canonical_ready=%s WHERE id=1",(rows-1 if rows else None,))
        return dsn,self.service(dsn)
    def result(self,fn):
        try:return ('ok',fn())
        except Exception as error:return (type(error).__name__,getattr(error,'sqlstate',None),str(error) if isinstance(error,Refusal) else None,failure_code(error))
    def diagnostic(self,service):
        # The service's inspection through the full diagnostic database.inspect():
        # the reference that lifecycle-only refusals must match.
        with d.connect(service) as db:
            if d.empty_destination(db):return {'empty':True}
        return {'empty':False,**d.inspect(service)}
    def current(self,service):return PostgresRuntime(SimpleNamespace(pg_service=service)).inspect()
    def parity(self,service):
        diagnostic,current=self.result(lambda:self.diagnostic(service)),self.result(lambda:self.current(service))
        if diagnostic[0]=='ok':
            self.assertEqual(current[0],'ok');self.assertEqual(current[1],{k:diagnostic[1][k] for k in current[1]})
        else:self.assertEqual(current,diagnostic)
        return diagnostic,current
    def test_full_diagnostic_contract_and_no_history_on_lifecycle(self):
        dsn,service=self.database(rows=4)
        before=d.inspect(service)
        self.assertEqual([x['id'] for x in before['generations']],[0,1,2,3])
        self.assertTrue(all('manifest' not in x for x in before['generations']))
        with patch.object(d.PgStore,'status',side_effect=AssertionError('lifecycle_loaded_full_diagnostics')):
            got=self.current(service)
        self.assertNotIn('generations',got);self.assertNotIn('bytes',got);self.assertNotIn('checkpoints',got)
        self.assertEqual(d.inspect(service),before);self.parity(service)
    def test_fresh_revocation_and_migration_state_not_cached(self):
        dsn,service=self.database();self.parity(service)
        with psycopg.connect(dsn,autocommit=True) as db:
            for state in ('importing','restoring','ready'):
                db.execute('UPDATE chain.service_state SET migration_state=%s,revoked=true WHERE id=1',(state,))
                self.parity(service);self.assertEqual(self.current(service)['migration_state'],state);self.assertTrue(self.current(service)['revoked'])
            db.execute('UPDATE chain.service_state SET revoked=false WHERE id=1')
        self.assertFalse(self.current(service)['revoked'])
    def test_empty_and_owned_restore_marker_parity(self):
        dsn,service=self.database(owned=False);self.assertEqual(self.current(service),{'empty':True});self.parity(service)
        with psycopg.connect(dsn,autocommit=True) as db:
            db.execute('CREATE SCHEMA db_sync_alt_restore_guard; CREATE TABLE db_sync_alt_restore_guard.identity(owner text,manifest_sha256 text)')
            db.execute('INSERT INTO db_sync_alt_restore_guard.identity VALUES(%s,%s)',(d.OWNER,'a'*64))
        old,new=self.parity(service);self.assertEqual(new[2],'incomplete_owned_destination')
    def test_missing_state_row_and_unsupported_identity_parity(self):
        for mutation in ("UPDATE chain.service_state SET owner='other'",'UPDATE chain.service_state SET schema_version=99','DELETE FROM chain.service_state'):
            with self.subTest(mutation=mutation):
                dsn,service=self.database()
                with psycopg.connect(dsn,autocommit=True) as db:db.execute(mutation)
                self.parity(service)
    def test_corrupt_contract_parity(self):
        dsn,service=self.database()
        with psycopg.connect(dsn,autocommit=True) as db:db.execute("UPDATE chain.metadata SET value='{}'::jsonb WHERE key='contract'")
        self.parity(service)
    def test_missing_relations_precede_migration_and_revoked_handling(self):
        for table in ('generations','checkpoints'):
            with self.subTest(table=table):
                dsn,service=self.database()
                with psycopg.connect(dsn,autocommit=True) as db:
                    db.execute("UPDATE chain.service_state SET migration_state='importing',revoked=true")
                    db.execute(sql.SQL('DROP TABLE chain.{} CASCADE').format(sql.Identifier(table)))
                diagnostic,current=self.parity(service);self.assertEqual(current[0],'UndefinedTable')
    def test_missing_generation_id_column_refusal_order(self):
        dsn,service=self.database()
        with psycopg.connect(dsn,autocommit=True) as db:db.execute('ALTER TABLE chain.generations RENAME COLUMN id TO changed_id')
        diagnostic,current=self.parity(service);self.assertEqual(current[0],'UndefinedColumn')
    def test_generation_column_access_parity_empty_and_nonempty(self):
        for rows in (0,1):
            with self.subTest(rows=rows):
                dsn,unused=self.database(rows=rows);params=psycopg.conninfo.conninfo_to_dict(dsn);role='inspect_role_'+uuid.uuid4().hex[:8];self.roles.append(role)
                self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
                self.admin.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(params['dbname']),sql.Identifier(role)))
                with psycopg.connect(dsn,autocommit=True) as db:
                    db.execute(sql.SQL('GRANT USAGE ON SCHEMA chain TO {}').format(sql.Identifier(role)))
                    db.execute(sql.SQL('GRANT SELECT ON chain.service_state,chain.metadata,chain.checkpoints TO {}').format(sql.Identifier(role)))
                    db.execute(sql.SQL('GRANT SELECT(id) ON chain.generations TO {}').format(sql.Identifier(role)))
                alias=self.service(psycopg.conninfo.make_conninfo(dsn,user=role));diagnostic,current=self.parity(alias)
                self.assertEqual(current[0],'ok' if rows==0 else 'InsufficientPrivilege')
    def test_checkpoint_column_access_matches_count_star(self):
        for allowed in (True,False):
            with self.subTest(allowed=allowed):
                dsn,unused=self.database();params=psycopg.conninfo.conninfo_to_dict(dsn);role='inspect_checkpoint_'+uuid.uuid4().hex[:8];self.roles.append(role)
                self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
                self.admin.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(params['dbname']),sql.Identifier(role)))
                with psycopg.connect(dsn,autocommit=True) as db:
                    db.execute(sql.SQL('GRANT USAGE ON SCHEMA chain TO {}').format(sql.Identifier(role)))
                    db.execute(sql.SQL('GRANT SELECT ON chain.service_state,chain.metadata,chain.generations TO {}').format(sql.Identifier(role)))
                    if allowed:db.execute(sql.SQL('GRANT SELECT(generation) ON chain.checkpoints TO {}').format(sql.Identifier(role)))
                alias=self.service(psycopg.conninfo.make_conninfo(dsn,user=role));diagnostic,current=self.parity(alias)
                self.assertEqual(current[0],'ok' if allowed else 'InsufficientPrivilege')
    def test_native_configure_state_gate_uses_bounded_lookup(self):
        dsn,service=self.database();params=dict(psycopg.conninfo.conninfo_to_dict(dsn),password='')
        args=SimpleNamespace(network='preprod',http_port=18099,node_security_parameter=None,node_configuration_dir=None)
        class GatePassed(Exception):pass
        with psycopg.connect(dsn,autocommit=True) as db:
            db.execute('UPDATE chain.service_state SET schema_version=5')
            with patch.object(d.PgStore,'status',side_effect=AssertionError('native_gate_loaded_history')),patch.object(launcher,'private',side_effect=GatePassed),self.assertRaises(GatePassed):
                launcher.configure(args,params,'fixture',self.root,db,True)
            db.execute('UPDATE chain.service_state SET revoked=true')
            with patch.object(launcher,'private',side_effect=GatePassed),self.assertRaisesRegex(launcher.LaunchError,'database_not_ready_for_native_launch'):
                launcher.configure(args,params,'fixture',self.root,db,True)
    def test_full_runtime_verification_still_runs_and_refuses_fact_corruption(self):
        dsn,service=self.database(owned=False);make_store(dsn)
        cfg=SimpleNamespace(pg_service=service,network='preprod')
        with d.PgStore(conninfo=dsn) as store:
            upgrade_store(store)
            with patch.object(d.PgStore,'status',side_effect=AssertionError('verify_loaded_full_status')):
                PostgresRuntime(cfg).verify(store)
                # Administrative corruption of this disposable fixture only;
                # product trigger guards remain unchanged and are tested separately.
                store.db.execute('ALTER TABLE chain.outputs DISABLE TRIGGER USER')
                try:store.db.execute('UPDATE chain.outputs SET value=value+1 WHERE value=100')
                finally:store.db.execute('ALTER TABLE chain.outputs ENABLE TRIGGER USER')
                with self.assertRaises(Refusal):PostgresRuntime(cfg).verify(store)

if __name__=='__main__':unittest.main(verbosity=2)
