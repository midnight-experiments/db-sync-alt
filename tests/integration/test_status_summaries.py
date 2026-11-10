"""`db-sync-alt status` lists one summary row per generation and reads no manifest; restore reports the database size."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from db_sync_alt.backup import backup, restore
from db_sync_alt.store import database as d
from fixtures.generations import PROFILE, make_store

SUMMARY = ['id','parent','status','source','start_height','start_index','target_height','witness_height']
# One manifest far larger than the whole status output may be.
PADDING = 16*1024*1024


class StatusSummaryTests(unittest.TestCase):
    def setUp(self):
        self.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.databases=[];self.roles=[];self.services=self.root/'pg_service.conf'
    def tearDown(self):
        for db in self.databases:self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(db)))
        for role in self.roles:self.admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
        self.admin.close();self.tmp.cleanup()
    def blank(self):
        name='db_sync_alt_test_status_'+uuid.uuid4().hex[:8];self.databases.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        return psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
    def service(self,dsn):
        name='status_'+uuid.uuid4().hex[:8]
        with self.services.open('a') as out:out.write('['+name+']\n'+''.join(k+'='+v+'\n' for k,v in psycopg.conninfo.conninfo_to_dict(dsn).items()))
        return name
    def database(self,rows,pad=None):
        """A ready store with a linear history of `rows` generations; generation `pad` carries a large manifest."""
        dsn=self.blank()
        with d.PgStore(conninfo=dsn,create=True,network='preprod') as store:
            store.set_meta('profile',PROFILE.identity)
            for n in range(rows):
                manifest={'historical_manifest':str(n),**({'padding':'x'*PADDING} if n==pad else {})}
                store.db.execute("INSERT INTO chain.generations VALUES(%s,%s,'ready','cardano-db-sync-postgresql',0,0,%s,%s,%s)",(n,n-1 if n else None,1+n,2161+n,Jsonb(manifest)))
            store.db.execute("UPDATE chain.service_state SET migration_state='ready',canonical_ready=%s WHERE id=1",(rows-1 if rows else None,))
        return dsn
    def limited(self,dsn,columns):
        """A login that may read the state, metadata and checkpoints, and only `columns` of chain.generations."""
        role='status_role_'+uuid.uuid4().hex[:8];self.roles.append(role)
        self.admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        self.admin.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(psycopg.conninfo.conninfo_to_dict(dsn)['dbname']),sql.Identifier(role)))
        with psycopg.connect(dsn,autocommit=True) as db:
            db.execute(sql.SQL('GRANT USAGE ON SCHEMA chain TO {}').format(sql.Identifier(role)))
            db.execute(sql.SQL('GRANT SELECT ON chain.service_state,chain.metadata,chain.checkpoints TO {}').format(sql.Identifier(role)))
            db.execute(sql.SQL('GRANT SELECT({}) ON chain.generations TO {}').format(sql.SQL(',').join(map(sql.Identifier,columns)),sql.Identifier(role)))
        return psycopg.conninfo.make_conninfo(dsn,user=role)
    def status(self,dsn):
        """Run `db-sync-alt status` as an operator does; returns its exit code, stdout and stderr."""
        env=dict(os.environ,PGSERVICEFILE=str(self.services))
        done=subprocess.run([sys.executable,'-m','db_sync_alt','status','--service',self.service(dsn)],env=env,capture_output=True,text=True)
        return done.returncode,done.stdout,done.stderr
    def summaries(self,rows):
        return [{'id':n,'parent':n-1 if n else None,'status':'ready','source':'cardano-db-sync-postgresql','start_height':0,'start_index':0,'target_height':1+n,'witness_height':2161+n} for n in range(rows)]

    def test_status_prints_summary_rows_without_manifests(self):
        dsn=self.database(rows=4,pad=2)
        code,out,err=self.status(dsn);self.assertEqual(code,0,err)
        body=json.loads(out)
        self.assertEqual(body['generations'],self.summaries(4))
        self.assertEqual((body['migration_state'],body['canonical_ready'],body['checkpoints']),('ready',3,0))
        self.assertIsInstance(body['bytes'],int)
        self.assertNotIn('historical_manifest',out);self.assertLess(len(out),PADDING//64)
        with d.PgStore(conninfo=dsn,readonly=True) as store:self.assertEqual(store.status()['generations'],self.summaries(4))
    def test_status_needs_no_access_to_manifests(self):
        dsn=self.database(rows=3,pad=1);limited=self.limited(dsn,SUMMARY)
        with psycopg.connect(limited,autocommit=True) as db,self.assertRaises(psycopg.errors.InsufficientPrivilege):db.execute('SELECT manifest FROM chain.generations')
        code,out,err=self.status(limited);self.assertEqual(code,0,err)
        self.assertEqual(json.loads(out)['generations'],self.summaries(3))
    def test_permission_limited_role_on_empty_history(self):
        for rows in (0,1):
            with self.subTest(rows=rows):
                limited=self.limited(self.database(rows=rows),['id'])
                code,out,err=self.status(limited)
                if rows==0:
                    self.assertEqual(code,0,err);self.assertEqual(json.loads(out)['generations'],[])
                else:
                    # Generation columns beyond id are needed as soon as history exists.
                    self.assertEqual((code,json.loads(err)),(2,{'error':'postgres_operation_failed'}))
                    with d.PgStore(conninfo=limited,readonly=True) as store,self.assertRaises(psycopg.errors.InsufficientPrivilege):store.status()
    def test_restore_reports_database_size_without_listing_generations(self):
        source=self.blank();make_store(source)
        from db_sync_alt.store.upgrade import upgrade_store
        with d.PgStore(conninfo=source) as store:upgrade_store(store)
        archive,manifest=self.root/'state.dump',self.root/'state.json';backup(archive,manifest,conninfo=source)
        destination=self.blank()
        with patch.object(d.PgStore,'status',side_effect=AssertionError('restore_listed_generations')):
            result=restore(archive,manifest,conninfo=destination)
        self.assertEqual(set(result),{'status','canonical_ready','database_bytes','profile','freshness','poll_restore_binding'})
        self.assertEqual((result['status'],result['canonical_ready']),('restored',2))
        with psycopg.connect(destination) as db:size=db.execute('SELECT pg_database_size(current_database())').fetchone()[0]
        self.assertIsInstance(result['database_bytes'],int);self.assertAlmostEqual(result['database_bytes'],size,delta=size//10)

if __name__=='__main__':unittest.main(verbosity=2)
