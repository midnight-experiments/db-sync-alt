# SPDX-License-Identifier: Apache-2.0
"""Real PostgreSQL + reader role + lifecycle, deterministic accounted idle provider."""
import os
from pathlib import Path
import tempfile
import time
import unittest
import uuid
from dataclasses import replace
import psycopg
from psycopg import sql
from db_sync_alt.store.database import PgStore
from db_sync_alt.backup import backup
from db_sync_alt.service.runtime import Service, ServiceConfig, PostgresRuntime
from db_sync_alt.errors import LifecycleFailure
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.store.reader import grant_reader
from db_sync_alt.service.health import HealthPolicy
from fixtures.generations import make_store
from fixtures.service import config as test_config, eventually, get, real_reader
from fixtures.blockfrost import provider_row


class IdleClient:
    def __init__(self,routes):self.routes=routes;self.used=0
    def status(self):return {'budget_used':self.used,'daily_limit':50000}
    def get(self,path,purpose):
        self.used+=1
        if path=='/metrics':return []
        return self.routes[path]




class RealServiceTests(unittest.TestCase):
    def setUp(self):
        self.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.databases=[];self.roles=[];self.services=[]
        self.old_servicefile=os.environ.get('PGSERVICEFILE');os.environ['PGSERVICEFILE']=str(self.root/'pg_service.conf')
        self.dsn,self.name,self.alias=self.database()
        make_store(self.dsn)
        # The schema-2 upgrade is explicit (schema-1 archives are covered by
        # integration.test_incremental). No implicit runtime upgrade.
        with PgStore(conninfo=self.dsn) as owned:upgrade_store(owned)
        with PgStore(conninfo=self.dsn,readonly=True) as store:
            gen=store.generation();self.target=gen['target_height']
            self.routes={'/blocks/latest':provider_row(store.block_at(gen['id'],gen['witness_height'])),
                         '/blocks/'+str(gen['target_height']):provider_row(store.block_at(gen['id'],gen['target_height']))}
    def tearDown(self):
        for service in self.services:service.stop()
        if self.old_servicefile is None:os.environ.pop('PGSERVICEFILE',None)
        else:os.environ['PGSERVICEFILE']=self.old_servicefile
        if self.admin.closed:self.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        for name in self.databases:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))
        for role in self.roles:self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
        self.admin.close();self.tmp.cleanup()
    def database(self):
        name='db_sync_alt_test_life_'+uuid.uuid4().hex[:8];self.databases.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        dsn=psycopg.conninfo.make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=name)
        settings=psycopg.conninfo.conninfo_to_dict(dsn);alias='service_'+uuid.uuid4().hex[:8]
        with (self.root/'pg_service.conf').open('a') as out:
            out.write('['+alias+']\n'+''.join(k+'='+v+'\n' for k,v in settings.items())+'\n')
        return dsn,name,alias
    def configuration(self,alias,dsn,**kwargs):
        """Service configuration for the database behind `alias`, with an operator-created reader of `dsn`."""
        reader=real_reader(self.admin,dsn,self.root/'pg_service.conf');self.roles.append(reader['role'])
        cfg=ServiceConfig(alias,reader,http_port=test_config().http_port,policy=HealthPolicy(poll_seconds=.1,heartbeat_seconds=.05),
            storage_reserves={'/tmp':1},**{'network':'preprod',**kwargs})
        return cfg,reader['role']
    def start(self,cfg):
        driver=SyncDriver(cfg,client=IdleClient(self.routes))
        service=Service(cfg,sync_driver=driver);self.services.append(service);service.start();return service
    def wait_ready(self,service):
        eventually(lambda:service.health.health()[0]==200 or service.failed.is_set(),seconds=15)
        self.assertFalse(service.failed.is_set(),service.failure)
        self.assertEqual(get(service,'/health')[0],200)
        self.assertEqual(service.health.snapshot()['schema_version'],'5')
    def test_owned_restart_duplicate_fencing_and_writer_connection_loss(self):
        cfg,role=self.configuration(self.alias,self.dsn)
        service=self.start(cfg);self.wait_ready(service)
        other=self.start(replace(cfg,http_port=test_config().http_port))
        eventually(other.failed.is_set);self.assertEqual(other.failure,'writer_busy');other.stop()
        with PgStore(conninfo=self.dsn,readonly=True) as store:
            job=store.sync_job(next(row[0] for row in store.db.execute("SELECT id FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%'")))
        # Terminate this DB's sessions, including writer. A new owner must fence afresh.
        self.admin.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s',(self.name,))
        eventually(service.failed.is_set,seconds=5);self.assertEqual(service.failure,'database_unavailable');service.stop()
        restarted=self.start(cfg);self.wait_ready(restarted)
        with PgStore(conninfo=self.dsn,readonly=True) as store:
            new=store.sync_job(next(row[0] for row in store.db.execute("SELECT id FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%'")))
            self.assertEqual(new['job_id'],job['job_id']);self.assertGreater(new['attempts'],job['attempts'])
            self.assertEqual(store.generation()['target_height'],self.target)
    def quota_job(self):
        with PgStore(conninfo=self.dsn) as store:
            ready=store.generation()
            job={'job_id':'quota-pause-fixture','attempts':17,'budget':10000,'state':'active',
                 'kind':'acquire','created_at':time.time(),'retry_at':time.time()+86400,
                 'failure_count':1,'error_code':'quota_exhausted','target':ready['target_height']+1,
                 'witness':ready['target_height']+2161,'parent':ready['id']}
            store.save_sync_job('provider-active',job)
            assets=store.db.execute('SELECT * FROM chain.assets ORDER BY id').fetchall()
        return job,assets

    def quota_service(self,cfg,*,observer_fails=False):
        class Runtime(PostgresRuntime):
            def open_writer(inner,health):
                store=super().open_writer(health);inner.pid=store.db.info.backend_pid;return store
            def writer_check(inner,store,timeout):
                super().writer_check(store,timeout);inner.checks=getattr(inner,'checks',0)+1
            def observe(inner):
                if observer_fails:raise ConnectionError('fixture independent observer')
                return super().observe()
        runtime=Runtime(cfg);client=IdleClient(self.routes)
        # The independent poller observes the same exhausted shared allowance;
        # a writer's acquisition backoff alone is not a global quota signal.
        client.status=lambda:{'budget_used':50000,'daily_limit':50000,'exhausted':True}
        service=Service(cfg,runtime=runtime,sync_driver=SyncDriver(cfg,client=client))
        self.services.append(service);service.start()
        eventually(lambda:service.health.snapshot()['sync']=='quota_wait' or service.failed.is_set(),seconds=15)
        self.assertFalse(service.failed.is_set(),service.failure);self.assertEqual(client.used,0)
        return service,runtime,client

    def test_quota_pause_writer_loss_exits_and_fresh_restart_preserves_retry(self):
        job,assets=self.quota_job();cfg,_=self.configuration(self.alias,self.dsn)
        service,runtime,client=self.quota_service(cfg)
        first_challenge=runtime._owner_challenge
        self.assertTrue(self.admin.execute('SELECT pg_terminate_backend(%s)',(runtime.pid,)).fetchone()[0])
        eventually(service.failed.is_set,seconds=8)
        self.assertEqual(service.failure,'database_unavailable');self.assertTrue(service.stop());self.assertEqual(client.used,0)
        restarted,other,new_client=self.quota_service(cfg)
        self.assertNotEqual(other.pid,runtime.pid);self.assertNotEqual(other._owner_challenge,first_challenge)
        time.sleep(.3);self.assertFalse(restarted.failed.is_set());self.assertEqual(new_client.used,0)
        with PgStore(conninfo=self.dsn,readonly=True) as store:
            self.assertEqual(store.sync_job('provider-active'),job)
            self.assertEqual(store.db.execute('SELECT * FROM chain.assets ORDER BY id').fetchall(),assets)
            self.assertEqual(store.generation()['target_height'],self.target)

    def test_long_quota_pause_and_independent_observer_failure_do_not_restart(self):
        job,assets=self.quota_job();cfg,_=self.configuration(self.alias,self.dsn)
        for observer_fails in (False,True):
            with self.subTest(observer_fails=observer_fails):
                service,runtime,client=self.quota_service(cfg,observer_fails=observer_fails)
                eventually(lambda:getattr(runtime,'checks',0)>=2 or service.failed.is_set(),seconds=8)
                self.assertFalse(service.failed.is_set(),service.failure)
                self.assertEqual(client.used,0);self.assertEqual(service.health.snapshot()['sync'],'quota_wait')
                if observer_fails:self.assertEqual(get(service,'/health')[0],503)
                with PgStore(conninfo=self.dsn,readonly=True) as store:
                    self.assertEqual(store.sync_job('provider-active'),job)
                    self.assertEqual(store.db.execute('SELECT * FROM chain.assets ORDER BY id').fetchall(),assets)
                self.assertTrue(service.stop())

    def test_reader_service_of_another_database_never_reports_serving(self):
        other_dsn,other_name,other_alias=self.database()
        make_store(other_dsn)
        cfg,role=self.configuration(self.alias,other_dsn)
        with PgStore(conninfo=other_dsn) as other:
            upgrade_store(other);grant_reader(other.db,role)
        service=self.start(cfg)
        # Another db-sync-alt database answers the reader's query, but not under this writer's lock.
        eventually(lambda:service.health.snapshot()['error_code']=='backend_identity_mismatch' or service.failed.is_set(),seconds=15)
        self.assertFalse(service.failed.is_set(),service.failure)
        value=service.health.snapshot()
        self.assertEqual((value['serving'],value['served_target']),('unready',None))
        self.assertEqual(get(service,'/health')[0],503)

    def test_observer_database_probe_and_reader_check_refuse_retargeted_services(self):
        other_dsn,other_name,other_alias=self.database()
        make_store(other_dsn)
        cfg,role=self.configuration(self.alias,self.dsn)
        other_reader=real_reader(self.admin,other_dsn,self.root/'pg_service.conf');self.roles.append(other_reader['role'])
        with PgStore(conninfo=other_dsn) as other:
            upgrade_store(other);grant_reader(other.db,other_reader['role'])
        runtime=PostgresRuntime(cfg)
        with PgStore(self.alias) as writer:
            report=runtime.bind_reader(writer)
            self.assertEqual((report['role'],report['privileges'],report['warnings']),(role,'node_relations_only',[]))
            self.assertEqual(report['settings'],{'statement_timeout':'60s','idle_in_transaction_session_timeout':'60s','search_path':'public',
                                                 'default_transaction_read_only':None,'connection_limit':40})
            challenge=runtime._owner_challenge
            self.assertNotEqual(challenge[0],36)
            self.assertTrue(runtime.database_check(.5));self.assertTrue(runtime.reader_check())
            self.assertEqual(runtime.observe().committed.height,self.target)
            for retargeted in (replace(cfg,pg_service=other_alias),replace(cfg,reader=other_reader)):
                runtime.config=retargeted
                calls=(runtime.observe,lambda:runtime.database_check(.5)) if retargeted.pg_service==other_alias else (runtime.reader_check,)
                for call in calls:
                    with self.assertRaises(LifecycleFailure) as failure:call()
                    self.assertEqual(str(failure.exception),'backend_identity_mismatch')
                    with psycopg.connect(other_dsn,autocommit=True) as probe:
                        self.assertTrue(probe.execute('SELECT pg_try_advisory_lock(%s,%s)',challenge).fetchone()[0])
                        probe.execute('SELECT pg_advisory_unlock(%s,%s)',challenge)
            # A reader service that logs in as another role is not the configured reader.
            runtime.config=replace(cfg,reader={'role':role,'pg_service':other_reader['pg_service']})
            with self.assertRaises(LifecycleFailure) as failure:runtime.reader_check()
            self.assertEqual(str(failure.exception),'invalid_reader_role')

    def test_reader_with_more_than_the_node_relations_is_refused(self):
        cfg,role=self.configuration(self.alias,self.dsn)
        reader=sql.Identifier(role)
        def refused(label):
            with PgStore(self.alias) as writer:
                with self.assertRaises(LifecycleFailure,msg=label) as failure:PostgresRuntime(cfg).bind_reader(writer)
                self.assertEqual(failure.exception.code,'invalid_reader_role',label)
        owner=self.admin.execute('SELECT current_user').fetchone()[0]
        cluster=[(sql.SQL('ALTER ROLE {} '+a).format(reader),sql.SQL('ALTER ROLE {} NO'+a).format(reader)) for a in ('SUPERUSER','CREATEDB','CREATEROLE','REPLICATION','BYPASSRLS')]
        cluster+=[(sql.SQL('ALTER ROLE {} NOLOGIN').format(reader),sql.SQL('ALTER ROLE {} LOGIN').format(reader)),
                  (sql.SQL('GRANT pg_read_all_data TO {}').format(reader),sql.SQL('REVOKE pg_read_all_data FROM {}').format(reader)),
                  (sql.SQL('GRANT {} TO {}').format(sql.Identifier(owner),reader),sql.SQL('REVOKE {} FROM {}').format(sql.Identifier(owner),reader))]
        database=[(sql.SQL('GRANT SELECT ON chain.outputs TO {}').format(reader),sql.SQL('REVOKE SELECT ON chain.outputs FROM {}').format(reader)),
                  (sql.SQL('GRANT USAGE ON SCHEMA serve TO {}').format(reader),sql.SQL('REVOKE USAGE ON SCHEMA serve FROM {}').format(reader)),
                  (sql.SQL('GRANT CREATE ON SCHEMA public TO {}').format(reader),sql.SQL('REVOKE CREATE ON SCHEMA public FROM {}').format(reader)),
                  (sql.SQL('GRANT TEMPORARY ON DATABASE {} TO {}').format(sql.Identifier(self.name),reader),sql.SQL('REVOKE TEMPORARY ON DATABASE {} FROM {}').format(sql.Identifier(self.name),reader)),
                  (sql.SQL('GRANT UPDATE ON public.block TO {}').format(reader),sql.SQL('REVOKE UPDATE ON public.block FROM {}').format(reader)),
                  (sql.SQL('GRANT EXECUTE ON FUNCTION chain.guard_truncate() TO {}').format(reader),sql.SQL('REVOKE EXECUTE ON FUNCTION chain.guard_truncate() FROM {}').format(reader))]
        with psycopg.connect(self.dsn,autocommit=True) as db:
            for changes,target in ((cluster,self.admin),(database,db)):
                for grant,revoke in changes:
                    target.execute(grant)
                    try:refused(grant.as_string(db))
                    finally:target.execute(revoke)
        with PgStore(self.alias) as writer:
            self.assertEqual(PostgresRuntime(cfg).bind_reader(writer)['privileges'],'node_relations_only')
        # Report-only: a reader without its defaults is accepted, and the missing settings are named.
        bare=real_reader(self.admin,self.dsn,self.root/'pg_service.conf',settings=False);self.roles.append(bare['role'])
        with PgStore(self.alias) as writer:
            report=PostgresRuntime(replace(cfg,reader=bare)).bind_reader(writer)
        self.assertEqual(report['warnings'],['reader_statement_timeout_unset','reader_idle_in_transaction_timeout_unset','reader_search_path_not_public'])
        with PgStore(self.alias) as writer:
            with self.assertRaises(LifecycleFailure) as failure:PostgresRuntime(replace(cfg,reader={'role':'missing_reader','pg_service':'missing'})).bind_reader(writer)
            self.assertEqual(failure.exception.code,'invalid_reader_role')

    def test_reader_keeps_answering_while_the_service_is_stopped(self):
        cfg,role=self.configuration(self.alias,self.dsn)
        service=self.start(cfg);self.wait_ready(service)
        with psycopg.connect(service=cfg.reader['pg_service'],autocommit=True) as node:
            before=node.execute('SELECT max(block_no) FROM block').fetchone()[0]
            self.assertTrue(service.stop())
            # Serving depends on the database state only: the updater's absence changes nothing.
            self.assertEqual(node.execute('SELECT max(block_no) FROM block').fetchone()[0],before)
        with psycopg.connect(service=cfg.reader['pg_service'],autocommit=True) as node:
            self.assertEqual(node.execute('SELECT max(block_no) FROM block').fetchone()[0],before)

    def test_empty_native_restore_then_restart_does_not_restore_again(self):
        archive=self.root/'recovery.dump';manifest=self.root/'recovery.json'
        backup(archive,manifest,conninfo=self.dsn)
        dst,name,alias=self.database();cfg,role=self.configuration(alias,dst,backup_archive=str(archive),backup_manifest=str(manifest))
        service=self.start(cfg)
        eventually(lambda:service.ready.is_set() or service.failed.is_set(),seconds=15)
        self.assertFalse(service.failed.is_set(),service.failure)
        self.wait_ready(service);service.stop()
        archive.unlink();manifest.unlink()  # Existing ready target must be sufficient.
        restarted=self.start(cfg);self.wait_ready(restarted)
        with PgStore(conninfo=dst,readonly=True) as store:self.assertEqual(store.generation()['target_height'],self.target)
    def test_first_run_from_a_schema3_backup_serves_schema_4(self):
        from fixtures.schema import to_schema3
        source,_,_=self.database();make_store(source)
        with PgStore(conninfo=source) as store:to_schema3(store)
        archive=self.root/'schema3.dump';manifest=self.root/'schema3.json'
        backup(archive,manifest,conninfo=source)
        dst,name,alias=self.database();cfg,role=self.configuration(alias,dst,backup_archive=str(archive),backup_manifest=str(manifest))
        service=self.start(cfg);self.wait_ready(service)
        self.assertEqual(service.health.snapshot()['integrity'],{'base':2,'checkpointed_at':None})
        with PgStore(conninfo=dst,readonly=True) as store:
            self.assertEqual((store.state()['schema_version'],store.generation()['target_height']),(5,self.target))
        with psycopg.connect(service=cfg.reader['pg_service'],autocommit=True) as node:
            self.assertEqual(node.execute('SELECT max(block_no) FROM block').fetchone()[0],self.target+2160)
    def test_actual_postgres_restart_exits_then_resumes_same_durable_database(self):
        cfg,role=self.configuration(self.alias,self.dsn)
        service=self.start(cfg);self.wait_ready(service)
        with PgStore(conninfo=self.dsn,readonly=True) as store:
            poll_key,poll_before=store.db.execute("SELECT id,body FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%' ORDER BY id DESC LIMIT 1").fetchone()
            policy_before=store.meta('poll-budget-policy:'+poll_before['day'])
            state_before=store.state();generation_before=store.generation()
            physical_before=store.db.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0]
        control=Path(os.environ['DB_SYNC_ALT_TEST_RESTART_DIR']);(control/'restart-request').write_text('owned test only')
        eventually(service.failed.is_set,seconds=20);self.assertEqual(service.failure,'database_unavailable');service.stop()
        eventually(lambda:(control/'restart-complete').exists(),seconds=90)
        self.admin.close();self.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        restarted=self.start(cfg);self.wait_ready(restarted)
        with PgStore(conninfo=self.dsn,readonly=True) as store:
            self.assertEqual(store.generation(),generation_before)
            self.assertEqual(store.state(),state_before)
            self.assertEqual(store.db.execute('SELECT system_identifier::text FROM pg_control_system()').fetchone()[0],physical_before)
            poll_after=store.sync_job(poll_key)
            self.assertEqual(poll_after['job_id'],poll_before['job_id'])
            self.assertEqual(poll_after['budget'],poll_before['budget'])
            self.assertGreater(poll_after['attempts'],poll_before['attempts'])
            self.assertEqual(store.meta('poll-budget-policy:'+poll_before['day']),policy_before)
            self.assertEqual(restarted.health.snapshot()['schema_version'],'5')


if __name__=='__main__':unittest.main(verbosity=2)
