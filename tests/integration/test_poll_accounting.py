"""Poller scheduling/health and actual PostgreSQL accounting controls, no provider."""
import os
import threading
import unittest
import uuid
from types import SimpleNamespace

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, PollDayChanged, day_at, owner_binding, _admission, bind_restored_poll_policy, identity
from db_sync_alt.errors import LifecycleFailure
from db_sync_alt.sources.blockfrost.sync import METRICS_EVERY_REQUESTS
from db_sync_alt.store.database import METRICS_MARKER
import fixtures.sync as sync_fakes
from fixtures.networks import PREPROD


class PollAccountingPGTests(unittest.TestCase):
    def setUp(self):
        self.admin=psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'],autocommit=True)
        self.name='follow_budget_'+uuid.uuid4().hex[:12]
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.name)))
        self.dsn=make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'],dbname=self.name)
        self.db=psycopg.connect(self.dsn,autocommit=True)
        self.db.execute('CREATE SCHEMA chain; CREATE TABLE chain.sync_jobs(id text PRIMARY KEY,body jsonb); CREATE TABLE chain.metadata(key text PRIMARY KEY,value jsonb)')
        self.db.execute('SELECT pg_advisory_lock(36,1)')
        self.owner=SimpleNamespace(db=self.db,require_writer=lambda:None)
        self.clock=sync_fakes.Clock();self.key='provider-poll-'+day_at(self.clock());self.poll=None
        self.body=dict(job_id='originalpoll',kind='poll',day=day_at(self.clock()),attempts=0,budget=10000,state='active',
                       created_at=self.clock()-50,retry_at=0,failure_count=0)
    def tearDown(self):
        if self.poll:self.poll.close()
        self.db.close();self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(self.name)));self.admin.close()
    def connect_poll(self):
        self.poll=PollBudgetStore(None,owner_binding(self.owner),conninfo=self.dsn,clock=self.clock);return self.poll
    def create(self):
        """Create today's poll job through the poll store, which also writes its policy marker."""
        p=self.connect_poll();self.assertIsNone(p.sync_job(self.key));p.save_sync_job(self.key,self.body);return p
    def test_job_identity_attempts_history_and_no_budget_override(self):
        p=self.create()
        marker=self.db.execute("SELECT value FROM chain.metadata WHERE key=%s",('poll-budget-policy:'+self.body['day'],)).fetchone()[0]
        self.assertEqual(marker['initial'],self.body);self.assertEqual(marker['key'],self.key)
        job=p.sync_job(self.key);self.assertEqual(job,self.body)
        p.save_sync_job(self.key,{**job,'attempts':399})
        job=p.sync_job(self.key)
        with self.assertRaises(LifecycleFailure):p.save_sync_job(self.key,{**job,'budget':9999})
        with self.assertRaises(LifecycleFailure):p.save_sync_job(self.key,{**job,'attempts':398})
        p.save_sync_job(self.key,{**job,'attempts':400})
        self.assertEqual(self.db.execute('SELECT body->>\'attempts\' FROM chain.sync_jobs').fetchone()[0],'400')
    def test_accounting_is_independent_of_busy_canonical_transaction_and_single_poller(self):
        p=self.create()
        with self.db.transaction():
            self.db.execute('SELECT 1')
            job=p.sync_job(self.key);p.save_sync_job(self.key,{**job,'attempts':400})
        with self.assertRaisesRegex(LifecycleFailure,'writer_busy'):
            PollBudgetStore(None,owner_binding(self.owner),conninfo=self.dsn,clock=self.clock)
        with self.assertRaises(LifecycleFailure):p.sync_job('provider-active')
        self.db.execute('SELECT pg_advisory_unlock(36,1)')
        with self.assertRaisesRegex(LifecycleFailure,'database_unavailable'):p.sync_job(self.key)
        failures=[];stop=threading.Event()
        worker=HeadPoller(lambda:p,lambda:sync_fakes.Client(self.clock),sync_fakes.health(),stop,on_fatal=failures.append,network=PREPROD)
        thread=worker.start();thread.join(2)
        self.assertFalse(thread.is_alive());self.assertEqual(len(failures),1)
        self.assertTrue(p.db.closed)
    def test_missing_or_changed_expected_row_cas_refuses(self):
        p=self.create();job=p.sync_job(self.key)
        self.db.execute('UPDATE chain.sync_jobs SET body=%s WHERE id=%s',(Jsonb({**job,'attempts':401}),self.key))
        with self.assertRaises(LifecycleFailure):p.save_sync_job(self.key,{**job,'attempts':400})
        self.assertEqual(self.db.execute('SELECT body->>\'attempts\' FROM chain.sync_jobs').fetchone()[0],'401')
    def test_new_day_admission_and_job_without_policy_refusal(self):
        self.clock.now+=86400
        p=self.connect_poll();key='provider-poll-'+day_at(self.clock());self.assertIsNone(p.sync_job(key))
        new={**self.body,'job_id':'newday','day':day_at(self.clock())}
        p.save_sync_job(key,new)
        p.close();self.poll=None
        self.assertIsNone(_admission(self.db,key,new,identity(self.db)))
        # A 10,000-request job written without its policy marker is never admitted.
        self.db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s)',(self.key,Jsonb(self.body)))
        with self.assertRaisesRegex(LifecycleFailure,'invalid_sync_job'):_admission(self.db,self.key,self.body,identity(self.db))

    def test_utc_rollover_refuses_old_key_and_clock_regression(self):
        p=self.create();p.sync_job(self.key)
        self.clock.now+=86400
        with self.assertRaises(PollDayChanged):p.sync_job(self.key)
        newkey='provider-poll-'+day_at(self.clock());self.assertIsNone(p.sync_job(newkey))
        self.clock.now-=86400
        with self.assertRaises(LifecycleFailure):p.sync_job(self.key)

    def test_restore_only_guard_and_wrong_system_same_oid_refuse(self):
        self.create();self.poll.close();self.poll=None
        row=self.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s',(self.key,)).fetchone()[0]
        physical=identity(self.db)
        with self.assertRaises(LifecycleFailure):_admission(self.db,self.key,row,('different-system',physical[1]))
        self.db.execute('CREATE TABLE chain.service_state(id int,migration_state text)')
        self.db.execute("INSERT INTO chain.service_state VALUES(1,'ready')")
        before=self.db.execute('SELECT key,value FROM chain.metadata ORDER BY key').fetchall()
        with self.assertRaises(LifecycleFailure):bind_restored_poll_policy(self.owner,'a'*64)
        self.assertEqual(self.db.execute('SELECT key,value FROM chain.metadata ORDER BY key').fetchall(),before)

    def test_metrics_marker_is_a_row_kept_across_restarts_and_days(self):
        """`/metrics` on the first request of a fresh store, then once per METRICS_EVERY_REQUESTS requests counted for
        the profile: the marker is a database row, kept across processes (here: a new poll store per tick) and UTC days."""
        client=sync_fakes.Client(self.clock)
        def tick():
            p=self.connect_poll();self.addCleanup(p.close)
            before=len(client.calls)
            HeadPoller(lambda:p,lambda:client,sync_fakes.health(),threading.Event(),clock=self.clock,network=PREPROD).tick(p,client)
            p.close();self.poll=None
            return [path for path,_ in client.calls[before:]]
        marker=lambda:self.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s',(METRICS_MARKER,)).fetchone()[0]
        self.assertEqual(tick(),['/metrics','/blocks/latest','/blocks/100'])
        self.assertEqual({k:v for k,v in marker().items() if k!='reconciled_at'},{'version':1,'day':day_at(self.clock()),'used':2,'counted':1})
        self.assertEqual(tick(),['/blocks/latest','/blocks/100'])
        # Requests the ledger counted elsewhere bring the count to the limit at the next observation: one `/metrics`.
        m=marker();client.used+=METRICS_EVERY_REQUESTS-m['counted']-(client.used-m['used'])
        self.assertEqual(tick(),['/metrics','/blocks/latest','/blocks/100'])
        self.assertEqual(marker()['counted'],1)
        # A new UTC day (the ledger's count restarts) is no reason to reconcile.
        self.clock.now+=86400;client.used=0
        self.assertEqual(tick(),['/blocks/latest','/blocks/100'])
        self.assertEqual((marker()['day'],marker()['counted']),(day_at(self.clock()),2))
        # A write that advances a marker someone else changed meanwhile is skipped; without `expected` it replaces it.
        p=self.connect_poll();current=marker()
        p.save_metrics_marker({**current,'counted':99},expected={**current,'counted':0})
        self.assertEqual(marker(),current)
        p.save_metrics_marker({**current,'counted':5},expected=current)
        self.assertEqual(marker()['counted'],5)
        p.save_metrics_marker({'version':1,'day':current['day'],'used':0,'counted':0})
        self.assertEqual(marker(),{'version':1,'day':current['day'],'used':0,'counted':0})
