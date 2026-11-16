"""A lock timeout (SQLSTATE 55P03) in the update or the poll is a pause; every other database error keeps its path."""
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import psycopg

from db_sync_alt import errors
from db_sync_alt.errors import LockTimeouts, TransientSyncFailure, lock_timeout
from db_sync_alt.service.health import HealthState
from db_sync_alt.service.runtime import failure_code
from db_sync_alt.sources.blockfrost.poller import HeadPoller, day_at
from db_sync_alt.sources.blockfrost.sync import SyncDriver
import fixtures.sync as sync_fakes
from fixtures.networks import PREPROD, PREPROD_PROFILE
from fixtures.service import eventually

LOCK = psycopg.errors.LockNotAvailable
OTHERS = {'57014': psycopg.errors.QueryCanceled, '08006': psycopg.errors.ConnectionFailure,
          '40P01': psycopg.errors.DeadlockDetected}


class LockedStore(sync_fakes.Store):
    """`fail(key, error, times, after)`: after `after` saves of job row `key` go through, the next `times` raise `error`."""
    def __init__(self):
        super().__init__();self.failing={};self.records=[];self.close=lambda:None
    def fail(self,key,error,times=1,after=0):self.failing[key]=[error,times,after]
    def save_sync_job(self,key,body):
        if 'last_failure' in body:self.records.append(key)
        failing=self.failing.get(key)
        if failing and failing[2]:failing[2]-=1
        elif failing and failing[1]:
            failing[1]-=1
            raise failing[0]('canceling statement due to lock timeout')
        super().save_sync_job(key,body)


class ClassificationTests(unittest.TestCase):
    def test_only_sqlstate_55p03_is_a_lock_timeout(self):
        self.assertTrue(lock_timeout(LOCK()))
        for code,cls in OTHERS.items():
            with self.subTest(sqlstate=code):
                self.assertEqual(cls().sqlstate,code)
                self.assertFalse(lock_timeout(cls()))
                self.assertEqual(failure_code(cls()),'database_unavailable')
        self.assertFalse(lock_timeout(type('Fake',(Exception,),{'sqlstate':'55P03'})()))
        # Once lock timeouts persist the original error fails the service as any database error does.
        self.assertEqual(failure_code(LOCK()),'database_unavailable')

    def test_backoff_doubles_to_30_seconds_and_reset_starts_over(self):
        clock=sync_fakes.Clock();series=LockTimeouts(clock,'test');delays=[]
        with self.assertLogs('db_sync_alt.errors','WARNING') as logs:
            for _ in range(8):
                pause=series.retry(LOCK());delays.append(pause.retry_at-clock.now);clock.now+=pause.retry_at-clock.now
        self.assertEqual(delays,[1,2,4,8,16,30,30,30])
        self.assertIn('database_lock_timeout_retry worker=test retry_in_seconds=1 series_seconds=0',logs.output[0])
        series.reset()
        self.assertEqual(series.retry(LOCK()).retry_at-clock.now,1)

    def test_the_series_fails_once_it_lasts_the_limit(self):
        clock=sync_fakes.Clock();series=LockTimeouts(clock,'test');error=LOCK()
        series.retry(error);clock.now+=299.9
        self.assertEqual(series.retry(error).code,'database_lock_timeout')
        clock.now+=0.1
        with self.assertLogs('db_sync_alt.errors','WARNING') as logs:
            with self.assertRaises(LOCK) as raised:series.retry(error)
        self.assertIs(raised.exception,error)
        self.assertIn('database_lock_timeout_persisted worker=test seconds=300',logs.output[0])
        with patch.object(errors,'LOCK_TIMEOUT_LIMIT_SECONDS',6):
            series=LockTimeouts(clock,'test');series.retry(error);clock.now+=5
            series.retry(error);clock.now+=1
            with self.assertRaises(LOCK):series.retry(error)

    def test_transient_code_is_reported_as_a_backoff(self):
        failure=TransientSyncFailure('database_lock_timeout',retry_at=1.0)
        self.assertEqual((failure.code,failure.retry_at),('database_lock_timeout',1.0))
        health=HealthState(service_version='1',schema_version=1,network='preprod',profile='test')
        health.set_error('database_lock_timeout');health.poll_error('database_lock_timeout')
        snapshot=health.snapshot()
        self.assertEqual((snapshot['error_code'],snapshot['poll_error_code']),('database_lock_timeout','database_lock_timeout'))
        self.assertIn('provider_poll_database_lock_timeout',snapshot['reason_codes'])


class WriterTests(unittest.TestCase):
    """`SyncDriver.run_once` without a poller (it polls itself) over the in-memory store."""
    def setUp(self):
        self.clock=sync_fakes.Clock();self.store=LockedStore();self.client=sync_fakes.Client(self.clock,confirmed=130)
        self.health=HealthState(service_version='1',schema_version=1,network='preprod',profile='test',
                                wall_clock=self.clock,monotonic=self.clock)
        self.update=sync_fakes.FixtureUpdate()
        self.driver=SyncDriver(SimpleNamespace(sync={}),client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=self.update)

    def run_once(self):return self.driver.run_once(self.store,self.health,threading.Event())

    def pause(self):
        with self.assertRaises(TransientSyncFailure) as raised:self.run_once()
        self.assertEqual(raised.exception.code,'database_lock_timeout')
        return raised.exception.retry_at-self.clock.now

    def test_a_lock_timeout_pauses_the_update_without_a_failure_record(self):
        # The job's creation and its reservation go through; the save after the answer times out.
        self.store.fail('provider-active',LOCK,after=2)
        with self.assertLogs('db_sync_alt.errors','WARNING') as logs:
            self.assertEqual(self.pause(),1)
        self.assertEqual(logs.output,['WARNING:db_sync_alt.errors:database_lock_timeout_retry worker=sync retry_in_seconds=1 series_seconds=0'])
        self.assertEqual(self.store.records,[])
        self.assertTrue(all('last_failure' not in job for job in self.store.jobs.values()))
        self.clock.now+=1;self.run_once()
        self.assertEqual((self.store.target,self.store.jobs['provider-active']['state']),(130,'completed'))
        self.assertEqual(self.store.records,[])

    def test_other_database_errors_keep_todays_failure_path(self):
        for code,cls in OTHERS.items():
            with self.subTest(sqlstate=code):
                self.setUp();self.store.fail('provider-active',cls,after=2)
                with self.assertRaises(cls) as raised:self.run_once()
                self.assertEqual(failure_code(raised.exception),'database_unavailable')
                # As in earlier releases, the failure is recorded on the job.
                self.assertEqual(self.store.records,['provider-active'])

    def test_backoff_doubles_to_30_seconds_and_a_success_resets_it(self):
        self.store.fail('provider-active',LOCK,times=10**6)
        delays=[]
        for _ in range(8):
            delays.append(self.pause());self.clock.now+=delays[-1]+2
        self.assertEqual(delays,[1,2,4,8,16,30,30,30])
        self.store.failing.clear();self.run_once();self.assertEqual(self.store.target,130)
        # The next update starts after the publication interval.
        self.client.confirmed=140;self.clock.now+=60;self.store.fail('provider-active',LOCK,times=10**6)
        self.assertEqual(self.pause(),1)

    def test_lock_timeouts_for_300_seconds_fail_as_a_database_error(self):
        self.store.fail('provider-active',LOCK,times=10**6)
        first=self.clock.now;elapsed=[]
        while True:
            try:self.run_once()
            except TransientSyncFailure as pause:
                elapsed.append(self.clock.now-first);self.clock.now=pause.retry_at+2;continue
            except LOCK as error:
                self.assertEqual(failure_code(error),'database_unavailable');break
        self.assertLess(elapsed[-1],300);self.assertGreaterEqual(self.clock.now-first,300)
        self.assertEqual(self.store.records,[])

    def test_another_wait_between_lock_timeouts_restarts_the_series(self):
        self.store.fail('provider-active',LOCK,times=10**6)
        self.assertEqual(self.pause(),1);self.clock.now+=3
        self.assertEqual(self.pause(),2);self.clock.now+=4
        # Blockfrost fails the poll: a provider wait, not a lock timeout.
        self.client.errors['/blocks/latest']='HTTP 503; counted'
        with self.assertRaisesRegex(TransientSyncFailure,'provider_unavailable'):self.run_once()
        self.client.errors.clear();self.clock.now+=400
        self.assertEqual(self.pause(),1)


class PollerTests(unittest.TestCase):
    """`HeadPoller.run` on its own thread, real clock, pauses shortened."""
    def setUp(self):
        self.store=LockedStore();self.client=sync_fakes.Client(time.time);self.stop=threading.Event()
        self.health=HealthState(service_version='1',schema_version=1,network='preprod',profile='test')
        self.codes=[];report=self.health.poll_error
        self.health.poll_error=lambda code:(self.codes.append(code),report(code))
        self.failures=[];self.key='provider-poll-'+day_at(time.time())
        self.poller=HeadPoller(lambda:self.store,lambda:self.client,self.health,self.stop,interval=.03,
                               on_fatal=self.failures.append,network=PREPROD)
        patcher=patch.object(errors,'LOCK_TIMEOUT_BACKOFF_SECONDS',(0.05,));patcher.start();self.addCleanup(patcher.stop)

    def start(self):
        thread=self.poller.start()
        self.addCleanup(lambda:(self.stop.set(),thread.join(2)))
        return thread

    def test_a_poll_lock_timeout_is_a_poll_error_and_polling_continues(self):
        # Today's job is created; its next three saves time out.
        self.store.fail(self.key,LOCK,times=3,after=1)
        with self.assertLogs('db_sync_alt.errors','WARNING') as logs:
            thread=self.start()
            eventually(lambda:self.poller.latest() is not None and self.codes[-1:]==[None],seconds=3)
        self.assertEqual(self.codes[:3],['database_lock_timeout']*3)
        self.assertTrue(all('worker=poll' in line for line in logs.output))
        self.assertTrue(thread.is_alive());self.assertEqual(self.failures,[]);self.assertEqual(self.store.records,[])
        self.assertIsNone(self.health.snapshot()['poll_error_code'])

    def test_poll_lock_timeouts_that_persist_stop_the_poller_as_a_database_error(self):
        self.store.fail(self.key,LOCK,times=10**6,after=1)
        with patch.object(errors,'LOCK_TIMEOUT_LIMIT_SECONDS',0.3):
            thread=self.start();eventually(lambda:self.failures,seconds=3);thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(self.failures[0],LOCK);self.assertEqual(failure_code(self.failures[0]),'database_unavailable')
        self.assertEqual(self.store.records,[])

    def test_other_poll_database_errors_stay_fatal(self):
        for code,cls in OTHERS.items():
            with self.subTest(sqlstate=code):
                self.setUp()
                # Creation and reservation go through; the save after the answer fails.
                self.store.fail(self.key,cls,after=2)
                thread=self.start();eventually(lambda:self.failures,seconds=3);thread.join(2)
                self.assertFalse(thread.is_alive());self.assertIsInstance(self.failures[0],cls)
                self.assertEqual(self.store.records,[self.key])
