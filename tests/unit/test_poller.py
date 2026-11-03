"""Poller scheduling/health and actual PostgreSQL accounting controls, no provider."""
import copy
import threading
import time
import unittest
from types import SimpleNamespace

from db_sync_alt.sources.blockfrost.poller import HeadPoller, day_at
from db_sync_alt.service.health import HealthState, ChainPoint
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure
from db_sync_alt.sources.blockfrost.sync import SyncDriver
import fixtures.sync as sync_fakes
from fixtures.networks import PREPROD, PREPROD_PROFILE


class PollTests(unittest.TestCase):
    def setUp(self):
        self.clock=sync_fakes.Clock(); self.store=sync_fakes.Store(); self.client=sync_fakes.Client(self.clock)
        self.health=HealthState(service_version='1',schema_version=2,network='preprod',profile='test',
                                wall_clock=self.clock,monotonic=self.clock)
        self.stop=threading.Event()
        self.poller=HeadPoller(lambda:self.store,lambda:self.client,self.health,self.stop,
                               clock=self.clock,monotonic=self.clock,network=PREPROD)
    def test_durable_before_every_get_and_verifier_timer_unchanged(self):
        self.health.begin_operation('verification');original=self.health.snapshot()['operation']
        def inspect(path):
            row=self.store.jobs['provider-poll-'+day_at(self.clock())]
            self.assertEqual(row['attempts'],len(self.client.calls))
        self.client.on_call=inspect
        self.poller.tick(self.store,self.client)
        self.assertEqual(self.health.snapshot()['operation'],original)
        self.assertEqual(self.poller.latest().attempts,3)
        self.assertEqual(self.poller.latest().confirmed['height'],100)
        self.poller.tick(self.store,self.client)
        self.assertEqual(self.poller.latest().attempts,5)
        self.assertEqual(self.client.used,5)
    def test_delayed_pair_uses_oldest_request_age_and_snapshot_copy(self):
        self.client.on_call=lambda path:setattr(self.clock,'now',self.clock.now+8)
        start=self.clock.now;self.poller.tick(self.store,self.client)
        snap=self.poller.latest();self.assertEqual(snap.observed_at,start)
        self.assertEqual(self.health.snapshot()['freshness']['provider_observation_age_seconds'],24)
        snap.head['height']=0
        self.assertGreater(snap.head['height'],0)
    def test_failed_pair_does_not_replace_previous_or_clear_writer_error(self):
        self.poller.tick(self.store,self.client);before=self.poller.latest()
        self.health.set_error('database_timeout');self.client.errors['/blocks/100']='HTTP 503; counted'
        with self.assertRaises(TransientSyncFailure):self.poller.tick(self.store,self.client)
        self.assertIs(self.poller.latest(),before)
        self.assertEqual(self.health.snapshot()['error_code'],'database_timeout')
        self.assertEqual(self.store.jobs['provider-poll-'+day_at(self.clock())]['attempts'],5)
    def test_lower_confirmed_source_preserves_snapshot_and_durable_backoff(self):
        self.poller.tick(self.store,self.client);before=self.poller.latest()
        committed=ChainPoint(101,'ab'*32,1700000000)
        self.health.set_coverage(committed=committed,served=committed,witness=ChainPoint(2261,'cd'*32,1700000020))
        with self.assertRaisesRegex(TransientSyncFailure,'provider_unavailable'):
            self.poller.tick(self.store,self.client)
        self.assertIs(self.poller.latest(),before)
        row=self.store.jobs['provider-poll-'+day_at(self.clock())]
        self.assertEqual(row['attempts'],5);self.assertGreater(row['retry_at'],self.clock())
        self.assertIn('provider_behind_served',self.health.snapshot()['reason_codes'])
    def test_stop_between_latest_and_confirmed_preserves_reservation(self):
        self.client.on_call=lambda path:self.stop.set() if path=='/blocks/latest' else None
        with self.assertRaises(Exception):self.poller.tick(self.store,self.client)
        self.assertIsNone(self.poller.latest());self.assertEqual(self.client.used,2)
        self.assertNotIn('last_failure',next(iter(self.store.jobs.values())))
    def test_rollover_new_day_keeps_old_row(self):
        self.poller.tick(self.store,self.client);old=copy.deepcopy(self.store.jobs)
        self.clock.now+=86400;self.poller.tick(self.store,self.client)
        for key,row in old.items():self.assertEqual(self.store.jobs[key],row)
        self.assertEqual(len(self.store.jobs),2)
    def test_polling_continues_while_writer_is_busy(self):
        # Thread/clock control, not a claim about real HTTP interval performance.
        h=sync_fakes.health();s=sync_fakes.Store();s.close=lambda:None
        client=sync_fakes.Client(sync_fakes.Clock());stop=threading.Event()
        h.begin_operation('verification')
        p=HeadPoller(lambda:s,lambda:client,h,stop,interval=.03,network=PREPROD)
        thread=p.start()
        try:
            deadline=time.monotonic()+2
            while (p.latest() is None or p.latest().sequence<3) and time.monotonic()<deadline:time.sleep(.01)
            self.assertGreaterEqual(p.latest().sequence,3)
            self.assertEqual(h.snapshot()['operation']['stage'],'verification')
        finally:stop.set();thread.join(2)
        self.assertFalse(thread.is_alive())
    def test_follow_consumes_observation_without_poll_or_completion_wait(self):
        self.client.confirmed=130;self.poller.tick(self.store,self.client)
        # Use production-clock freshness while deterministic wall clock drives jobs.
        snap=self.poller.snapshot
        self.poller.snapshot=type(snap)(**{**snap.__dict__,'observed_mono':time.monotonic()})
        update=sync_fakes.FixtureUpdate();driver=SyncDriver(SimpleNamespace(sync={}),client=self.client,
                    profile=PREPROD_PROFILE,clock=self.clock,update_fn=update);driver._poller=self.poller
        before=len(self.client.calls);driver.run_once(self.store,self.health,self.stop)
        self.assertEqual([x[0] for x in self.client.calls[before:]],['/metrics','/unit'])
        self.client.confirmed=131;self.clock.now+=1;self.poller.tick(self.store,self.client)
        snap=self.poller.snapshot;self.poller.snapshot=type(snap)(**{**snap.__dict__,'observed_mono':time.monotonic()})
        driver.run_once(self.store,self.health,self.stop)
        self.assertEqual(update.calls,[(101,130),(131,131)])
    def test_optional_coalescing_is_bounded_default_is_immediate(self):
        self.assertEqual(SyncDriver(SimpleNamespace(sync={}),profile=PREPROD_PROFILE).options['follow_min_blocks'],1)
        update=sync_fakes.FixtureUpdate();driver=SyncDriver(SimpleNamespace(sync={'follow_min_blocks':3,'follow_max_wait_seconds':40}),
            client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=update);driver._poller=self.poller
        self.client.confirmed=101;self.poller.tick(self.store,self.client)
        def use():
            snap=self.poller.snapshot;self.poller.snapshot=type(snap)(**{**snap.__dict__,'observed_mono':time.monotonic()})
            driver.run_once(self.store,self.health,self.stop)
        use();self.assertFalse(update.calls)
        self.clock.now+=40;self.poller.tick(self.store,self.client);use()
        self.assertEqual(update.calls,[(101,101)])
    def test_managed_schema1_refuses_silent_slow_mode(self):
        d=SyncDriver(SimpleNamespace(sync={}),profile=PREPROD_PROFILE)
        with self.assertRaisesRegex(LifecycleFailure,'unsupported_follow_mode'):
            d.start_polling(self.store,self.health,self.stop,lambda e:None)


