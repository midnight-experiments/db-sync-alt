"""Exact cooperative polling cancellation: no transport, budget or failure masking."""
import copy
import threading
import unittest
from types import SimpleNamespace
from fixtures.sync import Store,Client,Clock,FixtureUpdate,health
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.contract import Refusal
from db_sync_alt.errors import LifecycleFailure,CooperativeStop
from fixtures.networks import PREPROD_PROFILE

class CancellationTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.store=Store();self.client=Client(self.clock)
        self.stop=threading.Event();self.update=FixtureUpdate()
        self.driver=SyncDriver(SimpleNamespace(sync={}),client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=self.update)
    def test_stop_after_head_before_confirmed_is_cooperative_and_charged(self):
        self.client.on_call=lambda path:self.stop.set() if path=='/blocks/latest' else None
        self.driver.run_once(self.store,health(),self.stop)
        job=next(iter(self.store.jobs.values()))
        self.assertEqual([p for p,_ in self.client.calls],['/metrics','/blocks/latest'])
        self.assertEqual(job['attempts'],2);self.assertEqual(job['budget'],10000)
        self.assertNotIn('last_failure',job);self.assertEqual(self.store.target,100)
        self.assertIsNone(self.store.pending());self.assertEqual(self.update.calls,[])
        before=copy.deepcopy(self.store.jobs)
        self.driver.run_once(self.store,health(),self.stop)
        self.assertEqual(self.store.jobs,before)
    def test_non_cancellation_refusal_during_stop_propagates_and_records(self):
        def broken_head(path):
            if path=='/blocks/latest':self.stop.set()
        self.client.on_call=broken_head
        self.client.bad_hash=False
        original=self.client.get
        def malformed(path,purpose):
            row=original(path,purpose)
            if path=='/blocks/latest':row['hash']='not-a-hash'
            return row
        self.client.get=malformed
        with self.assertRaises(Refusal) as caught:self.driver.run_once(self.store,health(),self.stop)
        self.assertNotEqual(caught.exception.code,'interrupted')
        job=next(iter(self.store.jobs.values()))
        self.assertEqual(job['attempts'],2);self.assertEqual(job['last_failure']['code'],caught.exception.code)
    def test_interrupted_without_stop_is_not_swallowed(self):
        def error(*args):raise Refusal('interrupted')
        self.driver._run_once=error
        with self.assertRaisesRegex(Refusal,'interrupted'):self.driver.run_once(self.store,health(),self.stop)
    def test_same_named_lifecycle_failure_during_stop_is_not_swallowed(self):
        self.stop.set()
        def error(*args):raise LifecycleFailure('interrupted')
        self.driver._run_once=error
        with self.assertRaises(LifecycleFailure):self.driver.run_once(self.store,health(),self.stop)

    def test_owner_control_signal_skips_diagnostic_only_when_requested(self):
        from unittest.mock import Mock
        for requested in (False,True):
            with self.subTest(requested=requested):
                self.stop.set() if requested else self.stop.clear()
                recorder=Mock()
                def error(*args):
                    self.driver._diagnostic_provider=recorder
                    raise CooperativeStop()
                self.driver._run_once=error
                with self.assertRaises(CooperativeStop):self.driver.run_once(self.store,health(),self.stop)
                if requested:recorder.record_failure.assert_not_called()
                else:recorder.record_failure.assert_called_once()
    def test_unknown_error_during_stop_still_records_and_propagates(self):
        from unittest.mock import Mock
        self.stop.set();recorder=Mock()
        def error(*args):
            self.driver._diagnostic_provider=recorder
            raise RuntimeError('fixture failure')
        self.driver._run_once=error
        with self.assertRaises(RuntimeError):self.driver.run_once(self.store,health(),self.stop)
        recorder.record_failure.assert_called_once()

    def test_real_writer_failure_after_requested_stop_survives_clean_shutdown(self):
        from db_sync_alt.service.runtime import Service
        from dataclasses import replace
        from fixtures.service import Runtime,config
        for error,expected in [(LifecycleFailure('invalid_coverage'),'invalid_coverage'),
                               (RuntimeError('private fixture detail'),'internal_error')]:
            with self.subTest(expected=expected):
                entered=threading.Event()
                class FailingOnStop:
                    def run_once(inner,store,health,stop):
                        entered.set()
                        if not stop.wait(2):raise AssertionError('stop was not requested')
                        raise error
                runtime=Runtime()
                service=Service(replace(config(),shutdown_seconds=2),runtime=runtime,sync_driver=FailingOnStop())
                try:
                    service.start();self.assertTrue(entered.wait(2))
                    self.assertTrue(service.stop())
                    self.assertTrue(service.failed.is_set());self.assertEqual(service.failure,expected)
                    self.assertEqual(runtime.closed,1)
                    service._fail('database_unavailable')
                    self.assertEqual(service.failure,expected)
                finally:service.stop()

if __name__=='__main__':unittest.main()
