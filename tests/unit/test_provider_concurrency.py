# SPDX-License-Identifier: Apache-2.0
"""Several requests of one update in flight: the accounted provider's prefetch window against a threaded fake client.

The fake client answers each path on the calling thread after a delay, counts the requests in flight and records, at
each call, whether the job attempts saved so far cover every call; the fake store saves a little late, so a request
sent before its reservation is saved would be seen. Each run is compared with the same requests made one at a time.
"""
import datetime as dt
import threading
import time
import unittest
from types import SimpleNamespace

from db_sync_alt.contract import Refusal
from db_sync_alt.errors import LifecycleFailure
from db_sync_alt.sources.blockfrost.adapter import prefetch, prefetch_scope
from db_sync_alt.sources.blockfrost.prefetch import Dispatcher, Pacer
from db_sync_alt.sources.blockfrost.sync import AccountedProvider, SyncDriver
from fixtures.networks import PREPROD, PREPROD_PROFILE
from fixtures.sync import Clock, Store, health

KEY = 'provider-active'
PATHS = ['/unit/%d' % i for i in range(40)]


class SlowStore(Store):
    """Saves a job record a little after it is asked to."""
    def save_sync_job(self, key, body):
        time.sleep(0.003)
        super().save_sync_job(key, body)


class ThreadedClient:
    """A shared client answering `{'path': path}` after `delay` (or `delays[path]`) on the calling thread; `errors[path]`
    fails that request with the shared client's text; `hooks[path]()` runs when it is sent."""
    def __init__(self, store, delay=0.02, clock=time.monotonic):
        self.store, self.delay, self.clock = store, delay, clock
        self.delays, self.errors, self.hooks = {}, {}, {}
        self.lock = threading.Lock()
        self.calls, self.sent_at, self.uncovered = [], [], []
        self.flight = self.most = self.used = self.cooldown = 0
        self.limit = 50000

    def status(self):
        with self.lock:
            return {'budget_used': self.used, 'daily_limit': self.limit, 'cooldown_until': self.cooldown}

    def get(self, path, purpose):
        with self.lock:
            self.used += 1
            self.calls.append(path)
            self.sent_at.append(self.clock())
            self.flight += 1
            self.most = max(self.most, self.flight)
            if self.store.jobs[KEY]['attempts'] < len(self.calls):
                self.uncovered.append(path)
        try:
            if path in self.hooks:
                self.hooks[path]()
            time.sleep(self.delays.get(path, self.delay))
            if path in self.errors:
                raise RuntimeError(self.errors[path] + '; request counted, no automatic retry')
            return {'path': path}
        finally:
            with self.lock:
                self.flight -= 1


class Run:
    """One provider over a fresh store and client, `size` requests in flight."""
    def __init__(self, size, *, budget=10000, pacer=None, delay=0.02, clock=None, client_clock=time.monotonic):
        self.clock = clock or Clock()
        self.store = SlowStore()
        day = dt.datetime.fromtimestamp(self.clock(), dt.timezone.utc).date().isoformat()
        self.store.metrics = {'version': 1, 'day': day, 'used': 0, 'counted': 0}  # reconciled: no `/metrics` first
        self.store.save_sync_job(KEY, {'job_id': 'fixed', 'attempts': 0, 'budget': budget, 'state': 'active', 'kind': 'acquire',
                                       'parent': 0, 'target': 101, 'witness': 2261})
        self.client = ThreadedClient(self.store, delay, client_clock)
        self.stop = threading.Event()
        dispatcher = Dispatcher(size, pacer) if size > 1 or pacer else None
        self.provider = AccountedProvider(self.store, KEY, self.store.sync_job(KEY), self.client, health(), self.stop,
                                          clock=self.clock, network=PREPROD, dispatcher=dispatcher)

    def traverse(self, paths, *, hinted=None, optional=(), before=None):
        """Request `paths` in order in one scope that hints `hinted` (all by default); `before(i)` runs before the i-th."""
        out = []
        with prefetch_scope(self.provider, paths if hinted is None else hinted):
            for index, path in enumerate(paths):
                if before is not None:
                    before(index)
                out.append(self.provider.get_optional(path) if path in optional else self.provider.get(path))
        return out

    def attempts(self):
        return self.store.jobs[KEY]['attempts']

    def receipts(self):
        """The receipts without their attempt number and time."""
        return [{k: v for k, v in r.items() if k not in ('attempt', 'at')} for r in self.provider.receipts]


def outcome(run, paths, **kw):
    try:
        return ('ok', run.traverse(paths, **kw))
    except (LifecycleFailure, Refusal) as error:
        return (type(error).__name__, error.code, getattr(error, 'retry_at', None))


class ConcurrencyTests(unittest.TestCase):
    def test_one_request_in_flight_sends_what_it_sends_without_hints(self):
        self.assertIsNone(SyncDriver(SimpleNamespace(sync={}), client=object(), profile=PREPROD_PROFILE)._dispatcher)
        self.assertIsNone(SyncDriver(SimpleNamespace(sync={'provider_concurrency': 1}), client=object(), profile=PREPROD_PROFILE)._dispatcher)
        workers = sum(t.name.startswith('db-sync-alt-request') for t in threading.enumerate())
        runs = []
        for hinted in (PATHS, []):
            run = Run(1)
            self.assertIsNone(run.provider.window)
            self.assertEqual(run.traverse(PATHS, hinted=hinted), [{'path': p} for p in PATHS])
            runs.append((run.client.calls, run.provider.receipts, run.store.saved, run.client.most))
        self.assertEqual(runs[0], runs[1])
        self.assertEqual(runs[0][3], 1)
        self.assertEqual(sum(t.name.startswith('db-sync-alt-request') for t in threading.enumerate()), workers)

    def test_four_in_flight_consume_in_order_with_the_same_results_and_reservations_first(self):
        # Every fifth path is not hinted: it goes out directly, beside the requests sent ahead.
        hinted = [p for i, p in enumerate(PATHS) if i % 5]
        timed = {}
        for size in (1, 4):
            run = Run(size, delay=0.05)
            start = time.monotonic()
            results = run.traverse(PATHS, hinted=hinted)
            timed[size] = (time.monotonic() - start, run)
            self.assertEqual(results, [{'path': p} for p in PATHS])
            self.assertEqual([r['endpoint'] for r in run.provider.receipts], PATHS)
            self.assertEqual(run.client.uncovered, [])
            self.assertEqual(sorted(run.client.calls), sorted(PATHS))
            self.assertEqual(run.attempts(), len(PATHS))
            attempts = [r['attempt'] for r in run.provider.receipts]
            self.assertEqual(sorted(attempts), list(range(1, len(PATHS) + 1)))
        one, four = timed[1][1], timed[4][1]
        self.assertEqual(one.receipts(), four.receipts())
        self.assertEqual(one.client.most, 1)
        self.assertEqual(four.client.most, 4)
        self.assertLess(timed[4][0], timed[1][0] / 2)

    def test_the_job_budget_and_the_quota_bound_what_is_sent_ahead(self):
        # Budget 10, 20 hinted paths: 10 requests, then the 11th waits for the next UTC day as with one in flight.
        for size in (1, 4):
            run = Run(size, budget=10)
            result = outcome(run, PATHS[:20])
            self.assertEqual(result[:2], ('TransientSyncFailure', 'request_budget_exhausted'))
            self.assertEqual((len(run.client.calls), run.attempts()), (10, 10))
            self.assertEqual(len(run.provider.receipts), 10)
        # Five requests left in the day: five are sent, then the day's quota wait.
        for size in (1, 4):
            run = Run(size)
            run.client.limit = 5
            result = outcome(run, PATHS[:20])
            self.assertEqual(result[:2], ('TransientSyncFailure', 'quota_exhausted'))
            self.assertEqual(len(run.client.calls), 5)
            self.assertEqual(run.client.uncovered, [])
        # A cooldown that begins with the first request: nothing is sent ahead after it.
        run = Run(4)
        run.client.hooks[PATHS[0]] = lambda: setattr(run.client, 'cooldown', run.clock() + 3600)
        result = outcome(run, PATHS[:20])
        self.assertEqual(result[:2], ('TransientSyncFailure', 'provider_unavailable'))
        sent = len(run.client.calls)
        self.assertLessEqual(sent, 4)  # the first one and those reserved with it, before the cooldown began
        self.assertEqual(sorted(run.client.calls), sorted(PATHS[:sent]))
        self.assertEqual(len(run.provider.receipts), sent)

    def test_a_failure_is_raised_at_the_same_request_and_later_answers_are_dropped(self):
        k = 6
        cases = {
            'timeout': 'TimeoutError', 'connection': 'RemoteDisconnected', '429': 'HTTP 429', '418': 'HTTP 418',
            '402': 'HTTP 402', '403': 'HTTP 403', 'invalid JSON': 'Invalid JSON response',
            '404 epochs': 'HTTP 404', '404 optional': 'HTTP 404',
        }
        for name, text in cases.items():
            with self.subTest(case=name):
                paths = list(PATHS[:16])
                if name == '404 epochs':
                    paths[k] = '/epochs/3'
                optional = {paths[k]} if name == '404 optional' else set()
                results = {}
                for size in (1, 4):
                    run = Run(size)
                    run.client.errors[paths[k]] = text
                    results[size] = (outcome(run, paths, optional=optional), run)
                (one, first), (four, second) = results[1], results[4]
                self.assertEqual(one, four)
                self.assertEqual(first.receipts(), second.receipts())
                saved = lambda run: {key: run.store.jobs[KEY].get(key) for key in ('failure_count', 'retry_at', 'error_code', 'state')}
                self.assertEqual(saved(first), saved(second))
                if name == '404 optional':
                    self.assertEqual(one[0], 'ok')
                    self.assertIsNone(one[1][k])
                    self.assertEqual(second.attempts(), first.attempts())
                    continue
                self.assertNotEqual(one[0], 'ok')
                self.assertEqual(len(second.provider.receipts), k)
                # The requests sent ahead of the failed one stay counted; at most N - 1 of them.
                self.assertEqual(first.attempts(), k + 1)
                self.assertLessEqual(second.attempts(), k + 1 + 3)
                self.assertTrue(set(paths[:k + 1]) <= set(second.client.calls) <= set(paths[:k + 1 + 3]))
                self.assertLessEqual(len(second.client.calls), second.attempts())
        expected = {'timeout': 'provider_timeout', 'connection': 'provider_unavailable', '429': 'provider_unavailable',
                    '418': 'provider_unavailable', '402': 'quota_exhausted', '403': 'provider_request_rejected',
                    'invalid JSON': 'provider_request_rejected', '404 epochs': 'provider_epoch_not_ready'}
        for name, code in expected.items():
            with self.subTest(case=name):
                paths = list(PATHS[:16])
                if name == '404 epochs':
                    paths[k] = '/epochs/3'
                run = Run(4)
                run.client.errors[paths[k]] = cases[name]
                self.assertEqual(outcome(run, paths)[1], code)

    def test_a_stop_is_honoured_at_the_next_request_and_nothing_is_saved_after_it(self):
        run = Run(4)
        saved = []
        run.traverse(PATHS[:3])
        result = outcome(run, PATHS[3:20], before=lambda i: (run.stop.set(), saved.append(len(run.store.saved))) if i == 5 else None)
        self.assertEqual(result[:2], ('Refusal', 'interrupted'))
        self.assertEqual(len(run.store.saved), saved[0])
        self.assertEqual(len(run.provider.receipts), 3 + 5)
        # A stop while an answer sent ahead is awaited: honoured without waiting for it.
        run = Run(4)
        run.client.delays[PATHS[1]] = 5
        run.client.hooks[PATHS[2]] = lambda: run.stop.set()
        start = time.monotonic()
        result = outcome(run, PATHS[:10])
        self.assertEqual(result[:2], ('Refusal', 'interrupted'))
        self.assertLess(time.monotonic() - start, 2)
        self.assertEqual(len(run.provider.receipts), 1)

    def test_a_pace_spaces_direct_and_prefetched_sends(self):
        # 3,600 requests an hour on a clock that runs four times faster than real time.
        origin = time.monotonic()
        clock = lambda: (time.monotonic() - origin) * 4
        sleep = lambda seconds: time.sleep(seconds / 4)
        run = Run(4, pacer=Pacer(3600, clock=clock, sleep=sleep), delay=0.01, client_clock=clock)
        hinted = [p for i, p in enumerate(PATHS[:12]) if i % 4]
        self.assertEqual(run.traverse(PATHS[:12], hinted=hinted), [{'path': p} for p in PATHS[:12]])
        gaps = [b - a for a, b in zip(run.client.sent_at, run.client.sent_at[1:])]
        self.assertEqual(len(gaps), 11)
        self.assertGreaterEqual(min(gaps), 0.95, gaps)
        driver = SyncDriver(SimpleNamespace(sync={'provider_max_requests_per_hour': 3600}), client=object(), profile=PREPROD_PROFILE)
        self.assertEqual((driver._dispatcher.size, driver._dispatcher.pacer.interval), (1, 1.0))
        driver = SyncDriver(SimpleNamespace(sync={'provider_concurrency': 4, 'provider_max_requests_per_hour': 16000}),
                            client=object(), profile=PREPROD_PROFILE)
        self.assertEqual((driver._dispatcher.size, driver._dispatcher.pacer.interval), (4, 0.225))

    def test_hints_outside_a_scope_and_paths_never_hinted_are_sent_directly(self):
        run = Run(4)
        prefetch(run.provider, PATHS[:8])  # outside any scope: ignored
        self.assertEqual([run.provider.get(p) for p in PATHS[:8]], [{'path': p} for p in PATHS[:8]])
        self.assertEqual(run.client.most, 1)
        # A hinted path never requested is dropped when its scope ends: one request at most, never read.
        run = Run(4)
        with prefetch_scope(run.provider, ['/unused', PATHS[0], PATHS[1]]):
            run.provider.get(PATHS[0])
            run.provider.get(PATHS[1])
        time.sleep(0.1)
        self.assertTrue(set(PATHS[:2]) <= set(run.client.calls) <= {'/unused', *PATHS[:2]})
        self.assertEqual([r['endpoint'] for r in run.provider.receipts], PATHS[:2])
        self.assertEqual(run.attempts(), 3)
        self.assertEqual((run.provider.window.sent, list(run.provider.window.queued)), ([], []))


if __name__ == '__main__':
    unittest.main()
