"""`/metrics` once per METRICS_EVERY_REQUESTS requests counted for the profile, through both client adapters.

db-sync-alt's own key-file client (`PortableClient`, whose ledger restarts its count each UTC day) and the shared
adapter (`SharedClient` over an external client). Fake transports and a fake clock; the marker lives in the fake store.
"""
import json
from pathlib import Path
import tempfile
import threading
import unittest
import uuid

import db_sync_alt.sources.blockfrost.provider as p
from db_sync_alt.errors import TransientSyncFailure
from db_sync_alt.sources.blockfrost.sync import METRICS_EVERY_REQUESTS, AccountedProvider, metrics_counted, next_day
from fixtures.adapters import EXTERNAL_CLIENT, KEY
from fixtures.networks import PREPROD
from fixtures.sync import Clock, Store, block, health

PATH = '/blocks/5'


class Portable:
    """db-sync-alt's own client over a fake transport."""
    def __init__(self, root, clock):
        self.status = 200

        def transport(path):
            if self.status != 200:
                return self.status, {}, b'{}'
            return 200, {}, b'[]' if path == '/metrics' else json.dumps(block(5)).encode()
        self.client = p.PortableClient(KEY, root, network=PREPROD, transport=transport, clock=clock,
                                       sleep=lambda seconds: setattr(clock, 'now', clock.now + seconds))

    def others(self, count):
        """`count` requests that other programs sharing this ledger counted today."""
        ledger = json.loads(self.client.state.read_text())
        days = ledger['keys'][self.client.fingerprint]['days']
        days[self.client._day()] = days.get(self.client._day(), 0) + count
        p._atomic(self.client.state, ledger)

    def new_day(self):
        pass  # the ledger counts each UTC day apart


class Shared:
    """The shared adapter over an external client; a shared client's count restarts each UTC day as well."""
    def __init__(self, root, clock):
        directory = Path(root)
        (directory / 'client.py').write_text(EXTERNAL_CLIENT)
        (directory / 'answers.json').write_text(json.dumps({'/metrics': [200, '[]'], PATH: [200, json.dumps(block(5))]}))
        self.client = p.SharedClient(str(directory), network=PREPROD)
        self.external = self.client.client

    @property
    def status(self):
        return self.external.answers[PATH][0]

    @status.setter
    def status(self, code):
        for path in self.external.answers:
            self.external.answers[path][0] = code

    def others(self, count):
        self.external.used += count

    def new_day(self):
        self.external.used = 0


class MetricsMarkerTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()

    def each_adapter(self, check):
        for name, kind in (('portable', Portable), ('shared', Shared)):
            with self.subTest(adapter=name), tempfile.TemporaryDirectory() as root:
                self.clock.now = 1793491200.0  # 2026-11-01T00:00Z
                check(kind(root, self.clock), Store())

    def provider(self, adapter, store):
        """A new job, as each update and each poll day gets one."""
        job = dict(job_id=uuid.uuid4().hex, attempts=0, budget=10000, state='active', kind='poll', created_at=self.clock(),
                   retry_at=0, failure_count=0)
        return AccountedProvider(store, 'job-' + job['job_id'], job, adapter.client, health(), threading.Event(), clock=self.clock,
                                 network=PREPROD)

    def get(self, provider):
        """The endpoints one `get` sent."""
        provider.get(PATH)
        return [r['endpoint'] for r in provider.drain()['requests']]

    def used(self, adapter):
        return adapter.client.status()['budget_used']

    def test_reconciles_on_a_fresh_store_then_once_per_counted_requests(self):
        def check(adapter, store):
            start = self.clock()
            # A fresh store has no marker: its first request reconciles, and the marker is saved.
            self.assertEqual(self.get(self.provider(adapter, store)), ['/metrics', PATH])
            reconciled = store.metrics.pop('reconciled_at')
            self.assertEqual(store.metrics, {'version': 1, 'day': '2026-11-01', 'used': 1, 'counted': 0})
            self.assertTrue(start <= reconciled <= self.clock())
            # New jobs (each update, each poll day, a restarted process) find the marker: no `/metrics`.
            provider = self.provider(adapter, store)
            self.assertEqual(self.get(provider), [PATH])
            self.assertEqual(self.get(self.provider(adapter, store)), [PATH])
            # Requests other programs count in the same ledger count as well; one short of the limit, none.
            adapter.others(METRICS_EVERY_REQUESTS - store.metrics['counted'] - (self.used(adapter) - store.metrics['used']) - 1)
            self.assertEqual(self.get(provider), [PATH])
            self.assertEqual(store.metrics['counted'], METRICS_EVERY_REQUESTS - 1)
            # The request just counted reaches the limit: exactly one `/metrics`, then the count starts again.
            self.assertEqual(self.get(provider), ['/metrics', PATH])
            self.assertEqual((store.metrics['counted'], store.metrics['used']), (0, self.used(adapter) - 1))
            self.assertEqual(self.get(provider), [PATH])
        self.each_adapter(check)

    def test_the_count_carries_across_utc_days(self):
        def check(adapter, store):
            provider = self.provider(adapter, store)
            self.get(provider)
            adapter.others(6000)
            self.assertEqual(self.get(provider), [PATH])
            before = store.metrics['counted']
            self.assertEqual(before, 6001)
            # A new UTC day is no reason to reconcile: its requests add to the earlier day's (all but the earlier
            # day's requests after its last observation: here the one request just made).
            self.clock.now = next_day(self.clock()) + 1
            adapter.new_day()
            adapter.others(3000)
            self.assertEqual(self.get(provider), [PATH])
            self.assertEqual((store.metrics['day'], store.metrics['counted']), ('2026-11-02', before + 3000))
            adapter.others(METRICS_EVERY_REQUESTS - store.metrics['counted'] - 1)
            self.assertEqual(self.get(provider), ['/metrics', PATH])
        self.each_adapter(check)

    def test_quota_decisions_use_the_counted_requests_and_402_ends_the_day(self):
        def at_limit(adapter, store):
            provider = self.provider(adapter, store)
            self.get(provider)
            # At the daily limit by the client's own count: wait for the next UTC day, no request and no `/metrics`.
            adapter.others(50000 - self.used(adapter))
            used = self.used(adapter)
            with self.assertRaises(TransientSyncFailure) as error:
                provider.get(PATH)
            self.assertEqual((error.exception.code, error.exception.retry_at), ('quota_exhausted', next_day(self.clock())))
            self.assertEqual(self.used(adapter), used)

        def rejected(adapter, store):
            provider = self.provider(adapter, store)
            self.get(provider)
            # The provider's 402 (requests from elsewhere on the key) ends the day at once, as before.
            adapter.status = 402
            with self.assertRaises(TransientSyncFailure) as error:
                provider.get(PATH)
            self.assertEqual((error.exception.code, error.exception.retry_at), ('quota_exhausted', next_day(self.clock())))
            self.assertEqual(provider.job['error_code'], 'quota_exhausted')
            used = self.used(adapter)
            with self.assertRaises(TransientSyncFailure):
                provider.get(PATH)
            self.assertEqual(self.used(adapter), used)
            if isinstance(adapter, Portable):
                self.assertTrue(adapter.client.status()['exhausted'])
        self.each_adapter(at_limit)
        self.each_adapter(rejected)

    def test_marker_rules(self):
        marker = {'version': 1, 'day': '2026-11-01', 'used': 40, 'counted': 7, 'reconciled_at': 1.0}
        self.assertEqual(metrics_counted(marker, '2026-11-01', 45), {**marker, 'used': 45, 'counted': 12})
        self.assertEqual(metrics_counted(marker, '2026-11-02', 3), {**marker, 'day': '2026-11-02', 'used': 3, 'counted': 10})
        self.assertEqual(metrics_counted(marker, '2026-10-31', 99), marker)
        # No marker, an unreadable one, or a count that went back within a day (another ledger): reconcile.
        for bad in (None, {**marker, 'version': 2}, {**marker, 'counted': -1}, {**marker, 'used': True}, {**marker, 'day': None}, []):
            self.assertIsNone(metrics_counted(bad, '2026-11-01', 45))
        self.assertIsNone(metrics_counted(marker, '2026-11-01', 39))


if __name__ == '__main__':
    unittest.main()
