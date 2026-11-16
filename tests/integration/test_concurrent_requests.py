# SPDX-License-Identifier: Apache-2.0
"""Several Blockfrost requests in flight publish exactly what one request at a time publishes.

The chains of `test_sized_generations` (relevance generations sized by content, with a spend of an output created
earlier in the generation, a spend inside one block, a datum, a mint and a bridge transfer; the same chain in full
mode; a fork below the limit) and its epoch chain (a boundary inside a multi-block generation) are followed by the
service's sync driver with `provider_concurrency` 1 (twice) and 4 on fresh databases. The published generations are
equal: targets, fact digests, relevance proofs and every checkpoint receipt, whose requests are compared by endpoint,
digest and size (attempt numbers and times differ). Both differ only where two runs with 1 differ: in the hashes
over receipts, which cover each run's own job id, and in the references to a parent that carry them. Every request the run with 4 sends beyond the run with 1 is
one it sent ahead and dropped unread, at most three per generation here.
"""
import collections
import contextlib
import copy
import io
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from db_sync_alt.sources.blockfrost import adapter, provider, relevance, sync
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.prefetch import Window
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
import fixtures.blockfrost_generation as gen_fixture
from fixtures.checkpoints import CheckpointFixture
import fixtures.database as real
from fixtures.networks import PREPROD
import integration.test_sized_generations as sized

# Hashes over receipts, which name each run's own job, and the parent references that carry them: they differ
# between any two runs.
RUN_HASHES = frozenset({'original_receipt_sha256', 'sha256', 'root', 'checkpoints_sha256', 'reuse_receipt_sha256', 'parent_digest'})
# Receipt fields that name the run's own job or count its attempts.
VOLATILE = frozenset({'shared_accounting_purpose', 'attempts_this_invocation', 'attempts_cumulative_job',
                      'provider_attempts_this_invocation'})


def normal(value):
    """`value` with every provider receipt's requests as (endpoint, sha256, bytes) and the run's own fields left out."""
    if isinstance(value, dict):
        out = {k: normal(v) for k, v in value.items() if k not in VOLATILE}
        if isinstance(value.get('requests'), list):
            out['requests'] = [(r['endpoint'], r['sha256'], r['bytes']) for r in value['requests']]
        return out
    if isinstance(value, list):
        return [normal(v) for v in value]
    return value


def published(store):
    """Each ready generation after the root: target, digests, manifest and checkpoint receipts (with relevance proofs)."""
    ids = [row[0] for row in store.db.execute("SELECT id FROM chain.generations WHERE status='ready' ORDER BY id").fetchall()][1:]
    return [{'target': store.generation(g)['target_height'], 'digests': store.digests(g), 'manifest': normal(store.generation(g)['manifest']),
             'checkpoints': [(r['phase'], r['unit'], normal(r['receipt'])) for r in store.checkpoint_rows(generation=g)]} for g in ids]


def differences(a, b, path=''):
    """The paths of the leaves where `a` and `b` differ."""
    if isinstance(a, dict) and isinstance(b, dict):
        return [d for k in sorted(set(a) | set(b), key=str) for d in differences(a.get(k), b.get(k), path + '/' + str(k))]
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)) and len(a) == len(b):
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in differences(x, y, path + '/' + str(i))]
    return [] if a == b else [path]


@contextlib.contextmanager
def dropped_requests(paths):
    """Collect into `paths` every request sent ahead and dropped unread."""
    original = Window.drop

    def drop(window):
        paths.extend(ahead.path for ahead in window.sent)
        original(window)
    with patch.object(Window, 'drop', drop):
        yield


class Timed(real.AccountedFake):
    """Answers each request after 2 ms and keeps the most requests it had in flight at once."""
    def __init__(self, routes):
        super().__init__(routes)
        self.lock = threading.Lock()
        self.flight = self.most = 0

    def get(self, path, **kw):
        with self.lock:
            self.flight += 1
            self.most = max(self.most, self.flight)
        try:
            time.sleep(0.002)
            return super().get(path, **kw)
        finally:
            with self.lock:
                self.flight -= 1


class Flaky(Timed):
    """Fails the first request of `failing` with HTTP 503 (counted, as the shared client reports it)."""
    failing = None

    def get(self, path, **kw):
        with self.lock:
            fail = path == self.failing
            if fail:
                self.failing = None
        if fail:
            self.calls.append(path)
            raise RuntimeError('HTTP 503; request counted, no automatic retry')
        return super().get(path, **kw)


class ConcurrentRelevanceTests(unittest.TestCase):
    """The cardano-db-sync SQL root with the Mainnet profile."""
    @classmethod
    def setUpClass(cls):
        import fixtures.relevance as rf
        cls.rf = rf
        rf.configure('mainnet')
        rf.SQLFixture.setup()
        cls.decoded = {}

    @classmethod
    def tearDownClass(cls):
        cls.rf.SQLFixture.teardown()

    decoded_once = sized.SizedRelevanceTests.decoded_once
    prepare = sized.SizedRelevanceTests.prepare
    reopen = sized.SizedRelevanceTests.reopen
    relevant = sized.SizedRelevanceTests.relevant
    mixed = sized.SizedRelevanceTests.mixed

    def follow(self, chain, extra, size, *, limit, relevance_enabled=True, routes_of=None, client=Timed, before=None):
        """Follow `chain` with `size` requests in flight to its last block; returns the store, client, pauses and the
        requests dropped unread."""
        rf = self.rf
        store, routes = self.prepare(chain, extra)
        client = client(copy.deepcopy(routes_of(routes) if routes_of else routes))
        if before is not None:
            before(client)
        dropped = []
        with patch.object(relevance, 'MAX_KEYS', limit), dropped_requests(dropped):
            pauses = sized.run(store, client, rf.PROFILE, rf.T + len(chain),
                               sync_options={'relevance_enabled': relevance_enabled, 'provider_concurrency': size})
            verify_store(store)
        return store, client, pauses, dropped

    def assert_same_publication(self, one, again, four):
        """`four` (the run with 4) differs from `one` (a run with 1) only where `again` (another run with 1) does."""
        control = differences(published(one), published(again))
        self.assertTrue({path.rsplit('/', 1)[-1] for path in control} <= RUN_HASHES, control)
        self.assertEqual(differences(published(one), published(four)), control)

    def compare(self, chain, extra=None, *, limit, generations_at_least=1, **kw):
        again = self.follow(chain, extra, 1, limit=limit, **kw)[0]
        runs = {size: self.follow(chain, extra, size, limit=limit, **kw) for size in (1, 4)}
        (one, first, pauses_one, dropped_one), (four, second, pauses_four, dropped_four) = runs[1], runs[4]
        self.assertEqual((pauses_one, pauses_four, dropped_one), ([], [], []))
        result = published(four)
        self.assertGreaterEqual(len(result), generations_at_least)
        self.assertEqual([g['digests'] for g in published(one)], [g['digests'] for g in result])
        self.assert_same_publication(one, again, four)
        sent, alone = collections.Counter(second.calls), collections.Counter(first.calls)
        self.assertEqual(alone - sent, collections.Counter())
        extra_requests = sent - alone
        self.assertEqual(extra_requests - collections.Counter(dropped_four), collections.Counter())
        self.assertLessEqual(sum(extra_requests.values()), 3 * len(result))
        self.assertEqual(first.most, 1)
        self.assertGreater(second.most, 1)
        self.assertLessEqual(second.most, 4)
        return result, runs

    def test_the_mixed_chain_publishes_the_same_generations_with_four_in_flight(self):
        chain, extra = self.mixed()
        block_bounds = sized.bounds(chain)
        limit = max(sum(block_bounds[:4]), max(block_bounds))
        result, _ = self.compare(chain, extra, limit=limit, generations_at_least=2)
        self.assertEqual(result[-1]['target'], self.rf.T + len(chain))

    def test_the_mixed_chain_in_full_mode_publishes_the_same_generation(self):
        chain, extra = self.mixed()
        with patch.object(adapter, 'MAX_BYTES', provider.UTXOS_MAX_BYTES):
            self.compare(chain, extra, limit=relevance.MAX_KEYS, relevance_enabled=False)

    def test_a_fork_below_the_limit_publishes_the_same_canonical_blocks(self):
        rf = self.rf
        chain = [[self.relevant(7600 + b)] for b in range(6)]
        mapping = {rf.H(h).hex(): rf.H('fork' + str(h)).hex() for h in range(rf.T + 3, rf.T + rf.K + rf.FUTURE + 1)}

        def transform(value):
            if isinstance(value, list):
                return [transform(v) for v in value]
            if isinstance(value, dict):
                return {k: transform(v) for k, v in value.items()}
            return mapping.get(value, value) if isinstance(value, str) else value

        def fork(routes):
            out = {}
            for path, value in routes.items():
                parts = path.split('/')
                if len(parts) > 2 and parts[1] == 'blocks':
                    parts[2] = mapping.get(parts[2], parts[2])
                out['/'.join(parts)] = transform(value)
            return out
        result, _ = self.compare(chain, limit=sum(sized.bounds(chain)[:4]), routes_of=fork, generations_at_least=2)
        self.assertEqual(result[-1]['target'], rf.T + 6)

    def test_a_transient_failure_amid_requests_in_flight_waits_and_publishes_the_same(self):
        rf = self.rf
        chain, extra = self.mixed()
        block_bounds = sized.bounds(chain)
        limit = max(sum(block_bounds[:4]), max(block_bounds))
        reference, _, _, _ = self.follow(chain, extra, 1, limit=limit)
        # The look-ahead page of the third block: sent ahead with four in flight, failed once.
        store, client, pauses, _ = self.follow(chain, extra, 4, limit=limit, client=Flaky,
                                               before=lambda c: setattr(c, 'failing', sized.page(rf, rf.T + 3)))
        self.assertEqual([code for code, _ in pauses], ['provider_unavailable'])
        self.assertIsNone(client.failing)
        self.assertEqual([(g['target'], g['digests']) for g in published(store)],
                         [(g['target'], g['digests']) for g in published(reference)])

    def test_a_stop_amid_requests_in_flight_resumes_in_a_new_process_with_the_same_generations(self):
        rf = self.rf
        chain = [[self.relevant(7700 + b)] for b in range(6)]
        limit = sum(sized.bounds(chain)[:3])
        reference, _, _, _ = self.follow(chain, None, 1, limit=limit)
        for trigger in (sized.page(rf, rf.T + 2), '/txs/' + chain[1][0]['tx_hash'] + '/utxos'):
            with self.subTest(trigger=trigger), patch.object(relevance, 'MAX_KEYS', limit):
                store, routes = self.prepare(chain)
                stop = threading.Event()
                client = sized.Hooked(copy.deepcopy(routes))
                client.hook = lambda path, trigger=trigger: stop.set() if path == trigger else None
                sized.run(store, client, rf.PROFILE, rf.T + 6, sync_options={'relevance_enabled': True, 'provider_concurrency': 4},
                          stop=stop, until=stop.is_set)
                self.assertTrue(stop.is_set())
                store = self.reopen(store)
                self.assertEqual(sized.run(store, real.AccountedFake(copy.deepcopy(routes)), rf.PROFILE, rf.T + 6,
                                           sync_options={'relevance_enabled': True, 'provider_concurrency': 4}), [])
                verify_store(store)
                self.assertEqual([(g['target'], g['digests']) for g in published(store)],
                                 [(g['target'], g['digests']) for g in published(reference)])


class ConcurrentEpochTests(CheckpointFixture):
    """The Preprod generation fixture with an epoch boundary at height 12 (epoch 3), relevance on through trusted
    history: one generation of four blocks crosses it and reads the ended epochs' per-pool stakes."""
    chain = sized.SizedEpochTests.chain

    def follow(self, size):
        routes = self.chain(2174)
        dsn = self.make(transition=True)
        store = self.open(dsn)
        upgrade_store(store)
        store.certify_ready_full()
        relevance.trust_history(store, self.profile)
        store.certify_ready_full()
        health = self.state()
        stop = threading.Event()
        client = real.AccountedFake(copy.deepcopy(routes))
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        dropped = []
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            driver = sync.SyncDriver(SimpleNamespace(sync={'relevance_enabled': True, 'provider_concurrency': size}),
                                     client=client, profile=self.profile)
            driver._poller = poller
            with contextlib.redirect_stdout(io.StringIO()), dropped_requests(dropped):
                driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        verify_store(store)
        return store, client, dropped

    assert_same_publication = ConcurrentRelevanceTests.assert_same_publication

    def test_an_epoch_boundary_inside_a_generation_publishes_the_same_with_four_in_flight(self):
        (one, first, _), (again, _, _), (four, second, dropped) = self.follow(1), self.follow(1), self.follow(4)
        result = published(four)
        self.assertEqual([g['target'] for g in result], [gen_fixture.OFFSET + 14])
        self.assertEqual([row['unit'] for row in four.checkpoint_rows(generation=1, phase='epochs')],
                         [row['unit'] for row in one.checkpoint_rows(generation=1, phase='epochs')])
        self.assertEqual([g['digests'] for g in published(one)], [g['digests'] for g in result])
        self.assert_same_publication(one, again, four)
        sent, alone = collections.Counter(second.calls), collections.Counter(first.calls)
        self.assertEqual(alone - sent, collections.Counter())
        self.assertEqual(sent - alone - collections.Counter(dropped), collections.Counter())
        self.assertLessEqual(sum((sent - alone).values()), 3)


if __name__ == '__main__':
    unittest.main()
