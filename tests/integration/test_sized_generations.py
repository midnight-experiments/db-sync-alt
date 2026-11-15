# SPDX-License-Identifier: Apache-2.0
"""Relevance generations sized by their blocks' content, before they are declared.

Each block's key bound is computed here from its transactions' CBOR, apart from the code under test:
floor((66 * bytes + 65,536) / 63), and 0 for an empty block. A generation holds the longest run of blocks whose
bounds sum to at most MAX_KEYS (patched down where a test needs several generations), and always its first block.
The cardano-db-sync SQL root with the Mainnet profile (`fixtures.relevance`) runs the relevance follow; the Preprod
generation fixture with trusted history runs the epoch case.
"""
import collections
import contextlib
import copy
import hashlib
import io
import itertools
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from db_sync_alt.errors import TransientSyncFailure
from db_sync_alt.service.health import HealthState
from db_sync_alt.sources.blockfrost import adapter, provider, relevance, sync
from db_sync_alt.sources.blockfrost.follow import update_follow
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
import fixtures.blockfrost_generation as gen_fixture
from fixtures.blockfrost import Fake, FixtureHooks
from fixtures.cbor import enc
from fixtures.checkpoints import CheckpointFixture
import fixtures.database as real
from fixtures.networks import PREPROD

# 1,304 one-policy assets fill one output value of at most 5,000 bytes (maxValueSize).
NAMES = ([b''] + [bytes([i]) for i in range(256)] + [bytes([i, j]) for i in range(256) for j in range(256)])[:1304]
DATUM = enc(42)
DATUM_HASH = hashlib.blake2b(DATUM, digest_size=32).digest()


def bounds(transactions):
    """Each block's key bound, from the bytes of its transactions' CBOR."""
    return [(66 * sum(len(t['cbor']) // 2 for t in rows) + 65536) // 63 if rows else 0 for rows in transactions]


def sizes(block_bounds, limit, *, most=100):
    """The generation sizes the rule gives for `block_bounds` against `limit` keys, at most `most` blocks each."""
    out, start = [], 0
    while start < len(block_bounds):
        total, end = block_bounds[start], start + 1
        while end < len(block_bounds) and end - start < most and total + block_bounds[end] <= limit:
            total += block_bounds[end]
            end += 1
        out.append(end - start)
        start = end
    return out


def generations(store):
    return [row[0] for row in store.db.execute(
        "SELECT target_height FROM chain.generations WHERE status='ready' ORDER BY id").fetchall()][1:]


def page(rf, height, fork=None):
    """The `/txs/cbor` page path of the block at `height` (its hash under `fork`, a hash mapping)."""
    h = rf.H(height).hex()
    return '/blocks/' + (fork or {}).get(h, h) + '/txs/cbor?count=100&page=1&order=asc'


def run(store, client, profile, target, *, sync_options, now=None, stop=None, until=None):
    """The service's sync driver without a poller (one writer session) until the ready generation reaches `target`
    or `until()` holds, waiting out every pause on a simulated clock. Returns the pauses."""
    now = now if now is not None else [1700000000.0]
    driver = sync.SyncDriver(SimpleNamespace(sync=sync_options), client=client, profile=profile, clock=lambda: now[0])
    health = real.SyncFixture.state(None)
    pauses = []
    for _ in range(200):
        if store.generation()['target_height'] >= target or (until is not None and until()):
            return pauses
        try:
            driver.run_once(store, health, stop or threading.Event())
        except TransientSyncFailure as error:
            pauses.append((error.code, error.retry_at))
            now[0] = max(now[0] + 1, error.retry_at + 1)
        else:
            now[0] += 61
    raise AssertionError('no progress')


class Hooked(real.AccountedFake):
    """An accounted fake client that calls `hook(path)` after it answers each request; `exhausted` makes its
    ledger report the daily limit reached."""
    hook = None
    exhausted = False

    def get(self, path, **kw):
        value = super().get(path, **kw)
        if self.hook is not None:
            self.hook(path)
        return value

    def status(self):
        return {'budget_used': 50000 if self.exhausted else len(self.calls), 'daily_limit': 50000}


class SizedRelevanceTests(unittest.TestCase):
    """The cardano-db-sync SQL root with the Mainnet profile; relevance filtering on."""
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

    @contextlib.contextmanager
    def decoded_once(self):
        """The fixture renders each output by decoding its whole transaction: decode each one once."""
        from db_sync_alt.sources.blockfrost import cbor
        original = cbor.decode_transaction

        def decode(raw, expected):
            key = (raw, expected)
            if key not in self.decoded:
                self.decoded[key] = original(raw, expected)
            return self.decoded[key]
        with patch.object(cbor, 'decode_transaction', decode):
            yield

    def prepare(self, transactions, extra=None):
        with self.decoded_once():
            store, routes = self.rf.SQLFixture.prepare(transactions)
        self.addCleanup(store.close)
        if extra is not None:
            extra(routes)
        return store, routes

    def reopen(self, store):
        """The same database in a new writer session, as a new process opens it: no state of the old one."""
        dsn = store.db.info.dsn
        store.close()
        store = PgStore(conninfo=dsn)
        self.addCleanup(store.close)
        store.certify_ready_full()
        return store

    def full_follow(self, transactions, target, routes, extra=None):
        """Digests of the full follow (relevance off, one generation, whole provider answers) of the same chain."""
        reference, _ = self.prepare(transactions, extra)
        with patch.object(adapter, 'MAX_BYTES', provider.UTXOS_MAX_BYTES):
            update_follow(reference, self.rf.Provider(50000, Fake(routes)), self.rf.PROFILE, target, hooks=FixtureHooks(), relevance_enabled=False)
        return reference.digests(reference.generation()['id'])

    def two_block_follow(self, transactions, routes, extra=None):
        """Digests of the relevance follow in generations of two blocks, as the earlier release made them."""
        rf = self.rf
        reference, _ = self.prepare(transactions, extra)
        for target in range(rf.T + 2, rf.T + len(transactions) + 2, 2):
            update_follow(reference, rf.Provider(50000, Fake(routes)), rf.PROFILE, min(target, rf.T + len(transactions)),
                          hooks=FixtureHooks(), relevance_enabled=True)
        self.assertEqual([t - rf.T for t in generations(reference)], list(range(2, len(transactions), 2)) + [len(transactions)])
        return reference.digests(reference.generation()['id'])

    def relevant(self, fee, count=1):
        rf = self.rf
        return rf.make_tx(fee, outputs=[[rf.ADDRESS, [100, {rf.P: {rf.N: 3}}]] for _ in range(count)])

    def mixed(self):
        """Empty, small and large blocks; a spend of an output created earlier in the same generation; a datum; a
        mint; a bridge transfer to the ICS address with its metadata; a spend inside one block."""
        rf = self.rf
        from db_sync_alt.sources.blockfrost.cbor import Tag
        first = rf.make_tx(8001, relevant=True)
        spend = rf.make_tx(8002, inputs=[(bytes.fromhex(first['tx_hash']), 0)], relevant=True)
        datum = rf.make_tx(8003, outputs=[[rf.ADDRESS, [100, {rf.P: {rf.N: 3}}], DATUM_HASH]])
        recipient = hashlib.sha256(b'sized-recipient').digest()
        aux = enc(Tag(259, {0: {6500973: [recipient]}}))
        body = {0: [], 1: [[relevance.address_bytes(rf.PROFILE.ics, 'mainnet'), [2000000, {rf.P: {rf.N: 5}}]]], 2: 8004,
                7: hashlib.blake2b(aux, digest_size=32).digest()}
        raw = enc(body)
        bridge = {'cbor': (b'\x84' + raw + enc({}) + enc(True) + aux).hex(), 'tx_hash': hashlib.blake2b(raw, digest_size=32).hexdigest(),
                  'body': body, 'valid': True}
        created = rf.make_tx(8005, relevant=True)
        spender = rf.make_tx(8006, inputs=[(bytes.fromhex(created['tx_hash']), 0)])
        chain = [[first], [], [rf.make_tx(8007), rf.make_tx(8008, mint=7), datum], [spend], [], [self.relevant(8009, 60)],
                 [bridge], [created, spender], [], [rf.make_tx(8010, relevant=True)]]

        def extra(routes):
            routes['/txs/' + datum['tx_hash'] + '/utxos']['outputs'][0]['data_hash'] = DATUM_HASH.hex()
            routes['/scripts/datum/' + DATUM_HASH.hex() + '/cbor'] = {'cbor': DATUM.hex()}
            routes['/txs/' + bridge['tx_hash'] + '/metadata'] = [{'label': '6500973', 'json_metadata': ['0x' + recipient.hex()]}]
        return chain, extra

    def test_a_mixed_chain_is_sized_by_content_and_stores_what_the_full_and_two_block_follows_store(self):
        rf = self.rf
        chain, extra = self.mixed()
        block_bounds = bounds(chain)
        limit = max(sum(block_bounds[:4]), max(block_bounds))
        expected = sizes(block_bounds, limit)
        self.assertGreater(max(expected), 2)
        self.assertGreater(len(expected), 1)
        store, routes = self.prepare(chain, extra)
        with patch.object(relevance, 'MAX_KEYS', limit):
            pauses = run(store, real.AccountedFake(routes), rf.PROFILE, rf.T + len(chain), sync_options={'relevance_enabled': True})
            verify_store(store)
        self.assertEqual(pauses, [])
        self.assertEqual([t - rf.T for t in generations(store)], list(itertools.accumulate(expected)))
        g = store.generation()
        self.assertEqual(g['manifest']['relevance_coverage']['range'], [rf.T + len(chain) - expected[-1] + 1, rf.T + len(chain)])
        digests = store.digests(g['id'])
        self.assertGreater(digests['datums']['count'], store.digests(0)['datums']['count'])
        self.assertGreater(digests['mints']['count'], store.digests(0)['mints']['count'])
        self.assertGreater(digests['tx_metadata']['count'], store.digests(0)['tx_metadata']['count'])
        self.assertEqual(digests, self.full_follow(chain, rf.T + len(chain), routes, extra))
        self.assertEqual(digests, self.two_block_follow(chain, routes, extra))

    def test_bounds_summing_to_exactly_the_key_limit_form_one_generation_and_one_key_more_cuts(self):
        rf = self.rf
        # Blocks of 11 transactions of 100 cNIGHT outputs: about 2,200 keys and a bound of about 54,000 each.
        chain = [[self.relevant(1000 + 11 * b + i, 100) for i in range(11)] for b in range(4)]
        block_bounds = bounds(chain)
        for limit, first in ((sum(block_bounds[:2]), 2), (sum(block_bounds[:2]) - 1, 1)):
            with self.subTest(limit=limit):
                expected = sizes(block_bounds, limit)
                self.assertEqual(expected[0], first)
                store, routes = self.prepare(chain)
                client = real.AccountedFake(routes)
                with patch.object(relevance, 'MAX_KEYS', limit):
                    self.assertEqual(run(store, client, rf.PROFILE, rf.T + 4, sync_options={'relevance_enabled': True}), [])
                    verify_store(store)
                self.assertEqual([t - rf.T for t in generations(store)], list(itertools.accumulate(expected)))
                # One process: every block's page is read once by the look-ahead (for the acquisition traversal) and
                # once by the independent traversal, the first block past each generation included.
                self.assertEqual([client.calls.count(page(rf, rf.T + b)) for b in range(1, 5)], [2, 2, 2, 2])
                self.assertEqual(store.digests(store.generation()['id']), self.full_follow(chain, rf.T + 4, routes))

    def hostile(self, blocks):
        """Blocks of 4 transactions of 3 outputs to the ICS address (which keeps every asset), each holding 1,304
        assets of one policy with the cheapest names: the most keys per byte a block can hold."""
        rf = self.rf
        ics = relevance.address_bytes(rf.PROFILE.ics, 'mainnet')
        dust = [23000000, {hashlib.blake2b(b'ics-dust', digest_size=28).digest(): {name: 1 for name in NAMES}}]
        self.assertLessEqual(len(enc(dust)), 5000)
        chain = [[rf.make_tx(9000 + 4 * b + i, outputs=[[ics, dust]] * 3) for i in range(4)] for b in range(blocks)]

        def extra(routes):
            for rows in chain:
                for t in rows:
                    routes['/txs/' + t['tx_hash'] + '/metadata'] = []
        return chain, extra

    def test_blocks_of_the_densest_keys_publish_and_never_pass_their_bounds(self):
        rf = self.rf
        for limit in (relevance.MAX_KEYS, 40000):
            with self.subTest(limit=limit):
                blocks = 2
                chain, extra = self.hostile(blocks)
                block_bounds = bounds(chain)
                expected = sizes(block_bounds, limit)
                store, routes = self.prepare(chain, extra)
                with patch.object(relevance, 'MAX_KEYS', limit):
                    self.assertEqual(run(store, real.AccountedFake(routes), rf.PROFILE, rf.T + blocks, sync_options={'relevance_enabled': True}), [])
                    verify_store(store)
                self.assertEqual([t - rf.T for t in generations(store)], list(itertools.accumulate(expected)))
                # Real keys of each block, from its proof, never pass its bound; each generation's union fits the limit.
                for generation in range(1, store.generation()['id'] + 1):
                    for row in store.checkpoint_rows(generation=generation, phase='events'):
                        keys = sum(entry['count'] for entry in row['receipt']['relevance']['inventory'].values())
                        self.assertLessEqual(keys, block_bounds[row['receipt']['lo'] - rf.T - 1])
                        self.assertGreater(keys, 15000)
        # At the real limit three such blocks are one generation; at 40,000 each block is a generation of its own.
        self.assertEqual(sizes(bounds(self.hostile(3)[0]), relevance.MAX_KEYS), [3])
        self.assertEqual(sizes(bounds(self.hostile(2)[0]), relevance.MAX_KEYS), [2])
        self.assertEqual(sizes(bounds(self.hostile(2)[0]), 40000), [1, 1])

    def polled(self, store, client, *, sync_options, steps, before=None, health_network='mainnet'):
        """The service's sync driver with its head poller: before each step `before(step)` may move the provider's
        head, then the poller ticks once and the driver runs once."""
        health = HealthState(service_version='1', schema_version=1, network=health_network, profile='fixture')
        stop = threading.Event()
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=store.db.info.dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=self.rf.PROFILE.network)
            driver = sync.SyncDriver(SimpleNamespace(sync=sync_options), client=client, profile=self.rf.PROFILE)
            driver._poller = poller
            for step in range(steps):
                if before is not None:
                    before(step)
                poller.tick(pollstore, client)
                driver.run_once(store, health, stop)
        finally:
            pollstore.close()

    def receipts(self, store, generation):
        """Each checkpoint's provider requests of `generation`, by (phase, unit); the inherited-metadata receipt
        repeats the handoff receipt's requests and is left out."""
        out = {}
        for phase in ('handoff', 'metadata', 'events', 'epochs', 'final-anchors'):
            for row in store.checkpoint_rows(generation=generation, phase=phase):
                if phase == 'metadata' and row['unit'] == 'inherited':
                    continue
                receipt = row['receipt']
                # The final receipt is the provider receipt itself; the others hold it under `provider`.
                out[(phase, str(row['unit']))] = (receipt['provider'] if 'provider' in receipt else receipt)['requests']
        return out

    def test_requests_are_six_per_generation_once_per_inventory_and_all_in_the_receipts(self):
        rf = self.rf
        chain = [[self.relevant(7100 + b)] for b in range(6)]
        block_bounds = bounds(chain)
        limit = sum(block_bounds[:2])
        expected = sizes(block_bounds, limit)
        self.assertEqual(expected, [2, 2, 2])
        store, routes = self.prepare(chain)
        client = real.AccountedFake(routes)
        with patch.object(relevance, 'MAX_KEYS', limit):
            self.polled(store, client, sync_options={'relevance_enabled': True}, steps=3)
            verify_store(store)
        self.assertEqual([t - rf.T for t in generations(store)], list(itertools.accumulate(expected)))
        confirmed = rf.T + len(chain)
        polls = ['/blocks/latest', '/blocks/' + str(confirmed)] * 3 + ['/metrics']
        listed = collections.Counter()
        # Generations 1 and 2 end below the confirmed height, so their targets are read; generation 3 ends at it.
        for generation, (lo, hi) in ((1, (1, 2)), (2, (3, 4))):
            requests = self.receipts(store, generation)
            fixed = [r['endpoint'] for key, entries in requests.items() if key[0] != 'events' for r in entries]
            if generation == 1:
                session = [e for e in fixed if e == '/genesis' or e.startswith('/txs/')]
                self.assertIn('/genesis', session)
                fixed = [e for e in fixed if e not in session]
                fixed.remove('/blocks/' + str(rf.S))  # the start anchor, read once per session
            # Committed anchor, old witness, target; one suffix page; final target and witness.
            self.assertEqual(sorted(fixed), sorted(['/blocks/' + str(rf.T + lo - 1), '/blocks/' + str(rf.T + lo - 1 + rf.K),
                                                    '/blocks/' + str(rf.T + hi), '/blocks/' + rf.H(rf.T + lo - 1 + rf.K).hex() + '/next?count=100&page=1',
                                                    '/blocks/' + str(rf.T + hi), '/blocks/' + str(rf.T + hi + rf.K)]))
            for b in range(lo, hi + 1):
                events = [r['endpoint'] for r in requests[('events', str(rf.T + b))]]
                # The acquisition traversal's page (read ahead, the first block of generation 2 by generation 1)
                # and the independent traversal's own.
                self.assertEqual(events.count(page(rf, rf.T + b)), 2)
            listed.update(r['endpoint'] for entries in requests.values() for r in entries)
        for generation in range(3, store.generation()['id'] + 1):
            listed.update(r['endpoint'] for entries in self.receipts(store, generation).values() for r in entries)
        # Every request the client answered is a poll request or listed once in a generation's receipts.
        self.assertEqual(collections.Counter(client.calls) - collections.Counter(polls), listed)
        self.assertEqual([client.calls.count(page(rf, rf.T + b)) for b in range(1, 7)], [2] * 6)

    def test_follow_min_blocks_sets_the_generation_size_at_the_tip(self):
        rf = self.rf
        chain = [[self.relevant(7200 + b)] for b in range(6)]
        for minimum, expected in ((3, [3, 6]), (1, [1, 2, 3, 4, 5, 6])):
            with self.subTest(follow_min_blocks=minimum):
                store, routes = self.prepare(chain)
                routes['/blocks/latest'] = routes['/blocks/' + str(rf.T + rf.K)]
                client = real.AccountedFake(routes)

                def advance(step):  # the provider's head moves one block per poll
                    client.routes['/blocks/latest'] = routes['/blocks/' + str(rf.T + rf.K + step + 1)]
                self.polled(store, client, steps=6, before=advance,
                            sync_options={'relevance_enabled': True, 'follow_min_blocks': minimum, 'follow_max_wait_seconds': 120})
                self.assertEqual([t - rf.T for t in generations(store)], expected)
                verify_store(store)

    def stopping(self, chain, trigger, *, retarget=False):
        """Drive until `trigger(path)` sets the stop event (or, with `retarget`, until the job is lowered); then a
        new process (store session, driver and process state) completes the follow. Returns the store, the
        client of the second process and the saved targets of each acquisition job."""
        rf = self.rf
        store, routes = self.prepare(chain)
        saved = collections.defaultdict(list)
        original = PgStore.save_sync_job

        def record(self_, key, body):
            if key == 'provider-active':
                saved[body['job_id']].append(body['target'])
            return original(self_, key, body)
        stop = threading.Event()
        client = Hooked(copy.deepcopy(routes))
        client.hook = lambda path: stop.set() if trigger(path) else None
        lowered = sync.AccountedProvider.retarget

        def lower_then_stop(provider_, target):
            lowered(provider_, target)
            stop.set()
        with patch.object(PgStore, 'save_sync_job', record), \
                patch.object(sync.AccountedProvider, 'retarget', lower_then_stop) if retarget else contextlib.nullcontext():
            run(store, client, rf.PROFILE, rf.T + len(chain), sync_options={'relevance_enabled': True}, stop=stop, until=stop.is_set)
        self.assertTrue(stop.is_set())
        store = self.reopen(store)
        second = Hooked(copy.deepcopy(routes))
        with patch.object(PgStore, 'save_sync_job', record):
            run(store, second, rf.PROFILE, rf.T + len(chain), sync_options={'relevance_enabled': True})
        verify_store(store)
        return store, routes, second, saved

    def test_a_stop_or_crash_before_or_after_the_declaration_resumes_with_the_same_generation(self):
        rf = self.rf
        chain = [[self.relevant(7300 + b)] for b in range(6)]
        limit = sum(bounds(chain)[:3])
        expected = list(itertools.accumulate(sizes(bounds(chain), limit)))
        self.assertEqual(expected, [3, 6])
        cases = {
            'during the look-ahead': dict(trigger=lambda path: path == page(rf, rf.T + 2)),
            'after the job was lowered': dict(trigger=lambda path: False, retarget=True),
            'during the events': dict(trigger=lambda path: path == '/txs/' + chain[1][0]['tx_hash'] + '/utxos'),
        }
        for name, case in cases.items():
            with self.subTest(case=name), patch.object(relevance, 'MAX_KEYS', limit):
                store, routes, second, saved = self.stopping(chain, **case)
                self.assertEqual([t - rf.T for t in generations(store)], expected)
                # A job's target is lowered once to the generation's target, never raised.
                first = next(iter(saved.values()))
                self.assertEqual(first[0], rf.T + 6)
                self.assertEqual(sorted(set(first), reverse=True), [rf.T + 6, rf.T + 3])
                self.assertEqual(first, sorted(first, reverse=True))
                if name == 'during the events':
                    # The new process re-reads the committed block with both traversals.
                    self.assertEqual(second.calls.count(page(rf, rf.T + 1)), 2)
                self.assertEqual(store.digests(store.generation()['id']), self.full_follow(chain, rf.T + 6, routes))

    def test_a_wait_during_the_look_ahead_reads_nothing_again(self):
        rf = self.rf
        chain = [[self.relevant(7400 + b)] for b in range(6)]
        store, routes = self.prepare(chain)
        client = Hooked(copy.deepcopy(routes))

        def quota(path):  # the ledger reports the day's limit reached after the second block's page
            if path == page(rf, rf.T + 2) and not client.calls.count('quota'):
                client.exhausted = True
                client.calls.append('quota')
        client.hook = quota
        now = [1700000000.0]
        original = client.status

        def status():
            value = original()
            if client.exhausted and now[0] > 1700000000.0 + 3600:
                client.exhausted = False
                value = original()
            return value
        client.status = status
        pauses = run(store, client, rf.PROFILE, rf.T + 6, sync_options={'relevance_enabled': True}, now=now)
        self.assertEqual([code for code, _ in pauses], ['quota_exhausted'])
        self.assertEqual([t - rf.T for t in generations(store)], [6])
        # Each page once by the look-ahead (kept across the wait) and once by the independent traversal.
        self.assertEqual([client.calls.count(page(rf, rf.T + b)) for b in range(1, 7)], [2] * 6)
        verify_store(store)

    def test_a_fork_below_the_limit_sizes_and_publishes_the_canonical_blocks(self):
        rf = self.rf
        chain = [[self.relevant(7500 + b)] for b in range(6)]
        store, routes = self.prepare(chain)
        # The provider's chain differs from the stored provisional headers from height T+3.
        mapping = {rf.H(h).hex(): rf.H('fork' + str(h)).hex() for h in range(rf.T + 3, rf.T + rf.K + rf.FUTURE + 1)}

        def transform(value):
            if isinstance(value, list):
                return [transform(v) for v in value]
            if isinstance(value, dict):
                return {k: transform(v) for k, v in value.items()}
            if isinstance(value, str):
                return mapping.get(value, value)
            return value
        fork = {}
        for path, value in routes.items():
            parts = path.split('/')
            if len(parts) > 2 and parts[1] == 'blocks':
                parts[2] = mapping.get(parts[2], parts[2])
            fork['/'.join(parts)] = transform(value)
        limit = sum(bounds(chain)[:4])
        client = real.AccountedFake(fork)
        with patch.object(relevance, 'MAX_KEYS', limit):
            self.assertEqual(run(store, client, rf.PROFILE, rf.T + 6, sync_options={'relevance_enabled': True}), [])
            verify_store(store)
        self.assertEqual([t - rf.T for t in generations(store)], list(itertools.accumulate(sizes(bounds(chain), limit))))
        # Below the fork the stored headers' blocks were read; above it, only the provider's canonical ones.
        self.assertEqual([client.calls.count(page(rf, rf.T + b)) for b in range(1, 3)], [2, 2])
        self.assertEqual([client.calls.count(page(rf, rf.T + b)) for b in range(3, 7)], [0] * 4)
        self.assertEqual([client.calls.count(page(rf, rf.T + b, mapping)) for b in range(3, 7)], [2] * 4)
        g = store.generation()
        self.assertEqual(store.block_at(g['id'], rf.T + 4)['hash'].hex(), mapping[rf.H(rf.T + 4).hex()])
        reference, _ = self.prepare(chain)
        update_follow(reference, rf.Provider(50000, Fake(fork)), rf.PROFILE, rf.T + 6, hooks=FixtureHooks(), relevance_enabled=False)
        self.assertEqual(store.digests(g['id']), reference.digests(reference.generation()['id']))


class SizedEpochTests(CheckpointFixture):
    """The Preprod generation fixture with an epoch boundary at height 12 (epoch 3), relevance on through trusted
    history: one generation of four blocks crosses it."""
    def chain(self, top):
        routes, _ = gen_fixture.fixtures(transition=True)
        for h in range(8, top + 1):
            b = gen_fixture.block(h, transition=True)
            routes['/blocks/' + str(gen_fixture.OFFSET + h)] = b
            routes['/blocks/' + b['hash']] = b
            routes['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(n, transition=True) for n in range(h + 1, min(h + 101, top + 1))]
        routes['/blocks/latest'] = gen_fixture.block(top, transition=True)
        return routes

    def follow(self, store, dsn, routes, sync_options):
        health = self.state()
        stop = threading.Event()
        client = real.AccountedFake(routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            driver = sync.SyncDriver(SimpleNamespace(sync=sync_options), client=client, profile=self.profile)
            driver._poller = poller
            driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        return client

    def test_an_epoch_boundary_inside_a_multi_block_generation(self):
        OFFSET = gen_fixture.OFFSET
        routes = self.chain(2174)
        digests = {}
        for enabled in (True, False):
            with self.subTest(relevance=enabled):
                dsn = self.make(transition=True)
                store = self.open(dsn)
                upgrade_store(store)
                store.certify_ready_full()
                if enabled:
                    relevance.trust_history(store, self.profile)
                    store.certify_ready_full()
                with contextlib.redirect_stdout(io.StringIO()):
                    self.follow(store, dsn, copy.deepcopy(routes), {'relevance_enabled': enabled})
                g = store.generation()
                # Heights 11 to 14: block 11 ends epoch 2, block 12 starts epoch 3.
                self.assertEqual((g['id'], g['target_height']), (1, OFFSET + 14))
                self.assertEqual(g['manifest']['required_epochs'], [0, 1, 2, 3])
                self.assertEqual(g['manifest'].get('relevance_coverage') is not None, enabled)
                if enabled:
                    self.assertEqual(g['manifest']['relevance_coverage']['range'], [OFFSET + 11, OFFSET + 14])
                epochs = {row['unit']: row['receipt'] for row in store.checkpoint_rows(generation=1, phase='epochs')}
                self.assertEqual(epochs['2']['epoch_end'], {'height': OFFSET + 11, 'hash': gen_fixture.block(11, transition=True)['hash']})
                self.assertIn('3', epochs)
                verify_store(store)
                digests[enabled] = store.digests(1)
        self.assertEqual(digests[True], digests[False])


if __name__ == '__main__':
    unittest.main()
