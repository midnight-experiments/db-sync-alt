# SPDX-License-Identifier: Apache-2.0
"""Transaction volume and oversized provider answers never stop an update; the facts are the full update's.

Full mode runs on the synthetic Preprod generation 0 (`fixtures.blockfrost_generation`), relevance mode on the
cardano-db-sync SQL root with the Mainnet profile (`fixtures.relevance`), both against fake Blockfrost routes
through the service's sync driver. Each result is compared with the full update or full follow of the same chain.
"""
import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import psycopg

from db_sync_alt.errors import TransientSyncFailure
from db_sync_alt.networks import network_profile
from db_sync_alt.sources.blockfrost import adapter, provider, sync
from db_sync_alt.sources.blockfrost.adapter import update
from db_sync_alt.sources.blockfrost.cbor import Tag
from db_sync_alt.sources.blockfrost.follow import update_follow
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
import fixtures.blockfrost_generation as gen_fixture
from fixtures.blockfrost import CountingProvider, Fake, FixtureHooks
from fixtures.cbor import enc, tx
import fixtures.database as real
from fixtures.networks import PREPROD

OFFSET = gen_fixture.OFFSET
K = PREPROD.security
JUNK = bytes(range(28))
# 1,304 one-policy assets fill one output value of at most 5,000 bytes (maxValueSize).
NAMES = ([b''] + [bytes([i]) for i in range(256)] + [bytes([i, j]) for i in range(256) for j in range(256)])[:1304]


def drive(store, client, profile, target, *, sync_options=None, now=None):
    """The service's sync driver (one writer session) until the ready generation reaches `target`, waiting out
    every pause on a simulated clock. Returns the passes that ended in a pause."""
    now = now if now is not None else [1700000000.0]
    driver = sync.SyncDriver(SimpleNamespace(sync=sync_options or {}), client=client, profile=profile, clock=lambda: now[0])
    health = real.SyncFixture.state(None)
    pauses = []
    for _ in range(200):
        if store.generation()['target_height'] >= target:
            return pauses
        try:
            driver.run_once(store, health, threading.Event())
        except TransientSyncFailure as error:
            pauses.append((error.code, error.retry_at))
            now[0] = max(now[0] + 1, error.retry_at + 1)
        else:
            now[0] += 61  # publication spacing
    raise AssertionError('no progress')


def generations(store):
    return [(row[0], row[1]) for row in store.db.execute(
        "SELECT target_height,witness_height FROM chain.generations WHERE status='ready' ORDER BY id").fetchall()]


def flooded(heights, per_block):
    """`gen_fixture.block` with `per_block` transactions in each of `heights`."""
    original = gen_fixture.block

    def block(h, fork=0, transition=False, unrelated=0):
        row = original(h, fork, transition, unrelated)
        if h in heights:
            row['tx_count'] = per_block
        return row
    return block


def inventory(routes, block_hash, rows):
    """The block's `/txs/cbor` pages as Blockfrost pages them (a full last page is followed by an empty one)."""
    for page in range(1, len(rows) // 100 + 2):
        routes['/blocks/' + block_hash + '/txs/cbor?count=100&page=' + str(page) + '&order=asc'] = rows[(page - 1) * 100:page * 100]


class FullModeTests(unittest.TestCase):
    """Preprod generation 0 (target h=10, witness h=2170); the flood follows the fixture's own blocks 11 and 12."""
    @classmethod
    def setUpClass(cls):cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
    @classmethod
    def tearDownClass(cls):cls.admin.close()
    setUp = real.SyncFixture.setUp
    tearDown = real.SyncFixture.tearDown
    database = real.SyncFixture.database

    def chain(self, heights, per_block, last, extra=()):
        """(block function, routes): `per_block` unrelated transactions in each of `heights`, `extra` (raw, hash,
        utxos answer) appended to block 12, the chain served through `last` + k."""
        block = flooded(heights, per_block)
        if extra:
            base = block
            block = lambda h, *a, **kw: dict(base(h, *a, **kw), tx_count=base(h, *a, **kw)['tx_count'] + (len(extra) if h == 12 else 0))
        with patch.object(gen_fixture, 'block', side_effect=block):
            routes, _ = gen_fixture.fixtures()
        top = last + K
        for h in range(8, top + 1):
            b = block(h)
            routes['/blocks/' + str(OFFSET + h)] = b
            routes['/blocks/' + b['hash']] = b
            routes['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [block(n) for n in range(h + 1, min(h + 101, top + 1))]
        routes['/blocks/latest'] = block(top)
        index = 0
        for h in heights:
            rows = []
            for _ in range(per_block):
                index += 1
                raw, th = gen_fixture.unrelated_tx(index)
                rows.append({'tx_hash': th, 'cbor': raw})
                routes['/txs/' + th + '/utxos'] = {'hash': th, 'inputs': [], 'outputs': []}
            inventory(routes, block(h)['hash'], rows)
        for raw, th, answer in extra:
            routes['/blocks/' + block(12)['hash'] + '/txs/cbor?count=100&page=1&order=asc'].append({'tx_hash': th, 'cbor': raw})
            routes['/txs/' + th + '/utxos'] = answer
        return block, routes

    def ready(self, block):
        """Generation 0 under `block`, upgraded and certified like the service's writer; returns (dsn, store)."""
        dsn = self.database()
        with patch.object(gen_fixture, 'block', side_effect=block):
            with gen_fixture.baseline(dsn):
                pass
        store = PgStore(conninfo=dsn)
        self.addCleanup(store.close)
        upgrade_store(store)
        store.certify_ready_full()
        return dsn, store

    def reference(self, block, routes, target):
        """Digests of the full update (`adapter.update`, one generation, unlimited budget) of the same chain."""
        with patch.object(gen_fixture, 'block', side_effect=block):
            store = gen_fixture.baseline(self.database())
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                update(store, CountingProvider(50000, Fake(routes)), gen_fixture.PROFILE, OFFSET + target, hooks=FixtureHooks())
            return store.digests(store.generation()['id'])
        finally:
            store.close()

    def test_one_hundred_blocks_of_300_transactions_complete_in_full_mode(self):
        heights = range(13, 113)
        block, routes = self.chain(heights, 300, 112)
        _, store = self.ready(block)
        client = real.AccountedFake(routes)
        pauses = drive(store, client, gen_fixture.PROFILE, OFFSET + 112)
        self.assertEqual(pauses, [])
        # 304 requests per flooded block: 24 of them fit three quarters of one update's 10,000 requests.
        targets = [t - OFFSET for t, _ in generations(store)[1:]]
        self.assertEqual(targets, [36, 60, 84, 108, 112])
        self.assertEqual(store.sync_job('provider-active')['state'], 'completed')
        self.assertFalse(store.db.execute("SELECT 1 FROM chain.sync_jobs WHERE id LIKE 'provider-acquire-%'").fetchone())
        verify_store(store)
        self.assertEqual(store.digests(store.generation()['id']), self.reference(block, routes, 112))

    def test_an_exhausted_update_waits_for_the_next_utc_day_and_continues_on_postgresql(self):
        # A job sized to five blocks (a misestimate) that one 150-request allotment per day cannot finish.
        heights = range(13, 16)
        block, routes = self.chain(heights, 100, 15)
        _, store = self.ready(block)
        client = real.AccountedFake(routes)
        oversized = lambda store, ready, limit, budget, *, relevance: min(limit, ready['target_height'] + 5)
        with patch.object(sync, 'job_target', oversized):
            pauses = drive(store, client, gen_fixture.PROFILE, OFFSET + 15, sync_options={'acquisition_request_budget': 150})
        self.assertGreaterEqual(len(pauses), 2)
        self.assertTrue(all(code == 'request_budget_exhausted' for code, _ in pauses))
        self.assertEqual([t - OFFSET for t, _ in generations(store)[1:]], [15])
        final = store.sync_job('provider-active')
        archived = {row[0]: row[1] for row in store.db.execute(
            "SELECT id,body FROM chain.sync_jobs WHERE id LIKE 'provider-acquire-%'").fetchall()}
        self.assertEqual(len(archived), len(pauses))
        chain, job = [], final
        while 'resumes' in job:
            job = archived['provider-acquire-' + job['resumes']]
            chain.append(job)
        self.assertEqual(len(chain), len(pauses))
        self.assertTrue(all(j['state'] == 'exhausted' and j['attempts'] == j['budget'] == 150 and j['target'] == final['target'] for j in chain))
        self.assertEqual(final['prior_attempts'], 150 * len(pauses))
        self.assertEqual(final['state'], 'completed')
        verify_store(store)
        self.assertEqual(store.digests(store.generation()['id']), self.reference(block, routes, 15))

    def test_postgresql_replaces_an_exhausted_job_only_by_its_archived_later_day_continuation(self):
        _, store = self.ready(gen_fixture.block)
        day = 1700000000.0
        old = {'job_id': 'a' * 32, 'attempts': 10, 'budget': 10, 'state': 'exhausted', 'kind': 'acquire', 'parent': 0,
               'target': OFFSET + 11, 'witness': OFFSET + 11 + K, 'created_at': day, 'retry_at': sync.next_day(day),
               'failure_count': 0, 'exhausted_at': day, 'prior_attempts': 5}
        store.save_sync_job('provider-active', old)
        new = {'job_id': 'b' * 32, 'attempts': 0, 'budget': 10, 'state': 'active', 'kind': 'acquire', 'parent': 0,
               'target': OFFSET + 11, 'witness': OFFSET + 11 + K, 'created_at': sync.next_day(day), 'retry_at': 0,
               'failure_count': 0, 'resumes': 'a' * 32, 'prior_attempts': 15}
        refused = lambda body: self.assertRaisesRegex(Exception, 'sync_job_rollover_refused', store.save_sync_job, 'provider-active', body)
        refused(new)  # the exhausted record is not kept yet
        store.save_sync_job('provider-acquire-' + 'a' * 32, old)
        for change in ({'created_at': sync.next_day(day) - 1}, {'prior_attempts': 10}, {'target': OFFSET + 12},
                       {'resumes': 'c' * 32}, {'attempts': 1}):
            with self.subTest(change=change):
                refused({**new, **change})
        store.save_sync_job('provider-active', new)
        self.assertEqual(store.sync_job('provider-active'), new)
        self.assertEqual(store.sync_job('provider-acquire-' + 'a' * 32), old)

    def big_transaction(self, *, outputs, address_text):
        """A transaction with 300 reference inputs to outputs of 1,304 assets each: its `/utxos` answer passes 32 MiB.
        Returns (raw, hash, answer without outputs)."""
        refs = [[hashlib.blake2b(b'ref' + i.to_bytes(4, 'big'), digest_size=32).digest(), 0] for i in range(300)]
        spent = hashlib.blake2b(b'spent', digest_size=32).digest()
        raw, th = tx({0: [[spent, 0]], 1: outputs, 2: 777777, 18: refs})
        fat = [{'unit': 'lovelace', 'quantity': '23000000'}] + [{'unit': (JUNK + n).hex(), 'quantity': '1'} for n in NAMES]
        row = lambda h, reference: {'address': address_text, 'amount': fat if reference else fat[:1], 'tx_hash': h.hex(),
                                    'output_index': 0, 'data_hash': None, 'inline_datum': None, 'reference_script_hash': None,
                                    'collateral': False, 'reference': reference}
        answer = {'hash': th, 'inputs': [row(spent, False)] + [row(h, True) for h, _ in refs], 'outputs': None}
        return raw, th, answer

    def test_an_oversized_utxos_answer_beside_a_relevant_transaction_ingests_on_both_adapters(self):
        raw, th, answer = self.big_transaction(outputs=[[b'\x60' + bytes(28), 2000000]], address_text='addr_test1vz' + 'q' * 51)
        answer['outputs'] = [{'address': 'addr_test1vz' + 'q' * 51, 'amount': [{'unit': 'lovelace', 'quantity': '2000000'}],
                              'output_index': 0, 'data_hash': None, 'inline_datum': None, 'collateral': False}]
        block, routes = self.chain((), 0, 12, extra=[(raw, th, answer)])
        body = json.dumps(answer).encode()
        del answer
        self.assertGreater(len(body), provider.MAX_BYTES)
        routes['/txs/' + th + '/utxos'] = body
        ledger = self.enterContext(tempfile.TemporaryDirectory())
        now = [time.time()]
        built_in = provider.PortableClient('preprod' + 'synthetic0000000000000000', Path(ledger) / 'accounting', network=PREPROD,
                                           transport=Raw(routes), clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + s))
        shared = provider.SharedClient(str(shared_root(routes, self.enterContext(tempfile.TemporaryDirectory()), 'preprod', PREPROD)),
                                       'preprod', network=PREPROD)
        results = {}
        for name, client in (('built-in', built_in), ('shared', shared)):
            with self.subTest(adapter=name):
                _, store = self.ready(block)
                drive(store, client, gen_fixture.PROFILE, OFFSET + 12)
                verify_store(store)
                self.assertIsNone(store.fact('transactions', (bytes.fromhex(th),)))
                results[name] = store.digests(store.generation()['id'])
        # The reference reads the whole answer (the size bound raised only for it).
        routes['/txs/' + th + '/utxos'] = json.loads(body)
        with patch.object(adapter, 'MAX_BYTES', provider.UTXOS_MAX_BYTES):
            expected = self.reference(block, routes, 12)
        self.assertEqual(results, {'built-in': expected, 'shared': expected})


class Raw:
    """HTTP transport of db-sync-alt's own client: JSON routes encoded per request, bytes routes as given."""
    def __init__(self, routes):self.routes = routes
    def __call__(self, path):
        if path not in self.routes:
            return 404, {}, b'{"status_code":404}'
        value = self.routes[path]
        return 200, {}, value if isinstance(value, bytes) else json.dumps(value).encode()


def shared_root(routes, directory, name, network):
    """An external accounting client (tests/fixtures/shared_client) answering `routes` from files."""
    root = Path(directory)
    shutil.copy(Path(__file__).resolve().parents[1] / 'fixtures' / 'shared_client' / 'client.py', root / 'client.py')
    (root / 'profiles.json').write_text(json.dumps({name: {'base_url': network.blockfrost_base}}))
    (root / 'answers').mkdir()
    for path, value in routes.items():
        (root / 'answers' / hashlib.sha256(path.encode()).hexdigest()).write_bytes(value if isinstance(value, bytes) else json.dumps(value).encode())
    return root


class RelevanceModeTests(unittest.TestCase):
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

    def prepare(self, transactions, *, reuse_root=True):
        with self.decoded_once():
            store, routes = self.rf.SQLFixture.prepare(transactions, reuse_root=reuse_root)
        self.addCleanup(store.close)
        return store, routes

    def relevant_outputs(self, fee, count=100):
        rf = self.rf
        return rf.make_tx(fee, outputs=[[rf.ADDRESS, [100, {rf.P: {rf.N: 3}}]] for _ in range(count)])

    def full_follow(self, transactions, target, routes):
        """Digests of the full follow (relevance off, one generation, whole provider answers) of the same chain."""
        reference, _ = self.prepare(transactions)
        with patch.object(adapter, 'MAX_BYTES', provider.UTXOS_MAX_BYTES):
            update_follow(reference, self.rf.Provider(50000, Fake(routes)), self.rf.PROFILE, target, hooks=FixtureHooks(), relevance_enabled=False)
        return reference.digests(reference.generation()['id'])

    def test_blocks_of_1100_relevant_outputs_past_the_key_limit_complete_in_relevance_mode(self):
        rf = self.rf
        from db_sync_alt.sources.blockfrost import relevance
        # 11 transactions of 100 cNIGHT outputs per block: about 2,200 known fact keys per block. The scenario of
        # 100 such blocks against the 200,000-key limit is scaled by 1/10 to fit the CI job time: 10 blocks
        # (about 22,000 keys) against a 20,000-key limit. One update of all 10 blocks passes the limit; the
        # driver's relevance cap (MAX_BLOCKS, derived from the real limits in the unit tests) keeps it out of reach.
        transactions = [[self.relevant_outputs(1000 + 11 * b + i) for i in range(11)] for b in range(10)]
        store, routes = self.prepare(transactions)
        with patch.object(relevance, 'MAX_KEYS', 20000):
            pauses = drive(store, real.AccountedFake(routes), rf.PROFILE, rf.T + 10, sync_options={'relevance_enabled': True})
            verify_store(store)
        self.assertEqual(pauses, [])
        self.assertEqual([t - rf.T for t, _ in generations(store)[1:]], [6, 10])
        self.assertEqual(store.generation()['manifest']['relevance_coverage']['range'], [rf.T + 7, rf.T + 10])
        digests = store.digests(store.generation()['id'])
        self.assertGreaterEqual(digests['outputs']['count'], 11000)
        self.assertEqual(digests, self.full_follow(transactions, rf.T + 10, routes))

    def test_a_resume_after_a_wait_does_not_read_the_committed_blocks_again(self):
        rf = self.rf
        transactions = [[self.relevant_outputs(5000 + 3 * b + i, 2) for i in range(3)] for b in range(4)]
        third = transactions[2][0]['tx_hash']

        class Failing(Fake):
            """The provider fails the first time it is asked for the third block's first transaction."""
            failed = False
            def get(self, path, **kw):
                if path == '/txs/' + third + '/utxos' and not Failing.failed:
                    Failing.failed = True
                    self.calls.append(path)
                    raise RuntimeError('HTTP 503; request counted, no automatic retry')
                return super().get(path, **kw)
        pages = lambda calls, b: [c for c in calls if c.startswith('/blocks/' + rf.H(rf.T + b).hex() + '/txs/cbor')]

        def follow(store, client):
            return update_follow(store, rf.Provider(50000, client), rf.PROFILE, rf.T + 4, hooks=FixtureHooks(), relevance_enabled=True)

        for restart in (False, True):
            with self.subTest(restart=restart):
                Failing.failed = False
                store, routes = self.prepare(transactions)
                with self.assertRaisesRegex(Exception, 'provider_request_failed_counted'):
                    follow(store, Failing(copy.deepcopy(routes)))
                self.assertTrue(store.completed(store.pending()['id'], 'events', rf.T + 2))
                self.assertFalse(store.completed(store.pending()['id'], 'events', rf.T + 3))
                if restart:  # a new process: another writer session over the same database
                    dsn = store.db.info.dsn
                    store.close()
                    store = PgStore(conninfo=dsn)
                    self.addCleanup(store.close)
                    store.certify_ready_full()
                client = Fake(copy.deepcopy(routes))
                follow(store, client)
                committed = [len(pages(client.calls, b)) for b in (1, 2)]
                # Both passes (acquisition and independent) read each block's single page.
                self.assertEqual(committed, [2, 2] if restart else [0, 0])
                self.assertEqual([len(pages(client.calls, b)) for b in (3, 4)], [2, 2])
                verify_store(store)
                self.assertEqual(store.digests(store.generation()['id']), self.full_follow(transactions, rf.T + 4, routes))

    def spend_in_block(self):
        """Block 1: a relevant transaction. Block 2: a transaction creating a relevant output and one spending it,
        whose selection depends on the discovery state the first one leaves."""
        rf = self.rf
        first = rf.make_tx(7001, relevant=True)
        created = rf.make_tx(7002, relevant=True)
        spender = rf.make_tx(7003, inputs=[(bytes.fromhex(created['tx_hash']), 0)])
        return [[first], [created, spender]], created, spender

    def test_a_transient_failure_inside_a_block_rolls_that_block_back_and_reads_it_again(self):
        rf = self.rf
        from db_sync_alt.sources.blockfrost import follow
        transactions, created, spender = self.spend_in_block()
        store, routes = self.prepare(transactions)
        failing = '/txs/' + spender['tx_hash'] + '/utxos'

        class Client(real.AccountedFake):
            """A 503 the first time the spending transaction's /utxos is read, after the block's first transaction."""
            failed_at = None
            def get(self, path, **kw):
                if path == failing and self.failed_at is None:
                    self.calls.append(path)
                    self.failed_at = len(self.calls)
                    raise RuntimeError('HTTP 503; request counted, no automatic retry')
                return super().get(path, **kw)
        reused, original = [], follow._relevance_source

        def observe(store, *args, **kwargs):
            entry = original(store, *args, **kwargs)
            reused.append((entry['through'], set(entry['source'].discovery.created), set(entry['source'].auditor.discovery.created)))
            return entry
        client = Client(copy.deepcopy(routes))
        with patch.object(follow, '_relevance_source', observe):
            pauses = drive(store, client, rf.PROFILE, rf.T + 2, sync_options={'relevance_enabled': True})
        self.assertEqual([code for code, _ in pauses], ['provider_unavailable'])
        # The resumed pass reuses the state through block 1, without block 2's partial additions.
        key = (bytes.fromhex(created['tx_hash']), 0)
        self.assertEqual(len(reused), 2)
        through, main, independent = reused[1]
        self.assertEqual(through, rf.T + 1)
        self.assertNotIn(key, main)
        self.assertNotIn(key, independent)
        after = client.calls[client.failed_at:]
        page = lambda b: sum(c.startswith('/blocks/' + rf.H(rf.T + b).hex() + '/txs/cbor') for c in after)
        self.assertEqual((page(1), page(2)), (0, 2))
        # Each pass reads both answers; the spent output is not stored yet, so each pass also reads the creating
        # transaction's answer as the spent output's origin, as an uninterrupted run does.
        self.assertEqual([after.count('/txs/' + t['tx_hash'] + '/utxos') for t in (created, spender)], [4, 2])
        uninterrupted, _ = self.prepare(transactions)
        reads = Fake(copy.deepcopy(routes))
        update_follow(uninterrupted, rf.Provider(50000, reads), rf.PROFILE, rf.T + 2, hooks=FixtureHooks(), relevance_enabled=True)
        block = lambda calls: sorted(c for c in calls if c.startswith('/blocks/' + rf.H(rf.T + 2).hex() + '/txs/cbor')
                                     or c in ('/txs/' + created['tx_hash'] + '/utxos', '/txs/' + spender['tx_hash'] + '/utxos'))
        self.assertEqual(block(after), block(reads.calls))
        verify_store(store)
        self.assertEqual(store.digests(store.generation()['id']), self.full_follow(transactions, rf.T + 2, routes))

    def test_a_failure_after_the_block_keys_were_recorded_drops_the_state_and_reads_everything_again(self):
        rf = self.rf
        from db_sync_alt.sources.blockfrost import follow
        transactions, created, spender = self.spend_in_block()
        store, routes = self.prepare(transactions)
        original, failed = store.checkpoint, []

        def checkpoint(generation, phase, unit, receipt):
            # The block-2 commit fails once, after its work added its keys to `known`.
            if phase == 'events' and str(unit) == str(rf.T + 2) and not failed:
                failed.append(unit)
                raise RuntimeError('commit failed')
            return original(generation, phase, unit, receipt)
        with patch.object(store, 'checkpoint', side_effect=checkpoint):
            with self.assertRaisesRegex(RuntimeError, 'commit failed'):
                update_follow(store, rf.Provider(50000, Fake(copy.deepcopy(routes))), rf.PROFILE, rf.T + 2, hooks=FixtureHooks(), relevance_enabled=True)
        self.assertNotIn(store, follow._REBUILT)
        self.assertTrue(store.completed(store.pending()['id'], 'events', rf.T + 1))
        self.assertFalse(store.completed(store.pending()['id'], 'events', rf.T + 2))
        client = Fake(copy.deepcopy(routes))
        update_follow(store, rf.Provider(50000, client), rf.PROFILE, rf.T + 2, hooks=FixtureHooks(), relevance_enabled=True)
        page = lambda b: sum(c.startswith('/blocks/' + rf.H(rf.T + b).hex() + '/txs/cbor') for c in client.calls)
        self.assertEqual((page(1), page(2)), (2, 2))
        verify_store(store)
        self.assertEqual(store.digests(store.generation()['id']), self.full_follow(transactions, rf.T + 2, routes))

    def test_an_oversized_utxos_answer_beside_a_relevant_transaction_ingests_on_both_adapters(self):
        rf = self.rf
        mainnet = network_profile('mainnet')
        # A reference script on its output makes relevance mode read its /utxos (acquisition and independent pass).
        script = {0: rf.OTHER, 1: 2000000, 3: Tag(24, enc([0, bytes(10)]))}
        refs = [[hashlib.blake2b(b'ref' + i.to_bytes(4, 'big'), digest_size=32).digest(), 0] for i in range(300)]
        spent = hashlib.blake2b(b'spent', digest_size=32).digest()
        body = {0: [[spent, 0]], 1: [script], 2: 777777, 18: refs}
        raw, th = tx(body)
        # The fixture builds `/utxos` inputs from `body`; this transaction's answer is replaced below.
        big = {'cbor': raw, 'tx_hash': th, 'body': {0: [], 1: [script], 2: 777777}, 'valid': True}
        transactions = [[rf.make_tx(300, relevant=True), big]]
        store, routes = self.prepare(transactions)
        rendered = self.rf.output(big)
        address = rendered['address']
        fat = [{'unit': 'lovelace', 'quantity': '23000000'}] + [{'unit': (JUNK + n).hex(), 'quantity': '1'} for n in NAMES]
        row = lambda h, reference: {'address': address, 'amount': fat if reference else fat[:1], 'tx_hash': h.hex(), 'output_index': 0,
                                    'data_hash': None, 'inline_datum': None, 'reference_script_hash': None,
                                    'collateral': False, 'reference': reference}
        answer = {'hash': th, 'inputs': [row(spent, False)] + [row(h, True) for h, _ in refs], 'outputs': [dict(rendered, collateral=False)]}
        body = json.dumps(answer).encode()
        del answer
        self.assertGreater(len(body), provider.MAX_BYTES)
        routes['/txs/' + th + '/utxos'] = body
        ledger = self.enterContext(tempfile.TemporaryDirectory())
        now = [time.time()]
        built_in = provider.PortableClient('mainnet' + 'synthetic0000000000000000', Path(ledger) / 'accounting', network=mainnet,
                                           transport=Raw(routes), clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + s))
        shared = provider.SharedClient(str(shared_root(routes, self.enterContext(tempfile.TemporaryDirectory()), 'mainnet', mainnet)),
                                       'mainnet', network=mainnet)
        results = {}
        for name, client in (('built-in', built_in), ('shared', shared)):
            with self.subTest(adapter=name):
                store, _ = self.prepare(transactions)
                drive(store, client, rf.PROFILE, rf.T + 1, sync_options={'relevance_enabled': True})
                self.assertIsNotNone(store.generation()['manifest'].get('relevance_coverage'))
                verify_store(store)
                results[name] = store.digests(store.generation()['id'])
        routes['/txs/' + th + '/utxos'] = json.loads(body)
        expected = self.full_follow(transactions, rf.T + 1, routes)
        self.assertEqual(results, {'built-in': expected, 'shared': expected})


if __name__ == '__main__':
    unittest.main(verbosity=2)
