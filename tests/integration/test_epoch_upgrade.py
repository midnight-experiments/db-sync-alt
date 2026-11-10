# SPDX-License-Identifier: Apache-2.0
"""In-place upgrade of a nonce-only store to schema 5: authoritative epoch sizes, refusals, resume,
backups of the earlier format, and the node 2.1.101 Cost query afterwards. No live provider."""
import io
import json
import os
from pathlib import Path
import tempfile
import threading
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_sync_alt.backup import backup, restore
from db_sync_alt.bridge import parse_checkpoint
from db_sync_alt.contract import LEGACY_FORMAT, FORMAT, Refusal, digest
from db_sync_alt.networks import network_profile
from db_sync_alt.node_queries import named
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.relevance import trust_history
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.store import epoch_parameters, upgrade
from db_sync_alt.store.database import EPOCH_UPGRADE, PgStore, empty_destination
from db_sync_alt.store.epoch_parameters import ProviderEpochSource, SQLEpochSource
from db_sync_alt.store.reader import grant_reader
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import physical_inventory, verify_store
import fixtures.blockfrost_generation as gen_fixture
import fixtures.database as real
from fixtures.checkpoints import CheckpointFixture
from fixtures.legacy import nonce_only
from fixtures.networks import PREPROD

from conformance.client import TypedClient

OFFSET = gen_fixture.OFFSET
NONCES = {0: '6e' * 32, 1: '6e' * 32, 2: '6e' * 32, 3: 'cc' * 32}
# Distinct per epoch, zero included: never a protocol default.
SIZES = {0: 16384, 1: 0, 2: 12002, 3: 24000}
COST = named('get_max_tx_size')


def parameters(sizes=SIZES, nonces=NONCES):
    return {'/epochs/%d/parameters' % e: {'epoch': e, 'nonce': nonces[e], 'max_tx_size': size, 'min_fee_a': 44} for e, size in sizes.items()}


def chain(top):
    """The transition chain (epoch 3 from height 12) with a provider head at fixture index `top`."""
    routes, _ = gen_fixture.fixtures(transition=True)
    for h in range(8, top + 1):
        b = gen_fixture.block(h, transition=True)
        routes['/blocks/' + str(OFFSET + h)] = b
        routes['/blocks/' + b['hash']] = b
        routes['/blocks/' + b['hash'] + '/next?count=100&page=1'] = [gen_fixture.block(n, transition=True) for n in range(h + 1, min(h + 101, top + 1))]
    routes['/blocks/latest'] = gen_fixture.block(top, transition=True)
    return routes


class Counting(real.AccountedFake):
    """The accounted client the upgrade uses: every GET counted, status from the count."""


class EpochUpgradeTests(CheckpointFixture):
    def follow_to(self, store, dsn, target, top):
        routes = chain(top); health = self.state(); stop = threading.Event(); client = real.AccountedFake(routes)
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, stop, network=PREPROD)
            poller.tick(pollstore, client)
            driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=self.profile, fixed_target=target)
            driver._poller = poller
            driver.run_once(store, health, stop)
        finally:
            pollstore.close()
        self.assertEqual(store.generation()['target_height'], target)

    def legacy(self):
        """A nonce-only schema-4 store as the earlier release leaves it: generation 0 with epochs 0-2,
        a relevance anchor, and generation 1 (incremental) adding epoch 3 and completing epoch 2,
        published after the integrity base."""
        with nonce_only():
            dsn = self.make(transition=True)
            store = self.open(dsn)
            upgrade_store(store)
            store, _ = self.startup(dsn, store)
            trust_history(store, self.profile)
            store.certify_ready_full()
            self.follow_to(store, dsn, OFFSET + 12, 2172)
            self.assertEqual(store.state()['schema_version'], 4)
            store.close()
        return dsn

    def inventory(self, dsn):
        with PgStore(conninfo=dsn, readonly=True, allow_legacy=True) as store:
            return physical_inventory(store), store.db.execute(
                "SELECT count(*) FROM pg_attribute WHERE attrelid='chain.epoch_nonces'::regclass AND attname='max_tx_size' AND NOT attisdropped").fetchone()[0]

    def upgrade(self, dsn, routes=None, client=None):
        client = client or Counting(routes if routes is not None else parameters())
        with PgStore(conninfo=dsn, allow_legacy=True) as store:
            return upgrade_store(store, epoch_source=ProviderEpochSource(client, network_profile('preprod'))), client

    def cost(self, dsn, heights):
        role = self.reader(dsn)
        with PgStore(conninfo=dsn) as store:
            grant_reader(store.db, role)
        client = TypedClient(make_conninfo(dsn, user=role), timeout=20).connect()
        try:
            return {h: client.query(COST, ['int4'], [h]).rows for h in heights}
        finally:
            client.close()

    def test_nonce_only_store_opens_only_to_verify_and_upgrade(self):
        dsn = self.legacy()
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_upgrade_required'):
            PgStore(conninfo=dsn)
        with PgStore(conninfo=dsn, readonly=True, allow_legacy=True) as store:
            self.assertTrue(store.legacy_format)
            report = verify_store(store)
            self.assertEqual((report['generation'], report['format']), (1, LEGACY_FORMAT))
            status = store.status()
            self.assertEqual(status['format'], LEGACY_FORMAT)
            self.assertEqual(status['epoch_parameters_upgrade'], {'required': True, 'stored_epochs': 4, 'provider_requests': 8})
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_upgrade_required'):
            backup(folder / 'a.dump', folder / 'a.json', conninfo=dsn)
        before = self.inventory(dsn)
        with PgStore(conninfo=dsn, allow_legacy=True) as store, self.assertRaisesRegex(Refusal, 'epoch_parameters_source_required'):
            upgrade_store(store)
        self.assertEqual(self.inventory(dsn), before)
        out = io.StringIO()
        with patch.dict(os.environ, {'DBSYNC_PROVIDER_ADAPTER': 'portable'}, clear=False), redirect_stdout(out):
            os.environ.pop('DBSYNC_PROVIDER_KEY_FILE', None)
            service = 'db_sync_alt_upgrade_' + os.urandom(4).hex()
            config = Path(self.enterContext(tempfile.TemporaryDirectory())) / 'pg_service.conf'
            config.write_text('[' + service + ']\n' + ''.join(k + '=' + v + '\n' for k, v in psycopg.conninfo.conninfo_to_dict(dsn).items()))
            with patch.dict(os.environ, {'PGSERVICEFILE': str(config)}), self.assertRaises(SystemExit) as stopped, patch('sys.stderr', new_callable=io.StringIO) as err:
                upgrade.main(['--service', service])
        self.assertEqual(stopped.exception.code, 2)
        refusal = json.loads(err.getvalue())
        self.assertEqual(refusal['error'], 'epoch_parameters_source_required')
        self.assertIn('--source-service', refusal['required'])
        self.assertIn('DBSYNC_PROVIDER_KEY_FILE', refusal['required'])
        self.assertEqual(self.inventory(dsn), before)

    def test_upgrade_reads_every_stored_epoch_twice_and_serves_the_cost_query(self):
        dsn = self.legacy()
        with PgStore(conninfo=dsn, readonly=True, allow_legacy=True) as store:
            manifests = store.db.execute('SELECT id,manifest FROM chain.generations ORDER BY id').fetchall()
            receipts = store.db.execute('SELECT generation,phase,unit,receipt FROM chain.checkpoints ORDER BY 1,2,3').fetchall()
            checkpoints = store.db.execute('SELECT * FROM chain.integrity_checkpoints ORDER BY generation').fetchall()
            old_digests = {g: store.digests(g) for g in (0, 1)}
        result, client = self.upgrade(dsn)
        self.assertEqual(result['schema_version'], 5)
        self.assertEqual(result['epoch_parameters'], {'generation': 1, 'epochs': 4, 'source': 'blockfrost-cardano',
                                                      'provider_requests': 8, 'verification': 'offline-integrity'})
        # Two complete passes, one request per stored epoch each, nothing else (no /metrics).
        self.assertEqual(client.calls, ['/epochs/%d/parameters' % e for e in range(4)] * 2)
        # Bridge metadata coverage starts after the upgraded generation (target OFFSET + 12); nothing older is fetched.
        self.assertEqual(result['bridge'], {'metadata_key': 6500973, 'coverage_start': OFFSET + 13, 'checkpoint': None,
                                            'checkpoint_supported': None, 'reason': None})
        with PgStore(conninfo=dsn) as store:
            self.assertFalse(store.legacy_format)
            self.assertEqual(store.meta('contract'), json.loads(json.dumps(store.network.contract)))
            record = store.meta(EPOCH_UPGRADE)
            self.assertEqual((record['generation'], record['epochs'], record['provider_requests']), (1, [[e, SIZES[e]] for e in range(4)], 8))
            self.assertEqual(record['legacy_contract_sha256'], digest(store.network.legacy_contract))
            self.assertEqual(dict(store.db.execute('SELECT epoch,max_tx_size FROM chain.epoch_nonces').fetchall()), SIZES)
            self.assertEqual(dict(store.db.execute('SELECT epoch_no,max_tx_size FROM serve.epoch_param').fetchall()), SIZES)
            self.assertIsNone(store.sync_job(epoch_parameters.JOB))
            # Nothing sealed before the upgrade is rewritten; those generations verify as sealed.
            self.assertEqual(store.db.execute('SELECT id,manifest FROM chain.generations ORDER BY id').fetchall(), manifests)
            self.assertEqual(store.db.execute('SELECT generation,phase,unit,receipt FROM chain.checkpoints ORDER BY 1,2,3').fetchall(), receipts)
            self.assertEqual(store.db.execute('SELECT * FROM chain.integrity_checkpoints ORDER BY generation').fetchall(), checkpoints)
            self.assertEqual({g: store.digests(g) for g in (0, 1)}, old_digests)
            self.assertEqual(verify_store(store)['format'], LEGACY_FORMAT)
        with PgStore(conninfo=dsn) as store:
            self.assertEqual(upgrade_store(store), {'schema_version': 5, 'changed': False})
        # The released Cost query returns the cursor block's epoch size; an unknown block gives NULL.
        heights = {OFFSET + 8: 0, OFFSET + 9: 1, OFFSET + 10: 2, OFFSET + 12: 3}
        rows = self.cost(dsn, [*heights, 1])
        self.assertEqual({h: rows[h] for h in heights}, {h: [{'max': SIZES[e]}] for h, e in heights.items()})
        self.assertEqual(rows[1], [{'max': None}])
        # The service starts, then follows in the new format: new generations carry the size capability.
        store, _ = self.startup(dsn)
        self.follow_to(store, dsn, OFFSET + 13, 2173)
        g = store.generation()
        self.assertEqual((g['id'], g['manifest']['capabilities']['epoch_max_tx_size_v1'], g['manifest']['contracts']),
                         (2, True, digest(store.network.contract)))
        # The first update after the upgrade is covered; the coverage start stays where the upgrade put it.
        self.assertIs(g['manifest']['capabilities']['bridge_metadata_v1'], True)
        self.assertEqual(store.status()['bridge']['coverage_start'], OFFSET + 13)
        self.assertEqual({n: store.status(parse_checkpoint('block:%d' % n))['bridge']['checkpoint_supported'] for n in (OFFSET + 11, OFFSET + 12)},
                         {OFFSET + 11: False, OFFSET + 12: True})
        store, _ = self.startup(dsn, store)
        self.assertEqual(verify_store(store)['format'], FORMAT)
        self.assertEqual(store.integrity()['base'], 2)
        self.assertEqual(store.db.execute('SELECT digests FROM chain.integrity_checkpoints WHERE generation=2').fetchone()[0]['epoch_nonces'],
                         store.digests(2)['epoch_nonces'])
        # A size that differs from the record refuses verification.
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            db.execute('UPDATE chain.epoch_nonces SET max_tx_size=16383 WHERE epoch=0')
        store.close()
        with PgStore(conninfo=dsn, readonly=True) as store, self.assertRaisesRegex(Refusal, 'epoch_parameters_upgrade_invalid'):
            verify_store(store, 1)

    def test_disagreeing_reads_refuse_and_leave_the_store_as_it_was(self):
        dsn = self.legacy()
        before = self.inventory(dsn)
        routes = parameters()
        answers = [routes['/epochs/2/parameters'], dict(routes['/epochs/2/parameters'], max_tx_size=12003)]
        routes['/epochs/2/parameters'] = lambda: answers.pop(0)
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_changed'):
            self.upgrade(dsn, routes)
        self.assertEqual(self.inventory(dsn), before)
        self.assertEqual(before[1], 0)
        # The earlier release still verifies and starts on it.
        with nonce_only():
            store, _ = self.startup(dsn)
            self.assertEqual(store.state()['schema_version'], 4)
            store.close()

    def test_malformed_answers_refuse_before_any_change(self):
        dsn = self.legacy()
        before = self.inventory(dsn)
        cases = [('max_tx_size', True, 'invalid_epoch_max_tx_size'), ('max_tx_size', -1, 'invalid_epoch_max_tx_size'),
                 ('max_tx_size', 2**31, 'invalid_epoch_max_tx_size'), ('max_tx_size', '16384', 'invalid_epoch_max_tx_size'),
                 ('max_tx_size', None, 'invalid_epoch_max_tx_size'), ('nonce', 'dd' * 32, 'epoch_parameters_nonce_mismatch'),
                 ('epoch', 4, 'epoch_parameters_unavailable')]
        for field, value, code in cases:
            with self.subTest(field=field, value=value):
                routes = parameters()
                routes['/epochs/3/parameters'] = dict(routes['/epochs/3/parameters'], **{field: value})
                with self.assertRaisesRegex(Refusal, code):
                    self.upgrade(dsn, routes)
                self.assertEqual(self.inventory(dsn), before)
        routes = parameters(); del routes['/epochs/1/parameters']['max_tx_size']
        with self.assertRaisesRegex(Refusal, 'invalid_epoch_max_tx_size'):
            self.upgrade(dsn, routes)
        self.assertEqual(self.inventory(dsn), before)

    def test_interrupted_upgrade_resumes_without_repeating_answered_requests(self):
        dsn = self.legacy()
        # A request that fails in transport keeps what was already answered.
        routes = parameters(); failed = []
        def once():
            if not failed:
                failed.append(1); raise RuntimeError('connection reset')
            return parameters()['/epochs/1/parameters']
        routes['/epochs/1/parameters'] = once
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_source_unavailable'):
            self.upgrade(dsn, routes)
        with PgStore(conninfo=dsn, readonly=True, allow_legacy=True) as store:
            job = store.sync_job(epoch_parameters.JOB)
            self.assertEqual((job['attempts'], job['reads'][0]['sizes']), (2, {'0': SIZES[0]}))
        # Stopped after both reads are staged, before the commit: the rerun sends no request.
        with patch.object(upgrade, '_schema_v5', side_effect=RuntimeError('killed')), self.assertRaises(RuntimeError):
            self.upgrade(dsn)
        with PgStore(conninfo=dsn, readonly=True, allow_legacy=True) as store:
            self.assertTrue(store.legacy_format)
            job = store.sync_job(epoch_parameters.JOB)
            self.assertEqual(job['attempts'], 9)
            self.assertEqual([read['sizes'] for read in job['reads']], [{str(e): s for e, s in SIZES.items()}] * 2)
        with nonce_only():
            store, _ = self.startup(dsn)
            store.close()
        result, client = self.upgrade(dsn)
        self.assertEqual(client.calls, [])
        # Every request reserved is reported: 8 answered plus the one lost in transport.
        self.assertEqual(result['epoch_parameters']['provider_requests'], 9)
        with PgStore(conninfo=dsn) as store:
            self.assertEqual(dict(store.db.execute('SELECT epoch,max_tx_size FROM chain.epoch_nonces').fetchall()), SIZES)

    def test_refusals_before_any_request(self):
        dsn = self.legacy()
        client = Counting(parameters())
        client.status = lambda: {'budget_used': 49995, 'daily_limit': 50000}
        before = self.inventory(dsn)
        with self.assertRaisesRegex(Refusal, 'provider_budget_insufficient'):
            self.upgrade(dsn, client=client)
        self.assertEqual(client.calls, [])
        self.assertEqual(self.inventory(dsn), before)
        with psycopg.connect(dsn, autocommit=True) as db:
            g = db.execute('SELECT * FROM chain.generations WHERE id=1').fetchone()
            db.execute("INSERT INTO chain.generations VALUES(2,1,'pending',%s,%s,%s,%s,%s,%s)", (g[3], g[4], g[5], g[6] + 1, g[7] + 1, psycopg.types.json.Jsonb({'pending': True})))
        client = Counting(parameters())
        with self.assertRaisesRegex(Refusal, 'upgrade_pending_generation'):
            self.upgrade(dsn, client=client)
        self.assertEqual(client.calls, [])

    def test_spent_request_budget_discards_the_attempt(self):
        dsn = self.legacy()
        before = self.inventory(dsn)
        new_job = epoch_parameters._new_job
        def small(*args):
            return {**new_job(*args), 'budget': 3}
        with patch.object(epoch_parameters, '_new_job', small), self.assertRaisesRegex(Refusal, 'request_budget_exhausted'):
            self.upgrade(dsn)
        self.assertEqual(self.inventory(dsn), before)
        result, client = self.upgrade(dsn)
        self.assertEqual((len(client.calls), result['epoch_parameters']['provider_requests']), (8, 8))

    def test_upgrade_takes_datums_as_cardano_db_sync_stores_them(self):
        """The earlier release's datum columns (a value never NULL, an output datum hash that references a datum)
        take a NULL value and a hash without a preimage after the upgrade, in its one migration."""
        dsn = self.legacy()

        def constraints():
            with PgStore(conninfo=dsn, readonly=True, allow_legacy=True) as store:
                return store.db.execute(
                    "SELECT (SELECT attnotnull FROM pg_attribute WHERE attrelid='chain.datums'::regclass AND attname='value'),"
                    "(SELECT attnotnull FROM pg_attribute WHERE attrelid='serve.datum'::regclass AND attname='value'),"
                    "EXISTS(SELECT 1 FROM pg_constraint WHERE conname='outputs_datum_hash_fkey' AND conrelid='chain.outputs'::regclass)").fetchone()
        self.assertEqual(constraints(), (True, True, True))
        self.upgrade(dsn)
        self.assertEqual(constraints(), (False, False, False))
        with PgStore(conninfo=dsn, readonly=True) as store:
            self.assertEqual(store.state()['schema_version'], 5)
            verify_store(store)

    def test_upgraded_store_is_refused_by_the_nonce_only_release(self):
        dsn = self.legacy()
        self.upgrade(dsn)
        with nonce_only():
            # The schema is checked before the contract: schema 5 is unknown to the earlier release.
            for readonly in (False, True):
                with self.subTest(readonly=readonly), self.assertRaisesRegex(Refusal, 'unsupported_database_identity'):
                    PgStore(conninfo=dsn, readonly=readonly, allow_incomplete=True)

    def test_upgrade_from_a_cardano_db_sync_source(self):
        dsn = self.legacy()
        source = self.dbsync({0: [16384], 1: [0], 2: [12000, 12002, 12001], 3: [24000]})
        with PgStore(conninfo=dsn, allow_legacy=True) as store:
            result = upgrade_store(store, epoch_source=SQLEpochSource(source))
        self.assertEqual((result['epoch_parameters']['source'], result['epoch_parameters']['provider_requests']), ('cardano-db-sync-postgresql', 0))
        with PgStore(conninfo=dsn) as store:
            self.assertEqual(dict(store.db.execute('SELECT epoch,max_tx_size FROM chain.epoch_nonces').fetchall()), SIZES)
            self.assertEqual(store.meta(EPOCH_UPGRADE)['source']['kind'], 'cardano-db-sync-postgresql')
            verify_store(store)

    def dbsync(self, sizes):
        """A read-only cardano-db-sync shaped source holding only what the upgrade reads."""
        from db_sync_alt.contract import Profile
        from db_sync_alt.resources import profile_dir
        from db_sync_alt.sources.dbsync.reader import SQLSource
        root = Path(__file__).resolve().parents[1]
        name = 'dbsync_' + os.urandom(5).hex(); self.databases.append(name)
        self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
        source = make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=name)
        with psycopg.connect(source, autocommit=True) as c:
            c.execute((root / 'fixtures/data/dbsync_schema.sql').read_text())
            c.execute("ALTER TABLE block ADD COLUMN previous_id bigint; ALTER TABLE tx_out ADD COLUMN inline_datum_id bigint; CREATE TABLE schema_version(stage_one bigint NOT NULL,stage_two bigint NOT NULL,stage_three bigint NOT NULL);INSERT INTO schema_version VALUES(15,48,6);CREATE TABLE epoch_stake_progress(epoch_no word31type NOT NULL,completed boolean NOT NULL);CREATE TABLE meta(network_name varchar NOT NULL);INSERT INTO meta VALUES('preprod')")
            # db-sync keeps one row per epoch; duplicates exercise the node's MAX.
            c.execute('ALTER TABLE epoch_param DROP CONSTRAINT epoch_param_pkey')
            for epoch, values in sizes.items():
                for value in values:
                    c.execute('INSERT INTO epoch_param VALUES(%s,%s,%s)', (epoch, bytes.fromhex(NONCES[epoch]), value))
        config = Path(self.tmp.name) / ('pg_service_' + name + '.conf')
        config.write_text('[' + name + ']\n' + ''.join(k + '=' + v + '\n' for k, v in psycopg.conninfo.conninfo_to_dict(source).items()))
        self.enterContext(patch.dict(os.environ, {'PGSERVICEFILE': str(config)}))
        attestation = {'dbsync_version': '13.7.1.0', 'tx_in_retained': True, 'epoch_stake_retained': True, 'operator_attested': True}
        return SQLSource(name, Profile(profile_dir('preprod'), 'preprod'), attestation)

    def test_v3_backup_restores_then_upgrades_like_the_original_store(self):
        dsn = self.legacy()
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        archive, manifest = folder / 'v3.dump', folder / 'v3.json'
        with nonce_only():
            backup(archive, manifest, conninfo=dsn)
        body = json.loads(manifest.read_text())
        self.assertEqual((body['format'], body['schema_version']), ('db-sync-alt-postgresql-backup-v3', 4))
        destination = self.database()
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_source_required'):
            restore(archive, manifest, conninfo=destination)
        with psycopg.connect(destination, autocommit=True) as db:
            self.assertTrue(empty_destination(db))
        client = Counting(parameters())
        restore(archive, manifest, conninfo=destination, epoch_source=lambda network: ProviderEpochSource(client, network))
        self.assertEqual(len(client.calls), 8)
        self.upgrade(dsn)
        with PgStore(conninfo=dsn, readonly=True) as original, PgStore(conninfo=destination, readonly=True) as restored:
            for g in (0, 1):
                self.assertEqual(restored.digests(g), original.digests(g))
            self.assertEqual(epoch_parameters.family_digest(restored, 1), epoch_parameters.family_digest(original, 1))
            ignore = ('service_version',)
            self.assertEqual({k: v for k, v in restored.meta(EPOCH_UPGRADE).items() if k not in ignore},
                             {k: v for k, v in original.meta(EPOCH_UPGRADE).items() if k not in ignore})
            verify_store(restored)
            self.assertEqual(restored.db.execute('SELECT * FROM serve.epoch_param ORDER BY 1').fetchall(),
                             original.db.execute('SELECT * FROM serve.epoch_param ORDER BY 1').fetchall())

    def test_restore_whose_upgrade_refuses_is_left_ready_for_upgrade(self):
        dsn = self.legacy()
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        archive, manifest = folder / 'v3.dump', folder / 'v3.json'
        with nonce_only():
            backup(archive, manifest, conninfo=dsn)
        # Not enough of the day's budget for 2 requests per stored epoch: refused before the destination is touched.
        destination = self.database()
        poor = Counting(parameters()); poor.status = lambda: {'budget_used': 49995, 'daily_limit': 50000}
        with self.assertRaisesRegex(Refusal, 'provider_budget_insufficient'):
            restore(archive, manifest, conninfo=destination, epoch_source=lambda network: ProviderEpochSource(poor, network))
        self.assertEqual(poor.calls, [])
        with psycopg.connect(destination, autocommit=True) as db:
            self.assertTrue(empty_destination(db))
        # The upgrade step refuses after the archive is in: restored, ready, nonce-only, reader not granted.
        routes = parameters()
        answers = [routes['/epochs/2/parameters'], dict(routes['/epochs/2/parameters'], max_tx_size=12003)]
        routes['/epochs/2/parameters'] = lambda: answers.pop(0)
        role = self.reader(destination)
        with self.assertRaisesRegex(Refusal, 'epoch_parameters_changed'):
            restore(archive, manifest, conninfo=destination, reader_role=role, epoch_source=lambda network: ProviderEpochSource(Counting(routes), network))
        with PgStore(conninfo=destination, readonly=True, allow_legacy=True) as store:
            state = store.state()
            self.assertEqual((store.legacy_format, state['schema_version'], state['migration_state']), (True, 4, 'ready'))
            self.assertFalse(store.db.execute("SELECT has_database_privilege(%s,current_database(),'CONNECT')", (role,)).fetchone()[0])
            verify_store(store)
        # `upgrade` finishes it; the reader is granted afterwards.
        self.upgrade(destination)
        self.assertEqual(self.cost(destination, [OFFSET + 10]), {OFFSET + 10: [{'max': SIZES[2]}]})

    def test_v4_backup_round_trip_needs_no_source(self):
        dsn = self.legacy()
        self.upgrade(dsn)
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        archive, manifest = folder / 'v4.dump', folder / 'v4.json'
        backup(archive, manifest, conninfo=dsn)
        body = json.loads(manifest.read_text())
        self.assertEqual((body['format'], body['schema_version']), ('db-sync-alt-postgresql-backup-v4', 5))
        destination = self.database()
        unavailable = lambda network: self.fail('a v4 restore needs no epoch source')
        restore(archive, manifest, conninfo=destination, epoch_source=unavailable)
        with PgStore(conninfo=dsn, readonly=True) as original, PgStore(conninfo=destination, readonly=True) as restored:
            for g in (0, 1):
                self.assertEqual(restored.digests(g), original.digests(g))
            self.assertEqual(restored.meta(EPOCH_UPGRADE), original.meta(EPOCH_UPGRADE))
            self.assertEqual(restored.db.execute('SELECT * FROM serve.epoch_param ORDER BY 1').fetchall(),
                             original.db.execute('SELECT * FROM serve.epoch_param ORDER BY 1').fetchall())
            verify_store(restored)
        heights = [OFFSET + 8, OFFSET + 9, OFFSET + 10, OFFSET + 12]
        self.assertEqual(self.cost(destination, heights), self.cost(dsn, heights))
