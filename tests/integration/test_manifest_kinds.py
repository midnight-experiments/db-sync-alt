"""Integrity kinds of incremental generations, against actual PostgreSQL.

The writer binds every incremental generation it begins to the stored network
profile by the profile's digest (`identity_sha256`, kind certified-parent-delta-v2).
Generations of kind certified-parent-delta-v1 embed the profile; they verify
under their own rules, alone or in one lineage with v2 generations.
Deterministic fixture providers only; no live provider.
"""
import contextlib
import copy
import os
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import psycopg
from psycopg.types.json import Jsonb
from db_sync_alt.contract import Refusal, canonical, digest
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.incremental import KIND_V1, KIND_V2
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.sync import PgHooks, SyncDriver
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
import fixtures.database as real
from fixtures.networks import PREPROD
from integration.test_tip_fork import TOP, chain_routes

MAX_V2_MANIFEST = 8 * 1024
OTHER = digest('another profile')


def resealed(m):
    """Recompute the root and the seal of a changed semantic, as a forger would."""
    integrity = m['integrity']
    integrity['root'] = digest(integrity['semantic'])
    m['completion']['sha256'] = digest({'integrity': integrity, 'original_receipt_sha256': m['completion']['original_receipt_sha256']})


class ManifestKindPGTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
    @classmethod
    def tearDownClass(cls): cls.admin.close()
    setUp = real.SyncFixture.setUp
    tearDown = real.SyncFixture.tearDown
    database = real.SyncFixture.database
    make = real.SyncFixture.make
    state = real.SyncFixture.state

    def prepare(self):
        dsn = self.make()
        store = PgStore(conninfo=dsn); self.addCleanup(store.close)
        upgrade_store(store); store.certify_ready_full()
        return dsn, store

    def restart(self, store, dsn):
        """New writer session, as a process restart opens it; nothing certified yet."""
        store.close()
        store = PgStore(conninfo=dsn); self.addCleanup(store.close)
        return store

    def follow(self, store, dsn, top):
        """One follow step with the provider head at fixture block ``top``; returns the ready generation."""
        health = self.state(); client = real.AccountedFake(chain_routes(top))
        pollstore = PollBudgetStore(None, owner_binding(store), conninfo=dsn)
        try:
            poller = HeadPoller(lambda: pollstore, lambda: client, health, threading.Event(), network=PREPROD)
            poller.tick(pollstore, client)
            driver = SyncDriver(SimpleNamespace(sync={}), client=client, profile=self.profile)
            driver._poller = poller
            driver.run_once(store, health, threading.Event())
        finally:
            pollstore.close()
        return store.generation()['id']

    def follow_v1(self, store, dsn, top):
        """A follow step whose new generation embeds the profile (the v1 form)."""
        begin = store.begin_continuation
        def embedded(**kw):
            m = dict(kw['manifest']); del m['identity_sha256']; m['identity'] = self.profile.identity
            return begin(**{**kw, 'manifest': m})
        with patch.object(store, 'begin_continuation', embedded):
            return self.follow(store, dsn, top)

    def size(self, store, generation):
        return store.db.execute('SELECT length(manifest::text) FROM chain.generations WHERE id=%s', (generation,)).fetchone()[0]

    @contextlib.contextmanager
    def tampered(self, dsn, generation, change):
        """Rewrite a stored manifest as the superuser, past the store's guards, and put it back afterwards."""
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute('SET session_replication_role = replica')
            original = db.execute('SELECT manifest FROM chain.generations WHERE id=%s', (generation,)).fetchone()[0]
            m = copy.deepcopy(original); change(m)
            db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s', (Jsonb(m), generation))
            try:
                yield
            finally:
                db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s', (Jsonb(original), generation))

    def startup_refusal(self, store):
        """The refusal code of the startup audit (full verification from the immutable base)."""
        with self.assertRaises(Refusal) as caught:
            store.certify_ready_full()
        return caught.exception.code

    def test_new_generations_bind_the_profile_by_digest(self):
        dsn, store = self.prepare()
        identity = store.meta('profile')
        for number, top in ((1, TOP), (2, TOP + 1)):
            self.assertEqual(self.follow(store, dsn, top), number)
            m = store.generation(number)['manifest']; semantic = m['integrity']['semantic']
            self.assertNotIn('identity', m)
            self.assertNotIn('identity', semantic)
            self.assertEqual((m['identity_sha256'], semantic['identity_sha256']), (digest(identity), digest(identity)))
            self.assertEqual((m['integrity']['kind'], m['validation']['integrity_kind']), (KIND_V2, KIND_V2))
            self.assertLessEqual(self.size(store, number), MAX_V2_MANIFEST)
        verify_store(store)
        store = self.restart(store, dsn)
        store.certify_ready_full()

    def test_mixed_lineage_verifies_at_startup(self):
        """Generation 0 (full) -> one v1 generation -> v2 generations, audited from the base at startup."""
        dsn, store = self.prepare()
        identity = store.meta('profile')
        self.assertEqual(self.follow_v1(store, dsn, TOP), 1)
        self.assertEqual(self.follow(store, dsn, TOP + 1), 2)
        self.assertEqual(self.follow(store, dsn, TOP + 2), 3)
        self.assertEqual({g: store.generation(g)['manifest']['integrity']['kind'] for g in (1, 2, 3)}, {1: KIND_V1, 2: KIND_V2, 3: KIND_V2})
        v1 = store.generation(1)['manifest']
        self.assertEqual((v1['identity'], v1['integrity']['semantic']['identity'], v1['validation']['integrity_kind']), (identity, identity, KIND_V1))
        self.assertNotIn('identity_sha256', v1)
        self.assertGreater(self.size(store, 1), 2 * len(canonical(identity)))
        self.assertLessEqual(self.size(store, 2), MAX_V2_MANIFEST)
        self.assertEqual(store.generation(2)['manifest']['parent_ref'], {'kind': 'parent-delta-v2', 'sha256': v1['integrity']['root']})
        store = self.restart(store, dsn)
        store.certify_ready_full()
        for generation in (1, 2, 3):
            verify_store(store, generation)

    def test_pending_v1_generation_is_finished_in_its_form_after_restart(self):
        """A generation begun in the v1 form and interrupted, before or after its seal, is published as v1."""
        import db_sync_alt.sources.blockfrost.follow as follow_source
        dsn, store = self.prepare()
        def crash(*a, **kw): raise RuntimeError('simulated process death')
        with patch.object(follow_source, '_store_suffix', side_effect=crash):
            with self.assertRaises(RuntimeError):
                self.follow_v1(store, dsn, TOP)
        self.assertIn('identity', store.pending()['manifest'])
        store = self.restart(store, dsn); store.certify_ready_full()
        self.assertEqual(self.follow(store, dsn, TOP), 1)
        self.assertEqual(store.generation(1)['manifest']['integrity']['kind'], KIND_V1)
        # Sealed, not yet published: the stop arrives just before publication.
        original = PgHooks.before_publish
        def stop(hooks): hooks.stop_event.set(); original(hooks)
        with patch.object(PgHooks, 'before_publish', stop):
            self.assertEqual(self.follow_v1(store, dsn, TOP + 1), 1)
        self.assertEqual(store.pending()['manifest']['integrity']['kind'], KIND_V1)
        store = self.restart(store, dsn); store.certify_ready_full()
        self.assertEqual(self.follow(store, dsn, TOP + 1), 2)
        self.assertEqual(store.generation(2)['manifest']['integrity']['kind'], KIND_V1)
        self.assertEqual(self.follow(store, dsn, TOP + 2), 3)
        self.assertEqual(store.generation(3)['manifest']['integrity']['kind'], KIND_V2)
        store = self.restart(store, dsn)
        store.certify_ready_full()

    def test_tampered_profile_binding_is_refused_with_the_embedded_profile_codes(self):
        dsn, store = self.prepare()
        self.assertEqual(self.follow_v1(store, dsn, TOP), 1)
        # Reference: the v1 form, with the profile embedded, as the ready generation.
        def embedded(m): m['identity'] = dict(m['identity'], profile='another-profile')
        with self.tampered(dsn, 1, embedded):
            self.assertEqual(self.startup_refusal(store), 'manifest_contract_mismatch')
        self.assertEqual(self.follow(store, dsn, TOP + 1), 2)
        self.assertEqual(self.follow(store, dsn, TOP + 2), 3)
        store.certify_ready_full()

        def top_level(m): m['identity_sha256'] = OTHER
        def both(m): m['identity_sha256'] = OTHER; m['integrity']['semantic']['identity_sha256'] = OTHER; resealed(m)
        def semantic(m): m['integrity']['semantic']['identity_sha256'] = OTHER
        def semantic_resealed(m): semantic(m); resealed(m)
        def embedded_both(m): embedded(m); m['integrity']['semantic']['identity'] = m['identity']; resealed(m)
        def both_forms(m): m['identity'] = self.profile.identity
        def relabelled_v1(m): m['integrity']['kind'] = KIND_V1; m['validation']['integrity_kind'] = KIND_V1; resealed(m)
        def relabelled_v2(m): m['integrity']['kind'] = KIND_V2; m['validation']['integrity_kind'] = KIND_V2; resealed(m)
        # A consistent forgery of an ancestor changes its root, which its child binds: the
        # ready generation's inheritance receipt, or an older child's parent reference.
        cases = [
            ('ready v2: top-level digest', 3, top_level, 'manifest_contract_mismatch'),
            ('ready v2: top-level and semantic digest, resealed', 3, both, 'manifest_contract_mismatch'),
            ('ready v2: both profile fields', 3, both_forms, 'manifest_contract_mismatch'),
            ('ready v2: semantic digest', 3, semantic, 'completion_mismatch'),
            ('ready v2: semantic digest, resealed', 3, semantic_resealed, 'completion_mismatch'),
            ('ready v2: labelled v1', 3, relabelled_v1, 'completion_invalid'),
            ('ancestor v2: top-level digest', 2, top_level, 'completion_mismatch'),
            ('ancestor v2: semantic digest', 2, semantic, 'completion_mismatch'),
            ('ancestor v2: top-level and semantic digest, resealed', 2, both, 'invalid_inheritance_receipt'),
            ('ancestor v1: embedded profile', 1, embedded, 'completion_mismatch'),
            ('ancestor v1: embedded profile in both places, resealed', 1, embedded_both, 'completion_mismatch'),
            ('ancestor v1: labelled v2', 1, relabelled_v2, 'completion_invalid'),
        ]
        for name, generation, change, code in cases:
            with self.subTest(name):
                with self.tampered(dsn, generation, change):
                    self.assertEqual(self.startup_refusal(store), code)
        store = self.restart(store, dsn)
        store.certify_ready_full()


if __name__ == '__main__':
    unittest.main()
