"""Real SQL-root relevance history across released schema-4 checkpoints.

Providers are local fixtures. Old-form manifests are selected before generation
creation through the supported continuation API, never rewritten after sealing.
"""
import copy
import unittest
from unittest.mock import patch

from db_sync_alt.contract import Refusal
from db_sync_alt.sources.blockfrost import relevance
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
from fixtures import relevance as f
from fixtures.blockfrost import Fake


class RelevanceCheckpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        f.configure('mainnet');f.SQLFixture.setup()

    @classmethod
    def tearDownClass(cls):
        f.SQLFixture.teardown()

    def prepare(self,rows):
        store,routes=f.SQLFixture.prepare(rows,reuse_root=True)
        self.addCleanup(store.close)
        self.assertEqual(store.state()['schema_version'],4)
        return store,routes

    def follow_embedded(self,store,routes,target):
        begin=store.begin_continuation
        def embedded(**kw):
            manifest=copy.deepcopy(kw['manifest'])
            manifest.pop('identity_sha256');manifest['identity']=copy.deepcopy(f.PROFILE.identity)
            return begin(**{**kw,'manifest':manifest})
        with patch.object(store,'begin_continuation',embedded):
            return f.SQLFixture.follow(store,routes,target)

    def test_mixed_profile_forms_and_checkpoint_restart_keep_reference_facts_and_ordinals(self):
        created=f.make_tx(310,relevant=True,mint=3)
        spent=f.make_tx(311,inputs=[(bytes.fromhex(created['tx_hash']),0)],mint=-2)
        next_spent=f.make_tx(312,inputs=[(bytes.fromhex(f.ROOT['tx_hash']),0)])
        rows=[[created],[spent],[next_spent]]
        store,routes=self.prepare(rows);reference,full_routes=self.prepare(rows)
        self.follow_embedded(store,routes,f.T+1)
        self.assertEqual(store.generation()['manifest']['integrity']['kind'],'certified-parent-delta-v1')
        f.SQLFixture.follow(store,routes,f.T+2)
        self.assertEqual(store.generation()['manifest']['integrity']['kind'],'certified-parent-delta-v2')
        self.assertEqual(store.write_checkpoint(store.certify_checkpoint()),{'generation':2,'previous_base':0})
        dsn=store.db.info.dsn;store.close()
        with PgStore(conninfo=dsn) as restarted:
            restarted.certify_ready_full()
            self.assertEqual(restarted.integrity()['base'],2)
            self.assertIsNotNone(relevance.qualified_parent(restarted,f.PROFILE))
            f.SQLFixture.follow(restarted,routes,f.T+3);verify_store(restarted)
            # The full path has its own acquisition and independent verification.
            f.SQLFixture.follow(reference,full_routes,f.T+3,relevance_enabled=False);verify_store(reference)
            self.assertEqual(restarted.digests(3),reference.digests(1))
            key=(bytes.fromhex(spent['tx_hash']),bytes.fromhex(created['tx_hash']),0)
            self.assertEqual(restarted.ordinal('inputs',key),reference.ordinal('inputs',key))
            self.assertEqual(restarted.ordinal('inputs',key),0)

    def test_integrity_checkpoint_does_not_turn_full_scan_history_into_relevance_coverage(self):
        store,routes=self.prepare([[],[]]);f.SQLFixture.follow(store,routes,f.T+1,relevance_enabled=False)
        store.write_checkpoint(store.certify_checkpoint());dsn=store.db.info.dsn;store.close()
        with PgStore(conninfo=dsn) as restarted:
            restarted.certify_ready_full()
            self.assertIsNone(relevance.qualified_parent(restarted,f.PROFILE))
            client=Fake(routes)
            from db_sync_alt.sources.blockfrost.follow import update_follow
            with self.assertRaisesRegex(Refusal,'relevance_parent_unsupported'):
                update_follow(restarted,f.Provider(50000,client),f.PROFILE,f.T+2,hooks=f.FixtureHooks(),relevance_enabled=True)
            self.assertEqual(client.calls,[]);self.assertIsNone(restarted.pending())

    def test_hot_cache_new_prefix_and_checkpoint_invalidation_on_real_long_history(self):
        # Seventeen small transitions add fewer than 2,000 facts. The existing
        # SQL prerequisite/witness bootstrap and all security bounds are retained.
        store,routes=self.prepare([[] for _ in range(17)])
        for n in range(1,17):
            f.SQLFixture.follow(store,routes,f.T+n)
            self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
        reads=[];original=store.checkpoint_rows
        def counted(**kw):
            if kw.get('phase')=='relevance':reads.append(kw['generation'])
            return original(**kw)
        with patch.object(store,'checkpoint_rows',counted):
            for _ in range(8):self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
            self.assertEqual(reads,[])
            f.SQLFixture.follow(store,routes,f.T+17)
            before=len(reads);self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
            self.assertEqual(reads[before:],[17])
            store.write_checkpoint(store.certify_checkpoint())
            before=len(reads);self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
            self.assertEqual(len(reads)-before,17)
        verify_store(store)
        self.assertEqual(store.generation()['id'],17)

    def test_embedded_pending_missing_row_recovers_after_checkpoint_and_fresh_writer(self):
        created=f.make_tx(314,relevant=True)
        spent=f.make_tx(315,inputs=[(bytes.fromhex(created['tx_hash']),0)])
        store,routes=self.prepare([[created],[spent]])
        self.follow_embedded(store,routes,f.T+1);store.write_checkpoint(store.certify_checkpoint())
        checkpoint=store.checkpoint
        def omit(generation,phase,unit,receipt):
            if phase!='relevance':return checkpoint(generation,phase,unit,receipt)
        with patch.object(store,'checkpoint',omit),patch.object(store,'stage_metadata_reuse',side_effect=Refusal('interrupted')):
            with self.assertRaisesRegex(Refusal,'interrupted'):self.follow_embedded(store,routes,f.T+2)
        pending=store.pending();self.assertIn('identity',pending['manifest'])
        self.assertNotIn('identity_sha256',pending['manifest']);self.assertFalse(store.completed(2,'relevance','coverage'))
        dsn=store.db.info.dsn;store.close()
        with PgStore(conninfo=dsn) as restarted:
            restarted.certify_ready_full();before=restarted.digests(1)
            cert=relevance.pending_certificate(restarted,f.PROFILE,2)
            self.assertEqual(cert,pending['manifest']['relevance_coverage'])
            self.assertEqual(restarted.digests(1),before)
            self.assertEqual(restarted.checkpoint_rows(generation=2,phase='relevance',unit='coverage')[0]['receipt'],cert)
            f.SQLFixture.follow(restarted,routes,f.T+2);verify_store(restarted)
            self.assertEqual(restarted.generation()['manifest']['integrity']['kind'],'certified-parent-delta-v1')
            self.assertEqual(restarted.ordinal('inputs',(bytes.fromhex(spent['tx_hash']),bytes.fromhex(created['tx_hash']),0)),0)

    def test_missing_pending_row_with_numeric_coercion_refuses_without_repair(self):
        from psycopg.types.json import Jsonb
        from db_sync_alt.sources.blockfrost.follow import update_follow
        for field,value in (('version',True),('version',1.0),('generation',False),('generation',0.0),('range',float(f.T+1))):
            with self.subTest(field=field,value=value):
                store,routes=self.prepare([[f.make_tx(313,relevant=True)]])
                checkpoint=store.checkpoint
                def omit(generation,phase,unit,receipt):
                    if phase!='relevance':return checkpoint(generation,phase,unit,receipt)
                with patch.object(store,'checkpoint',omit),patch.object(store,'stage_metadata_reuse',side_effect=Refusal('interrupted')):
                    with self.assertRaisesRegex(Refusal,'interrupted'):f.SQLFixture.follow(store,routes,f.T+1)
                manifest=store.pending()['manifest'];cert=manifest['relevance_coverage']
                if field=='generation':cert['root'][field]=value
                elif field=='range':cert['range'][0]=value
                else:cert[field]=value
                store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=1',(Jsonb(manifest),))
                client=Fake(routes);before=store.digests(0)
                with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
                    update_follow(store,f.Provider(50000,client),f.PROFILE,f.T+1,hooks=f.FixtureHooks(),relevance_enabled=True)
                self.assertEqual(client.calls,[]);self.assertEqual(store.digests(0),before)
                self.assertFalse(store.completed(1,'relevance','coverage'))
