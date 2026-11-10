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
from fixtures.checkpoints import CheckpointFixture


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
        self.assertEqual(store.state()['schema_version'],5)
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
            if kw.get('phase')=='relevance' and kw.get('unit')=='coverage':reads.append(kw['generation'])
            return original(**kw)
        with patch.object(store,'checkpoint_rows',counted):
            for _ in range(8):self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
            self.assertEqual(reads,[])
            f.SQLFixture.follow(store,routes,f.T+17)
            before=len(reads);self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
            self.assertEqual(reads[before:],[17])
            store.write_checkpoint(store.certify_checkpoint())
            before=len(reads);self.assertIsNotNone(relevance.qualified_parent(store,f.PROFILE))
            self.assertEqual(reads[before:],[])
            anchor=store.checkpoint_rows(generation=17,phase='relevance',unit='anchor')[0]['receipt']
            self.assertEqual((anchor['kind'],anchor['origin']),('certified','certified'))
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


    def test_mainnet_trust_refuses_without_installation_and_rejects_forged_trusted_origin(self):
        store,_=self.prepare([[]]);before=store.db.execute("SELECT prosrc FROM pg_proc WHERE oid='chain.guard_immutable()'::regprocedure").fetchone()[0]
        with self.assertRaisesRegex(Refusal,'relevance_trust_mainnet_refused'):relevance.trust_history(store,f.PROFILE)
        self.assertFalse(store.completed(0,'relevance','anchor'))
        self.assertEqual(store.db.execute("SELECT prosrc FROM pg_proc WHERE oid='chain.guard_immutable()'::regprocedure").fetchone()[0],before)
        from db_sync_alt.store.incremental import integrity_reference
        g=store.generation();a={'version':1,'kind':'certified','generation':g['id'],'integrity_ref':integrity_reference(g),'profile':relevance.digest(f.PROFILE.identity),'origin':'trusted-history'}
        with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):relevance.validate_anchor(store,g,[{'receipt':a}])


class RelevanceAnchorTests(unittest.TestCase):
    """Only isolated synthetic non-Mainnet stores are eligible for explicit trust."""
    @classmethod
    def setUpClass(cls):
        f.configure('preprod');f.SQLFixture.setup()
    @classmethod
    def tearDownClass(cls):f.SQLFixture.teardown()
    def prepare(self):
        store,routes=f.SQLFixture.prepare([[],[],[],[]],reuse_root=True)
        self.addCleanup(store.close)
        f.SQLFixture.follow(store,routes,f.T+1,relevance_enabled=False)
        self.assertIsNone(relevance.qualified_parent(store,f.PROFILE))
        return store,routes
    def guard(self,store):
        return store.db.execute("SELECT prosrc FROM pg_proc WHERE oid='chain.guard_immutable()'::regprocedure").fetchone()[0]
    def anchor(self,store,generation=None):
        return store.checkpoint_rows(generation=store.generation()['id'] if generation is None else generation,phase='relevance',unit='anchor')
    def test_trusted_fullscan_enables_equal_facts_and_preserves_origin_at_checkpoint_restart(self):
        from db_sync_alt.service.health import HealthState
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        from db_sync_alt.store.incremental import compute_proof
        from types import SimpleNamespace
        store,routes=self.prepare();reference,full_routes=self.prepare()
        self.assertEqual(relevance.trust_history(store,f.PROFILE),{'generation':1,'relevance':'trusted-history'})
        self.assertEqual(store.status()['relevance']['history'],'trusted-history')
        f.SQLFixture.follow(store,routes,f.T+2)
        before=copy.deepcopy(store.generation()['manifest']);proof=compute_proof(store,store.generation())
        cap=store.certify_checkpoint();store._verified_parent=None
        store.write_checkpoint(cap)
        self.assertEqual(store.generation()['manifest'],before)
        self.assertEqual(compute_proof(store,store.generation()),proof)
        a=self.anchor(store)[0]['receipt']
        self.assertEqual((a['kind'],a['origin']),('certified','trusted-history'))
        self.assertEqual(a['generation'],2)
        verify_store(store)
        dsn=store.db.info.dsn;store.close()
        with PgStore(conninfo=dsn) as restarted:
            restarted.certify_ready_full()
            generations=[];queries=[];old_generation=restarted.generation;execute=restarted.db.execute
            def generation(value=None):
                result=old_generation(value);generations.append(result['id']);return result
            def sql(query,params=None,**kw):
                if 'WHERE l.id<>%s' in str(query):queries.append(params)
                return execute(query,params,**kw)
            with patch.object(restarted,'generation',generation),patch.object(restarted.db,'execute',sql):
                value=relevance.qualified_parent(restarted,f.PROFILE)
                self.assertEqual(value['root']['generation'],2)
                self.assertEqual(value['origin'],'trusted-history')
                f.SQLFixture.follow(restarted,routes,f.T+3)
                verify_store(restarted)
            # Core integrity's constant checkpoint-origin lookup is independent
            # of relevance. Relevance ancestry SQL must stop at anchor2, never1/0.
            self.assertTrue(queries);self.assertTrue(all(params[1]>=2 for params in queries))
            with patch.object(restarted,'generation',generation):
                generations.clear();relevance._QUALIFIED.pop(restarted,None)
                self.assertEqual(relevance.qualified_parent(restarted,f.PROFILE)['root']['generation'],2)
                self.assertTrue(generations);self.assertGreaterEqual(min(generations),2)
            status=HealthState(service_version='0.3.0',schema_version=4,network='preprod',profile='synthetic')
            driver=SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),profile=f.PROFILE)
            driver._check_relevance(restarted,status,restarted.generation(),None)
            snapshot=status.snapshot();self.assertEqual(snapshot['relevance']['reason'],'trusted-history')
            self.assertNotIn('trusted-history',snapshot['reason_codes'])
            self.assertNotIn('internal_error',snapshot['reason_codes'])
            self.assertEqual(restarted.status()['relevance']['history'],'trusted-history')
            f.SQLFixture.follow(reference,full_routes,f.T+3,relevance_enabled=False)
            self.assertEqual(restarted.digests(3),reference.digests(2))
    def test_later_fullscan_ready_can_be_trusted_again_and_filtering_qualifies(self):
        from db_sync_alt.service.health import HealthState
        from db_sync_alt.sources.blockfrost.sync import SyncDriver
        from types import SimpleNamespace
        transaction=f.make_tx(620,relevant=False)
        rows=[[],[],[transaction]]
        store,routes=f.SQLFixture.prepare(rows,reuse_root=True);self.addCleanup(store.close)
        reference,full_routes=f.SQLFixture.prepare(rows,reuse_root=True);self.addCleanup(reference.close)
        f.SQLFixture.follow(store,routes,f.T+1,relevance_enabled=False)
        relevance.trust_history(store,f.PROFILE);first=copy.deepcopy(self.anchor(store,1)[0]['receipt'])
        f.SQLFixture.follow(store,routes,f.T+2,relevance_enabled=False)
        self.assertIsNone(relevance.qualified_parent(store,f.PROFILE))
        # Stored provenance is intentionally unchanged by a later full scan.
        self.assertEqual(store.status()['relevance']['history'],'trusted-history')
        self.assertEqual(relevance.trust_history(store,f.PROFILE),{'generation':2,'relevance':'trusted-history'})
        self.assertEqual(self.anchor(store,1)[0]['receipt'],first)
        self.assertEqual(self.anchor(store,2)[0]['receipt']['origin'],'trusted-history')
        with self.assertRaisesRegex(Refusal,'relevance_trust_already_initialized'):relevance.trust_history(store,f.PROFILE)
        self.assertEqual(relevance.qualified_parent(store,f.PROFILE)['root']['generation'],2)
        _,calls=f.SQLFixture.follow(store,routes,f.T+3)
        self.assertNotIn('/txs/'+transaction['tx_hash']+'/utxos',calls)
        self.assertEqual(store.generation()['manifest']['relevance_coverage']['origin'],'trusted-history')
        health=HealthState(service_version='0.3.0',schema_version=4,network='preprod',profile='synthetic')
        SyncDriver(SimpleNamespace(sync={'relevance_enabled':True}),profile=f.PROFILE)._check_relevance(store,health,store.generation(),None)
        self.assertEqual(health.snapshot()['relevance']['reason'],'trusted-history')
        self.assertEqual(store.status()['relevance']['history'],'trusted-history')
        f.SQLFixture.follow(reference,full_routes,f.T+3,relevance_enabled=False)
        self.assertEqual(store.digests(3),reference.digests(1));verify_store(store)
    def test_ready_anchor_is_validated_before_hot_cache_and_before_older_base(self):
        store,_=self.prepare();relevance.trust_history(store,f.PROFILE)
        original=store.generation;reads=[]
        def generation(value=None):
            result=original(value);reads.append(result['id']);return result
        with patch.object(store,'generation',generation):
            relevance.qualified_parent(store,f.PROFILE);relevance.qualified_parent(store,f.PROFILE)
        self.assertEqual(set(reads),{1})
        original_rows=store.checkpoint_rows
        def corrupt(**kw):
            rows=original_rows(**kw)
            for row in rows:
                if row['phase']=='relevance' and row['unit']=='anchor':row['receipt']['integrity_ref']['sha256']='00'*32
            return rows
        with patch.object(store,'checkpoint_rows',corrupt):
            with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):relevance.qualified_parent(store,f.PROFILE)
            with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):verify_store(store)
    def test_anchor_strict_generation_profile_integrity_kind_and_numeric_types(self):
        store,_=self.prepare();relevance.trust_history(store,f.PROFILE);g=store.generation();a=self.anchor(store)[0]['receipt']
        changes=({'version':True},{'version':1.0},{'generation':True},{'generation':1.0},{'generation':2},
                 {'profile':'00'*32},{'integrity_ref':{'kind':'parent-delta-v2','sha256':'00'*32}},
                 {'kind':'other'},{'origin':'certified'},{'unexpected':1})
        for change in changes:
            with self.subTest(change=change),self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
                relevance.validate_anchor(store,g,[{'receipt':{**copy.deepcopy(a),**change}}])
    def test_writer_pending_repeated_profile_refusals_do_not_install_or_change_guard(self):
        from db_sync_alt.cli import relevance_history
        from db_sync_alt.contract import Profile
        from db_sync_alt.resources import profile_dir
        store,routes=self.prepare();guard=self.guard(store)
        service=store.db.info.dbname
        # SQLFixture.database writes this exact service to its temporary PGSERVICEFILE.
        import os
        from pathlib import Path
        self.assertIn('['+service+']',Path(os.environ['PGSERVICEFILE']).read_text())
        with patch('sys.stderr') as errors:
            self.assertEqual(relevance_history.main(['--destination-service',service,'--network','preprod']),1)
            self.assertIn('writer_busy',''.join(str(call.args[0]) for call in errors.write.call_args_list))
        self.assertEqual(self.guard(store),guard);self.assertEqual(self.anchor(store),[])
        with self.assertRaisesRegex(Refusal,'relevance_parent_changed'):
            relevance.trust_history(store,Profile(profile_dir('preview')))
        self.assertEqual(self.guard(store),guard)
        with patch.object(store,'stage_metadata_reuse',side_effect=Refusal('interrupted')):
            with self.assertRaisesRegex(Refusal,'interrupted'):f.SQLFixture.follow(store,routes,f.T+2,relevance_enabled=False)
        with self.assertRaisesRegex(Refusal,'relevance_trust_pending'):relevance.trust_history(store,f.PROFILE)
        self.assertEqual(self.guard(store),guard);self.assertEqual(self.anchor(store),[])
        other,_=self.prepare();relevance.trust_history(other,f.PROFILE);guard=self.guard(other);rows=self.anchor(other)
        with self.assertRaisesRegex(Refusal,'relevance_trust_already_initialized'):relevance.trust_history(other,f.PROFILE)
        self.assertEqual(self.guard(other),guard);self.assertEqual(self.anchor(other),rows)
    def test_hidden_command_success_with_stopped_writer_and_reopen_filtering(self):
        import io
        import json
        from db_sync_alt import cli
        store,routes=self.prepare();dsn=store.db.info.dsn;service=store.db.info.dbname;store.close()
        output=io.StringIO()
        with patch('sys.stdout',output):
            self.assertEqual(cli.main(['_trust-relevance-history','--destination-service',service,'--network','preprod']),0)
        self.assertEqual(json.loads(output.getvalue()),{'generation':1,'relevance':'trusted-history'})
        with PgStore(conninfo=dsn) as reopened:
            reopened.certify_ready_full()
            self.assertEqual(relevance.qualified_parent(reopened,f.PROFILE)['origin'],'trusted-history')
            f.SQLFixture.follow(reopened,routes,f.T+2);verify_store(reopened)
            self.assertEqual(reopened.generation()['manifest']['relevance_coverage']['origin'],'trusted-history')
    def test_guard_installation_and_anchor_insert_roll_back_on_sql_failure(self):
        import psycopg
        from psycopg.types.json import Jsonb
        store,_=self.prepare();before=self.guard(store);execute=store.db.execute
        def fail(query,params=None,**kw):
            if str(query).startswith("INSERT INTO chain.checkpoints VALUES(%s,'relevance','anchor'"):
                return execute(query,(999999,Jsonb({})),**kw)
            return execute(query,params,**kw)
        with patch.object(store.db,'execute',fail),self.assertRaisesRegex(psycopg.errors.CheckViolation,'immutable_relevance_anchor'):
            relevance.trust_history(store,f.PROFILE)
        self.assertEqual(self.guard(store),before);self.assertEqual(self.anchor(store),[])
        relevance.trust_history(store,f.PROFILE)
        self.assertNotEqual(self.guard(store),before)
    def test_atomic_ready_identity_recheck_rolls_back_before_guard_installation(self):
        import contextlib
        store,_=self.prepare();before=self.guard(store);atomic=store.atomic;generation=store.generation;inside=False
        @contextlib.contextmanager
        def changed():
            nonlocal inside
            with atomic():
                inside=True
                try:yield
                finally:inside=False
        def identity(value=None):
            g=generation(value)
            if inside:g['target_height']+=1
            return g
        with patch.object(store,'atomic',changed),patch.object(store,'generation',identity),self.assertRaisesRegex(Refusal,'relevance_parent_changed'):
            relevance.trust_history(store,f.PROFILE)
        self.assertEqual(self.guard(store),before);self.assertEqual(self.anchor(store),[])
    def test_anchor_guard_keeps_unrelated_ready_dml_and_update_delete_refused(self):
        import psycopg
        from psycopg.types.json import Jsonb
        store,_=self.prepare();relevance.trust_history(store,f.PROFILE);a=self.anchor(store)[0]['receipt'];before=self.guard(store)
        queries=[("INSERT INTO chain.checkpoints VALUES(%s,'relevance','anchor',%s)",(1,Jsonb(a))),
                 ("UPDATE chain.checkpoints SET receipt=receipt WHERE generation=1 AND phase='relevance' AND unit='anchor'",None),
                 ("DELETE FROM chain.checkpoints WHERE generation=1 AND phase='relevance' AND unit='anchor'",None),
                 ("UPDATE chain.blocks SET height=height WHERE first_generation=0",None),
                 ("INSERT INTO chain.checkpoints VALUES(1,'other','ready','{}')",None)]
        for query,params in queries:
            with self.subTest(query=query),self.assertRaises(psycopg.errors.CheckViolation),store.atomic():store.db.execute(query,params)
        self.assertEqual(self.guard(store),before);self.assertEqual(self.anchor(store)[0]['receipt'],a)
        verify_store(store)
    def test_schema4_trust_status_preserves_summary_only_role_and_no_manifest_loads(self):
        import psycopg
        import uuid
        from psycopg import sql
        from db_sync_alt.store.database import GENERATION_SUMMARY
        store,_=self.prepare();relevance.trust_history(store,f.PROFILE)
        role='anchor_status_'+uuid.uuid4().hex[:10]
        f.SQLFixture.admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        try:
            store.db.execute(sql.SQL('GRANT USAGE ON SCHEMA chain TO {}').format(sql.Identifier(role)))
            store.db.execute(sql.SQL('GRANT SELECT ON chain.service_state,chain.metadata,chain.checkpoints TO {}').format(sql.Identifier(role)))
            store.db.execute(sql.SQL('GRANT SELECT({}) ON chain.generations TO {}').format(sql.SQL(',').join(map(sql.Identifier,GENERATION_SUMMARY)),sql.Identifier(role)))
            dsn=psycopg.conninfo.make_conninfo(store.db.info.dsn,user=role)
            with PgStore(conninfo=dsn,readonly=True) as status:
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):status.db.execute('SELECT manifest FROM chain.generations')
                with patch.object(status,'generation',side_effect=AssertionError('status loaded a full manifest')):
                    result=status.status()
                self.assertEqual(result['relevance']['history'],'trusted-history')
                self.assertTrue(all('manifest' not in row for row in result['generations']))
        finally:
            store.db.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            f.SQLFixture.admin.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))
    def test_pending_older_same_origin_anchor_substitution_refuses_without_repair(self):
        from db_sync_alt.sources.blockfrost.follow import update_follow
        for missing in (False,True):
            with self.subTest(missing=missing):
                store,routes=self.prepare();relevance.trust_history(store,f.PROFILE)
                first=self.anchor(store,1)[0]['receipt']
                f.SQLFixture.follow(store,routes,f.T+2);store.write_checkpoint(store.certify_checkpoint())
                expected=relevance.certificate(store,f.PROFILE,f.T+3)
                self.assertEqual(expected['root']['generation'],2)
                forged={**copy.deepcopy(expected),'root':{'generation':1,'anchor_sha256':relevance.digest(first)}}
                self.assertEqual(forged['origin'],expected['origin'])
                checkpoint=store.checkpoint
                def receipt(generation,phase,unit,value):
                    if not (missing and phase=='relevance' and unit=='coverage'):checkpoint(generation,phase,unit,value)
                # A creator captured a different valid ancestor. Persist both
                # forms through the real pending APIs; do not rewrite a ready row.
                with patch.object(relevance,'certificate',return_value=forged),patch.object(store,'checkpoint',receipt),patch.object(store,'stage_metadata_reuse',side_effect=Refusal('interrupted')):
                    with self.assertRaisesRegex(Refusal,'interrupted'):f.SQLFixture.follow(store,routes,f.T+3)
                pending=copy.deepcopy(store.pending());rows=store.checkpoint_rows(generation=3,phase='relevance',unit='coverage');facts=store.digests(2)
                marker={'job_id':'anchor-negative','attempts':7,'budget':100,'state':'active'};store.save_sync_job('anchor-budget-negative',marker)
                client=Fake(routes)
                with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
                    update_follow(store,f.Provider(50000,client),f.PROFILE,f.T+3,hooks=f.FixtureHooks(),relevance_enabled=True)
                self.assertEqual(client.calls,[]);self.assertEqual(store.pending(),pending)
                self.assertEqual(store.checkpoint_rows(generation=3,phase='relevance',unit='coverage'),rows)
                self.assertEqual(store.digests(2),facts);self.assertEqual(store.sync_job('anchor-budget-negative'),marker)
    def test_pending_keeps_legacy_certificate_across_integrity_checkpoint(self):
        store,routes=f.SQLFixture.prepare([[],[]],reuse_root=True);self.addCleanup(store.close)
        f.SQLFixture.follow(store,routes,f.T+1)
        with patch.object(store,'stage_metadata_reuse',side_effect=Refusal('interrupted')):
            with self.assertRaisesRegex(Refusal,'interrupted'):f.SQLFixture.follow(store,routes,f.T+2)
        pending=copy.deepcopy(store.pending()['manifest']['relevance_coverage'])
        store.write_checkpoint(store.certify_checkpoint())
        self.assertEqual(self.anchor(store),[])
        self.assertEqual(relevance.pending_certificate(store,f.PROFILE,2),pending)
        f.SQLFixture.follow(store,routes,f.T+2);verify_store(store)


class TrustedLegacyHistoryTests(CheckpointFixture):
    def test_existing_supported_fullscan_without_completion_seal_can_be_trusted(self):
        _,store=self.history(0)
        manifest=copy.deepcopy(store.generation()['manifest'])
        self.assertNotIn('completion',manifest)
        before=verify_store(store)
        self.assertEqual(relevance.trust_history(store,self.profile),{'generation':0,'relevance':'trusted-history'})
        self.assertEqual(store.generation()['manifest'],manifest)
        self.assertEqual(verify_store(store),before)
        self.assertEqual(relevance.qualified_parent(store,self.profile)['origin'],'trusted-history')
        self.assertEqual(store.status()['relevance']['history'],'trusted-history')
