"""Profile-form, checkpoint and bounded lineage-cache behavior without a database.

Completion validators are spies here. Genuine SQL-root/fact equivalence and
checkpoint certification are covered separately by PostgreSQL regressions.
"""
import copy
import datetime as dt
import unittest
from unittest.mock import patch

from db_sync_alt.contract import TABLES,Profile,Refusal,digest
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost import relevance
from db_sync_alt.store.incremental import integrity_reference
from db_sync_alt.verify.completion import SQL_ADAPTER,SQL_VALIDATION


def h(value):return f'{value:064x}'


class Capability:
    def __init__(self,store):
        g=store.generation()
        self._descriptor={'generation':g['id'],'manifest':copy.deepcopy(g['manifest']),
                          'profile':copy.deepcopy(store.profile.identity),
                          'integrity_ref':integrity_reference(g)}
    @property
    def descriptor(self):return copy.deepcopy(self._descriptor)


class LineageStore:
    def __init__(self,profile):
        self.profile=profile;self.rev=0;self.revoked=False;self.supports_checkpoints=True
        self.base=0;self.checkpoint=None;self.coverage_reads=[];self.ready=0
        facts={name:{'count':0,'sha256':h(0)} for name in TABLES}
        self.generations={0:{'id':0,'parent':None,'status':'ready',
            'source':'cardano-db-sync-postgresql','target_height':profile.start[0],
            'manifest':{'identity':copy.deepcopy(profile.identity),'digests':facts,
                        'source_identity':{'adapter':SQL_ADAPTER},
                        'validation':{'kind':SQL_VALIDATION},'completion':{'sha256':h(1)}}}}
        self.cap=Capability(self)
    def generation(self,generation=None):return copy.deepcopy(self.generations[self.ready if generation is None else generation])
    def current_verified_parent(self):
        if self.revoked:raise Refusal('incremental_parent_uncertified')
        return self.cap
    def checkpoint_rows(self,*,generation,phase,unit):
        self.coverage_reads.append((generation,phase,unit))
        cert=self.generations[generation]['manifest'].get('relevance_coverage')
        return [] if cert is None else [{'receipt':copy.deepcopy(cert)}]
    def append(self,kind):
        parent=self.generations[self.ready];number=self.ready+1
        manifest={'integrity':{'version':2,'kind':kind,'root':h(number+10)},
                  'completion':{'sha256':h(number+20)}}
        if kind=='certified-parent-delta-v1':manifest['identity']=copy.deepcopy(self.profile.identity)
        else:manifest['identity_sha256']=digest(self.profile.identity)
        manifest['relevance_coverage']={'kind':relevance.KIND,'version':1,
            'root':{'generation':0,'completion':h(1)},'profile':digest(self.profile.identity),
            'parent_ref':integrity_reference(parent),
            'range':[parent['target_height']+1,parent['target_height']+1]}
        self.generations[number]={'id':number,'parent':self.ready,'status':'ready',
            'source':'blockfrost-cardano','target_height':parent['target_height']+1,
            'manifest':manifest}
        self.ready=number;self.rev+=1;self.cap=Capability(self)
    def checkpoint_here(self):
        self.base=self.ready
        self.checkpoint={'generation':self.ready,'previous_base':0,
            'root':integrity_reference(self.generations[self.ready])['sha256'],
            'digests':copy.deepcopy(self.generations[0]['manifest']['digests']),
            'service_version':'0.2.0','checkpointed_at':dt.datetime(2026,1,1,tzinfo=dt.timezone.utc)}


class RelevanceLineageTests(unittest.TestCase):
    def setUp(self):
        relevance._QUALIFIED.clear();self.profile=Profile(profile_dir('preprod'))
        self.sql=patch('db_sync_alt.verify.completion.validate_completion',side_effect=lambda store,g,**kw:g['manifest']['completion']).start()
        self.delta=patch('db_sync_alt.store.incremental.validate_completion_v2',side_effect=lambda store,g,**kw:g['manifest']['completion']).start()
        patch('db_sync_alt.store.incremental.revision',side_effect=lambda store:store.rev).start()
        patch('db_sync_alt.store.incremental.integrity_base',side_effect=lambda store:(store.base,copy.deepcopy(store.checkpoint))).start()
        self.addCleanup(patch.stopall);self.addCleanup(relevance._QUALIFIED.clear)
    def store(self,count=0):
        store=LineageStore(self.profile)
        for n in range(count):store.append('certified-parent-delta-v1' if n%2==0 else 'certified-parent-delta-v2')
        return store
    def test_fresh_checkpoint_parent_supports_mixed_profile_forms(self):
        store=self.store(4);store.checkpoint_here()
        value=relevance.qualified_parent(store,self.profile)
        self.assertEqual(value['root'],{'generation':0,'completion':h(1)})
        self.assertEqual(value['profile'],digest(self.profile.identity))
        self.assertEqual(self.delta.call_count,4);self.assertEqual(self.sql.call_count,1)
    def test_checkpoint_never_creates_missing_relevance_coverage(self):
        store=self.store(2);store.generations[1]['manifest'].pop('relevance_coverage');store.checkpoint_here()
        self.assertIsNone(relevance.qualified_parent(store,self.profile))
    def test_checkpoint_change_invalidates_hot_certification_without_blanket_refusal(self):
        store=self.store(3);relevance.qualified_parent(store,self.profile);reads=len(store.coverage_reads)
        relevance.qualified_parent(store,self.profile);self.assertEqual(len(store.coverage_reads),reads)
        store.checkpoint_here()
        self.assertIsNotNone(relevance.qualified_parent(store,self.profile))
        self.assertEqual(len(store.coverage_reads)-reads,3)
    def test_long_lineage_has_one_cold_walk_then_constant_new_prefix_work(self):
        store=self.store(256);relevance.qualified_parent(store,self.profile)
        self.assertEqual(len(store.coverage_reads),256);self.assertEqual(self.sql.call_count,1)
        for _ in range(20):self.assertIsNotNone(relevance.qualified_parent(store,self.profile))
        self.assertEqual(len(store.coverage_reads),256)
        store.append('certified-parent-delta-v2')
        self.assertIsNotNone(relevance.qualified_parent(store,self.profile))
        self.assertEqual(len(store.coverage_reads),257);self.assertEqual(self.sql.call_count,1)
        self.assertEqual(len(relevance._QUALIFIED),1)
    def test_revision_session_and_revocation_are_not_hidden_by_cache(self):
        store=self.store(2);relevance.qualified_parent(store,self.profile);reads=len(store.coverage_reads)
        store.rev+=1;self.assertIsNotNone(relevance.qualified_parent(store,self.profile))
        self.assertEqual(len(store.coverage_reads)-reads,2)
        reads=len(store.coverage_reads);store.cap=Capability(store)
        self.assertIsNotNone(relevance.qualified_parent(store,self.profile))
        self.assertEqual(len(store.coverage_reads)-reads,2)
        store.revoked=True;self.assertIsNone(relevance.qualified_parent(store,self.profile))
    def test_historical_profile_digest_and_conflicting_root_refuse(self):
        for mutation in ('profile','root'):
            store=self.store(3)
            if mutation=='profile':store.generations[2]['manifest']['identity_sha256']=h(99)
            else:store.generations[2]['manifest']['relevance_coverage']['root']['completion']=h(99)
            with self.subTest(mutation=mutation),self.assertRaises(Refusal):
                relevance.qualified_parent(store,self.profile)
    def test_caller_cannot_mutate_cached_certificate(self):
        store=self.store(2);value=relevance.qualified_parent(store,self.profile)
        value['root']['completion']=h(99)
        self.assertEqual(relevance.qualified_parent(store,self.profile)['root']['completion'],h(1))
    def test_wrong_pending_profile_refuses_before_checkpoint_repair(self):
        store=self.store(1);pending=store.generation();pending['status']='pending'
        pending['manifest']['identity_sha256']=h(99);pending['manifest'].pop('identity',None)
        with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
            relevance.requested_coverage(store,self.profile,pending['target_height'],pending,True)
        self.assertEqual(store.coverage_reads,[])

    def test_ready_coverage_refuses_numeric_coercion_at_preview_height_one(self):
        mutations=(('version',True),('version',1.0),('generation',False),('generation',0.0),
                   ('lo',True),('lo',1.0),('hi',True),('hi',1.0))
        for field,value in mutations:
            store=LineageStore(Profile(profile_dir('preview')));store.append('certified-parent-delta-v2')
            cert=store.generations[1]['manifest']['relevance_coverage']
            if field=='generation':cert['root'][field]=value
            elif field in ('lo','hi'):cert['range'][0 if field=='lo' else 1]=value
            else:cert[field]=value
            with self.subTest(field=field,value=value):
                # The changed manifest first invalidates the captured parent capability.
                with self.assertRaisesRegex(Refusal,'relevance_parent_changed'):
                    relevance.qualified_parent(store,store.profile)
                # Refresh the fixture capability to reach independent certificate validation.
                store.cap=Capability(store)
                with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
                    relevance.qualified_parent(store,store.profile)

    def pending_at_one(self):
        store=LineageStore(Profile(profile_dir('preview')));store.append('certified-parent-delta-v2')
        pending=store.generations[1];pending['status']='pending';store.ready=0;store.cap=Capability(store)
        return store,pending

    def test_pending_numeric_coercion_never_repairs_missing_row(self):
        for field,value in (('version',True),('version',1.0),('generation',False),('generation',0.0),
                            ('lo',True),('lo',1.0),('hi',True),('hi',1.0)):
            store,pending=self.pending_at_one();cert=pending['manifest']['relevance_coverage']
            if field=='generation':cert['root'][field]=value
            elif field in ('lo','hi'):cert['range'][0 if field=='lo' else 1]=value
            else:cert[field]=value
            with self.subTest(field=field,value=value),patch.object(store,'checkpoint_rows',return_value=[]),patch.object(store,'checkpoint',create=True) as write:
                with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
                    relevance.requested_coverage(store,store.profile,1,copy.deepcopy(pending),True)
                write.assert_not_called()

    def test_pending_receipt_numeric_coercion_cannot_equal_valid_manifest(self):
        store,pending=self.pending_at_one();forged=copy.deepcopy(pending['manifest']['relevance_coverage']);forged['range'][0]=True
        with patch.object(store,'checkpoint_rows',return_value=[{'receipt':forged}]),patch.object(store,'checkpoint',create=True) as write:
            with self.assertRaisesRegex(Refusal,'relevance_coverage_invalid'):
                relevance.requested_coverage(store,store.profile,1,copy.deepcopy(pending),True)
            write.assert_not_called()
