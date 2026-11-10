"""Real-store completion/restart controls; deterministic provider, no live calls."""
import contextlib
import copy
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from psycopg.types.json import Jsonb
import fixtures.database as integration
from fixtures.database import AccountedFake
import fixtures.blockfrost_generation as gen_fixture
from db_sync_alt.sources.blockfrost.adapter import update
from db_sync_alt.verify.completion import validate_completion, MAX_RECEIPT_BYTES
from db_sync_alt.contract import Refusal, digest, jsonable
from db_sync_alt.store.database import PgStore
from db_sync_alt.verify.store import verify_store
from db_sync_alt.sources.blockfrost.sync import SyncDriver
from db_sync_alt.errors import LifecycleFailure

class StopAtBoundary(Exception): pass

class CompletionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):integration.SyncFixture.setUpClass()
    @classmethod
    def tearDownClass(cls):integration.SyncFixture.tearDownClass()
    def setUp(self):
        self.fixture=integration.SyncFixture();self.fixture.setUp()
        self.dsn=self.fixture.make(transition=True)
        self.routes,_=gen_fixture.fixtures(transition=True);self.now=1700000000.;self.calls=[]
    def tearDown(self):self.fixture.tearDown()
    def owner(self):return PgStore(conninfo=self.dsn)
    def verify(self,store,gen=None,**kw):return verify_store(store,gen,**kw)
    def state(self,store):
        pending=store.pending();gen=pending or store.generation()
        return jsonable({'ready':store.meta('canonical_ready'),'generation':gen,'digests':store.digests(gen['id']),
            'checkpoints':store.checkpoint_rows(generation=gen['id']),
            'job':store.sync_job('provider-active'),'service':store.state()})
    def manifest(self,store,generation,value):
        store.db.execute('UPDATE chain.generations SET manifest=%s WHERE id=%s',(Jsonb(value),generation))
    def receipt(self,store,generation,phase,unit,value):
        store.db.execute('UPDATE chain.checkpoints SET receipt=%s WHERE generation=%s AND phase=%s AND unit=%s',(Jsonb(value),generation,phase,unit))
    def invoke(self,store,boundary=None,routes=None):
        client=AccountedFake(routes if routes is not None else self.routes);self.calls.append(client.calls)
        def actual(*args,**kwargs):
            hooks=kwargs['hooks'];old_atomic=store.atomic
            def stop():raise StopAtBoundary(boundary)
            if boundary=='after_seal':hooks.before_publish=stop
            if boundary=='publication_rollback':
                @contextlib.contextmanager
                def fail_commit():
                    with old_atomic():
                        yield
                        raise StopAtBoundary(boundary)
                hooks.before_publish=lambda:setattr(store,'atomic',fail_commit)
            try:
                with contextlib.ExitStack() as stack:
                    if boundary=='before_seal_commit':
                        original=store.checkpoint
                        def checkpoint(g,p,u,r):
                            original(g,p,u,r)
                            if p=='final-anchors':raise StopAtBoundary(boundary)
                        stack.enter_context(patch.object(store,'checkpoint',side_effect=checkpoint))
                    result=update(*args,**kwargs)
                if boundary=='after_publish':stop()
                return result
            finally:store.atomic=old_atomic
        self.now+=86400
        SyncDriver(SimpleNamespace(sync={}),client=client,profile=self.fixture.profile,clock=lambda:self.now,update_fn=actual).run_once(store,self.fixture.state(),threading.Event())
    def stop(self,store,boundary='after_seal'):
        with self.assertRaises(StopAtBoundary):self.invoke(store,boundary)
    def assert_refusal(self,store,code,*,early=True):
        before=self.state(store);calls=len(self.calls)
        with self.assertRaisesRegex((Refusal,LifecycleFailure),code):self.invoke(store)
        after=self.state(store)
        if early:self.assertEqual(after,before);self.assertEqual(self.calls[calls],[])
        else:
            job=after.pop('job');old=before.pop('job');self.assertEqual(after,before)
            if job:self.assertEqual(job['job_id'],old['job_id']);self.assertEqual(job['budget'],old['budget']);self.assertGreater(job['attempts'],old['attempts'])
        before=self.state(store)
        with self.assertRaises(Refusal):store.publish(1,store.generation(1)['manifest'])
        self.assertEqual(self.state(store),before)

    def test_inherited_anchor_provenance_survives_actual_store_completion(self):
        with self.owner() as store:
            parent=store.generation(0)['manifest'];parent['anchors']['start'].update(epoch=2,source_id=4500199,tx_count=5)
            self.manifest(store,0,parent);expected=copy.deepcopy(parent['anchors']['start'])
            self.stop(store);m=store.pending()['manifest']
            self.assertEqual(m['anchors']['start'],expected);self.assertEqual(m['completion']['semantic']['anchors']['start'],expected)
        with self.owner() as store:
            self.invoke(store);self.assertEqual(store.generation()['manifest']['anchors']['start'],expected);self.verify(store)

    def test_sealed_revalidation_twice_and_one_publication(self):
        with self.owner() as store:
            old=self.verify(store)['digests'];self.stop(store);saved=self.state(store)
            original=store.checkpoint_rows(generation=1,phase='final-anchors')[0]
            seal=store.generation(1)['manifest'];validate_completion(store,store.pending(),store.digests(1),require_seal=True)
        previous=seal['completion']['original_receipt_sha256']
        for count in (1,2):
            with self.owner() as store:
                self.stop(store);self.assertEqual(store.generation(1)['manifest'],seal)
                self.assertEqual(store.checkpoint_rows(generation=1,phase='final-anchors'),[original])
                rows=store.checkpoint_rows(generation=1,phase='completion-revalidation');self.assertEqual(len(rows),1)
                self.assertEqual(rows[0]['receipt']['count'],count);self.assertEqual(rows[0]['receipt']['previous'],previous);previous=rows[0]['receipt']['sha256']
                self.assertEqual(store.digests(0),old)
                job=store.sync_job('provider-active');self.assertEqual(job['job_id'],saved['job']['job_id']);self.assertEqual(job['budget'],saved['job']['budget']);self.assertGreater(job['attempts'],saved['job']['attempts'])
        with self.owner() as store:
            self.invoke(store);self.assertEqual(store.meta('canonical_ready'),1);self.assertIsNone(store.pending())
            final=self.verify(store)['digests'];self.assertEqual(store.generation(1)['manifest'],seal)
            self.assertEqual(store.checkpoint_rows(generation=1,phase='final-anchors'),[original])
            self.assertEqual(store.state()['publication_serial'],saved['service']['publication_serial']+1)
            self.assertEqual(final,self.fixture.reference(self.routes,transition=True)['digests']);self.assertEqual(len(final),11)

    def test_first_seal_and_original_receipt_roll_back_together(self):
        with self.owner() as store:
            self.stop(store,'before_seal_commit')
            self.assertNotIn('completion',store.pending()['manifest']);self.assertNotIn('digests',store.pending()['manifest'])
            self.assertFalse(store.checkpoint_rows(generation=1,phase='final-anchors'));self.assertEqual(store.meta('canonical_ready'),0)
            old=store.digests(1)
            self.assertEqual(old['inputs']['count'],2)
            candidate=copy.deepcopy(store.pending()['manifest']);candidate.update(digests=copy.deepcopy(old),validation={'kind':'provider-enumeration'})
            candidate['digests']['inputs']['count']=True
            before=self.state(store)
            with self.assertRaisesRegex(Refusal,'completion_mismatch'):store.finalize_pending(1,candidate,{},old)
            self.assertEqual(self.state(store),before)
        with self.owner() as store:self.invoke(store);self.assertEqual(store.digests(1),old);self.assertEqual(store.meta('canonical_ready'),1)

    def test_publication_transaction_rolls_back_sealed_state(self):
        with self.owner() as store:
            serial=store.state()['publication_serial']
            self.stop(store,'publication_rollback');saved=self.state(store)
            self.assertEqual(store.meta('canonical_ready'),0);self.assertEqual(store.pending()['status'],'pending')
            validate_completion(store,store.pending(),store.digests(1),require_seal=True)
            self.assertEqual(store.state()['publication_serial'],serial)
        with self.owner() as store:
            self.assertEqual(self.state(store),saved);self.invoke(store);self.assertEqual(store.meta('canonical_ready'),1)

    def test_after_commit_driver_recovers_bookkeeping_without_update(self):
        with self.owner() as store:
            self.stop(store,'after_publish');self.assertEqual(store.meta('canonical_ready'),1)
            saved=self.state(store);self.assertEqual(saved['job']['state'],'active')
        for _ in range(2):
            with self.owner() as store:
                client=AccountedFake(self.routes)
                def forbidden(*a,**kw):raise AssertionError('completed target must not update')
                SyncDriver(SimpleNamespace(sync={}),client=client,profile=self.fixture.profile,clock=lambda:self.now,update_fn=forbidden).run_once(store,self.fixture.state(),threading.Event())
                after=self.state(store);self.assertEqual(after['job']['state'],'completed')
                self.assertEqual(after['job']['attempts'],saved['job']['attempts']);self.assertEqual(after['job']['job_id'],saved['job']['job_id'])
                self.assertEqual(after['service'],saved['service']);self.assertEqual(after['generation'],saved['generation']);self.assertEqual(after['checkpoints'],saved['checkpoints'])

    def test_malformed_seals_and_parent_identity_refuse_without_mutation(self):
        with self.owner() as store:
            self.stop(store);original=copy.deepcopy(store.pending()['manifest'])
            variants=[]
            for version in (2,True,None):
                m=copy.deepcopy(original);m['completion']['version']=version;variants.append((m,'completion_invalid'))
            m=copy.deepcopy(original);m['completion']['sha256']='0'*64;variants.append((m,'completion_invalid'))
            m=copy.deepcopy(original);m['completion']['semantic']['generation']=2;m['completion']['sha256']=digest(m['completion']['semantic']);variants.append((m,'completion_mismatch'))
            m=copy.deepcopy(original);m['parent_digest']='0'*64;variants.append((m,'completion_mismatch'))
            m=copy.deepcopy(original);m['anchors']['target']['hash']='0'*64;variants.append((m,'completion_mismatch'))
            for m,code in variants:
                self.manifest(store,1,m);self.assert_refusal(store,code);self.manifest(store,1,original)
            original_receipt=store.checkpoint_rows(generation=1,phase='final-anchors')[0]['receipt']
            self.receipt(store,1,'final-anchors','complete',{'changed':True});self.assert_refusal(store,'completion_invalid')
            self.receipt(store,1,'final-anchors','complete',original_receipt)
            parent=store.generation(0)['manifest'];wrong=copy.deepcopy(parent);wrong['digests']['mints']['count']+=1
            self.manifest(store,0,wrong);self.assert_refusal(store,'completion_mismatch');self.manifest(store,0,parent)

    def test_changed_pending_facts_retain_charged_attempts_and_expected_seal(self):
        with self.owner() as store:
            self.stop(store)
            store.db.execute('UPDATE chain.mints SET quantity=quantity+1 WHERE first_generation=1')
            self.assert_refusal(store,'completion_mismatch',early=False)

    def test_old_unsealed_pending_a1_a2_normal_and_direct_refuse(self):
        with self.owner() as store:
            initial={}
            def old_finalize(g,manifest,receipt,computed):
                initial['manifest']=copy.deepcopy(store.generation(g)['manifest']);initial['final']=copy.deepcopy(manifest)
                with store.atomic():store.checkpoint(g,'final-anchors','complete',receipt)
                return manifest
            with patch.object(store,'finalize_pending',side_effect=old_finalize):self.stop(store)
            self.assertEqual(store.pending()['manifest'],initial['manifest'])
            self.assert_refusal(store,'completion_recovery_required')
            self.manifest(store,1,initial['final']);self.assert_refusal(store,'completion_recovery_required')

    def test_revalidation_bound_and_corruption_refuse_atomically(self):
        with self.owner() as store:
            self.stop(store);saved=self.state(store);g=store.pending()
            with self.assertRaisesRegex(Refusal,'completion_invalid'):
                store.finalize_pending(1,g['manifest'],{'oversized':'x'*MAX_RECEIPT_BYTES},store.digests(1))
            self.assertEqual(self.state(store),saved)
            self.stop(store);record=store.checkpoint_rows(generation=1,phase='completion-revalidation')[0]['receipt']
            for key,value in [('count',True),('count',0),('sha256','0'*64),('previous','0'*64)]:
                bad=copy.deepcopy(record);bad[key]=value;self.receipt(store,1,'completion-revalidation','latest',bad)
                self.assert_refusal(store,'completion_invalid');self.receipt(store,1,'completion-revalidation','latest',record)
            bad=copy.deepcopy(record);bad['count']=2**63-1;bad['sha256']=digest({k:v for k,v in bad.items() if k!='sha256'});self.receipt(store,1,'completion-revalidation','latest',bad)
            before=self.state(store)
            with self.assertRaisesRegex(Refusal,'completion_invalid'):store.finalize_pending(1,store.pending()['manifest'],{},store.digests(1))
            self.assertEqual(self.state(store),before)

    def test_seal_without_original_receipt_and_original_without_seal_refuse(self):
        with self.owner() as store:
            self.stop(store);manifest=store.pending()['manifest'];original=store.checkpoint_rows(generation=1,phase='final-anchors')[0]['receipt']
            # Deliberate corruption on a disposable fixture; never recovery logic.
            store.db.execute("DELETE FROM chain.checkpoints WHERE generation=1 AND phase='final-anchors'")
            self.assert_refusal(store,'completion_invalid')
            store.checkpoint(1,'final-anchors','complete',original)
            altered=copy.deepcopy(manifest);del altered['completion'];self.manifest(store,1,altered)
            self.assert_refusal(store,'completion_recovery_required')

    def test_direct_publish_cannot_replace_sealed_expected_manifest(self):
        with self.owner() as store:
            self.stop(store);before=self.state(store);m=copy.deepcopy(store.pending()['manifest']);m['digests']['mints']['count']+=1
            with self.assertRaisesRegex(Refusal,'completion_mismatch'):store.publish(1,m)
            self.assertEqual(self.state(store),before)
            with self.assertRaises(Exception):
                with store.atomic():store.checkpoint(1,'final-anchors','complete',{})
            self.assertEqual(self.state(store),before)

    def test_ready_unsealed_history_remains_compatible(self):
        with self.owner() as store:
            self.assertNotIn('completion',store.generation()['manifest']);self.verify(store)
            self.invoke(store);m=store.generation()['manifest'];old=copy.deepcopy(m);del old['completion']
            self.manifest(store,1,old);before=self.state(store);self.verify(store)
            self.assertEqual(self.state(store),before)

    def test_changed_pending_anchor_refuses_without_revalidation_write(self):
        with self.owner() as store:
            self.stop(store);before=self.state(store);routes=copy.deepcopy(self.routes)
            routes['/blocks/'+str(gen_fixture.OFFSET+12)]['hash']='f'*64
            with self.assertRaises((Refusal,LifecycleFailure)):self.invoke(store,routes=routes)
            after=self.state(store);self.assertEqual(after['generation'],before['generation']);self.assertEqual(after['checkpoints'],before['checkpoints'])
            self.assertEqual(after['digests'],before['digests']);self.assertEqual(after['ready'],0)

    def test_committed_anchor_conflict_revokes_durably(self):
        with self.owner() as store:
            self.stop(store);m=copy.deepcopy(store.pending()['manifest']);routes=copy.deepcopy(self.routes)
            routes['/blocks/'+str(gen_fixture.OFFSET+10)]['hash']='f'*64
            with self.assertRaises((Refusal,LifecycleFailure)):self.invoke(store,routes=routes)
            self.assertTrue(store.meta('source_conflict'));self.assertIsNone(store.meta('canonical_ready'));self.assertEqual(store.generation(1)['manifest'],m)
        with self.owner() as store:self.assertTrue(store.meta('source_conflict'));self.assertIsNone(store.meta('canonical_ready'))


if __name__=='__main__':unittest.main(verbosity=2)
