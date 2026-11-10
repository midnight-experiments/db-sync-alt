"""The full Blockfrost update (`adapter.update`) on a PostgreSQL store holding the synthetic generation 0."""
import unittest
from db_sync_alt.contract import Refusal
from fixtures.blockfrost import FixtureHooks
from fixtures.blockfrost_generation import CLEAN_AUXILIARY, HOSTILE_AUXILIARY, OFFSET, advance, baseline, block, fixtures
from fixtures.database import SyncFixture


class GenerationTests(SyncFixture):
    def store(self,transition=False):
        s=baseline(self.database(),transition=transition);self.addCleanup(s.close);return s
    def advance(self,s,routes,budget=200,hooks=None):return advance(s,routes,budget,hooks)
    def test_real_store_update_input_only_mint_only_preserves_g0(self):
        s=self.store()
        s.set_meta('archive_sealed',True)  # Unrelated metadata survives the update unchanged.
        before=s.generation(0);g0=s.digests(0);routes,_=fixtures();result=self.advance(s,routes)
        self.assertIs(s.meta('archive_sealed'),True)
        self.assertEqual(result['generation'],1);self.assertEqual(s.generation(0),before);self.assertEqual(s.digests(0),g0)
        self.assertEqual(result['digests']['inputs']['count'],2);self.assertEqual(result['digests']['mints']['count'],1)
        self.assertEqual(s.generation()['source'],'blockfrost-cardano');self.assertEqual(s.generation(0)['source'],'cardano-db-sync-postgresql')
    def test_quota_pending_resume_equal_fresh(self):
        s=self.store();fresh=self.store()
        routes,_=fixtures();old=s.digests(0)
        with self.assertRaisesRegex(Refusal,'budget'):self.advance(s,routes,15)
        self.assertEqual(s.meta('canonical_ready'),0);self.assertIsNotNone(s.pending());self.assertEqual(s.digests(0),old)
        resumed=self.advance(s,routes);expected=self.advance(fresh,routes);self.assertEqual(resumed['digests'],expected['digests'])
    def test_resumed_event_progress_starts_at_durable_checkpoint(self):
        s=self.store()
        routes,_=fixtures();broken=dict(routes)
        del broken['/blocks/'+block(12)['hash']+'/txs/cbor?count=100&page=1&order=asc']
        with self.assertRaisesRegex(Refusal,'provider_request_failed_counted'):self.advance(s,broken)
        self.assertTrue(s.completed(1,'events',OFFSET+11));self.assertFalse(s.completed(1,'events',OFFSET+12))
        hooks=FixtureHooks();self.advance(s,routes,hooks=hooks)
        starts=[fields for fields in hooks.progress if fields.get('phase')=='events' and 'committed' in fields]
        self.assertEqual(starts[0]['committed'],OFFSET+11)
    def test_uncommitted_tail_replacement(self):
        s=self.store()
        before=s.digests(0);routes,_=fixtures(fork=1);result=self.advance(s,routes)
        self.assertEqual(s.digests(0),before);self.assertEqual(result['generation'],1)
        old=s.db.execute('SELECT hash FROM chain.generation_blocks WHERE generation=0 AND height=4500198').fetchone()[0];new=s.db.execute('SELECT hash FROM chain.generation_blocks WHERE generation=1 AND height=4500198').fetchone()[0];self.assertNotEqual(old,new)
    def test_committed_conflict_revokes_readiness(self):
        s=self.store()
        routes,_=fixtures();routes['/blocks/'+str(OFFSET+10)]=dict(routes['/blocks/'+str(OFFSET+10)],hash='ff'*32)
        with self.assertRaisesRegex(Refusal,'committed_source_conflict'):self.advance(s,routes)
        self.assertIsNone(s.meta('canonical_ready'));self.assertTrue(s.meta('source_conflict'));self.assertEqual(s.generation(0)['status'],'ready')
    def test_handoff_semantic_conflict_revokes_readiness(self):
        s=self.store()
        routes,original=fixtures();routes['/txs/'+original[1]+'/utxos']['outputs'][0]['amount'][0]['quantity']='999'
        with self.assertRaisesRegex(Refusal,'handoff_fact_conflict'):self.advance(s,routes)
        self.assertIsNone(s.meta('canonical_ready'))
    def test_partial_epoch_becomes_completed_and_new_epoch_partial(self):
        s=self.store(transition=True)
        before=s.digests(0);routes,_=fixtures(transition=True);self.advance(s,routes)
        receipt=lambda generation,unit:s.db.execute("SELECT receipt FROM chain.checkpoints WHERE generation=%s AND phase='epochs' AND unit=%s",(generation,unit)).fetchone()[0]
        old=receipt(0,'2');self.assertIsNone(old['epoch_end'])
        completed=receipt(1,'2');self.assertEqual(completed['epoch_end']['height'],OFFSET+11);self.assertEqual(completed['stake_inherited_from'],0)
        new=receipt(1,'3');self.assertIsNone(new['epoch_end']);self.assertEqual(new['epoch_anchor']['height'],OFFSET+12)
        self.assertEqual(s.digests(0),before);self.assertEqual(s.db.execute('SELECT first_generation FROM chain.epoch_stakes WHERE epoch=2').fetchone()[0],0)
    def test_hostile_auxiliary_data_beside_a_relevant_transaction_stores_the_clean_facts(self):
        # Block h=12 gains two unrelated transactions, so it is not generation 0's provisional block (fork=1).
        digests=lambda auxiliary:self.advance(self.store(),fixtures(fork=1,auxiliary=auxiliary)[0])['digests']
        hostile,clean,plain=digests(HOSTILE_AUXILIARY),digests(CLEAN_AUXILIARY),digests(())
        self.assertEqual(hostile,clean)
        self.assertEqual({k:v for k,v in hostile.items() if k!='blocks'},{k:v for k,v in plain.items() if k!='blocks'})
        self.assertEqual(hostile['mints']['count'],1)
    def test_wrong_network_no_pending(self):
        s=self.store()
        routes,_=fixtures();routes['/genesis']['network_magic']=2
        with self.assertRaisesRegex(Refusal,'wrong_provider_network'):self.advance(s,routes)
        self.assertIsNone(s.pending());self.assertEqual(s.meta('canonical_ready'),0)

if __name__=='__main__':unittest.main()
