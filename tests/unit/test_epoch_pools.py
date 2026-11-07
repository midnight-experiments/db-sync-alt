import collections
import copy
import json
from pathlib import Path
import unittest

from db_sync_alt.contract import Profile, Refusal, canonical, digest
from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.adapter import OPENAPI
from db_sync_alt.sources.blockfrost.epoch_pools import PoolCollector

FIXTURE = json.loads((Path(__file__).parents[1] / 'fixtures/data/epoch-pools-q4.json').read_text())
PROFILE = Profile(profile_dir('mainnet'), 'mainnet')

class ReplayProvider:
    def __init__(self, mutate=None):
        self.calls=[];self.counts=collections.Counter();self.mutate=mutate;self.receipts=[]
    def get(self,path):
        self.calls.append(path);count=self.counts[path];self.counts[path]+=1
        rows=FIXTURE['routes'][path];response=copy.deepcopy(rows[min(count,len(rows)-1)])
        if self.mutate:response=self.mutate(path,count+1,response)
        self.receipts=[{'endpoint':path,'sha256':digest(response),'bytes':len(canonical(response).encode()),'attempt':len(self.calls)}]
        return response
    def drain(self):
        value={'source':'blockfrost-cardano','shared_profile':'mainnet','adapter':PROFILE.network.adapter,'openapi':OPENAPI,'requests':self.receipts};self.receipts=[];return value

def collect(provider=None):
    provider=provider or ReplayProvider();collector=PoolCollector(PROFILE,666)
    while collector.step(provider):pass
    return collector,provider

class PoolTests(unittest.TestCase):
    def test_complete_Q4_replay_discovers_retired_without_SQL_hints(self):
        collector,provider=collect();result=collector.result();want={r['pool_hash']:r['amount'] for r in FIXTURE['expected']['rows']}
        self.assertEqual(result['positive'],want);self.assertEqual(result['active_stake'],'21327334991838362');self.assertEqual(len(want),2669)
        self.assertEqual([len(p['registered']) for p in collector.passes],[2883,2883]);self.assertEqual([len(p['retired']) for p in collector.passes],[3289,3289])
        self.assertEqual([sum(v['active_stake']=='0' for v in p['registered'].values()) for p in collector.passes],[218,218])
        specific=[p for p in provider.calls if p.startswith('/pools/') and '?' not in p]
        self.assertEqual(len(specific),8);self.assertEqual(specific[:4],specific[4:]);self.assertEqual(result['requests'],149)
    def test_same_sum_redistribution_in_independent_pass_is_rejected(self):
        def mutate(path,count,row):
            if '/pools/extended?' in path and 'page=1&' in path and count==2:
                positive=[r for r in row if int(r['active_stake'])>10];positive[0]['active_stake']=str(int(positive[0]['active_stake'])+1);positive[1]['active_stake']=str(int(positive[1]['active_stake'])-1)
            return row
        with self.assertRaisesRegex(Refusal,'epoch_preparation_distribution_changed'):collect(ReplayProvider(mutate))
    def test_independent_registered_omission_is_rejected(self):
        def mutate(path,count,row):
            return row[:-1] if path.startswith('/pools/extended?') and 'page=29&' in path and count==2 else row
        with self.assertRaisesRegex(Refusal,'epoch_preparation_inventory_changed'):collect(ReplayProvider(mutate))
    def test_independent_retired_omission_is_rejected(self):
        def mutate(path,count,row):
            return row[:-1] if path.startswith('/pools/retired?') and 'page=33&' in path and count==2 else row
        with self.assertRaisesRegex(Refusal,'epoch_preparation_inventory_changed'):collect(ReplayProvider(mutate))
    def test_duplicate_retired_is_rejected(self):
        def mutate(path,count,row):
            if path.startswith('/pools/retired?') and 'page=1&' in path:row[1]=row[0]
            return row
        with self.assertRaisesRegex(Refusal,'epoch_preparation_duplicate'):collect(ReplayProvider(mutate))
    def test_negative_and_overlarge_amounts_refused(self):
        for amount in ('-1','9999999999999999999999999999'):
            def mutate(path,count,row):
                if path.startswith('/pools/extended?'):row[0]['active_stake']=amount
                return row
            with self.subTest(amount=amount),self.assertRaises(Refusal):collect(ReplayProvider(mutate))
    def test_changed_nonce_and_epoch_and_anchor_refused(self):
        for kind in ('nonce','epoch','anchor'):
            def mutate(path,count,row):
                if kind=='nonce' and path=='/epochs/666/parameters' and count==2:row['nonce']='aa'*32
                if kind=='epoch' and path=='/blocks/latest' and count==2:row['epoch']=667
                if kind=='anchor' and path.startswith('/blocks/14162192') and count==2:row['hash']='ab'*32
                return row
            with self.subTest(kind=kind),self.assertRaises(Refusal):collect(ReplayProvider(mutate))
    def test_confirmation_growth_is_not_header_change(self):
        def mutate(path,count,row):
            if path.startswith('/blocks/14162192'):row['confirmations']+=count
            return row
        collector,_=collect(ReplayProvider(mutate));self.assertTrue(collector.complete)
    def test_registered_only_cannot_close_total(self):
        def mutate(path,count,row):
            return [] if path.startswith('/pools/retired?') else row
        with self.assertRaises(Refusal):collect(ReplayProvider(mutate))
    def test_resume_replays_same_independent_source_records(self):
        provider=ReplayProvider();old=PoolCollector(PROFILE,666)
        for _ in range(50):old.step(provider)
        before=list(provider.calls);new=PoolCollector.replay(PROFILE,666,copy.deepcopy(old.records));self.assertEqual(provider.calls,before)
        while new.step(provider):pass
        self.assertEqual(new.result()['positive'],{r['pool_hash']:r['amount'] for r in FIXTURE['expected']['rows']})
    def test_response_or_receipt_corruption_is_rejected(self):
        collector,_=collect()
        for kind in ('body','receipt'):
            records=copy.deepcopy(collector.records)
            if kind=='body':records[0]['response']['epoch']=667
            else:records[0]['provider']['requests'][0]['sha256']='aa'*32
            with self.subTest(kind=kind),self.assertRaises(Refusal):PoolCollector.replay(PROFILE,666,records)
    def test_other_network_is_not_silently_opted_in(self):
        with self.assertRaises(Refusal):PoolCollector(Profile(profile_dir('preprod'),'preprod'),666)

    def test_current_endpoint_is_never_relabelled_historical(self):
        collector=PoolCollector(PROFILE,665);provider=ReplayProvider()
        with self.assertRaisesRegex(Refusal,'epoch_preparation_epoch_changed'):
            while collector.step(provider):pass
    def test_wrong_provider_network_refused_before_pool_reads(self):
        def mutate(path,count,row):
            if path=='/genesis':row['network_magic']=1
            return row
        provider=ReplayProvider(mutate)
        with self.assertRaisesRegex(Refusal,'wrong_provider_network'):collect(provider)
        self.assertFalse(any(path.startswith('/pools/') for path in provider.calls))
