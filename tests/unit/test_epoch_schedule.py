"""Conservative serial-work timing model exercising real preparation admission.

Pool latency30s; ordinary job cost from measured00042p95 endpoint inputs plus
4s local envelope. This is synthetic timing, never a live acceptance claim.
"""
import copy
import hashlib
import json
from types import SimpleNamespace
import threading
import unittest

from db_sync_alt.contract import digest
from db_sync_alt.sources.blockfrost.epoch_pools import PoolCollector
from db_sync_alt.sources.blockfrost.epoch_prepare import EpochPreparer,PriorityClient
from unit.test_epoch_pools import PROFILE,FIXTURE

class Clock:
    def __init__(self):self.elapsed=0
    def __call__(self):return 1793923200+self.elapsed
    def mono(self):return self.elapsed

class MemoryStage:
    def __init__(self):self.job=None;self.records=[];self.manifest=None
    def ensure(self,epoch):
        if self.job is None:
            self.job={'job_id':'schedule-test','kind':'epoch','epoch':epoch,'profile_sha256':digest(PROFILE.identity),'attempts':0,'budget':8000,'state':'active','created_at':1793923200,'retry_at':0,'failure_count':0}
            self.manifest={'state':'collecting','root':'0'*64,'records':0}
        return copy.deepcopy(self.job)
    def sync_job(self,key):return copy.deepcopy(self.job)
    def save_sync_job(self,key,job):
        assert job['attempts']>=self.job['attempts'] and job['budget']==8000;self.job=copy.deepcopy(job)
    def _meta(self,key):return copy.deepcopy(self.manifest)
    def load(self,epoch):return PoolCollector.replay(PROFILE,epoch,self.records),copy.deepcopy(self.manifest)
    def append(self,epoch,old,record,collector):
        assert old==self.manifest;self.records.append(copy.deepcopy(record));self.manifest={'state':'complete' if collector.complete else 'collecting','root':digest(self.records),'records':len(self.records)};return copy.deepcopy(self.manifest)
    def prune(self,epoch):pass
    def close(self):pass

class API:
    H0=14164352
    def __init__(self,clock):self.clock=clock;self.used=0;self.counts={};self.slow_calls=0
    def status(self):return {'budget_used':self.used,'daily_limit':50000}
    def block(self,height):
        h=lambda n:hashlib.sha256(str(n).encode()).hexdigest()
        return {'height':height,'hash':h(height),'previous_block':h(height-1),'epoch':665 if height<self.H0-100 else 666,'slot':height*20,'time':1793923200+(height-self.H0)*20,'tx_count':0,'confirmations':1}
    def get(self,path,purpose):
        if path.startswith('/ordinary-job/'):
            _,_,requests,seconds=path.split('/');self.used+=int(requests);self.clock.elapsed+=float(seconds);return {}
        self.used+=1;count=self.counts.get(path,0);self.counts[path]=count+1
        duration=30 if path.startswith('/pools/extended?') else .672
        if duration==30:self.slow_calls+=1
        self.clock.elapsed+=duration
        if path=='/metrics':return []
        if path=='/blocks/latest':return self.block(self.H0+int(self.clock.elapsed//20))
        if path.startswith('/blocks/'):return self.block(int(path.rsplit('/',1)[1]))
        rows=FIXTURE['routes'][path];return copy.deepcopy(rows[min(count,len(rows)-1)])

class Observation:
    def __init__(self,clock):self.clock=clock;self.at=clock.elapsed;self.head={'epoch':666}
    def fresh(self,maximum):return 0<=self.clock.elapsed-self.at<=maximum

class Health:
    def __init__(self,clock):self.clock=clock;self.policy=SimpleNamespace(observation_max_age_seconds=120);self.observed=0;self.served=0;self.operation=None;self.prepared=None
    def snapshot(self):
        lag=max(0,self.observed-self.served)
        return {'healthy':lag<=9,'operation':self.operation,'sync':'idle','lifecycle':'ready','database':{'reachable':True},'lag':{'blocks':lag}}
    def update(self,**kwargs):pass
    def begin_operation(self,*args,**kwargs):pass
    def end_operation(self):pass
    def preparation(self,**kwargs):self.prepared=kwargs


def model(dense_seconds):
    clock=Clock();api=API(clock);stop=threading.Event();gate=PriorityClient(api,stop);health=Health(clock);obs=Observation(clock);poller=SimpleNamespace(latest=lambda:obs);store=MemoryStage()
    prep=EpochPreparer(lambda:store,gate,PROFILE,poller,health,stop,clock=clock,monotonic=clock.mono)
    next_poll=0;wait_since=None;peak=0;done=None;max_poll_gap=0;last_poll=None;publications=0
    limit=43200
    while clock.elapsed<limit:
        if clock.elapsed>=next_poll:
            started=clock.elapsed
            if last_poll is not None:max_poll_gap=max(max_poll_gap,started-last_poll)
            last_poll=started;gate.get('/ordinary-job/2/1.344','poll');health.observed=int(clock.elapsed//20);obs=Observation(clock);next_poll=(int(clock.elapsed//20)+1)*20
        lag=health.observed-health.served
        if lag>0 and wait_since is None:wait_since=clock.elapsed
        if lag>=3 or (lag>0 and wait_since is not None and clock.elapsed-wait_since>=60):
            batch=min(lag,3);selected=4 if clock.elapsed<dense_seconds else 1
            requests=6+2*batch+4*batch*selected
            seconds=4+6*.672+2*batch*1.199+2*batch*selected*(.562+.958)
            health.operation={'stage':'provider'};gate.get('/ordinary-job/'+str(requests)+'/'+str(seconds),'follow')
            peak=max(peak,int(clock.elapsed//20)-health.served);health.served+=batch;publications+=1;health.operation=None;wait_since=None
        else:
            prep.tick();peak=max(peak,int(clock.elapsed//20)-health.served)
            if store.manifest and store.manifest['state']=='complete' and done is None:done=clock.elapsed
            clock.elapsed+=1
    result={'dense_burst_seconds':dense_seconds,'preparation_complete_seconds':done,'lead_seconds':limit,'peak_true_lag_blocks':peak,'final_true_lag_blocks':int(clock.elapsed//20)-health.served,'max_poll_start_gap_seconds':max_poll_gap,'modeled_requests':api.used,'preparation_attempts':store.job['attempts'] if store.job else 0,'source_pool_pages_30s':api.slow_calls,'publications':publications,'clock_kind':'synthetic_conservative_whole_job_serial;not_live_acceptance'}
    print('EPOCH_SCHEDULE='+json.dumps(result,sort_keys=True),flush=True);return result

class ScheduleTests(unittest.TestCase):
    def test_normal_load_completes_staged_independent_reads_within_confirmation_lead(self):
        result=model(0);self.assertIsNotNone(result['preparation_complete_seconds']);self.assertLess(result['preparation_complete_seconds'],43200);self.assertLessEqual(result['peak_true_lag_blocks'],9);self.assertEqual(result['source_pool_pages_30s'],58);self.assertLess(result['modeled_requests'],50000)
    def test_hour_dense_burst_yields_and_recovers_without_relaxing_limits(self):
        result=model(3600);self.assertIsNotNone(result['preparation_complete_seconds']);self.assertLess(result['preparation_complete_seconds'],43200);self.assertLessEqual(result['peak_true_lag_blocks'],9);self.assertEqual(result['source_pool_pages_30s'],58);self.assertLess(result['modeled_requests'],50000)
