import threading
import time
from types import SimpleNamespace
import unittest
from db_sync_alt.contract import Refusal
from db_sync_alt.sources.blockfrost.epoch_prepare import PriorityClient, PreparationYield, EpochPreparer, daily_slack
from unit.test_epoch_pools import PROFILE

class Client:
    def __init__(self):self.entered=threading.Event();self.release=threading.Event();self.calls=[]
    def status(self):return {'daily_limit':50000,'budget_used':0}
    def get(self,path,purpose):
        self.calls.append(path)
        if path=='/slow':self.entered.set();self.release.wait(2)
        return path

class PriorityTests(unittest.TestCase):
    def test_ordinary_waiter_wins_before_next_preparation(self):
        client=Client();stop=threading.Event();gate=PriorityClient(client,stop);normal_done=threading.Event()
        def prepare():
            with gate.preparation_slot():gate.get('/slow','test')
        low=threading.Thread(target=prepare);low.start();self.assertTrue(client.entered.wait(1))
        high=threading.Thread(target=lambda:(gate.get('/ordinary','test'),normal_done.set()));high.start()
        deadline=time.time()+1
        while gate.waiting==0 and time.time()<deadline:time.sleep(.001)
        with self.assertRaises(PreparationYield):
            with gate.preparation_slot():pass
        client.release.set();low.join(1);high.join(1);self.assertTrue(normal_done.is_set());self.assertEqual(client.calls,['/slow','/ordinary'])
    def test_stop_waiting_normal_does_not_send(self):
        client=Client();stop=threading.Event();gate=PriorityClient(client,stop);errors=[]
        def prepare():
            with gate.preparation_slot():gate.get('/slow','test')
        low=threading.Thread(target=prepare);low.start();self.assertTrue(client.entered.wait(1))
        def ordinary():
            try:gate.get('/ordinary','test')
            except Refusal as error:errors.append(error.code)
        high=threading.Thread(target=ordinary);high.start();stop.set();high.join(1);client.release.set();low.join(1)
        self.assertEqual(errors,['interrupted']);self.assertEqual(client.calls,['/slow'])
    def test_daily_slack_reserves_ordinary_day_and_safety(self):
        self.assertTrue(daily_slack({'daily_limit':50000,'budget_used':0},0))
        self.assertFalse(daily_slack({'daily_limit':50000,'budget_used':1100},0))
        self.assertTrue(daily_slack({'daily_limit':50000,'budget_used':24999},43200))
        self.assertFalse(daily_slack({'daily_limit':50000,'budget_used':26000},43200))

class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.health=SimpleNamespace(policy=SimpleNamespace(observation_max_age_seconds=120))
        self.client=PriorityClient(Client(),threading.Event());self.prep=EpochPreparer(None,self.client,PROFILE,None,self.health,self.client.stop,clock=lambda:0)
        self.observation=SimpleNamespace(head={'epoch':666},fresh=lambda limit:True)
        self.snapshot={'healthy':True,'operation':None,'sync':'idle','lifecycle':'ready','database':{'reachable':True},'lag':{'blocks':0}}
    def test_steady_preparation_yields_to_lag_and_busy_writer(self):
        self.assertTrue(self.prep.eligible(self.snapshot,self.observation))
        self.assertFalse(self.prep.eligible({**self.snapshot,'lag':{'blocks':2}},self.observation))
        self.assertFalse(self.prep.eligible({**self.snapshot,'operation':{'stage':'verification'}},self.observation))
    def test_missing_required_stage_can_recover_large_lag_honestly(self):
        large={**self.snapshot,'healthy':False,'sync':'backoff','lag':{'blocks':1000}}
        self.assertFalse(self.prep.eligible(large,self.observation));self.prep.required_epoch=666
        self.assertTrue(self.prep.eligible(large,self.observation))
        self.assertFalse(self.prep.eligible({**large,'database':{'reachable':False}},self.observation))
    def test_recovery_does_not_relax_observation_or_quota(self):
        self.prep.required_epoch=666
        self.assertFalse(self.prep.eligible(self.snapshot,SimpleNamespace(head={'epoch':666},fresh=lambda _:False)))
        self.client.client.status=lambda:{'daily_limit':50000,'budget_used':50000}
        self.assertFalse(self.prep.eligible(self.snapshot,self.observation))

class ReadinessTests(unittest.TestCase):
    def test_required_epoch_is_nonready_even_inside_nine_block_lag(self):
        from db_sync_alt.service.health import HealthState,ChainPoint
        health=HealthState(service_version='test',schema_version=3,network='mainnet',profile=PROFILE.identity['profile'])
        health.update(lifecycle='ready',sync='idle',serving='ready');health.database_observed(True)
        health.heartbeat('sync');health.heartbeat('sql');health.observe_provider(ChainPoint(2260,'ab'*32),ChainPoint(100,'cd'*32))
        health.set_coverage(committed=ChainPoint(99,'ef'*32),served=ChainPoint(99,'ef'*32),witness=ChainPoint(2259,'ab'*32))
        # Supply chain timestamps so the ordinary freshness/lag monitor is ready.
        now=time.time();health.observe_provider(ChainPoint(2260,'ab'*32,now),ChainPoint(100,'cd'*32,now))
        health.set_coverage(committed=ChainPoint(99,'ef'*32,now),served=ChainPoint(99,'ef'*32,now),witness=ChainPoint(2259,'ab'*32,now))
        self.assertEqual(health.health()[0],200)
        health.preparation(epoch=666,state='waiting-required',attempts=10,error='epoch_preparation_not_ready',recovery=True)
        self.assertEqual(health.health()[0],503);self.assertIn('epoch_preparation_not_ready',health.snapshot()['reason_codes'])
        health.preparation(epoch=666,state='adopted',attempts=170,recovery=False);health.record_success();self.assertEqual(health.health()[0],200)
