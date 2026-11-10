# SPDX-License-Identifier: Apache-2.0
"""Deterministic orchestration/accounting tests: no source or provider network."""
import copy
import json
import subprocess
import sys
import threading
import unittest
from types import SimpleNamespace
from db_sync_alt.contract import Refusal
from db_sync_alt.service.runtime import LifecycleFailure, TransientSyncFailure
from db_sync_alt.sources.blockfrost.sync import AccountedProvider, SyncDriver, next_day
from fixtures.sync import Client, Clock, FixtureUpdate, Store, health
from fixtures.networks import PREPROD, PREPROD_PROFILE














class BlockUpdate(FixtureUpdate):
    """One request per block, each block committed for good: a resumed update starts at its first uncommitted block."""
    def __init__(self):super().__init__();self.done=0
    def __call__(self,store,provider,profile,target,*,hooks):
        self.calls.append((store.target+1,target))
        store._pending={'target_height':target,'witness_height':target+self.security,'parent':store.gid}
        while self.done<target-store.target:provider.get('/unit');self.done+=1
        hooks.before_publish()
        store.target=target;store.gid+=1;store._pending=None;self.done=0
        return {'generation':store.gid}


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.store=Store();self.client=Client(self.clock);self.health=health()
        self.stop=threading.Event();self.update=FixtureUpdate()
        self.config=SimpleNamespace(sync={})
    def driver(self):return SyncDriver(self.config,client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=self.update)
    def run_once(self):self.driver().run_once(self.store,self.health,self.stop)
    def test_idle_is_two_reads_plus_metrics_without_full_update(self):
        self.run_once();self.run_once()
        self.assertEqual([p for p,_ in self.client.calls],['/metrics','/blocks/latest','/blocks/100','/blocks/latest','/blocks/100'])
        self.assertEqual(self.update.calls,[])
        self.assertEqual(self.health.snapshot()['confirmed_target']['height'],100)
    def test_events_start_after_committed_target_never_witness(self):
        self.client.confirmed=130;self.run_once()
        self.assertEqual(self.update.calls,[(101,130)])
        self.assertEqual(self.store.jobs['provider-active']['target'],130)
        self.assertEqual(self.store.jobs['provider-active']['state'],'completed')
    def test_explicit_600_spacing_survives_restart_and_catchup_bypasses_spacing(self):
        self.config.sync={'publication_interval_seconds':600}
        self.client.confirmed=130;self.run_once()
        first=self.store.jobs['provider-active']['job_id']
        self.client.confirmed=150;self.clock.now+=60;self.run_once()
        self.assertEqual(self.update.calls,[(101,130)])
        self.assertEqual(self.store.jobs['provider-active']['job_id'],first)
        self.clock.now+=600;self.run_once()
        self.assertEqual(self.update.calls[-1],(131,150))
        self.client.confirmed=1000;self.clock.now+=60;self.run_once()
        self.assertEqual(self.update.calls[-1],(151,250))
        self.assertNotEqual(self.store.jobs['provider-active']['job_id'],first)
    def test_default_60_spacing_uses_completion_and_survives_restart(self):
        self.assertEqual(self.driver().options['publication_interval_seconds'],60)
        self.client.confirmed=130
        # Useful acquisition/verification time does not consume the subsequent
        # spacing interval. The durable completion timestamp is authoritative.
        self.update.before_commit=lambda:setattr(self.clock,'now',self.clock.now+195)
        self.run_once()
        completed=self.store.jobs['provider-active']['completed_at']
        first=copy.deepcopy(self.store.jobs['provider-active'])
        self.client.confirmed=150;self.update.before_commit=None
        self.clock.now=completed+59;self.run_once()  # new driver instance
        self.assertEqual(self.update.calls,[(101,130)])
        self.assertEqual(self.store.jobs['provider-active'],first)
        self.clock.now=completed+60;self.run_once()
        self.assertEqual(self.update.calls,[(101,130),(131,150)])
        self.assertNotEqual(self.store.jobs['provider-active']['job_id'],first['job_id'])
        self.assertEqual(self.store.jobs['provider-active']['budget'],10000)

    def test_pending_retry_preserves_target_job_attempts_across_restart_and_day(self):
        self.client.confirmed=140;self.client.errors['/unit']='HTTP 503; request counted, no automatic retry'
        with self.assertRaises(TransientSyncFailure):self.run_once()
        job=copy.deepcopy(self.store.jobs['provider-active']);calls=len(self.client.calls)
        with self.assertRaises(TransientSyncFailure):self.run_once()
        self.assertEqual(len(self.client.calls),calls)
        self.clock.now=next_day(self.clock.now)+1;self.client.confirmed=160;del self.client.errors['/unit']
        self.run_once();new=self.store.jobs['provider-active']
        self.assertEqual(new['job_id'],job['job_id']);self.assertEqual(new['target'],140)
        self.assertGreater(new['attempts'],job['attempts']);self.assertEqual(new['budget'],job['budget'])
        self.assertEqual(self.update.calls[-1],(101,140))
    def test_exhausted_update_waits_for_the_next_utc_day_then_continues_its_target(self):
        # Each allotment of 3 requests holds one usage check and two committed blocks of a 5-block update.
        self.config.sync={'acquisition_request_budget':3};self.client.confirmed=105;self.update=BlockUpdate()
        with self.assertRaises(TransientSyncFailure) as error:self.run_once()
        self.assertEqual((error.exception.code,error.exception.retry_at),('request_budget_exhausted',next_day(self.clock.now)))
        first=copy.deepcopy(self.store.jobs['provider-active'])
        self.assertEqual((first['state'],first['attempts'],first['target'],first['exhausted_at']),('exhausted',3,105,self.clock.now))
        self.assertEqual(self.update.done,2)
        # Later the same UTC day: no request and no change.
        calls=len(self.client.calls);self.clock.now=next_day(self.clock.now)-1
        with self.assertRaises(TransientSyncFailure) as error:self.run_once()
        self.assertEqual(error.exception.code,'request_budget_exhausted')
        self.assertEqual((len(self.client.calls),self.store.jobs['provider-active']),(calls,first))
        # Each later UTC day continues the same target under a new record; the exhausted one is kept unchanged.
        self.clock.now+=2
        with self.assertRaises(TransientSyncFailure):self.run_once()
        second=copy.deepcopy(self.store.jobs['provider-active'])
        self.assertEqual(self.store.jobs['provider-acquire-'+first['job_id']],first)
        self.assertEqual((second['resumes'],second['prior_attempts'],second['target'],second['witness'],second['parent']),
                         (first['job_id'],3,105,first['witness'],first['parent']))
        self.assertEqual((second['state'],second['attempts'],self.update.done),('exhausted',3,4))
        self.clock.now=next_day(self.clock.now)+1;self.run_once()
        third=self.store.jobs['provider-active']
        self.assertEqual(self.store.jobs['provider-acquire-'+second['job_id']],second)
        self.assertEqual((third['resumes'],third['prior_attempts'],third['state'],third['target']),(second['job_id'],6,'completed',105))
        self.assertEqual((self.store.target,self.update.calls),(105,[(101,105)]*3))
    def test_continuation_rule_never_resets_or_retargets(self):
        from db_sync_alt.store.database import continues
        old={'job_id':'a','kind':'acquire','state':'exhausted','attempts':10,'budget':10,'parent':0,'target':105,'witness':2265,
             'prior_attempts':7,'exhausted_at':self.clock.now}
        new={'job_id':'b','kind':'acquire','state':'active','attempts':0,'budget':10,'parent':0,'target':105,'witness':2265,
             'resumes':'a','prior_attempts':17,'created_at':next_day(self.clock.now)}
        self.assertTrue(continues(old,new))
        for change in ({'created_at':next_day(self.clock.now)-1},{'prior_attempts':10},{'prior_attempts':True},{'target':106},
                       {'witness':2266},{'parent':1},{'resumes':'c'},{'attempts':1},{'state':'exhausted'},{'kind':'poll'}):
            with self.subTest(change=change):self.assertFalse(continues(old,{**new,**change}))
        self.assertFalse(continues({**old,'state':'active'},new))
        # A job of 0.3.x has no `exhausted_at`: its last reservation dates the exhaustion.
        legacy={k:v for k,v in old.items() if k!='exhausted_at'}
        self.assertFalse(continues(legacy,new))
        self.assertTrue(continues({**legacy,'reserved_at':self.clock.now},new))
        store=Store();store.jobs['provider-active']=old
        with self.assertRaisesRegex(AssertionError,'nonterminal_rollover'):store.save_sync_job('provider-active',new)
        store.save_sync_job('provider-acquire-a',old);store.save_sync_job('provider-active',new)
        self.assertEqual(store.jobs['provider-active'],new)
    def test_job_is_sized_to_the_transactions_of_its_blocks(self):
        # A block of 300 transactions costs 4 inventory pages (the last one empty) and 300 /utxos: 24 blocks fit 3/4 of 10,000.
        self.client.confirmed=1000;self.store.tx_counts.update({h:300 for h in range(101,1101)})
        self.run_once()
        self.assertEqual(self.update.calls,[(101,124)])
        self.assertEqual((self.store.jobs['provider-active']['target'],self.store.jobs['provider-active']['budget']),(124,10000))
        self.run_once()
        self.assertEqual(self.update.calls[-1],(125,148))
    def test_one_block_heavier_than_the_budget_is_a_job_of_its_own(self):
        self.config.sync={'acquisition_request_budget':100};self.client.confirmed=1000;self.store.tx_counts[101]=500
        self.run_once()
        self.assertEqual(self.update.calls,[(101,101)])
    def test_sizing_counts_both_relevance_passes_and_caps_relevance_generations(self):
        from db_sync_alt.sources.blockfrost.sync import job_target
        ready=self.store.generation();top=ready['target_height']
        self.assertEqual(job_target(self.store,ready,top+100,10000,relevance=False),top+100)
        self.assertEqual(job_target(self.store,ready,top+100,10000,relevance=True),top+6)
        self.store.tx_counts.update({h:1000 for h in range(top+1,top+101)})
        # 2 x (11 pages + 1,000 /utxos) per block: three blocks fit 7,500.
        self.assertEqual(job_target(self.store,ready,top+100,10000,relevance=True),top+3)
        self.assertEqual(job_target(self.store,ready,top+100,10000,relevance=False),top+7)
    def test_sizing_ends_at_the_stored_witness(self):
        from db_sync_alt.sources.blockfrost.sync import job_target
        store=Store(security=3);ready=store.generation()
        self.assertEqual(job_target(store,ready,ready['target_height']+100,10000,relevance=False),ready['target_height']+3)
    def test_fixed_relevance_target_beyond_six_blocks_is_refused_before_any_request(self):
        self.config.sync={'relevance_enabled':True}
        driver=SyncDriver(self.config,client=self.client,profile=PREPROD_PROFILE,clock=self.clock,update_fn=self.update,fixed_target=107)
        driver._check_relevance=lambda *a:None
        with self.assertRaisesRegex(LifecycleFailure,'invalid_sync_configuration'):driver.run_once(self.store,self.health,self.stop)
        self.assertEqual(self.client.calls,[])
    def test_daily_poll_exhaustion_pauses_until_new_utc_day_with_new_budget(self):
        self.config.sync={'daily_poll_request_budget':2}
        with self.assertRaises(TransientSyncFailure) as error:self.run_once()
        self.assertEqual(error.exception.code,'quota_exhausted')
        oldkey=next(k for k in self.store.jobs if k.startswith('provider-poll-'))
        old=copy.deepcopy(self.store.jobs[oldkey]);self.clock.now=next_day(self.clock.now)+1
        with self.assertRaises(TransientSyncFailure):self.run_once()
        keys=[k for k in self.store.jobs if k.startswith('provider-poll-')]
        self.assertEqual(len(keys),2);self.assertEqual(self.store.jobs[oldkey],old)
    def test_committed_hash_conflict_on_idle_revokes_without_acquisition(self):
        self.client.bad_hash=True
        with self.assertRaisesRegex(LifecycleFailure,'canonical_conflict'):self.run_once()
        self.assertTrue(self.store.revoked);self.assertFalse(self.update.calls)
    def test_fatal_conflict_and_unsupported_capability_are_durably_revoked(self):
        for code,public in [('historical_selected_event_changed','canonical_conflict'),('unsupported_nonempty_candidates','unsupported_capability')]:
            with self.subTest(code=code):
                self.store=Store();self.client.confirmed=101;self.update.error=Refusal(code)
                with self.assertRaisesRegex(LifecycleFailure,public):self.run_once()
                self.assertTrue(self.store.revoked)
    def test_stop_before_publish_keeps_old_generation_and_pending_job(self):
        self.client.confirmed=101;self.update.before_commit=self.stop.set;self.run_once()
        self.assertEqual(self.store.target,100);self.assertEqual(self.store.jobs['provider-active']['state'],'active')
    def test_crash_after_commit_recovers_completed_job_without_republishing(self):
        self.client.confirmed=101;self.run_once()
        job=self.store.jobs['provider-active'];job['state']='active';job.pop('completed_at')
        count=len(self.update.calls);self.run_once()
        self.assertEqual(len(self.update.calls),count);self.assertEqual(self.store.jobs['provider-active']['state'],'completed')
    def test_pending_without_durable_job_is_not_given_fresh_budget(self):
        self.store._pending={'target_height':101}
        with self.assertRaisesRegex(LifecycleFailure,'pending_job_missing'):self.run_once()
        self.assertEqual(self.client.calls,[])
    def test_corrupt_reloaded_budget_is_refused_without_request(self):
        self.client.confirmed=101;self.run_once()
        self.store.jobs['provider-active']['attempts']=-1
        calls=len(self.client.calls)
        with self.assertRaisesRegex(LifecycleFailure,'invalid_sync_job'):self.run_once()
        self.assertEqual(len(self.client.calls),calls)

    def test_runtime_does_not_import_test_fixtures(self):
        script="import sys;import db_sync_alt.sources.blockfrost.sync as pg_sync;assert 'fixtures' not in sys.modules"
        result=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)


class AccountingTests(unittest.TestCase):
    def setUp(self):
        self.clock=Clock();self.store=Store();self.client=Client(self.clock);self.health=health();self.stop=threading.Event()
        self.job={'job_id':'fixed','attempts':0,'budget':10,'state':'active','kind':'acquire','parent':0,'target':101,'witness':2261}
        self.store.save_sync_job('provider-active',self.job)
    def provider(self):return AccountedProvider(self.store,'provider-active',self.store.sync_job('provider-active'),self.client,self.health,self.stop,clock=self.clock,network=PREPROD)
    def test_attempt_reserved_before_shared_request_and_failed_attempt_remains_counted(self):
        def check(path):self.assertGreater(self.store.sync_job('provider-active')['attempts'],0)
        self.client.on_call=check;self.client.errors['/unit']='HTTP 503; request counted, no automatic retry'
        with self.assertRaises(TransientSyncFailure):self.provider().get('/unit')
        self.assertEqual(self.store.sync_job('provider-active')['attempts'],2)
        self.assertEqual(self.client.used,2)
        self.assertTrue(all(purpose=='db-sync-alt:fixed' for _,purpose in self.client.calls))
    def test_quota_and_recorded_cooldown_do_not_send_requests(self):
        self.client.used=50000
        with self.assertRaises(TransientSyncFailure) as quota:self.provider().get('/unit')
        self.assertEqual(quota.exception.retry_at,next_day(self.clock.now));self.assertEqual(self.client.calls,[])
        self.clock.now=next_day(self.clock.now)+1;self.client.used=0;self.client.cooldown=self.clock.now+3600
        with self.assertRaises(TransientSyncFailure) as cooldown:self.provider().get('/unit')
        self.assertEqual(cooldown.exception.retry_at,self.client.cooldown);self.assertEqual(self.client.calls,[])
    def test_epoch_not_ready_is_classified_but_auth_failure_is_not_retried(self):
        self.client.errors['/epochs/3']='HTTP 404; request counted, no automatic retry'
        with self.assertRaisesRegex(Refusal,'provider_epoch_not_ready'):self.provider().get('/epochs/3')
        self.client.errors['/unit']='HTTP 403; request counted, no automatic retry'
        with self.assertRaisesRegex(LifecycleFailure,'provider_request_rejected'):self.provider().get('/unit')
    def test_secret_error_is_never_persisted_or_serialized(self):
        self.client.errors['/unit']='https://secret-key@private-directory'
        with self.assertRaisesRegex(LifecycleFailure,'provider_request_rejected'):self.provider().get('/unit')
        self.assertNotIn('secret-key',json.dumps(self.store.jobs));self.assertNotIn('secret-key',json.dumps(self.health.snapshot()))


if __name__=='__main__':unittest.main(verbosity=2)
