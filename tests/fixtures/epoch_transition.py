"""Actual13.8 SQL -> qualified Mainnet parent665, then API-only prepared666.

Chain/transaction history is synthetic; epoch666 weights are the independently
qualified Q4 public distribution. No live provider, expected-ID selector or
publication/checkpoint shortcut is used.
"""
import copy
import hashlib
from types import SimpleNamespace
import threading
import time

from db_sync_alt.sources.dbsync.acquire import CountedSQLSource, acquire
from db_sync_alt.store.database import PgStore
from db_sync_alt.store.upgrade import upgrade_store
from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
from db_sync_alt.sources.blockfrost.epoch_stage import EpochStageStore
from db_sync_alt.sources.blockfrost.epoch_prepare import EpochPreparer, PriorityClient
from db_sync_alt.sources.blockfrost.sync import SyncDriver, point
from db_sync_alt.service.health import HealthState
from integration.test_dbsync_versions import apply_upstream, insert
from fixtures.cbor import tx
from unit.test_epoch_pools import PROFILE, FIXTURE

S=PROFILE.start[0];K=PROFILE.security;FUTURE=120
H=lambda n:hashlib.sha256(str(n).encode()).digest()
TIME=1767225600

def address_bytes(text):
    alphabet='qpzry9x8gf2tvdw0s3jn54khce6mua7l';values=[alphabet.index(c) for c in text.rsplit('1',1)[1][:-6]];acc=bits=0;raw=[]
    for value in values:
        acc=(acc<<5)|value;bits+=5
        while bits>=8:bits-=8;raw.append((acc>>bits)&255)
    return bytes(raw)
P=bytes.fromhex(PROFILE.night[:56]);N=bytes.fromhex(PROFILE.night[56:]);ADDR=address_bytes(PROFILE.mapping)
PRE=tx({0:[],1:[[ADDR,[100,{P:{N:3}}]]],2:1})
SPEND=tx({0:[[bytes.fromhex(PRE[1]),0]],1:[[ADDR,[100,{P:{N:3}}]]],2:2})
ROOT=tx({0:[],1:[[ADDR,[100,{P:{N:3}}]]],2:3})
DUMMY=tx({0:[],1:[[b'\x61'+b'o'*28,100]],2:4})

def block(h):
    epoch=663 if h==S-100 else 664 if h==S-50 else 665 if h<=S else 666
    return {'hash':H(h).hex(),'height':h,'previous_block':H(h-1).hex(),'epoch':epoch,'slot':h,'time':TIME,'tx_count':2 if h in (S-1,S) else 0}

def output():return {'output_index':0,'address':PROFILE.mapping,'amount':[{'unit':'lovelace','quantity':'100'},{'unit':PROFILE.night,'quantity':'3'}],'data_hash':None}

def seed_source(db):
    apply_upstream(db,'13.8.0.0');insert(db,'meta',dict(network_name='mainnet',version='13.6.0.5',start_time='2020-01-01'))
    insert(db,'slot_leader',dict(id=1,hash=b'l'*28,description='fixture'))
    with db.transaction():
        for h in [S-100,S-50,S-1]+list(range(S,S+K+FUTURE+1)):
            b=block(h);insert(db,'block',dict(id=h,hash=H(h),block_no=h,previous_id=h-1 if h!=S-100 else None,epoch_no=b['epoch'],slot_no=h,time='2026-01-01',tx_count=b['tx_count'],slot_leader_id=1))
        for ident,item,h,index in [(1,PRE,S-1,0),(2,SPEND,S-1,1),(3,ROOT,S,PROFILE.start[1]),(4,DUMMY,S,0)]:
            insert(db,'tx',dict(id=ident,hash=bytes.fromhex(item[1]),block_id=h,block_index=index,valid_contract=True))
        insert(db,'multi_asset',dict(id=1,policy=P,name=N,fingerprint='fixture_asset'))
        for ident in (1,2,3):
            insert(db,'tx_out',dict(id=ident,tx_id=ident,index=0,address=PROFILE.mapping,value=100,data_hash=None,address_has_script=True));insert(db,'ma_tx_out',dict(id=ident,quantity=3,tx_out_id=ident,ident=1))
        insert(db,'tx_out',dict(id=4,tx_id=4,index=0,address='irrelevant',value=100,data_hash=None,address_has_script=False));insert(db,'tx_in',dict(id=1,tx_in_id=2,tx_out_id=1,tx_out_index=0))
        pool_ids={};pool_views={}
        from db_sync_alt.sources.blockfrost.adapter import pool_hash
        for path,responses in FIXTURE['routes'].items():
            if path.startswith('/pools/extended?'):
                for r in responses[0]:pool_views[r['hex']]=r['pool_id']
            elif path.startswith('/pools/retired?'):
                for r in responses[0]:pool_views[pool_hash(r['pool_id']).hex()]=r['pool_id']
        expected=FIXTURE['expected']['rows']
        for number,r in enumerate(expected,1):
            pool_ids[r['pool_hash']]=number;insert(db,'pool_hash',dict(id=number,hash_raw=bytes.fromhex(r['pool_hash']),view=pool_views[r['pool_hash']]))
            insert(db,'stake_address',dict(id=number,hash_raw=b'\xe1'+number.to_bytes(28,'big'),view='stake1_fixture_'+str(number)))
            insert(db,'epoch_stake',dict(id=10000+number,epoch_no=666,pool_id=number,addr_id=number,amount=int(r['amount'])))
        for epoch,h in [(663,S-100),(664,S-50),(665,S),(666,S+1)]:
            nonce=bytes.fromhex(FIXTURE['expected']['nonce']) if epoch==666 else H('epoch'+str(epoch))
            insert(db,'epoch_param',dict(epoch_no=epoch,nonce=nonce,block_id=h));insert(db,'epoch_stake_progress',dict(epoch_no=epoch,completed=True))
            if epoch!=666:insert(db,'epoch_stake',dict(id=epoch,epoch_no=epoch,pool_id=1,addr_id=1,amount=123))
        insert(db,'extra_migrations',dict(token='StakeDistrEnded'))

class Client:
    def __init__(self):
        self.calls=[];self.counts={};self.mutate=None;self.routes={}
        for h in [S-100,S-50,S-1]+list(range(S,S+K+FUTURE+1)):
            b=block(h);self.routes['/blocks/'+str(h)]=b;self.routes['/blocks/'+b['hash']]=b
            if h>=S:self.routes['/blocks/'+b['hash']+'/next?count=100&page=1']=[block(n) for n in range(h+1,min(h+101,S+K+FUTURE+1))]
        self.routes['/blocks/latest']=block(S+K+FUTURE)
        for raw,key,h,index,inputs in [(PRE[0],PRE[1],S-1,0,[]),(SPEND[0],SPEND[1],S-1,1,[{**output(),'tx_hash':PRE[1],'reference':False,'collateral':False}]),(ROOT[0],ROOT[1],S,PROFILE.start[1],[])]:
            self.routes['/txs/'+key+'/cbor']={'cbor':raw};self.routes['/txs/'+key]={'hash':key,'block':H(h).hex(),'block_height':h,'index':index,'valid_contract':True};self.routes['/txs/'+key+'/utxos']={'hash':key,'inputs':inputs,'outputs':[output()]}
    def status(self):return {'daily_limit':50000,'budget_used':len(self.calls)}
    def get(self,path,purpose):
        self.calls.append(path);count=self.counts.get(path,0);self.counts[path]=count+1
        if path=='/metrics':return []
        if path in self.routes:response=copy.deepcopy(self.routes[path])
        else:
            rows=FIXTURE['routes'][path];response=copy.deepcopy(rows[min(count,len(rows)-1)])
        return self.mutate(path,response) if self.mutate else response

class Clock:
    def __init__(self):self.now=time.time()
    def __call__(self):return self.now

class Environment:
    def __init__(self,dsn,alias,source_alias):
        self.dsn,self.alias=dsn,alias;self.source_alias=source_alias;self.clock=Clock();self.stop=threading.Event();self.client=Client();self.gate=PriorityClient(self.client,self.stop)
        self.profile=PROFILE;self.owner=PgStore(conninfo=dsn,create=True,network='mainnet')
        acquire(CountedSQLSource(source_alias,PROFILE,{'dbsync_version':'13.8.0.0','tx_in_retained':True,'epoch_stake_retained':True,'operator_attested':True}),self.owner,PROFILE,target_height=S,window_blocks=10000)
        from fixtures.sql_prerequisites import assert_v2_root
        assert_v2_root(self.owner)
        upgrade_store(self.owner);self.owner.certify_ready_full()
        from db_sync_alt.sources.blockfrost.relevance import qualified_parent
        assert qualified_parent(self.owner,PROFILE) is not None
        self.health=HealthState(service_version='test',schema_version=3,network='mainnet',profile=PROFILE.identity['profile'],wall_clock=self.clock)
        self.health.update(lifecycle='ready',sync='idle',serving='ready');self.health.database_observed(True);self.health.heartbeat('sync');self.health.heartbeat('sql')
        ready=self.owner.generation();target=self.owner.block_at(ready['id'],ready['target_height']);witness=self.owner.block_at(ready['id'],ready['witness_height']);self.health.set_coverage(committed=point(target),served=point(target),witness=point(witness))
        self.binding=owner_binding(self.owner);self.pollstore=PollBudgetStore(None,self.binding,conninfo=dsn);self.poller=HeadPoller(lambda:self.pollstore,lambda:self.gate,self.health,self.stop,network=PROFILE.network,clock=self.clock)
        self.poller.tick(self.pollstore,self.gate)
        self.driver=SyncDriver(SimpleNamespace(sync={'epoch_preparation':True},network='mainnet',shutdown_seconds=30),client=self.gate,profile=PROFILE,clock=self.clock)
        self.driver._poller=self.poller;self.make_preparer()
    def make_preparer(self):
        self.preparer=EpochPreparer(lambda:EpochStageStore(None,self.binding,PROFILE,conninfo=self.dsn,clock=self.clock),self.gate,PROFILE,self.poller,self.health,self.stop,clock=self.clock,interval=0)
        self.driver._preparer=self.preparer
    def restart_preparer(self):
        if self.preparer.store:self.preparer.store.close();self.preparer.store=None
        self.make_preparer()
    def restart_all(self):
        if self.preparer.store:self.preparer.store.close()
        self.pollstore.close();self.owner.close()
        self.owner=PgStore(conninfo=self.dsn);self.owner.certify_ready_full()
        self.binding=owner_binding(self.owner);self.gate=PriorityClient(self.client,self.stop)
        self.pollstore=PollBudgetStore(None,self.binding,conninfo=self.dsn)
        self.poller=HeadPoller(lambda:self.pollstore,lambda:self.gate,self.health,self.stop,network=PROFILE.network,clock=self.clock)
        self.poller.tick(self.pollstore,self.gate)
        self.driver=SyncDriver(SimpleNamespace(sync={'epoch_preparation':True},network='mainnet',shutdown_seconds=30),client=self.gate,profile=PROFILE,clock=self.clock)
        self.driver._poller=self.poller;self.make_preparer()
    def prepare(self,steps=None):
        for count in range(300):
            self.health.end_operation();self.health.database_observed(True);self.poller.tick(self.pollstore,self.gate)
            self.preparer.tick()
            manifest=self.preparer.store._meta('epoch-preparation-v1:666:manifest')
            if manifest['state']=='complete':return
            if steps is not None and count+1>=steps:return
        raise AssertionError('preparation did not complete')
    def close(self):
        self.stop.set()
        if self.preparer.thread and self.preparer.thread.is_alive():self.preparer.thread.join(5)
        if self.preparer.store:self.preparer.store.close()
        self.pollstore.close();self.owner.close()
