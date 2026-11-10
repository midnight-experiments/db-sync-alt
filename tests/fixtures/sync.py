"""In-memory store, clock, provider client and update fakes for the sync driver."""
import copy
from db_sync_alt.service.health import HealthState
from db_sync_alt.store.database import ARCHIVE, continues


class Clock:
    def __init__(self):self.now = 1700000000.0
    def __call__(self):return self.now

def block(height):
    return {'height': height, 'hash': f'{height:064x}', 'previous_block': f'{height-1:064x}',
            'epoch': 2, 'slot': height*20, 'time': 1700000000 + height*20, 'tx_count': 0}

def health():
    return HealthState(service_version='1', schema_version=1, network='preprod', profile='midnight-1x')

class Store:
    def __init__(self,security=2160):
        self.jobs={};self.target=100;self.gid=0;self._pending=None;self.revoked=False;self.saved=[];self.security=security
        self.tx_counts={}
    def generation(self):
        return {'id':self.gid,'target_height':self.target,'witness_height':self.target+self.security,
                'manifest':{'anchors':{'target':{'height':self.target,'hash':block(self.target)['hash']}}}}
    def pending(self):return ({'id':self.gid+1,'source':'blockfrost-cardano','status':'pending','manifest':{},**self._pending} if self._pending else None)
    def checkpoint_rows(self,**filters):return []
    def block_at(self,generation,height):
        # Provisional headers through the ready witness; `tx_counts` sets a block's transaction count (default 0).
        return {**block(height),'tx_count':self.tx_counts.get(height,0)} if height<=self.target+self.security else None
    def sync_job(self,key):return copy.deepcopy(self.jobs.get(key))
    def save_sync_job(self,key,body):
        old=self.jobs.get(key)
        if old and old['job_id']==body['job_id']:
            if body['attempts']<old['attempts'] or body['budget']!=old['budget']:raise AssertionError('budget_reset')
        elif old and old['state']!='completed' and not (continues(old,body) and self.jobs.get(ARCHIVE+old['job_id'])==old):
            raise AssertionError('nonterminal_rollover')
        self.jobs[key]=copy.deepcopy(body);self.saved.append((key,copy.deepcopy(body)))
    def mark_conflict(self):self.revoked=True

class Client:
    def __init__(self,clock,confirmed=100,security=2160):
        self.clock=clock;self.confirmed=confirmed;self.security=security;self.calls=[];self.used=0;self.limit=50000
        self.errors={};self.cooldown=0;self.on_call=None;self.bad_hash=False
    def status(self):return {'budget_used':self.used,'daily_limit':self.limit,'cooldown_until':self.cooldown}
    def get(self,path,purpose):
        self.used+=1;self.calls.append((path,purpose))
        if self.on_call:self.on_call(path)
        if path in self.errors:raise RuntimeError(self.errors[path])
        if path=='/metrics':return []
        if path=='/blocks/latest':return block(self.confirmed+self.security)
        if path.startswith('/blocks/'):
            value=block(int(path.rsplit('/',1)[1]))
            if self.bad_hash:value['hash']='f'*64
            return value
        if path=='/unit':return {'fixture':True}
        raise AssertionError('unexpected fixture request')

class FixtureUpdate:
    def __init__(self,security=2160):self.calls=[];self.error=None;self.before_commit=None;self.security=security
    def __call__(self,store,provider,profile,target,*,hooks):
        self.calls.append((store.target+1,target))
        store._pending={'target_height':target,'witness_height':target+self.security,'parent':store.gid}
        provider.get('/unit')
        if self.error:raise self.error
        if self.before_commit:self.before_commit()
        hooks.before_publish()
        store.target=target;store.gid+=1;store._pending=None
        return {'generation':store.gid}
