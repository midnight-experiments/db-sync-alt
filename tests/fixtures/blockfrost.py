"""Fake Blockfrost client, counting provider, update hooks, Preprod pool id and provider rows."""
import copy
import datetime as dt
import hashlib
import time
import uuid

from db_sync_alt.contract import Refusal
from db_sync_alt.sources.blockfrost.adapter import OPENAPI, ProviderClient, bounded_answer, input_cut
from db_sync_alt.sources.blockfrost.provider import UTXOS
from fixtures.networks import PREPROD


POOL='pool13m26ky08vz205232k20u8ft5nrg8u68klhn0xfsk9m4gsqsc44v'

class Fake:
    def __init__(self,routes):self.routes=routes;self.calls=[]
    def get(self,p,**kw):
        self.calls.append(p)
        if p not in self.routes:raise RuntimeError('secret error must not leak')
        r=self.routes[p]
        return r() if callable(r) else copy.deepcopy(r)


class CountingProvider(ProviderClient):
    """Budgeted provider over a fake client: counts attempts and keeps compact receipts, like the service's accounted
    provider. With `profile`, an oversized `/utxos` answer is cut as the service cuts it."""
    def __init__(self,budget,client,profile=None):
        if type(budget) is not int or not 1<=budget<=50000:raise Refusal('invalid_request_budget')
        self.budget=budget;self.job_id=uuid.uuid4().hex;self.attempts=0;self.receipts=[];self.metrics_day=None;self.metrics_at=0;self.metrics_count=0
        self.client=client;self.profile=profile
    def get(self,path):
        if not isinstance(path,str) or not path.startswith('/') or path.startswith('//') or any(c in path for c in ('#','\\','\n','\r')):raise Refusal('invalid_provider_path')
        day=dt.datetime.now(dt.timezone.utc).date().isoformat()
        if path!='/metrics' and (self.metrics_day!=day or time.monotonic()-self.metrics_at>=600 or self.attempts-self.metrics_count>=1000):self.get('/metrics')
        if self.attempts>=self.budget:raise Refusal('request_budget_exhausted')
        self.attempts+=1
        try:result=self.client.get(path,purpose='fixture:'+self.job_id)
        except Exception:raise Refusal('provider_request_failed_counted') from None
        result,encoded=bounded_answer(result,input_cut(self.profile) if self.profile is not None and UTXOS.fullmatch(path) else None)
        self.receipts.append({'endpoint':path,'sha256':hashlib.sha256(encoded).hexdigest(),'bytes':len(encoded),'attempt':self.attempts,'at':dt.datetime.now(dt.timezone.utc).isoformat()})
        if len(self.receipts)>20000:raise Refusal('provider_receipt_unit_limit')
        if path=='/metrics':self.metrics_day=day;self.metrics_at=time.monotonic();self.metrics_count=self.attempts
        return result
    def drain(self):
        r={'source':'blockfrost-cardano','shared_profile':'preprod','shared_accounting_purpose':'fixture:'+self.job_id,'adapter':PREPROD.adapter,'openapi':OPENAPI,'attempts_this_invocation':self.attempts,'requests':self.receipts}
        self.receipts=[];return r


class FixtureHooks:
    """`adapter.update` hooks without a service: the service's verifier; progress fields are recorded in `progress`."""
    def __init__(self):self.progress=[]
    def verify(self,store,generation=None,**kwargs):
        from db_sync_alt.verify.store import verify_store
        return verify_store(store,generation,**kwargs)
    def check_space(self,store):pass
    def update(self,**fields):self.progress.append(fields)
    def sample(self,store,**fields):self.progress.append(fields)
    def stopped(self):return False
    def before_publish(self):
        if self.stopped():raise Refusal('interrupted')


def provider_row(row):
    return {'hash':bytes(row['hash']).hex(),'height':row['height'],
            'previous_block':bytes(row['parent_hash']).hex() if row['parent_hash'] else None,
            'epoch':row['epoch'],'slot':row['slot'],'time':int(dt.datetime.fromisoformat(row['time'].replace('Z','+00:00')).timestamp()),
            'tx_count':row['tx_count']}
