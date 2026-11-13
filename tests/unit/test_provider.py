"""Public provider safety/accounting; fake transport only, never live credentials."""
import concurrent.futures
import datetime as dt
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
import db_sync_alt.sources.blockfrost.provider as p
from fixtures.networks import PREPROD
KEY='preprod'+'synthetic0000000000000000'

def ok(path):return 200,{},b'[]'
def process_call(root):p.PortableClient(KEY,root,network=PREPROD,transport=ok).get('/metrics')
def process_crash(root):
    def die(path):os._exit(0)
    p.PortableClient(KEY,root,network=PREPROD,transport=die).get('/metrics')

class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.now=[1793491200.0]
    def tearDown(self):self.tmp.cleanup()
    def client(self,transport=ok,**kwargs):
        return p.PortableClient(KEY,self.root,network=PREPROD,transport=transport,clock=lambda:self.now[0],sleep=lambda n:self.now.__setitem__(0,self.now[0]+n),**kwargs)
    def test_deeply_nested_answers_are_cut_never_failed(self):
        # Anyone can put metadata on chain that nests deeper than Python's JSON decoder can recurse.
        deep=b'['*20000+b']'*20000
        for count,path in enumerate(('/txs/'+'ab'*32+'/metadata','/scripts/datum/'+'cd'*32),1):
            with self.subTest(path=path):
                c=self.client(lambda _:(200,{},b'{"a":'+deep+b',"b":[1,"]}["]}'))
                value=c.get(path);inner=value['a'];above=1  # containers above the cut one
                while not isinstance(inner,p.TooDeep):inner=inner[0];above+=1
                self.assertEqual((above,value['b'],inner),(p.RESPONSE_DEPTH,[1,']}['],{}))
                self.assertEqual(json.dumps(value)[:6],'{"a": ')
                self.assertEqual(c.status()['budget_used'],count)  # one counted request per answer, no retry
        # The key check still reads the decoded strings of a cut answer.
        escaped=('\\u%04x'%ord(KEY[0])+KEY[1:]).encode()
        c=self.client(lambda _:(200,{},b'{"k":"'+escaped+b'","a":'+deep+b'}'))
        with self.assertRaises(p.ProviderError) as error:c.get('/blocks/latest')
        self.assertEqual(str(error.exception),'unsafe_provider_response')
        # Malformed JSON stays a counted failure; shallow answers decode exactly as before.
        for bad in (deep[:-1],b'['+deep+b'}',b'["'+deep):
            with self.assertRaises(p.ProviderError):self.client(lambda _,bad=bad:(200,{},bad)).get('/blocks/latest')
        for text in ('[]','{"a":[1,2.5,{"b":null}],"c":"x\\"]"}','1152921504606847000','[1e+21]'):
            self.assertEqual(p.loads(text.encode()),json.loads(text))
        exact='['*p.RESPONSE_DEPTH+']'*p.RESPONSE_DEPTH
        self.assertEqual(json.dumps(p.loads(exact.encode()),separators=(',',':')),exact)
    def test_shared_adapter_decodes_with_the_bound(self):
        import hashlib
        import shutil
        root=self.root/'shared';(root/'answers').mkdir(parents=True)
        shutil.copy(Path(__file__).resolve().parents[1]/'fixtures'/'shared_client'/'client.py',root/'client.py')
        (root/'profiles.json').write_text(json.dumps({'preprod':{'base_url':PREPROD.blockfrost_base}}))
        deep=b'['*20000+b']'*20000
        answers={'/plain':b'{"a":[1,2.5,null],"b":"]["}','/deep':b'{"a":'+deep+b',"b":1}','/malformed':b'{"a":[1}'}
        for path,body in answers.items():(root/'answers'/hashlib.sha256(path.encode()).hexdigest()).write_bytes(body)
        c=p.SharedClient(str(root),'preprod',network=PREPROD)
        # The module's own settings and usage files decode as before; normal answers equal the standard library's.
        self.assertEqual(c.get('/plain'),json.loads(answers['/plain']))
        self.assertEqual(c.status()['budget_used'],1)
        value=c.get('/deep');inner=value['a'];above=1
        while not isinstance(inner,p.TooDeep):inner=inner[0];above+=1
        self.assertEqual((above,value['b']),(p.RESPONSE_DEPTH,1))
        with self.assertRaises(p.ProviderError) as error:c.get('/malformed')
        self.assertEqual(str(error.exception),'provider_request_failed_counted')
        with self.assertRaises(p.ProviderError):c.get('/missing')
        self.assertEqual(c.status()['budget_used'],4)
        # The bounded module is the standard one except for `loads`.
        module=p.bounded_json()
        self.assertIs(module.dumps,json.dumps);self.assertIs(module.JSONDecodeError,json.JSONDecodeError)
        self.assertEqual(module.loads('[1e+21]'),[1e21]);self.assertEqual(module.loads('{"a":1}',object_pairs_hook=list),[('a',1)])
        with self.assertRaises(json.JSONDecodeError):module.loads(b'['*5000+b'}')
        # Without the bound the external client fails the counted request on the deep answer.
        with patch.object(p,'bounded_json',lambda:json):plain=p.SharedClient(str(root),'preprod',network=PREPROD)
        with self.assertRaises(p.ProviderError):plain.get('/deep')
    def test_status_offline_and_failure_charged_no_retry(self):
        calls=[]
        def fail(path):calls.append(path);raise RuntimeError(KEY)
        c=self.client(fail);self.assertEqual(c.status()['budget_used'],0);self.assertEqual(calls,[])
        with self.assertRaisesRegex(p.ProviderError,'failed_counted') as error:c.get('/blocks/latest')
        self.assertNotIn(KEY,str(error.exception));self.assertEqual(calls,['/blocks/latest']);self.assertEqual(c.status()['budget_used'],1)
    def test_ceiling_utc_rollover_and_durable_old_day(self):
        c=self.client(limit=2);c.get('/metrics');c.get('/metrics')
        with self.assertRaisesRegex(p.ProviderError,'budget'):c.get('/metrics')
        self.now[0]+=86400;c.get('/metrics');self.assertEqual(c.status()['budget_used'],1)
        self.assertEqual(len(json.loads(c.state.read_text())['keys'][c.fingerprint]['days']),2)
    def test_metrics_reconcile_never_decrease_and_count_self(self):
        c=self.client(lambda path:(200,{},json.dumps([{'time':int(self.now[0]),'calls':40000}]).encode()))
        c.get('/metrics');self.assertEqual(c.status()['budget_used'],40001)
        c.transport=ok;c.get('/metrics');self.assertEqual(c.status()['budget_used'],40002)
    def test_cooldown_persisted_across_day_and_http_failure(self):
        c=self.client(lambda path:(429,{'Retry-After':'3600'},b'bad'))
        with self.assertRaises(p.ProviderError):c.get('/metrics')
        other=self.client();self.assertGreater(other.status()['cooldown_until'],self.now[0])
        with self.assertRaisesRegex(p.ProviderError,'cooldown'):other.get('/metrics')
        self.assertEqual(other.status()['budget_used'],1)
    def test_quota_http_and_malformed_response_stay_spent(self):
        c=self.client(lambda path:(402,{},b'bad'))
        with self.assertRaises(p.ProviderError):c.get('/metrics')
        self.assertTrue(c.status()['exhausted'])
    def test_corrupt_and_missing_ledger_fail_closed(self):
        c=self.client();c.status();c.state.write_text('{bad')
        with self.assertRaisesRegex(p.ProviderError,'corrupt'):c.status()
        c.state.unlink()
        with self.assertRaisesRegex(p.ProviderError,'missing'):c.get('/metrics')
    def test_invalid_ledger_fields(self):
        c=self.client();c.status();data=json.loads(c.state.read_text());data['next_request']=float('nan');c.state.write_text(json.dumps(data))
        with self.assertRaisesRegex(p.ProviderError,'corrupt'):c.get('/metrics')
    def test_path_origin_and_secret_guards_before_reservation(self):
        c=self.client()
        for path in ('https://evil.invalid','//evil.invalid','/../x','/%2e%2e/x','/a\\b','/a#x','/a\n','/%2fexample','/x?key='+KEY):
            with self.subTest(path=path),self.assertRaises(p.ProviderError):c.get(path)
        self.assertEqual(c.status()['budget_used'],0)
    def test_redirect_never_followed_and_secret_response_refused(self):
        calls=[]
        def redirect(path):calls.append(path);return 302,{'Location':'https://evil.invalid'},b''
        c=self.client(redirect)
        with self.assertRaises(p.ProviderError):c.get('/metrics')
        self.assertEqual(calls,['/metrics'])
        c.transport=lambda path:(200,{},json.dumps(KEY).encode())
        with self.assertRaises(p.ProviderError) as error:c.get('/blocks/latest')
        self.assertNotIn(KEY,str(error.exception));self.assertEqual(c.status()['budget_used'],2)
    def test_escaped_response_secret_refused_and_optimized_ledger_checked(self):
        escaped=''.join('\\u%04x'%ord(ch) for ch in KEY)
        c=self.client(lambda path:(200,{},('{"nested":{"'+escaped+'":"value"}}').encode()))
        with self.assertRaises(p.ProviderError):c.get('/blocks/latest')
        data=json.loads(c.state.read_text());data['keys'][c.fingerprint]['days'][c._day()]=-1;c.state.write_text(json.dumps(data))
        code="from db_sync_alt.sources.blockfrost.provider import PortableClient,ProviderError; import sys; from db_sync_alt.networks import network_profile; c=PortableClient(sys.argv[1],sys.argv[2],network=network_profile('preprod')); c.status()"
        result=subprocess.run([sys.executable,'-O','-c',code,KEY,str(self.root)],capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0);self.assertIn('provider_ledger_corrupt',result.stderr);self.assertNotIn(KEY,result.stderr)
    def test_real_http_client_pins_host_and_does_not_follow_redirect(self):
        class Response:
            status=301
            def read(self,size):return b''
            def getheaders(self):return [('Location','https://evil.invalid')]
        class Connection:
            def __init__(self,*args,**kwargs):self.args=args;connections.append(self)
            def request(self,*args,**kwargs):self.request_args=args
            def getresponse(self):return Response()
            def close(self):pass
        connections=[];c=self.client();c.transport=c._https
        with patch.object(p.http.client,'HTTPSConnection',Connection),self.assertRaises(p.ProviderError):c.get('/metrics')
        self.assertEqual(len(connections),1);self.assertEqual(connections[0].args,('cardano-preprod.blockfrost.io',));self.assertEqual(connections[0].request_args,('GET','/api/v0/metrics'))
    def test_threads_share_lock_and_eight_per_second(self):
        times=[]
        def call(_):
            c=p.PortableClient(KEY,self.root,network=PREPROD,transport=lambda path:(times.append(time.monotonic()) or (200,{},b'[]')))
            c.get('/metrics')
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:list(pool.map(call,range(5)))
        self.assertEqual(p.PortableClient(KEY,self.root,network=PREPROD).status()['budget_used'],5)
        self.assertTrue(all(b-a>=.115 for a,b in zip(times,times[1:])),times)
    def test_processes_and_crash_reservations_survive(self):
        ctx=multiprocessing.get_context('fork');jobs=[ctx.Process(target=process_call,args=(self.root,)) for _ in range(3)]
        for job in jobs:job.start()
        for job in jobs:job.join(10);self.assertEqual(job.exitcode,0)
        job=ctx.Process(target=process_crash,args=(self.root,));job.start();job.join(10);self.assertEqual(job.exitcode,0)
        state=p.PortableClient(KEY,self.root,network=PREPROD).status()
        self.assertEqual(state['budget_used'],4);self.assertGreater(state['cooldown_until'],time.time())
    def test_strictest_alias_budget_persists_and_permissions(self):
        c=self.client(limit=2);c.get('/metrics');self.client(limit=50000).get('/metrics')
        with self.assertRaises(p.ProviderError):self.client(limit=50000).get('/metrics')
        self.root.chmod(0o755)
        with self.assertRaises(p.ProviderError):self.client()
    def test_initialization_interruption_cannot_reset_ledger(self):
        c=self.client()
        real=p._atomic
        def interrupt(path,value):
            if path.name=='usage.json':raise OSError('synthetic interruption')
            return real(path,value)
        with patch.object(p,'_atomic',side_effect=interrupt),self.assertRaises(OSError):c.status()
        self.assertTrue((self.root/'.initialized').exists())
        with self.assertRaisesRegex(p.ProviderError,'missing'):c.get('/metrics')
    def test_service_retries_preserve_sanitized_http_classification(self):
        from fixtures.sync import Store,health
        from db_sync_alt.sources.blockfrost.sync import AccountedProvider
        from db_sync_alt.errors import TransientSyncFailure,LifecycleFailure
        from db_sync_alt.contract import Refusal
        for code,path,exception in [(429,'/blocks/latest',TransientSyncFailure),(503,'/blocks/latest',TransientSyncFailure),(404,'/epochs/123',Refusal),(403,'/blocks/latest',LifecycleFailure)]:
            with self.subTest(code=code),tempfile.TemporaryDirectory() as tmp:
                client=p.PortableClient(KEY,tmp,network=PREPROD,transport=lambda path,code=code:(code,{},b'{}'),clock=lambda:self.now[0],sleep=lambda seconds:None)
                store=Store();day=dt.datetime.fromtimestamp(self.now[0],dt.timezone.utc).date().isoformat()
                store.metrics={'version':1,'day':day,'used':0,'counted':0}  # reconciled already: no /metrics first
                job={'job_id':'synthetic','attempts':0,'budget':10,'state':'active','kind':'poll','created_at':self.now[0]}
                provider=AccountedProvider(store,'job',job,client,health(),threading.Event(),clock=lambda:self.now[0],network=PREPROD)
                with self.assertRaises(exception):provider.get(path)
                self.assertEqual(client.status()['budget_used'],1);self.assertEqual(store.jobs['job']['attempts'],1)
                if code in (429,503):self.assertGreater(store.jobs['job']['retry_at'],self.now[0])
    def test_optional_get_returns_none_for_provider_status_404(self):
        from fixtures.sync import Store,health
        from db_sync_alt.sources.blockfrost.sync import AccountedProvider
        from db_sync_alt.errors import TransientSyncFailure,LifecycleFailure
        day=dt.datetime.fromtimestamp(self.now[0],dt.timezone.utc).date().isoformat()
        def accounted(client):
            job={'job_id':'synthetic','attempts':0,'budget':10,'state':'active','kind':'poll','created_at':self.now[0]}
            store=Store();store.metrics={'version':1,'day':day,'used':0,'counted':0}  # reconciled already: no /metrics first
            return store,AccountedProvider(store,'job',job,client,health(),threading.Event(),clock=lambda:self.now[0],network=PREPROD)
        class StatusOnly:
            """Raises the provider client's error with an HTTP status and no machine text."""
            def __init__(self,code):self.code=code;self.calls=[]
            def status(self):return {'budget_used':len(self.calls),'daily_limit':50000}
            def get(self,path,**kwargs):
                self.calls.append(path);raise p.ProviderError('provider_request_failed_counted',http_status=self.code)
        with tempfile.TemporaryDirectory() as tmp:
            portable=p.PortableClient(KEY,tmp,network=PREPROD,transport=lambda path:(404,{},b'{}'),clock=lambda:self.now[0],sleep=lambda seconds:None)
            for client in (portable,StatusOnly(404)):
                with self.subTest(client=type(client).__name__):
                    store,provider=accounted(client)
                    self.assertIsNone(provider.get_optional('/blocks/5362560'))
                    self.assertEqual(client.status()['budget_used'],1);self.assertEqual(store.jobs['job']['attempts'],1)
                    self.assertFalse(store.jobs['job'].get('retry_at'))
                    with self.assertRaises(LifecycleFailure) as error:provider.get('/blocks/5362560')
                    self.assertEqual(error.exception.code,'provider_request_rejected')
                    self.assertEqual(client.status()['budget_used'],2);self.assertEqual(store.jobs['job']['attempts'],2)
        # Quota and cooldown answers are classified first and are never a missing block.
        store,provider=accounted(StatusOnly(429))
        with self.assertRaises(TransientSyncFailure) as error:provider.get_optional('/blocks/5362560')
        self.assertEqual(error.exception.code,'provider_unavailable');self.assertGreater(store.jobs['job']['retry_at'],self.now[0])
    def test_timeout_connection_and_shared_errors_are_sanitized(self):
        from fixtures.sync import Store,health
        from db_sync_alt.sources.blockfrost.sync import AccountedProvider
        from db_sync_alt.errors import TransientSyncFailure
        for exception,expected in [(TimeoutError(KEY),'provider_timeout'),(ConnectionError(KEY),'provider_unavailable')]:
            with tempfile.TemporaryDirectory() as tmp:
                def fail(path):raise exception
                c=p.PortableClient(KEY,tmp,network=PREPROD,transport=fail,clock=lambda:self.now[0])
                day=dt.datetime.fromtimestamp(self.now[0],dt.timezone.utc).date().isoformat()
                job={'job_id':'synthetic','attempts':0,'budget':10,'state':'active','kind':'poll','created_at':self.now[0]}
                store=Store();store.metrics={'version':1,'day':day,'used':0,'counted':0}  # reconciled already: no /metrics first
                provider=AccountedProvider(store,'job',job,c,health(),threading.Event(),clock=lambda:self.now[0],network=PREPROD)
                with self.assertRaises(TransientSyncFailure) as error:provider.get('/blocks/latest')
                self.assertEqual(store.jobs['job']['error_code'],expected);self.assertNotIn(KEY,str(error.exception))
        shared=object.__new__(p.SharedClient)
        class Client:
            def get(self,*args,**kwargs):raise RuntimeError('HTTP 429; '+KEY)
        shared.client=Client()
        with self.assertRaises(p.ProviderError) as error:shared.get('/metrics')
        self.assertEqual(error.exception.http_status,429);self.assertNotIn(KEY,str(error.exception))
    def test_shared_adapter_preserves_external_client_and_cooldown(self):
        class Lock:
            def __enter__(self):pass
            def __exit__(self,*args):pass
        class Client:
            base=PREPROD.blockfrost_base;directory=Path('/synthetic')
            def __init__(self,profile,root):self.profile=profile;self.root=root
            def status(self):return {'budget_used':7,'daily_limit':50000}
            def _load(self):return {'cooldown_until':123}
            def get(self,path,purpose):return [path,purpose]
        class Module:pass
        module=Module();module.Client=Client;module.locked=lambda path:Lock()
        with patch.object(p,'external_module',return_value=module):
            c=p.SharedClient('/synthetic',network=PREPROD);self.assertEqual(c.client.profile,'preprod');self.assertEqual(c.status()['cooldown_until'],123);self.assertEqual(c.get('/metrics',purpose='test'),['/metrics','test'])
        self.assertEqual(c.client.root,Path('/synthetic'))

if __name__=='__main__':unittest.main()
