"""Subprocess-only injection for the real `db-sync-alt service` entry point.

Loaded as sitecustomize from a temporary directory by a unit test. No provider
or database is used; a daemon shuts the real entrypoint down after a retry state.
"""
import os
from pathlib import Path
import signal
import sys
import threading
import time
import types

# The fakes below replace the store, verifier, reader grants and sync driver modules that the
# service imports lazily; exceptions must keep the identity of db_sync_alt.errors.
import db_sync_alt.service.runtime as pg_service
from db_sync_alt.errors import TransientSyncFailure

class ProbeResult:
    status=2;ntuples=1;nfields=1
    def get_value(self,*args):return b'1'

class ProbeConnection:
    nonblocking=1
    def send_query(self,query):
        if os.environ.get('ENTRYPOINT_MODE')=='db_wait_loss':raise pg_service.LifecycleFailure('database_unavailable')
        if os.environ.get('ENTRYPOINT_MODE')=='db_wait_timeout':raise pg_service.LifecycleFailure('database_timeout')
        self.remaining=1
    def flush(self):return 0
    def consume_input(self):pass
    def is_busy(self):return False
    def get_result(self):
        if self.remaining:self.remaining=0;return ProbeResult()
        return None

class Store:
    def __init__(self,*a,**kw):self.db=self;self.info=types.SimpleNamespace(dbname="fixture",user="node_reader",transaction_status=0);self.result=(False,);self.autocommit=True;self.closed=False;self.lock=threading.Lock();self.pgconn=ProbeConnection()
    def __enter__(self):return self
    def __exit__(self,*a):pass
    def close(self):pass
    def require_writer(self):pass
    def execute(self,query,args=(),**kw):
        self.result=(False,)
        return self
    def fetchone(self):return self.result
    def status(self):return {'migration_state':'ready','canonical_ready':0,'revoked':False}
    def state(self):return dict(self.status(),schema_version=4)
    def lifecycle_state(self):return self.state()
    def meta(self,key):return {'network':'preprod'}

pg_store=types.ModuleType('db_sync_alt.store.database')
pg_store.connect=lambda *a,**kw:Store()
pg_store.empty_destination=lambda db:False
pg_store.inspect=lambda *a,**kw:{'migration_state':'ready','canonical_ready':0,'revoked':False}
pg_store.inspect_lifecycle=pg_store.inspect
pg_store.PgStore=Store
pg_store.SCHEMA_VERSION=4
sys.modules['db_sync_alt.store.database']=pg_store
reader=types.ModuleType('db_sync_alt.store.reader')
reader.grant_reader=lambda db,role:{'reader':role}
reader.verify_reader=lambda db,role:{'role':role,'privileges':'node_relations_only','settings':{},'warnings':[]}
sys.modules['db_sync_alt.store.reader']=reader
verify=types.ModuleType('db_sync_alt.verify.store');verify.verify_store=lambda *a,**kw:None;sys.modules['db_sync_alt.verify.store']=verify

psycopg=types.ModuleType('psycopg');psycopg.pq=types.SimpleNamespace(TransactionStatus=types.SimpleNamespace(IDLE=0),ExecStatus=types.SimpleNamespace(TUPLES_OK=2));psycopg.connect=lambda **kw:Store();psycopg.sql=types.SimpleNamespace(SQL=str,Identifier=str);sys.modules['psycopg']=psycopg

class Driver:
    def __init__(self,*a):pass
    def run_once(self,*a):
        mode=os.environ.get('ENTRYPOINT_MODE','retry')
        if mode=='db_loss':raise pg_service.LifecycleFailure('database_unavailable')
        if mode=='permanent':raise pg_service.LifecycleFailure('invalid_identity')
        if mode.startswith('lifecycle:'):raise pg_service.LifecycleFailure(mode.split(':',1)[1])
        # Service must catch this as transient, remain alive, and stop by SIGTERM.
        def finish():
            time.sleep(0.1)
            Path(os.environ['ENTRYPOINT_MARKER']).write_text('transient survived')
            os.kill(os.getpid(),signal.SIGTERM)
        threading.Thread(target=finish,daemon=True).start()
        raise TransientSyncFailure('provider_unavailable',retry_at=time.time()+1)

sync=types.ModuleType('db_sync_alt.sources.blockfrost.sync');sync.SyncDriver=Driver;sys.modules['db_sync_alt.sources.blockfrost.sync']=sync
