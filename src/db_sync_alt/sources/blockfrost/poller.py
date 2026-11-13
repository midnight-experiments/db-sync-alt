"""Independent counted observations. Only PollBudgetStore writes poll metadata.

The canonical PgStore connection is never shared with this worker. Chain facts,
acquisition jobs, canonical revocation and publication remain writer-owned.
"""
import copy
from dataclasses import dataclass
import datetime as dt
import hashlib
import json
import math
import threading
import time
import uuid

from db_sync_alt.contract import canonical, Refusal
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure
from db_sync_alt.store.database import connect, metrics_marker, save_metrics_marker
from psycopg.types.json import Jsonb

POLL_LOCK = (36, 3)
POLICY = 'poll-20s-10000-v1'


class PollDayChanged(Exception):
    pass


def day_at(now):
    return dt.datetime.fromtimestamp(now, dt.timezone.utc).date().isoformat()


def identity(db):
    return tuple(db.execute('SELECT system_identifier::text,(SELECT oid FROM pg_database WHERE datname=current_database()) FROM pg_control_system()').fetchone())


def owner_binding(store):
    pid, started = store.db.execute('SELECT pid,backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()').fetchone()
    return identity(store.db), pid, started


def _hash(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _same_poll_lineage(row, floor):
    from db_sync_alt.sources.blockfrost.sync import validate_job
    validate_job(row, kind='poll'); validate_job(floor, kind='poll')
    if (any(row.get(k)!=floor.get(k) for k in ('job_id','kind','day','budget','created_at'))
            or row['attempts']<floor['attempts']
            or row.get('reserved_at',0)<floor.get('reserved_at',0)
            or (floor['state'] in {'completed','exhausted'} and row['state']!=floor['state'])
            or (row['attempts']==floor['attempts'] and row.get('retry_at',0)<floor.get('retry_at',0))):
        raise LifecycleFailure('invalid_sync_job')


def _policy_binding(db, key, row):
    """The day's policy marker binds the job to the database it was created in."""
    name='poll-budget-policy:'+row['day']
    found=db.execute('SELECT value FROM chain.metadata WHERE key=%s',(name,)).fetchone()
    if found is None:raise LifecycleFailure('invalid_sync_job')
    m=found[0];initial=m.get('initial')
    if (not isinstance(initial,dict) or m.get('policy')!=POLICY or len(m.get('physical',[]))!=2
            or m.get('key')!=key or row['budget']!=10000 or initial.get('budget')!=10000
            or row['job_id']!=initial.get('job_id') or row['created_at']!=initial.get('created_at')
            or row.get('day')!=initial.get('day') or row['attempts']<initial.get('attempts',10001)
            or initial.get('attempts')!=0 or initial.get('state')!='active' or m.get('initial_sha256')!=_hash(initial)):
        raise LifecycleFailure('invalid_sync_job')
    _same_poll_lineage(row,initial)
    # A single append-only chain, rooted at the original immutable policy. Every
    # restore adds one receipt after comparing the accepted inventory.
    ref={'key':name,'sha256':_hash(m)};physical=tuple(m['physical']);floor=initial
    receipts=dict(db.execute('SELECT key,value FROM chain.metadata WHERE key LIKE %s',
                            ('poll-budget-restore:'+row['day']+':%',)).fetchall())
    while receipts:
        children=[(k,v) for k,v in receipts.items() if v.get('parent')==ref]
        if len(children)!=1:raise LifecycleFailure('invalid_sync_job')
        name,r=children[0];saved=r.get('row')
        current=r.get('physical',[])
        if (r.get('version')!=1 or r.get('policy')!=POLICY or r.get('key')!=key
                or r.get('source_physical')!=list(physical) or len(current)!=2
                or name!='poll-budget-restore:'+row['day']+':'+str(current[0])+':'+str(current[1])
                or not isinstance(saved,dict) or r.get('row_sha256')!=_hash(saved)
                or not isinstance(r.get('manifest_sha256'),str) or len(r['manifest_sha256'])!=64
                or any(c not in '0123456789abcdef' for c in r['manifest_sha256'])):
            raise LifecycleFailure('invalid_sync_job')
        _same_poll_lineage(saved,floor);_same_poll_lineage(row,saved)
        physical=tuple(current);floor=saved;ref={'key':name,'sha256':_hash(r)}
        del receipts[name]
    return physical,ref


def _admission(db,key,row,physical):
    """Refuse a poll job whose policy chain does not end at this physical database."""
    bound,_=_policy_binding(db,key,row)
    if bound!=physical:raise LifecycleFailure('invalid_sync_job')


def _create_poll_job(db,key,body,physical,now):
    from db_sync_alt.sources.blockfrost.sync import validate_job
    validate_job(body,kind='poll')
    day=body.get('day')
    if (key!='provider-poll-'+day or body['budget']!=10000
            or body['attempts']!=0 or body['state']!='active'):
        raise LifecycleFailure('invalid_sync_job')
    if db.execute('SELECT 1 FROM chain.metadata WHERE key=%s',('poll-budget-policy:'+day,)).fetchone():
        raise LifecycleFailure('invalid_sync_job')
    marker={'policy':POLICY,'physical':physical,'key':key,'initial':body,
            'initial_sha256':_hash(body),'at':now}
    db.execute('INSERT INTO chain.metadata VALUES(%s,%s)',('poll-budget-policy:'+day,Jsonb(marker)))
    if db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s) ON CONFLICT DO NOTHING',(key,Jsonb(body))).rowcount!=1:
        raise LifecycleFailure('invalid_sync_job')


def bind_poll_job(store,key,body,*,clock=time.time):
    """The same poll policy for explicit one-shot polling; never a budget override."""
    store.require_writer()
    with store.db.transaction():
        if not store.db.execute('SELECT pg_try_advisory_xact_lock(%s,%s)',POLL_LOCK).fetchone()[0]:
            raise LifecycleFailure('writer_busy')
        old=store.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s FOR UPDATE',(key,)).fetchone()
        if old:
            if old[0]!=body:raise LifecycleFailure('invalid_sync_job')
            _admission(store.db,key,body,identity(store.db))
        else:_create_poll_job(store.db,key,body,identity(store.db),clock())


def bind_restored_poll_policy(store, manifest_sha256):
    """Restore-only audit append, AFTER exact source inventory equality.

    No online CLI or normal-startup rebind exists. The original job and marker
    remain byte-for-byte unchanged, including spent calls/backoff/history.
    """
    from db_sync_alt.store.database import OWNER,RESTORE_GUARD
    store.require_writer()
    db=store.db
    if (not isinstance(manifest_sha256,str) or len(manifest_sha256)!=64
            or any(c not in '0123456789abcdef' for c in manifest_sha256)
            or db.execute('SELECT migration_state FROM chain.service_state WHERE id=1').fetchone()!=('restoring',)
            or not db.execute('SELECT to_regclass(%s)',(RESTORE_GUARD+'.identity',)).fetchone()[0]):
        raise LifecycleFailure('invalid_sync_job')
    if db.execute('SELECT owner,manifest_sha256 FROM '+RESTORE_GUARD+'.identity').fetchall()!=[(OWNER,manifest_sha256)]:
        raise LifecycleFailure('invalid_sync_job')
    physical=identity(db);added=[]
    with db.transaction():
        if not db.execute('SELECT pg_try_advisory_xact_lock(%s,%s)',POLL_LOCK).fetchone()[0]:
            raise LifecycleFailure('writer_busy')
        rows=db.execute("SELECT id,body FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%' ORDER BY id FOR UPDATE").fetchall()
        for key,row in rows:
            if row.get('budget')!=10000:continue  # only 10,000-request policy jobs carry a physical binding
            source,parent=_policy_binding(db,key,row)
            name='poll-budget-restore:'+row['day']+':'+str(physical[0])+':'+str(physical[1])
            if source==physical or db.execute('SELECT 1 FROM chain.metadata WHERE key=%s',(name,)).fetchone():
                raise LifecycleFailure('invalid_sync_job')
            receipt={'version':1,'policy':POLICY,'key':key,'source_physical':list(source),
                     'physical':list(physical),'parent':parent,'manifest_sha256':manifest_sha256,
                     'row':row,'row_sha256':_hash(row)}
            db.execute('INSERT INTO chain.metadata VALUES(%s,%s)',(name,Jsonb(receipt)))
            added.append({'key':name,'sha256':_hash(receipt),'job_id':row['job_id'],'attempts':row['attempts']})
    return {'copied_inventory_verified_before_append':True,'accounting_rows_unchanged':True,
            'manifest_sha256':manifest_sha256,'physical':list(physical),'added_metadata':added}


class PollBudgetStore:
    """Narrow metadata API; no canonical store/mutation methods are exposed."""
    def __init__(self, service, binding, *, conninfo='', clock=time.time):
        self.db = connect(service, conninfo=conninfo)
        self.binding, self.clock, self.previous = binding, clock, {}
        try:
            self.db.execute("SET statement_timeout='2000ms'")
            self.db.execute("SET lock_timeout='1000ms'")
            if identity(self.db) != binding[0]:
                raise LifecycleFailure('backend_identity_mismatch')
            if not self.db.execute('SELECT pg_try_advisory_lock(%s,%s)', POLL_LOCK).fetchone()[0]:
                raise LifecycleFailure('writer_busy')
            self._owner()
            row = self.db.execute("SELECT max(id) FROM chain.sync_jobs WHERE id LIKE 'provider-poll-%'").fetchone()[0]
            self.last_day = row.removeprefix('provider-poll-') if row else None
        except BaseException:
            self.db.close()
            raise

    def _owner(self):
        if not self.db.execute('''SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
            WHERE l.locktype='advisory' AND l.classid=36 AND l.objid=1 AND l.objsubid=2 AND l.granted
            AND l.pid=%s AND a.backend_start::text=%s AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()))''', self.binding[1:]).fetchone()[0]:
            raise LifecycleFailure('database_unavailable')

    def _key(self, key):
        today = day_at(self.clock())
        if self.last_day and today < self.last_day:
            raise LifecycleFailure('invalid_sync_job')
        if key != 'provider-poll-' + today:
            if key.startswith('provider-poll-') and key[14:] < today:
                raise PollDayChanged()
            raise LifecycleFailure('invalid_sync_job')
        self.last_day = today
        return today

    def sync_job(self, key):
        self._key(key); self._owner()
        row = self.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s', (key,)).fetchone()
        value = row[0] if row else None
        if value is not None and value.get('budget')==10000:
            _admission(self.db,key,value,self.binding[0])
        self.previous[key] = copy.deepcopy(value)
        return value

    def save_sync_job(self, key, body):
        from db_sync_alt.sources.blockfrost.sync import validate_job
        day = self._key(key); self._owner()
        validate_job(body, kind='poll')
        if body.get('day') != day or body['budget'] != 10000 or key not in self.previous:
            raise LifecycleFailure('invalid_sync_job')
        old = self.previous[key]
        if old:
            validate_job(old, kind='poll')
            if (old['job_id'] != body['job_id'] or old['budget'] != body['budget']
                    or old['attempts'] > body['attempts'] or old['created_at'] != body['created_at']
                    or body.get('reserved_at',0) < old.get('reserved_at',0)
                    or (old['state'] in {'completed','exhausted'} and body['state'] == 'active')):
                raise LifecycleFailure('invalid_sync_job')
            changed = self.db.execute('UPDATE chain.sync_jobs SET body=%s WHERE id=%s AND body=%s',
                                      (Jsonb(body), key, Jsonb(old))).rowcount
        else:
            if body['attempts']!=0 or body['state']!='active':raise LifecycleFailure('invalid_sync_job')
            with self.db.transaction():
                _create_poll_job(self.db,key,body,self.binding[0],self.clock())
                changed=1
        if changed != 1:
            raise LifecycleFailure('invalid_sync_job')
        self.previous[key] = copy.deepcopy(body)

    def metrics_marker(self):
        return metrics_marker(self.db)

    def save_metrics_marker(self, body, expected=None):
        self._owner()
        save_metrics_marker(self.db, body, expected)

    def close(self):
        self.db.close()


@dataclass(frozen=True)
class HeadSnapshot:
    sequence: int
    head_json: str
    confirmed_json: str
    receipt_json: str
    observed_at: float
    observed_mono: float
    completed_at: float
    job_id: str
    day: str
    attempts: int

    @property
    def head(self): return json.loads(self.head_json)
    @property
    def confirmed(self): return json.loads(self.confirmed_json)
    @property
    def receipt(self): return json.loads(self.receipt_json)
    def fresh(self, maximum, *, monotonic=time.monotonic):
        age = monotonic() - self.observed_mono
        return math.isfinite(age) and 0 <= age <= maximum


class PollHealth:
    # AccountedProvider's writer-operation instrumentation must not run here.
    def __init__(self, health): self.health = health
    def update(self, *, quota): self.health.update(quota=quota)
    def begin_operation(self, *args, **kwargs): pass
    def end_operation(self): pass


class HeadPoller:
    def __init__(self, store_factory, client_factory, health, stop_event, *, network, interval=20,
                 clock=time.time, monotonic=time.monotonic, on_fatal=None):
        if isinstance(interval,bool) or not isinstance(interval,(int,float)) or not math.isfinite(interval) or interval <= 0:
            raise LifecycleFailure('invalid_sync_configuration')
        self.store_factory,self.client_factory,self.health,self.stop = store_factory,client_factory,health,stop_event
        self.interval,self.clock,self.mono,self.on_fatal,self.network = interval,clock,monotonic,on_fatal,network
        self.condition = threading.Condition(); self.snapshot = None; self.failure = None; self.retry_at = 0
        self.thread = None; self.sequence = 0; self.last_day = None
        self.health.configure_follow('incremental', interval)
        self.previous_start = None; self.max_gap = 0; self.missed_ticks = 0

    def start(self):
        if self.thread is not None: raise LifecycleFailure('startup_failed')
        self.thread = threading.Thread(target=self.run, name='pg-provider-poll', daemon=True)
        self.thread.start(); return self.thread

    def latest(self):
        with self.condition:
            if self.failure: raise self.failure
            return self.snapshot

    def wait(self, sequence, timeout):
        with self.condition:
            if not self.stop.is_set() and not self.failure and (self.snapshot is None or self.snapshot.sequence <= sequence):
                self.condition.wait(timeout)

    def tick(self, store, client):
        from db_sync_alt.sources.blockfrost.sync import AccountedProvider, validate_job, point
        from db_sync_alt.sources.blockfrost.adapter import block_row
        day = day_at(self.clock())
        if self.last_day and day < self.last_day: raise LifecycleFailure('invalid_sync_job')
        self.last_day = day; key = 'provider-poll-' + day
        job = store.sync_job(key)
        if job is None:
            job = dict(job_id=uuid.uuid4().hex, attempts=0,budget=10000,state='active',kind='poll',day=day,
                       created_at=self.clock(),retry_at=0,failure_count=0)
            store.save_sync_job(key,job)
        validate_job(job,kind='poll')
        if job['budget'] != 10000 or job.get('day') != day: raise LifecycleFailure('invalid_sync_configuration')
        provider = AccountedProvider(store,key,job,client,PollHealth(self.health),self.stop,clock=self.clock,network=self.network)
        provider.set_failure_context('poll')
        started, mono = self.clock(), self.mono()
        try:
            head_raw = provider.get('/blocks/latest'); head = block_row(head_raw)
            k = self.network.security
            if head['height'] < k: raise LifecycleFailure('invalid_coverage')
            confirmed_raw = provider.get('/blocks/' + str(head['height']-k)); confirmed=block_row(confirmed_raw)
            if confirmed['height'] != head['height']-k: raise LifecycleFailure('invalid_coverage')
            committed=self.health.snapshot()['committed_target']
            if committed and confirmed['height']<committed['height']:
                provider._wait('provider_unavailable')
        except Exception as error:
            if not (isinstance(error,Refusal) and error.code=='interrupted' and self.stop.is_set()):
                provider.record_failure(error)
            raise
        if self.stop.is_set(): return
        self.sequence += 1
        snapshot = HeadSnapshot(self.sequence,canonical(head_raw),canonical(confirmed_raw),canonical(provider.drain()),
                                started,mono,self.clock(),job['job_id'],day,provider.attempts)
        self.health.observe_provider(point(head),point(confirmed),observed_at=started,observed_mono=mono)
        self.health.poll_error(None)
        with self.condition:
            self.snapshot=snapshot; self.condition.notify_all()

    def run(self):
        store = None; due = self.mono()
        try:
            store = self.store_factory(); client = self.client_factory()
            while not self.stop.is_set():
                delay=max(0,due-self.mono(),self.retry_at-self.clock())
                if self.stop.wait(delay): break
                started_mono, started_at = self.mono(), self.clock()
                gap = 0 if self.previous_start is None else max(0, started_mono-self.previous_start)
                self.previous_start=started_mono; self.max_gap=max(self.max_gap,gap)
                lateness=max(0,started_mono-due)
                timing=dict(started_at=started_at,completed_at=None,start_gap_seconds=gap,
                            max_gap_seconds=self.max_gap,lateness_seconds=lateness,missed_ticks=self.missed_ticks)
                self.health.heartbeat('poll'); self.health.poll_timing(**timing)
                try:
                    self.tick(store,client); self.retry_at=0
                except PollDayChanged:
                    self.retry_at=0
                except TransientSyncFailure as error:
                    self.retry_at=error.retry_at; self.health.poll_error(error.code)
                finally:
                    timing['completed_at']=self.clock(); self.health.poll_timing(**timing)
                now=self.mono()
                skipped=max(0,math.floor((now-due)/self.interval))
                self.missed_ticks+=skipped
                due += (skipped+1)*self.interval
        except Exception as error:
            if not (isinstance(error,Refusal) and error.code=='interrupted' and self.stop.is_set()):
                with self.condition:
                    self.failure=error; self.condition.notify_all()
                if self.on_fatal: self.on_fatal(error)
        finally:
            if store is not None: store.close()
            with self.condition: self.condition.notify_all()

