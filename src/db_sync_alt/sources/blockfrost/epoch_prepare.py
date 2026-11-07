"""Bounded private preparation with ordinary-request priority and fresh adoption."""
import contextlib
import re
import datetime as dt
import threading
import time

from psycopg.types.json import Jsonb
from db_sync_alt.contract import Refusal, digest
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure
from db_sync_alt.sources.blockfrost.adapter import block_row
from db_sync_alt.sources.blockfrost.epoch_pools import header, refuse
from db_sync_alt.sources.blockfrost.epoch_stage import job_key, prefix, data_get, data_put
from db_sync_alt.sources.blockfrost.poller import PollHealth

INTERVAL = 60
INFLIGHT_SECONDS = 30
FOLLOW_DAILY_RESERVE = 48000
SAFETY_RESERVE = 1000


class PreparationYield(Exception):
    pass


class PriorityClient:
    """Process-local priority before HTTP; shared client remains the only sender."""
    def __init__(self, client, stop):
        self.client, self.stop = client, stop
        self.condition = threading.Condition()
        self.active, self.waiting = False, 0
        self.local = threading.local()

    def status(self):
        return self.client.status()

    def get(self, path, purpose):
        if getattr(self.local, 'preparation', False):
            return self.client.get(path, purpose=purpose)
        with self.condition:
            self.waiting += 1
            try:
                while self.active:
                    if self.stop.is_set():
                        raise Refusal('interrupted')
                    self.condition.wait(.1)
                if self.stop.is_set():
                    raise Refusal('interrupted')
                self.active = True
            finally:
                self.waiting -= 1
        try:
            return self.client.get(path, purpose=purpose)
        finally:
            with self.condition:
                self.active = False
                self.condition.notify_all()

    @contextlib.contextmanager
    def preparation_slot(self):
        with self.condition:
            if self.active or self.waiting or self.stop.is_set():
                raise PreparationYield()
            self.active = True
            self.local.preparation = True
        try:
            yield
        finally:
            self.local.preparation = False
            with self.condition:
                self.active = False
                self.condition.notify_all()


def daily_slack(usage, now):
    """Do not spend the epoch's whole cap ahead of ordinary daily demand."""
    if (type(usage.get('budget_used')) is not int or type(usage.get('daily_limit')) is not int
            or not 0 <= usage['budget_used'] <= usage['daily_limit'] <= 50000):
        return False
    until_midnight = 86400 - now % 86400
    ordinary = FOLLOW_DAILY_RESERVE * until_midnight / 86400
    return usage['daily_limit'] - usage['budget_used'] > ordinary + SAFETY_RESERVE


class EpochPreparer:
    def __init__(self, store_factory, client, profile, poller, health, stop, *, clock=time.time, monotonic=time.monotonic,
                 interval=INTERVAL):
        self.factory, self.client, self.profile = store_factory, client, profile
        self.poller, self.health, self.stop = poller, health, stop
        self.clock, self.mono, self.interval = clock, monotonic, interval
        self.lock = threading.RLock()
        self.store = None
        self.thread = None
        self.required_epoch = None
        self.last_started = None
        self.cached = None

    def start(self):
        if self.thread is not None:
            raise LifecycleFailure('startup_failed')
        self.thread = threading.Thread(target=self.run, name='pg-epoch-preparation', daemon=True)
        self.thread.start()
        return self.thread

    def _status(self, epoch, state, *, attempts=None, error=None, recovery=False):
        self.health.preparation(epoch=epoch, state=state, attempts=attempts, error=error, recovery=recovery)

    def _opened(self):
        if self.store is None:
            self.store = self.factory()
        return self.store

    def eligible(self, snapshot, observation):
        if observation is None or not observation.fresh(min(30, self.health.policy.observation_max_age_seconds)):
            return False
        if snapshot.get('operation') is not None or self.stop.is_set():
            return False
        epoch = observation.head.get('epoch')
        recovery = self.required_epoch == epoch
        if not recovery:
            if (not snapshot.get('healthy') or snapshot.get('sync') not in ('idle', 'syncing')
                    or type(snapshot.get('lag', {}).get('blocks')) is not int or snapshot['lag']['blocks'] > 1):
                return False
        else:
            # Explicit boundary recovery: old publication stays non-ready, not a
            # promise to attain near-tip before the missing facts can exist.
            if snapshot.get('lifecycle') != 'ready' or snapshot.get('database', {}).get('reachable') is not True:
                return False
        return daily_slack(self.client.status(), self.clock())

    def _provider(self, store, epoch):
        from db_sync_alt.sources.blockfrost.sync import AccountedProvider
        job = store.sync_job(job_key(epoch))
        if job is None or job['state'] != 'active':
            refuse('epoch_preparation_not_ready')
        return AccountedProvider(store, job_key(epoch), job, self.client, PollHealth(self.health), self.stop,
                                 network=self.profile.network, clock=self.clock)

    def tick(self):
        observation = self.poller.latest()
        snapshot = self.health.snapshot()
        if not self.eligible(snapshot, observation):
            return False
        now = self.mono()
        if self.last_started is not None and now - self.last_started < self.interval:
            return False
        epoch = observation.head['epoch']
        with self.lock:
            store = self._opened()
            job = store.ensure(epoch)
            observed_manifest = store._meta(prefix(epoch) + 'manifest')
            if self.cached is not None and self.cached[0] == epoch and self.cached[2] == observed_manifest:
                collector, manifest = self.cached[1:]
            else:
                collector, manifest = store.load(epoch)
                self.cached = (epoch, collector, manifest)
            if manifest['state'] in ('complete', 'adopted', 'failed'):
                self._status(epoch, manifest['state'], attempts=job['attempts'], error=manifest.get('error'))
                return False
            if job.get('retry_at', 0) > self.clock():
                self._status(epoch, 'backoff', attempts=job['attempts'])
                return False
            if job['state'] == 'exhausted':
                self._status(epoch, 'exhausted', attempts=job['attempts'], error='request_budget_exhausted')
                return False
            if job['budget'] - job['attempts'] < 10:
                self._status(epoch, 'exhausted', attempts=job['attempts'], error='request_budget_exhausted')
                return False  # preserve fresh adoption checks; never roll over this job
            with self.client.preparation_slot():
                # Recheck real lag/freshness after waiting for the DB/local lock.
                if not self.eligible(self.health.snapshot(), self.poller.latest()):
                    return False
                self.last_started = self.mono()
                provider = self._provider(store, epoch)
                job = provider.job
                day = dt.datetime.fromtimestamp(self.clock(), dt.timezone.utc).date().isoformat()
                if (job.get('metrics_day') != day or self.clock() - job.get('metrics_at', 0) >= 600
                        or provider.attempts - job.get('metrics_count', 0) >= 1000):
                    provider.get('/metrics')
                    provider.drain()
                else:
                    collector.step(provider)
                    manifest = store.append(epoch, manifest, collector.records[-1], collector)
                    self.cached = (epoch, collector, manifest)
                self._status(epoch, manifest['state'], attempts=provider.attempts, recovery=self.required_epoch == epoch)
                store.prune(epoch)
                return True

    def run(self):
        try:
            while not self.stop.is_set():
                try:
                    self.tick()
                except PreparationYield:
                    pass
                except TransientSyncFailure as error:
                    self._status(self.required_epoch, 'backoff', error=error.code, recovery=self.required_epoch is not None)
                except Refusal as error:
                    epoch = self.required_epoch
                    observation = self.poller.latest()
                    if observation is not None:
                        epoch = observation.head.get('epoch')
                    if self.store is not None and type(epoch) is int:
                        with self.lock:
                            self.store.fail(epoch, error.code)
                    self._status(epoch, 'failed', error='epoch_preparation_invalid', recovery=self.required_epoch is not None)
                except Exception:
                    self._status(self.required_epoch, 'failed', error='epoch_preparation_invalid', recovery=self.required_epoch is not None)
                    break
                self.stop.wait(1)
        finally:
            with self.lock:
                if self.store is not None:
                    self.store.close()
                    self.store = None

    def published(self, writer):
        current = self.health.snapshot().get('epoch_preparation')
        if not current or current['state'] == 'adopted':
            return
        ready = writer.generation()
        epoch = writer.block_at(ready['id'], ready['target_height'])['epoch']
        if current['epoch'] != epoch:
            return
        with self.lock:
            store = self._opened()
            marker = store._meta(prefix(epoch) + 'manifest')
            if marker is not None and marker['state'] == 'adopted' and writer.epoch_nonce(epoch, ready['id']) is not None:
                job = store.sync_job(job_key(epoch))
                self.required_epoch = None
                self._status(epoch, 'adopted', attempts=job['attempts'], recovery=False)

    def adopt(self, writer, generation, epoch, target):
        """Fresh checks spend the same epoch budget; no future fact is inserted here."""
        with self.lock:
            store = self._opened()
            store.ensure(epoch)
            collector, manifest = store.load(epoch)
            if manifest['state'] != 'complete' or not collector.complete:
                self.required_epoch = epoch
                self._status(epoch, 'waiting-required', error='epoch_preparation_not_ready', recovery=True)
                refuse('epoch_preparation_not_ready')
            result = collector.result()
            provider = self._provider(store, epoch)
            head = block_row(provider.get('/blocks/latest'))
            summary = provider.get('/epochs/' + str(epoch))
            params = provider.get('/epochs/' + str(epoch) + '/parameters')
            target_row = block_row(provider.get('/blocks/' + str(target)))
            original = header(provider.get('/blocks/' + str(result['anchor']['height'])))
            end_head = block_row(provider.get('/blocks/latest'))
            expected = writer.block_at(generation, target)
            if (head['epoch'] != epoch or end_head['epoch'] != epoch or end_head['height'] < head['height']
                    or target > head['height'] - self.profile.security or target_row['height'] != target or target_row['epoch'] != epoch
                    or expected is None or target_row['hash'] != expected['hash'] or target_row['parent_hash'] != expected['parent_hash']
                    or summary.get('epoch') != epoch or summary.get('active_stake') != result['active_stake']
                    or params.get('epoch') != epoch or params.get('nonce') != result['nonce'] or original != result['anchor']):
                refuse('epoch_preparation_adoption_changed')
            proof = {k: result[k] for k in ('version', 'kind', 'semantics', 'semantics_commit', 'profile_sha256', 'network',
                     'source_identity', 'epoch', 'nonce', 'active_stake', 'positive', 'positive_sha256', 'pass_sha256',
                     'inventories_sha256', 'transcript_sha256')}
            proof.update(stage_root=manifest['root'], result_sha256=digest(result))
            stakes = [{'epoch': epoch, 'pool_hash': bytes.fromhex(raw), 'amount': amount} for raw, amount in sorted(result['positive'].items())]
            receipt = {'epoch': epoch, 'source_kind': 'blockfrost-cardano', 'epoch_end': None,
                       'active_stake': result['active_stake'], 'epoch_anchor': {'height': target, 'hash': target_row['hash'].hex(), 'kind': 'confirmed-target'},
                       'preparation': proof, 'provider': provider.drain()}
            self.required_epoch = None
            return {'epoch': epoch, 'nonce': bytes.fromhex(result['nonce'])}, stakes, target_row, receipt


def commit_adoption(writer, epoch, proof):
    """Called INSIDE the writer's fact/checkpoint transaction: commit all or none."""
    writer.require_writer()
    if writer.db.info.transaction_status.name == 'IDLE':
        refuse('epoch_preparation_stage_invalid')
    locked = writer.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s FOR UPDATE', (prefix(epoch) + 'manifest',)).fetchone()
    value = data_get(writer.db, prefix(epoch) + 'manifest') if locked is not None else None
    row = (value,) if value is not None else None
    job = writer.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s FOR UPDATE', (job_key(epoch),)).fetchone()
    if (row is None or job is None or row[0]['state'] != 'complete' or row[0]['root'] != proof['stage_root']
            or row[0]['result_sha256'] != proof['result_sha256'] or row[0].get('positive_sha256') != proof['positive_sha256']
            or digest(proof['positive']) != proof['positive_sha256'] or job[0]['state'] != 'active'):
        refuse('epoch_preparation_stage_invalid')
    observed = dict((bytes(r[0]).hex(), str(r[1])) for r in writer.db.execute(
        'SELECT pool_hash,amount FROM chain.epoch_stakes WHERE epoch=%s AND first_generation<=%s', (epoch, writer.pending()['id'])))
    if observed != proof['positive'] or digest(observed) != proof['positive_sha256']:
        refuse('epoch_preparation_distribution_changed')
    data_put(writer.db, prefix(epoch) + 'manifest', {**row[0], 'state': 'adopted'})
    writer.db.execute('UPDATE chain.sync_jobs SET body=%s WHERE id=%s', (Jsonb({**job[0], 'state': 'completed', 'completed_at': time.time()}), job_key(epoch)))


def verify_epoch_receipt(store, generation, checkpoint):
    """Independent adopted map remains bound to actual canonical epoch facts."""
    receipt = checkpoint['receipt']
    proof = receipt.get('preparation')
    if proof is None:
        return
    from db_sync_alt.sources.blockfrost.epoch_pools import VERSION, SEMANTICS, SEMANTICS_COMMIT, amount
    try:
        epoch = proof['epoch']
        expected = proof['positive']
        if (type(epoch) is not int or checkpoint['unit'] != str(epoch) or receipt.get('epoch') != epoch
                or proof.get('version') != VERSION or proof.get('kind') != 'current-pool-aggregate-two-independent-passes'
                or proof.get('semantics') != SEMANTICS
                or proof.get('semantics_commit') != SEMANTICS_COMMIT or proof.get('network') != 'mainnet'
                or proof.get('profile_sha256') != digest(store.generation(generation)['manifest']['identity'])
                or proof.get('source_identity') != store.network.source_identity
                or not isinstance(expected, dict) or not expected or len(expected) > 10000
                or len(proof.get('pass_sha256', [])) != 2 or len(proof.get('inventories_sha256', [])) != 2
                or proof['inventories_sha256'][0] != proof['inventories_sha256'][1]
                or any(not isinstance(value, str) or re.fullmatch('[0-9a-f]{64}', value) is None
                       for value in [*proof['pass_sha256'], *proof['inventories_sha256'], proof.get('transcript_sha256'),
                                     proof.get('stage_root'), proof.get('result_sha256'), proof.get('nonce')])
                or digest(expected) != proof.get('positive_sha256')):
            refuse('epoch_preparation_stage_invalid')
        if any(len(bytes.fromhex(raw)) != 28 or amount(value) <= 0 for raw, value in expected.items()):
            refuse('epoch_preparation_stage_invalid')
        observed = {bytes(raw).hex(): str(value) for raw, value in store.db.execute(
            'SELECT pool_hash,amount FROM chain.epoch_stakes WHERE epoch=%s AND first_generation<=%s', (epoch, generation))}
        nonce = store.epoch_nonce(epoch, generation)
        marker = data_get(store.db, prefix(epoch) + 'manifest')
        if (observed != expected or sum(map(int, observed.values())) != amount(proof['active_stake'])
                or receipt.get('active_stake') != proof['active_stake'] or nonce is None or nonce['nonce'].hex() != proof['nonce']
                or not isinstance(marker, dict) or marker.get('state') not in ('adopted', 'expired')
                or (marker.get('prior_root') if marker['state'] == 'expired' else marker.get('root')) != proof['stage_root']
                or marker.get('result_sha256') != proof['result_sha256'] or marker.get('positive_sha256') != proof['positive_sha256']):
            refuse('epoch_preparation_stage_invalid')
    except (KeyError, TypeError, ValueError):
        refuse('epoch_preparation_stage_invalid')
