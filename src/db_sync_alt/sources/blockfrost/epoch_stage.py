"""Private, owner-bound preparation records; no canonical fact mutation API."""
import copy
import base64
import json
import zlib
import time
import uuid
from psycopg.types.json import Jsonb

from db_sync_alt.contract import canonical, digest
from db_sync_alt.errors import LifecycleFailure
from db_sync_alt.store.database import connect
from db_sync_alt.sources.blockfrost.poller import identity
from db_sync_alt.sources.blockfrost.epoch_pools import PoolCollector, MAX_TRANSCRIPT_BYTES, refuse

LOCK = (36, 4)
VERSION = 1
BUDGET = 8000
PREFIX = 'epoch-preparation-v1:'
ZERO = '0' * 64


def job_key(epoch):
    return 'provider-epoch-v1-' + str(epoch)


def prefix(epoch):
    return PREFIX + str(epoch) + ':'


BLOB_KIND = 'epoch-stage-blob-v1'
CHUNK_KIND = 'epoch-stage-chunk-v1'
CHUNK_BYTES = 24000


def _decode_blob(key, head, rows):
    if (not isinstance(head, dict) or head.get('kind') != BLOB_KIND or head.get('version') != VERSION
            or type(head.get('chunks')) is not int or not 1 <= head['chunks'] <= 2000
            or type(head.get('bytes')) is not int or not 0 <= head['bytes'] <= MAX_TRANSCRIPT_BYTES):
        refuse('epoch_preparation_stage_invalid')
    names = [key + '~' + str(number).zfill(5) for number in range(head['chunks'])]
    if {name for name in rows if name.startswith(key + '~')} != set(names):
        refuse('epoch_preparation_stage_invalid')
    compressed = []
    try:
        for name in names:
            chunk = rows[name]
            if chunk.get('kind') != CHUNK_KIND:
                refuse('epoch_preparation_stage_invalid')
            compressed.append(base64.b64decode(chunk['data'], validate=True))
        decoder = zlib.decompressobj()
        raw = decoder.decompress(b''.join(compressed), MAX_TRANSCRIPT_BYTES + 1)
        if (not decoder.eof or decoder.unused_data or decoder.unconsumed_tail or len(raw) != head['bytes']
                or len(raw) > MAX_TRANSCRIPT_BYTES or canonical(json.loads(raw)).encode() != raw
                or digest(json.loads(raw)) != head.get('sha256')):
            refuse('epoch_preparation_stage_invalid')
        return json.loads(raw)
    except (KeyError, ValueError, TypeError, zlib.error):
        refuse('epoch_preparation_stage_invalid')


def data_get(db, key):
    row = db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s', (key,)).fetchone()
    if row is None:
        return None
    rows = dict(db.execute('SELECT id,body FROM chain.sync_jobs WHERE id LIKE %s', (key + '~%',)).fetchall())
    return _decode_blob(key, row[0], rows)


def data_put(db, key, value):
    raw = canonical(value).encode()
    if len(raw) > MAX_TRANSCRIPT_BYTES or len(key) > 220:
        refuse('epoch_preparation_size_limit')
    compressed = zlib.compress(raw, 1)
    chunks = [compressed[start:start + CHUNK_BYTES] for start in range(0, len(compressed), CHUNK_BYTES)]
    head = {'kind': BLOB_KIND, 'version': VERSION, 'chunks': len(chunks), 'bytes': len(raw), 'sha256': digest(value)}
    with db.transaction():
        db.execute('DELETE FROM chain.sync_jobs WHERE id LIKE %s', (key + '~%',))
        db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s) ON CONFLICT(id) DO UPDATE SET body=excluded.body', (key, Jsonb(head)))
        with db.cursor() as cur:
            cur.executemany('INSERT INTO chain.sync_jobs VALUES(%s,%s)',
                [(key + '~' + str(number).zfill(5), Jsonb({'kind': CHUNK_KIND, 'data': base64.b64encode(chunk).decode()})) for number, chunk in enumerate(chunks)])


def data_scan(db, start):
    rows = dict(db.execute('SELECT id,body FROM chain.sync_jobs WHERE id LIKE %s ORDER BY id', (start + '%',)).fetchall())
    result = []
    for key, value in sorted(rows.items()):
        if '~' not in key:
            result.append((key, _decode_blob(key, value, rows)))
    if (any(not isinstance(value, dict) or value.get('kind') not in (BLOB_KIND, CHUNK_KIND) for value in rows.values())
            or any('~' in key and key.rsplit('~', 1)[0] not in rows for key in rows)):
        refuse('epoch_preparation_stage_invalid')
    return result


class EpochStageStore:
    """A separate operational connection fenced to one live canonical owner."""
    def __init__(self, service, binding, profile, *, conninfo='', clock=time.time):
        self.db = connect(service, conninfo=conninfo)
        self.binding, self.profile, self.clock = binding, profile, clock
        self.previous = {}
        try:
            self.db.execute("SET statement_timeout='2000ms'")
            self.db.execute("SET lock_timeout='1000ms'")
            if identity(self.db) != binding[0]:
                raise LifecycleFailure('backend_identity_mismatch')
            if not self.db.execute('SELECT pg_try_advisory_lock(%s,%s)', LOCK).fetchone()[0]:
                raise LifecycleFailure('writer_busy')
            self._owner()
        except BaseException:
            self.close()
            raise

    def close(self):
        self.db.close()

    def _owner(self):
        if not self.db.execute('''SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
            WHERE l.locktype='advisory' AND l.classid=36 AND l.objid=1 AND l.objsubid=2 AND l.granted
            AND l.pid=%s AND a.backend_start::text=%s AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()))''', self.binding[1:]).fetchone()[0]:
            raise LifecycleFailure('database_unavailable')

    def _meta(self, key):
        return data_get(self.db, key)

    def _put(self, key, value):
        data_put(self.db, key, value)

    def _policy(self, key, row):
        from db_sync_alt.sources.blockfrost.sync import validate_job
        validate_job(row, kind='epoch')
        policy = self._meta(prefix(row['epoch']) + 'policy')
        if (key != job_key(row['epoch']) or not isinstance(policy, dict) or policy.get('version') != VERSION
                or policy.get('physical') != list(self.binding[0]) or policy.get('profile_sha256') != digest(self.profile.identity)
                or policy.get('source_identity') != self.profile.network.source_identity
                or policy.get('initial_sha256') != digest(policy.get('initial'))):
            refuse('epoch_preparation_stage_invalid')
        initial = policy['initial']
        if not isinstance(initial, dict):
            refuse('epoch_preparation_stage_invalid')
        if (any(row.get(k) != initial.get(k) for k in ('job_id', 'kind', 'epoch', 'profile_sha256', 'budget', 'created_at'))
                or row['attempts'] < initial['attempts'] or initial['attempts'] != 0 or initial['state'] != 'active'):
            refuse('epoch_preparation_stage_invalid')
        return policy

    def sync_job(self, key):
        self._owner()
        found = self.db.execute('SELECT body FROM chain.sync_jobs WHERE id=%s', (key,)).fetchone()
        row = found[0] if found else None
        if row is not None:
            self._policy(key, row)
            self._reservations(row)
        self.previous[key] = copy.deepcopy(row)
        return row

    def _reservations(self, row):
        records = data_scan(self.db, prefix(row['epoch']) + 'reservation:')
        if len(records) != row['attempts']:
            refuse('epoch_preparation_stage_invalid')
        root = ZERO
        for number, (key, record) in enumerate(records, 1):
            if (key != prefix(row['epoch']) + 'reservation:' + str(number).zfill(5)
                    or record.get('version') != VERSION or record.get('attempt') != number
                    or record.get('job_id') != row['job_id'] or record.get('budget') != row['budget']):
                refuse('epoch_preparation_stage_invalid')
            root = digest({'previous': root, 'reservation': record})
        if root != row.get('reservation_root'):
            refuse('epoch_preparation_stage_invalid')

    def save_sync_job(self, key, body):
        self._owner()
        if self.db.info.transaction_status.name != 'IDLE' or len(canonical(body).encode()) > 65536:
            refuse('epoch_preparation_stage_invalid')
        self._policy(key, body)
        old = self.previous.get(key)
        if (old is None or any(old.get(k) != body.get(k) for k in ('job_id', 'epoch', 'budget', 'kind', 'created_at', 'profile_sha256'))
                or old['attempts'] > body['attempts'] or body.get('reserved_at', 0) < old.get('reserved_at', 0)
                or (old['state'] in ('completed', 'exhausted') and body['state'] != old['state'])):
            refuse('epoch_preparation_stage_invalid')
        delta = body['attempts'] - old['attempts']
        if delta not in (0, 1) or body.get('reservation_root') != old.get('reservation_root'):
            refuse('epoch_preparation_stage_invalid')
        reservation = None
        if delta:
            reservation = {'version': VERSION, 'job_id': body['job_id'], 'attempt': body['attempts'],
                           'budget': body['budget'], 'at': body.get('reserved_at')}
            body['reservation_root'] = digest({'previous': old['reservation_root'], 'reservation': reservation})
        with self.db.transaction():
            if self.db.execute('UPDATE chain.sync_jobs SET body=%s WHERE id=%s AND body=%s', (Jsonb(body), key, Jsonb(old))).rowcount != 1:
                refuse('epoch_preparation_stage_invalid')
            if reservation is not None:
                key_name = prefix(body['epoch']) + 'reservation:' + str(body['attempts']).zfill(5)
                if self._meta(key_name) is not None:
                    refuse('epoch_preparation_stage_invalid')
                self._put(key_name, reservation)
        self.previous[key] = copy.deepcopy(body)

    def ensure(self, epoch):
        self._owner()
        if type(epoch) is not int or not 0 <= epoch < 2**31 or self.profile.network.network != 'mainnet':
            refuse('epoch_preparation_unsupported')
        key = job_key(epoch)
        old = self.sync_job(key)
        manifest = self._meta(prefix(epoch) + 'manifest')
        if old is not None:
            if manifest is None:
                refuse('epoch_preparation_stage_invalid')
            return old
        high = self._meta(PREFIX + 'highest-epoch')
        if high is not None and (type(high) is not int or high < 0):
            refuse('epoch_preparation_stage_invalid')
        if manifest is not None or self._meta(prefix(epoch) + 'policy') is not None or (high is not None and epoch <= high):
            refuse('epoch_preparation_stage_invalid')
        row = dict(job_id=uuid.uuid4().hex, kind='epoch', epoch=epoch, profile_sha256=digest(self.profile.identity),
                   budget=BUDGET, attempts=0, state='active', created_at=self.clock(), retry_at=0, failure_count=0, reservation_root=ZERO)
        marker = {'version': VERSION, 'physical': list(self.binding[0]), 'profile_sha256': digest(self.profile.identity),
                  'source_identity': self.profile.network.source_identity, 'initial': row, 'initial_sha256': digest(row)}
        manifest = {'version': VERSION, 'epoch': epoch, 'profile_sha256': digest(self.profile.identity), 'state': 'collecting',
                    'job_id': row['job_id'], 'records': 0, 'bytes': 0, 'root': ZERO, 'result_sha256': None}
        with self.db.transaction():
            if self.db.execute('SELECT 1 FROM chain.sync_jobs WHERE id=%s', (key,)).fetchone():
                refuse('epoch_preparation_stage_invalid')
            self._put(prefix(epoch) + 'policy', marker)
            self._put(prefix(epoch) + 'manifest', manifest)
            self._put(PREFIX + 'highest-epoch', epoch)
            self.db.execute('INSERT INTO chain.sync_jobs VALUES(%s,%s)', (key, Jsonb(row)))
        self.previous[key] = copy.deepcopy(row)
        return row

    def load(self, epoch):
        row = self.sync_job(job_key(epoch))
        manifest = self._meta(prefix(epoch) + 'manifest')
        if (row is None or not isinstance(manifest, dict) or manifest.get('version') != VERSION
                or manifest.get('epoch') != epoch or manifest.get('profile_sha256') != digest(self.profile.identity)
                or manifest.get('job_id') != row['job_id'] or manifest.get('state') not in ('collecting', 'complete', 'adopted', 'failed')):
            refuse('epoch_preparation_stage_invalid')
        records = data_scan(self.db, prefix(epoch) + 'record:')
        if type(manifest.get('records')) is not int or len(records) != manifest['records'] or len(records) > BUDGET:
            refuse('epoch_preparation_stage_invalid')
        root, size, transcript = ZERO, 0, []
        for number, (key, record) in enumerate(records, 1):
            if key != prefix(epoch) + 'record:' + str(number).zfill(5):
                refuse('epoch_preparation_stage_invalid')
            root = digest({'previous': root, 'record': record})
            size += len(canonical(record).encode())
            if size > MAX_TRANSCRIPT_BYTES:
                refuse('epoch_preparation_size_limit')
            transcript.append(record)
        if root != manifest.get('root') or size != manifest.get('bytes'):
            refuse('epoch_preparation_stage_invalid')
        collector = PoolCollector.replay(self.profile, epoch, transcript)
        if collector.complete != (manifest['state'] in ('complete', 'adopted')):
            if manifest['state'] != 'failed':
                refuse('epoch_preparation_stage_invalid')
        if collector.complete and (digest(collector.result()) != manifest.get('result_sha256')
                                   or digest(collector.result()['positive']) != manifest.get('positive_sha256')):
            refuse('epoch_preparation_stage_invalid')
        if transcript and transcript[-1]['provider']['requests'][-1]['attempt'] > row['attempts']:
            refuse('epoch_preparation_stage_invalid')
        return collector, manifest

    def append(self, epoch, old_manifest, record, collector):
        self._owner()
        if old_manifest.get('state') != 'collecting' or len(collector.records) != old_manifest['records'] + 1 or collector.records[-1] != record:
            refuse('epoch_preparation_stage_invalid')
        fresh = {**old_manifest, 'records': len(collector.records), 'bytes': collector.transcript_bytes,
                 'root': digest({'previous': old_manifest['root'], 'record': record})}
        if collector.complete:
            fresh.update(state='complete', result_sha256=digest(collector.result()), positive_sha256=digest(collector.result()['positive']))
        with self.db.transaction():
            self._owner()
            if self._meta(prefix(epoch) + 'manifest') != old_manifest:
                refuse('epoch_preparation_stage_invalid')
            self._put(prefix(epoch) + 'record:' + str(fresh['records']).zfill(5), record)
            self._put(prefix(epoch) + 'manifest', fresh)
        return fresh

    def fail(self, epoch, code):
        self._owner()
        with self.db.transaction():
            manifest = self._meta(prefix(epoch) + 'manifest')
            if not manifest or manifest['state'] != 'collecting':
                return
            self._put(prefix(epoch) + 'manifest', {**manifest, 'state': 'failed', 'error': code})

    def prune(self, current_epoch):
        """Keep two full transcripts. Small spent-budget rows/policies never reset."""
        self._owner()
        keys = [(key, data_get(self.db, key)) for (key,) in self.db.execute("SELECT id FROM chain.sync_jobs WHERE id LIKE %s", (PREFIX + '%:manifest',)).fetchall()]
        for key, manifest in keys:
            if type(manifest.get('epoch')) is not int:
                refuse('epoch_preparation_stage_invalid')
            if manifest.get('state') == 'expired' or manifest['epoch'] >= current_epoch - 1:
                continue
            # Preserve a fail-closed tombstone and all attempt/policy history.
            with self.db.transaction():
                self.db.execute('DELETE FROM chain.sync_jobs WHERE id LIKE %s', (prefix(manifest['epoch']) + 'record:%',))
                self._put(key, {**manifest, 'state': 'expired', 'records': 0, 'bytes': 0, 'prior_root': manifest['root'], 'root': ZERO})
