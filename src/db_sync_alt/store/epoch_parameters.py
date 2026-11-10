"""Epoch parameters for a store written before them: the in-place upgrade of a nonce-only store.

midnight-node 2.1.101 reads MAX(epoch_param.max_tx_size) of the cursor block's epoch. A store of
the nonce-only fact format holds every epoch's nonce but no size. `db-sync-alt upgrade` adds the
size of every stored epoch from an authoritative source, never a default:

1. Every stored epoch is read twice, in two complete passes: a configured cardano-db-sync (one
   read-only snapshot per pass, the same validated MAX aggregate acquisition uses) or Blockfrost
   (`GET /epochs/{n}/parameters`, one counted request per epoch and pass, nothing else). Each answer
   must name the epoch, carry the stored nonce and a size 0..2147483647; both passes must agree.
2. Answers are staged as they arrive in one sync job, so an interrupted upgrade continues where it
   stopped; once both passes are staged a rerun makes no request. Every provider request is
   reserved in the job before it is sent and goes through the shared accounted client.
3. One fenced transaction adds the column with the staged values, replaces the stored contract,
   writes the immutable upgrade record, creates the empty bridge metadata family (its coverage
   starts after the upgraded generation), moves the store to schema 5 and verifies it in full. Any
   refusal rolls it back: the store stays the nonce-only schema-4 store it was.

Generations sealed before the upgrade keep their seals, receipts and checkpoints; they are verified
over the columns they were sealed with, and the record binds the sizes added to their epochs.
"""
import hashlib

from db_sync_alt.contract import Refusal, canonical, digest, epoch_size, jsonable

JOB = 'epoch-parameters-upgrade'
VERSION = 1
PURPOSE = 'db-sync-alt:' + JOB
# Refusals that keep the staged answers: the source was unavailable, nothing it answered was wrong.
TRANSIENT = 'epoch_parameters_source_unavailable'
KEEP_STAGED = frozenset({TRANSIENT, 'source_timeout', 'source_query_failed',
                         'provider_accounting_unavailable', 'provider_budget_insufficient'})


def admit(store, source):
    """Before anything is read or changed: a source, and no unpublished generation of the old format."""
    if source is None:
        raise Refusal('epoch_parameters_source_required')
    if store.pending() is not None:
        raise Refusal('upgrade_pending_generation')


def _chain(previous, epoch, evidence):
    return digest([previous, epoch, evidence])


class ProviderEpochSource:
    """Blockfrost through the network's accounted client: one request per epoch and pass."""
    kind = 'blockfrost-cardano'

    def __init__(self, client, network):
        self.client, self.network = client, network

    @property
    def identity(self):
        return {'kind': self.kind, **self.network.source_identity}

    @property
    def counted(self):
        return True

    def check_budget(self, needed):
        """The shared daily budget must cover the requests still to make."""
        try:
            status = self.client.status()
            used, limit = status['budget_used'], status['daily_limit']
        except Exception:
            raise Refusal('provider_accounting_unavailable') from None
        if type(used) is not int or type(limit) is not int or used < 0 or limit < 1:
            raise Refusal('provider_accounting_unavailable')
        if limit - used < needed:
            raise Refusal('provider_budget_insufficient')

    def read(self, epoch):
        try:
            answer = self.client.get('/epochs/' + str(epoch) + '/parameters', purpose=PURPOSE)
        except Exception:
            raise Refusal(TRANSIENT) from None
        if not isinstance(answer, dict) or type(answer.get('epoch')) is not int or answer['epoch'] != epoch:
            raise Refusal('epoch_parameters_unavailable')
        nonce = answer.get('nonce')
        if not isinstance(nonce, str) or len(nonce) != 64:
            raise Refusal('epoch_parameters_unavailable')
        try:
            nonce = bytes.fromhex(nonce)
        except ValueError:
            raise Refusal('epoch_parameters_unavailable') from None
        return nonce, epoch_size(answer.get('max_tx_size')), hashlib.sha256(canonical(answer).encode()).hexdigest()

    def passes(self):
        return _NoSnapshot()


class _NoSnapshot:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class SQLEpochSource:
    """A read-only cardano-db-sync of the network: one snapshot per pass."""
    kind = 'cardano-db-sync-postgresql'

    def __init__(self, source):
        self.source = source

    @property
    def identity(self):
        if getattr(self.source, 'validated_identity', None) is None:
            with self.source.snapshot():
                self.source.check_source()
        return {'kind': self.kind, **self.source.validated_identity}

    @property
    def counted(self):
        return False

    def check_budget(self, needed):
        pass

    def passes(self):
        source = self.source

        class Snapshot:
            def __enter__(inner):
                inner.context = source.snapshot()
                inner.context.__enter__()
                source.check_source()
                return inner

            def __exit__(inner, *exc):
                return inner.context.__exit__(*exc)
        return Snapshot()

    def read(self, epoch):
        row = self.source.epoch_parameters(epoch)
        return row['nonce'], row['max_tx_size'], digest(jsonable(row))


def stored_epochs(store):
    """(epoch, nonce) of every stored epoch, in epoch order."""
    return [(epoch, bytes(nonce)) for epoch, nonce in store.db.execute('SELECT epoch,nonce FROM chain.epoch_nonces ORDER BY epoch')]


def _job_id(store, generation, epochs, source):
    return digest({'job': JOB, 'version': VERSION, 'generation': generation, 'legacy_contract': digest(store.network.legacy_contract),
                   'epochs': [[e, n.hex()] for e, n in epochs], 'source': source.identity})[:32]


def _new_job(job_id, epochs, generation, source):
    # One complete repeat of both passes for interrupted requests; a spent attempt is never returned.
    budget = min(10000, 4 * len(epochs)) if source.counted else 1
    return {'job_id': job_id, 'attempts': 0, 'budget': max(1, budget), 'state': 'active', 'kind': 'epoch-parameters',
            'version': VERSION, 'generation': generation, 'source': source.identity,
            'reads': [{'sizes': {}, 'sha256': '0' * 64}, {'sizes': {}, 'sha256': '0' * 64}]}


def _resumed(store, job_id, generation, source):
    """This upgrade's staged job, or None when it has not started."""
    job = store.sync_job(JOB)
    if job is None or job.get('job_id') != job_id:
        return None
    if (job.get('state') != 'active' or job.get('kind') != 'epoch-parameters' or job.get('version') != VERSION
            or job.get('generation') != generation or job.get('source') != source.identity
            or not isinstance(job.get('reads'), list) or len(job['reads']) != 2):
        raise Refusal('invalid_sync_job')
    return job


def _start(store, job_id, epochs, generation, source):
    """A new staged job; the job of an earlier attempt that never committed is closed first."""
    job = store.sync_job(JOB)
    if job is not None and job.get('state') != 'completed':
        # A job of an earlier attempt that never committed (another ready generation or source):
        # close it, keeping its spent attempts, and start this one.
        store.save_sync_job(JOB, {**job, 'state': 'completed'})
    fresh = _new_job(job_id, epochs, generation, source)
    store.save_sync_job(JOB, fresh)
    return fresh


def _discard(store):
    """A refused answer: the staged job goes, so the store is exactly as before the upgrade began."""
    store.db.execute('DELETE FROM chain.sync_jobs WHERE id=%s', (JOB,))


def read_twice(store, epochs, generation, source):
    """Both complete passes over `epochs`, staged durably; returns {epoch: size} and the job."""
    job_id = _job_id(store, generation, epochs, source)
    job = _resumed(store, job_id, generation, source)
    stored = dict(epochs)
    remaining = 2 * len(epochs) if job is None else sum(1 for read in job['reads'] for epoch, _ in epochs if str(epoch) not in read['sizes'])
    if remaining and source.counted:
        source.check_budget(remaining)
    if job is None:
        job = _start(store, job_id, epochs, generation, source)
    try:
        for number, read in enumerate(job['reads']):
            todo = [epoch for epoch, _ in epochs if str(epoch) not in read['sizes']]
            if not todo:
                continue
            with source.passes():
                for epoch in todo:
                    if source.counted:
                        if job['attempts'] >= job['budget']:
                            # This attempt's requests are spent (the provider ledger keeps the day's
                            # count); an explicit rerun starts the reads over.
                            raise Refusal('request_budget_exhausted')
                        job['attempts'] += 1
                        store.save_sync_job(JOB, job)  # reserved before the request is sent
                    nonce, size, evidence = source.read(epoch)
                    if nonce != stored[epoch]:
                        raise Refusal('epoch_parameters_nonce_mismatch')
                    read['sizes'][str(epoch)] = size
                    read['sha256'] = _chain(read['sha256'], epoch, evidence)
                    store.save_sync_job(JOB, job)
                    store.progress('epoch_parameters_read', read=number + 1, epoch=epoch)
    except Refusal as error:
        if error.code not in KEEP_STAGED:
            _discard(store)
        raise
    first, second = (read['sizes'] for read in job['reads'])
    if first != second:
        _discard(store)
        raise Refusal('epoch_parameters_changed')
    return {epoch: first[str(epoch)] for epoch, _ in epochs}, job


def family_digest(store, generation):
    """The epoch_nonces family of `generation` hashed with its size, as store digests hash a family."""
    from db_sync_alt.store.database import logical_row
    hasher = hashlib.sha256(); count = 0
    for row in store.db.execute('SELECT epoch,nonce,max_tx_size FROM chain.epoch_nonces WHERE first_generation<=%s ORDER BY epoch', (generation,)):
        value = logical_row('epoch_nonces', dict(zip(('epoch', 'nonce', 'max_tx_size'), row)))
        hasher.update((canonical(jsonable(value)) + '\n').encode()); count += 1
    return {'count': count, 'sha256': hasher.hexdigest()}


BRIDGE_METADATA_DDL = (
    'CREATE TABLE chain.tx_metadata(tx_hash public.hash32type PRIMARY KEY REFERENCES chain.transactions(hash),present integer NOT NULL CHECK(present IN (0,1)),json jsonb,first_generation bigint NOT NULL REFERENCES chain.generations(id),CHECK(present=1 OR json IS NULL))',
    'CREATE INDEX delta_tx_metadata ON chain.tx_metadata(first_generation)',
    'CREATE TRIGGER immutable_guard BEFORE INSERT OR UPDATE OR DELETE ON chain.tx_metadata FOR EACH ROW EXECUTE FUNCTION chain.guard_immutable()',
    'CREATE TRIGGER no_truncate BEFORE TRUNCATE ON chain.tx_metadata EXECUTE FUNCTION chain.guard_truncate()',
    'REVOKE ALL ON chain.tx_metadata FROM PUBLIC',
)


def create_bridge_metadata(db):
    """The bridge metadata fact family of a store written before it, with the guards every fact family
    has from schema 2 (a store created by this release has it from its schema)."""
    for statement in BRIDGE_METADATA_DDL:
        db.execute(statement)


def upgrade_epoch_parameters(store, state, source):
    """Schema 4 nonce-only store -> schema 5 with every stored epoch's max_tx_size (see module doc)."""
    from db_sync_alt import __version__
    from db_sync_alt.store.database import EPOCH_UPGRADE, PROJECTION_WRITE, SERVING_LOCK
    from db_sync_alt.store.upgrade import _schema_v5
    from db_sync_alt.verify.store import verify_store
    from psycopg import sql
    from psycopg.types.json import Jsonb
    admit(store, source)
    if state['schema_version'] != 4 or not store.legacy_format:
        raise Refusal('upgrade_requires_ready')
    generation = state['canonical_ready']
    epochs = stored_epochs(store)
    sizes, job = read_twice(store, epochs, generation, source)
    network = store.network
    record = {'version': VERSION, 'generation': generation,
              'legacy_contract_sha256': digest(network.legacy_contract), 'contract_sha256': digest(network.contract),
              'epochs': [[epoch, sizes[epoch]] for epoch, _ in epochs], 'source': source.identity,
              'reads_sha256': [read['sha256'] for read in job['reads']],
              'provider_requests': job['attempts'] if source.counted else 0, 'service_version': __version__}
    try:
        with store.atomic():
            store.db.execute("SET LOCAL lock_timeout='5s'")
            store.db.execute('SELECT pg_advisory_xact_lock(%s,%s)', SERVING_LOCK)
            if store.state() != state or store.pending() is not None or stored_epochs(store) != epochs:
                raise Refusal('upgrade_state_changed')
            store.db.execute('SELECT set_config(%s,%s,true)', (PROJECTION_WRITE, 'on'))
            db = store.db
            db.execute('ALTER TABLE chain.epoch_nonces ADD COLUMN max_tx_size public.word31type')
            # Existing fact rows are immutable; this one fenced statement adds their size, as the
            # schema-3 upgrade adds ledger ordinals.
            db.execute('ALTER TABLE chain.epoch_nonces DISABLE TRIGGER immutable_guard')
            changed = db.execute('UPDATE chain.epoch_nonces e SET max_tx_size=v.size FROM unnest(%s::integer[],%s::integer[]) v(epoch,size) WHERE e.epoch=v.epoch',
                                 ([e for e, _ in epochs], [sizes[e] for e, _ in epochs])).rowcount
            db.execute('ALTER TABLE chain.epoch_nonces ENABLE TRIGGER immutable_guard')
            if changed != len(epochs):
                raise Refusal('upgrade_state_changed')
            db.execute('ALTER TABLE chain.epoch_nonces ALTER COLUMN max_tx_size SET NOT NULL')
            record['epoch_nonces'] = family_digest(store, generation)
            db.execute('ALTER TABLE chain.metadata DISABLE TRIGGER immutable_guard')
            db.execute("UPDATE chain.metadata SET value=%s WHERE key='contract'", (Jsonb(network.contract),))
            db.execute('INSERT INTO chain.metadata VALUES(%s,%s)', (EPOCH_UPGRADE, Jsonb(record)))
            db.execute('ALTER TABLE chain.metadata ENABLE TRIGGER immutable_guard')
            db.execute('DELETE FROM chain.sync_jobs WHERE id=%s', (JOB,))
            # The bridge metadata family starts empty: its coverage begins after this generation.
            create_bridge_metadata(db)
            # Any capability certified over the nonce-only contents is void.
            db.execute('UPDATE chain.integrity_state SET revision=revision+1 WHERE id=1')
            _schema_v5(store)
            db.execute(sql.SQL('ANALYZE {}').format(sql.Identifier('serve', 'epoch_param')))
            store.bind_format()
            report = verify_store(store)
    except BaseException:
        store.bind_format(allow_legacy=True)
        raise
    return {'generation': generation, 'epochs': len(epochs), 'source': source.kind,
            'provider_requests': record['provider_requests'], 'verification': report['verification']}


def verify_record(store):
    """The upgrade record against the stored sizes of the epochs it covers."""
    record = store.epoch_upgrade
    if record is None:
        return
    try:
        network = store.network
        keys = {'version', 'generation', 'legacy_contract_sha256', 'contract_sha256', 'epochs', 'source', 'reads_sha256',
                'provider_requests', 'service_version', 'epoch_nonces'}
        if (set(record) != keys or type(record['version']) is not int or record['version'] != VERSION
                or record['legacy_contract_sha256'] != digest(network.legacy_contract) or record['contract_sha256'] != digest(network.contract)
                or digest(store.meta('contract')) != digest(network.contract) or not isinstance(record['epochs'], list)):
            raise Refusal('epoch_parameters_upgrade_invalid')
        generation = store.generation(record['generation'])
        if generation['status'] != 'ready':
            raise Refusal('epoch_parameters_upgrade_invalid')
        expected = [[epoch, epoch_size(size)] for epoch, size in record['epochs']]
        stored = [[epoch, size] for epoch, size in store.db.execute(
            'SELECT epoch,max_tx_size FROM chain.epoch_nonces WHERE first_generation<=%s ORDER BY epoch', (record['generation'],))]
        if stored != expected or family_digest(store, record['generation']) != record['epoch_nonces']:
            raise Refusal('epoch_parameters_upgrade_invalid')
    except (KeyError, TypeError, ValueError):
        raise Refusal('epoch_parameters_upgrade_invalid') from None
