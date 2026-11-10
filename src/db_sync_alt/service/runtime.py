# SPDX-License-Identifier: Apache-2.0
"""Managed PostgreSQL lifecycle.

The writer thread owns one PgStore connection throughout verification and sync.
HTTP, the reader's serving check, and DB observations run independently. midnight-node
reads PostgreSQL directly with the reader role; the service grants and verifies that role
and checks, as the reader, that the node-facing relations answer. Sync drivers must
cooperate with stop_event and check it before publication; Python cannot forcibly
cancel arbitrary driver code, so a stuck writer is never replaced in-process.
"""
import argparse
from dataclasses import dataclass, field, replace
import datetime as dt
import importlib
import json
import math
from pathlib import Path
import re
import signal
import select
import secrets
import shutil
import sys
import threading
import time

from db_sync_alt.service.health import (BoundedDatabaseProbe, ChainPoint, HealthHTTPServer,
                       HealthPolicy, HealthState)


from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure, CooperativeStop as _CooperativeStop


SAFE_FAILURES = frozenset({
    'relevance_parent_unsupported', 'relevance_mode_mismatch', 'relevance_coverage_invalid', 'relevance_parent_changed',
    'relevance_inventory_invalid', 'relevance_output_unqualified', 'relevance_profile_address_invalid', 'relevance_work_unit_limit',
    'unsupported_follow_mode', 'completion_recovery_required', 'completion_invalid', 'completion_mismatch',
    'writer_busy', 'invalid_identity', 'invalid_backup', 'incomplete_restore',
    'missing_backup', 'database_unavailable', 'database_timeout', 'invalid_coverage',
    'canonical_conflict', 'unsupported_capability', 'sync_unavailable',
    'sql_unavailable', 'internal_error', 'shutdown_timeout', 'startup_failed', 'storage_low',
    'provider_client_unavailable', 'provider_accounting_unavailable', 'request_budget_exhausted',
    'provider_request_rejected', 'invalid_sync_configuration', 'pending_job_missing', 'invalid_sync_job', 'unsupported_postgres_version',
    'postgres_backup_tool_unavailable', 'postgres_backup_tool_version_mismatch', 'backend_identity_mismatch', 'invalid_reader_role',
    'schema_upgrade_required', 'pending_target_changed', 'pending_suffix_base_changed', 'pending_anchor_changed',
})


def failure_code(error):
    code = getattr(error, 'code', None)
    if isinstance(code, str) and code in SAFE_FAILURES:
        return code
    # The core Refusal carries fixed machine codes in args, not .code.
    raw = error.args[0] if error.args and isinstance(error.args[0], str) else None
    if raw in SAFE_FAILURES:
        return raw
    if raw == 'backup_missing':
        return 'missing_backup'
    if raw in {'backup_manifest_checksum_mismatch', 'invalid_backup_manifest', 'unsupported_backup_identity',
               'backup_checksum_mismatch', 'backup_profile_mismatch', 'restored_database_mismatch',
               'invalid_backup_canonical', 'unsafe_backup_path', 'unsafe_or_existing_backup',
               'postgres_backup_tool_failure'}:
        return 'invalid_backup'
    if raw in {'incomplete_owned_destination', 'restore_incomplete'}:
        return 'incomplete_restore'
    if raw in {'unowned_database', 'unsupported_database_identity', 'unsupported_format', 'network_mismatch',
               'untrusted_profile_identity', 'manifest_contract_mismatch', 'destination_not_empty'}:
        return 'invalid_identity'
    if raw in {'canonical_block_conflict', 'immutable_fact_conflict', 'conflicting_inherited_membership',
               'historical_selected_event_changed', 'historical_state_changed', 'historical_epoch_changed'}:
        return 'canonical_conflict'
    if raw in {'unsupported_capabilities', 'unsupported_source_kind', 'unsupported_candidate_activity'}:
        return 'unsupported_capability'
    if raw in {'reader_role_missing', 'invalid_reader_role', 'reader_privileges_not_exact'}:
        return 'invalid_reader_role'
    if raw in {'reader_requires_current_schema', 'epoch_parameters_upgrade_required'}:
        return 'schema_upgrade_required'
    if raw in {'invalid_epoch_max_tx_size', 'epoch_parameter_receipt_mismatch', 'epoch_parameters_upgrade_invalid'}:
        return 'invalid_coverage'
    if any(base.__module__.startswith('psycopg') and base.__name__ in {'OperationalError', 'InterfaceError'}
           for base in type(error).__mro__):
        return 'database_unavailable'
    return 'internal_error'


def _label(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', value):
        raise ValueError('invalid_configuration')
    return value


@dataclass(frozen=True)
class ServiceConfig:
    pg_service: str
    # The node's login: {"role": name, "pg_service": libpq service that logs in as that role}.
    reader: dict = field(repr=False)
    # The node's own values, compared with the profile and reported (never enforced):
    # {"security_parameter": k, "configuration_dir": the node's res/<network> directory}.
    node: dict | None = field(default=None, repr=False)
    sync: dict = field(default_factory=dict)
    network: str | None = None
    profile: str | None = None
    service_version: str = '0.3.0'
    http_host: str = '127.0.0.1'
    http_port: int = 18080
    http_allow_external: bool = False
    backup_archive: str | None = field(default=None, repr=False)
    backup_manifest: str | None = field(default=None, repr=False)
    policy: HealthPolicy = field(default_factory=HealthPolicy)
    shutdown_seconds: float = 5
    storage_reserves: dict = field(default_factory=lambda: {'/': 10 * 1024**3}, repr=False)

    def __post_init__(self):
        _label(self.pg_service)
        if not isinstance(self.storage_reserves, dict) or not self.storage_reserves:
            raise ValueError('storage_reserves_required')
        for path, floor in self.storage_reserves.items():
            if not isinstance(path, str) or not Path(path).is_absolute() or type(floor) is not int or floor <= 0:
                raise ValueError('invalid_storage_reserve')
        if self.network is None:
            raise ValueError('network_required')
        from db_sync_alt.contract import Refusal
        from db_sync_alt.networks import network_profile
        try:
            network = network_profile(self.network)
        except Refusal:
            raise ValueError('unsupported_configuration_profile') from None
        if self.profile is None:
            object.__setattr__(self, 'profile', network.profile)
        if self.profile != network.profile:
            raise ValueError('unsupported_configuration_profile')
        if not isinstance(self.policy, HealthPolicy):
            raise ValueError('invalid_health_policy')
        if self.policy.serving_lag_max_blocks is None:
            object.__setattr__(self, 'policy', replace(self.policy, serving_lag_max_blocks=network.serving_lag_max_blocks))
        elif self.policy.serving_lag_max_blocks >= network.stability_margin:
            # /health must report the lag before the node stops verifying blocks.
            raise ValueError('serving_lag_above_node_margin')
        if type(self.http_port) is not int or not 0 <= self.http_port <= 65535:
            raise ValueError('invalid_http_port')
        if type(self.http_allow_external) is not bool:
            raise ValueError('invalid_http_binding')
        if (not isinstance(self.reader, dict) or set(self.reader) != {'role', 'pg_service'}
                or not isinstance(self.reader['role'], str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,62}', self.reader['role'])):
            raise ValueError('invalid_reader_configuration')
        if _label(self.reader['pg_service']) == self.pg_service:
            raise ValueError('reader_service_must_differ')
        if self.node is not None:
            if (not isinstance(self.node, dict) or not self.node or not set(self.node) <= {'security_parameter', 'configuration_dir'}
                    or ('security_parameter' in self.node and (type(self.node['security_parameter']) is not int
                                                               or not 0 < self.node['security_parameter'] < 2**31))
                    or ('configuration_dir' in self.node and (not isinstance(self.node['configuration_dir'], str)
                                                              or not Path(self.node['configuration_dir']).is_absolute()))):
                raise ValueError('invalid_node_configuration')
        if bool(self.backup_archive) != bool(self.backup_manifest):
            raise ValueError('backup_pair_required')
        if (isinstance(self.shutdown_seconds, bool) or not isinstance(self.shutdown_seconds, (int, float))
                or not math.isfinite(self.shutdown_seconds) or not 0 < self.shutdown_seconds <= 30):
            raise ValueError('invalid_shutdown_deadline')

    @classmethod
    def load(cls, filename):
        path = Path(filename)
        if path.stat().st_size > 65536:
            raise ValueError('configuration_too_large')
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            raise ValueError('invalid_configuration')
        if 'policy' in data:
            data['policy'] = HealthPolicy(**data['policy'])
        return cls(**data)


def node_requirement_warnings(config):
    """Report-only: the node's k and network configuration files against the profile's.

    midnight-node's settings are invisible to the database; a node whose k is smaller than the
    profile's, or whose Cardano addresses and policies differ, gets empty answers. The operator
    may state the node's values in the service configuration; mismatches become warnings.
    """
    if config.node is None:
        return ['node_requirements_unchecked']
    from db_sync_alt.networks import directory, network_profile
    out = []
    if 'security_parameter' in config.node and config.node['security_parameter'] != network_profile(config.network).security:
        out.append('node_security_parameter_mismatch')
    if 'configuration_dir' in config.node:
        profile = directory(config.network)
        for name in sorted(json.loads((profile / 'source-manifest.json').read_text())['files']):
            try:
                same = json.loads((Path(config.node['configuration_dir']) / name).read_text()) == json.loads((profile / name).read_text())
            except (OSError, ValueError):
                same = False
            if not same:
                out.append('node_configuration_mismatch')
                break
    return out


@dataclass(frozen=True)
class DatabaseObservation:
    committed: ChainPoint | None
    witness: ChainPoint | None
    revoked: bool = False
    ready: bool = True


def _point(row):
    if row is None:
        return None
    timestamp = dt.datetime.fromisoformat(row['time'].replace('Z', '+00:00')).timestamp()
    return ChainPoint(row['height'], bytes(row['hash']).hex(), timestamp)


class PostgresRuntime:
    """Real core adapter; imports are lazy so pure lifecycle tests need no driver."""
    def __init__(self, config):
        self.config = config
        self._owner_challenge = None

    def inspect(self):
        from db_sync_alt.store.database import connect, empty_destination, inspect_lifecycle
        with connect(self.config.pg_service) as db:
            if empty_destination(db):
                return {'empty': True}
        return {'empty': False, **inspect_lifecycle(self.config.pg_service)}

    def reserve_check(self):
        try:
            if any(shutil.disk_usage(path).free < floor for path, floor in self.config.storage_reserves.items()):
                raise LifecycleFailure('storage_low')
        except OSError:
            raise LifecycleFailure('storage_low') from None

    def restore(self):
        self.reserve_check()
        from db_sync_alt.backup import restore
        restore(self.config.backup_archive, self.config.backup_manifest,
                destination_service=self.config.pg_service, reserve_check=self.reserve_check,
                expected_network=self.config.network, reader_role=self.config.reader['role'])

    def open_writer(self, health):
        from db_sync_alt.store.database import PgStore
        def checkpoint(_phase, **_fields):
            # A store callback signifies actual completed local work; neither
            # arbitrary fields nor exception text enter public status.
            try:
                health.operation_progress()
            except ValueError:
                pass
        self.reserve_check()
        return PgStore(self.config.pg_service, progress_callback=checkpoint, reserve_check=self.reserve_check,
                       network=self.config.network)

    def writer_check(self, store, timeout=1.0):
        """Check the actual owner session, in its own thread, without reconnect.

        libpq's nonblocking protocol gives this constant SELECT a client deadline
        even if the server/network stops responding. No observer connection can
        substitute for the session holding the lifetime writer/challenge locks.
        """
        from psycopg import pq
        db=store.db
        if db.closed:raise LifecycleFailure('database_unavailable')
        store.require_writer()
        if not db.autocommit or db.info.transaction_status!=pq.TransactionStatus.IDLE:
            raise LifecycleFailure('internal_error')
        deadline=time.monotonic()+timeout
        with db.lock:
            conn=db.pgconn
            if not conn.nonblocking:raise LifecycleFailure('internal_error')
            def wait(*,write=False):
                left=deadline-time.monotonic()
                if left<=0:raise LifecycleFailure('database_timeout')
                readable,writable,_=select.select([] if write else [conn.socket],
                                                  [conn.socket] if write else [],[],left)
                if not readable and not writable:raise LifecycleFailure('database_timeout')
            conn.send_query(b'SELECT 1')
            while conn.flush():wait(write=True)
            results=0
            while True:
                conn.consume_input()
                while conn.is_busy():
                    wait();conn.consume_input()
                result=conn.get_result()
                if result is None:break
                if (result.status!=pq.ExecStatus.TUPLES_OK or result.ntuples!=1
                        or result.nfields!=1 or result.get_value(0,0)!=b'1'):
                    raise LifecycleFailure('database_unavailable')
                results+=1
            if results!=1:raise LifecycleFailure('database_unavailable')

    def verify(self, store):
        from db_sync_alt.store.database import SCHEMA_VERSION
        from db_sync_alt.verify.store import verify_store
        if store.state()['schema_version'] != SCHEMA_VERSION:
            # The service runs only on the current schema; `db-sync-alt upgrade` comes first.
            raise LifecycleFailure('schema_upgrade_required')
        if getattr(store, 'supports_incremental', False):
            store.certify_ready_full()
            if getattr(store, 'supports_checkpoints', False):
                # The startup audit becomes the integrity base: the next start re-hashes it and
                # walks only what is published after it.
                capability = store.verified_checkpoint()
                if capability.generation > store.integrity()['base']:
                    store.write_checkpoint(capability)
        else:
            verify_store(store)
        status = store.lifecycle_state()
        if status['migration_state'] != 'ready' or status['canonical_ready'] is None:
            raise LifecycleFailure('invalid_coverage')
        if status['revoked']:
            raise LifecycleFailure('canonical_conflict')
        identity = store.meta('profile')
        if identity.get('network') != self.config.network:
            raise LifecycleFailure('invalid_identity')

    def bind_reader(self, store):
        """Grant the configured reader the node's relations, verify it can do nothing more, and
        hold a random lock on the writer session that every observer and reader check must see.

        The lock proves that later connections (the owner's observer, the reader's check) reach
        the database this writer holds, not another server or database of the same name.
        """
        from db_sync_alt.contract import Refusal
        from db_sync_alt.store.reader import grant_reader, verify_reader
        try:
            grant_reader(store.db, self.config.reader['role'])
            report = verify_reader(store.db, self.config.reader['role'])
        except Refusal as error:
            raise LifecycleFailure(failure_code(error)) from None
        signed = lambda value: value if value < 2**31 else value - 2**32
        first = signed(secrets.randbits(32))
        while first == 36:  # Reserved writer/publication advisory-lock namespace.
            first = signed(secrets.randbits(32))
        challenge = (first, signed(secrets.randbits(32)))
        store.db.execute('SELECT pg_advisory_lock(%s,%s)', challenge)
        self._owner_challenge = challenge
        return report

    def _check_owner(self, db):
        challenge = self._owner_challenge
        if challenge is None:
            raise LifecycleFailure('backend_identity_mismatch')
        acquired = db.execute('SELECT pg_try_advisory_lock(%s,%s)', challenge).fetchone()[0]
        if acquired is not False:
            if acquired is True:
                db.execute('SELECT pg_advisory_unlock(%s,%s)', challenge)
            raise LifecycleFailure('backend_identity_mismatch')

    def observe(self):
        from db_sync_alt.store.database import PgStore
        with PgStore(self.config.pg_service, readonly=True) as store:
            self._check_owner(store.db)
            # One local snapshot ties ready state and target/witness metadata.
            with store.db.transaction():
                store.db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                current = store.state()
                if current['revoked']:
                    return DatabaseObservation(None, None, revoked=True, ready=False)
                if current['migration_state'] != 'ready' or current['canonical_ready'] is None:
                    return DatabaseObservation(None, None, ready=False)
                generation = store.generation(current['canonical_ready'])
                committed = _point(store.block_at(generation['id'], generation['target_height']))
                witness = _point(store.block_at(generation['id'], generation['witness_height']))
                # Pending progress belongs to the updater's durable checkpoint
                # adapter; this reader must not overwrite its completed counts.
                return DatabaseObservation(committed, witness)

    def reader_check(self):
        """As the reader, in the writer's database: one gated node-facing relation answers."""
        import psycopg
        with psycopg.connect(service=self.config.reader['pg_service'], connect_timeout=2,
                             options='-c statement_timeout=2000 -c lock_timeout=1000', autocommit=True) as db:
            if db.info.user != self.config.reader['role']:
                raise LifecycleFailure('invalid_reader_role')
            self._check_owner(db)
            return db.execute('SELECT count(*) FROM (SELECT 1 FROM block LIMIT 1) s').fetchone()[0] == 1

    def database_check(self, timeout_seconds):
        import psycopg
        # libpq connect timeout has whole-second granularity; HTTP's outer
        # single-flight check remains <=0.5s even if connection setup blocks.
        with psycopg.connect(service=self.config.pg_service, connect_timeout=1,
                             options='-c statement_timeout=400 -c lock_timeout=200',
                             autocommit=True) as db:
            self._check_owner(db)
            return db.execute('SELECT 1').fetchone()[0] == 1


def load_sync_driver(config):
    """Fixed runtime module, never a user-supplied import path or a fallback.

    `sources.blockfrost.sync.SyncDriver(config).run_once(store, health, stop_event)`.
    Each call is one bounded acquisition unit; it owns observation/progress and
    safe shared accounting, raises TransientSyncFailure for retry/quota waits,
    and checks stop_event before publication. No implementation means fail closed.
    """
    try:
        module = importlib.import_module('db_sync_alt.sources.blockfrost.sync')
        return module.SyncDriver(config)
    except ImportError:
        raise LifecycleFailure('sync_unavailable') from None


class Service:
    def __init__(self, config, *, runtime=None, sync_driver=None):
        self.config = config
        self.health = HealthState(service_version=config.service_version, schema_version=1,
                                  network=config.network, profile=config.profile, policy=config.policy)
        self.runtime = runtime or PostgresRuntime(config)
        self.sync_driver = sync_driver
        self.stop_event = threading.Event()
        self.failed = threading.Event()
        self.ready = threading.Event()
        self._state_lock = threading.Lock()
        self._threads = []
        self._started = False
        self.failure = None
        self.probe = BoundedDatabaseProbe(self.runtime.database_check)
        self.http = None

    def start(self):
        if self._started:
            raise LifecycleFailure('startup_failed')
        self._started = True
        self.health.node_requirements(node_requirement_warnings(self.config))
        self.http = HealthHTTPServer(self.health, host=self.config.http_host, port=self.config.http_port,
                                     allow_external=self.config.http_allow_external, probe=self.probe)
        self._spawn('pg-http', lambda: self.http.serve_forever(poll_interval=0.05))
        self._spawn('pg-writer', self._writer)
        self._spawn('pg-observer', self._observer)
        self._spawn('pg-heartbeat', self._heartbeat)
        return self

    def _spawn(self, name, target):
        thread = threading.Thread(target=target, name=name, daemon=True)
        self._threads.append(thread)
        thread.start()
        return thread

    def _fail(self, code):
        with self._state_lock:
            # Expected cancellation is handled by its typed control path.
            # A genuine failure during requested shutdown must still survive.
            if self.failed.is_set() or self.failure is not None:
                return
            self.failure = code if code in SAFE_FAILURES else 'internal_error'
            self.health.set_error(self.failure)
            # Revocation of canonical data is the writer's durable responsibility: the database gate
            # refuses every node query of a revoked database, whether this process runs or not. Any
            # other failure leaves the last published generation served by PostgreSQL, unobserved here.
            self.health.update(lifecycle='halted' if code in {'canonical_conflict', 'unsupported_capability'} else 'failed',
                               sync='halted', serving='revoked' if code == 'canonical_conflict' else 'unknown')
            self.failed.set()
            self.stop_event.set()
            self.ready.clear()

    def _wait_with_writer(self, store, until):
        # Intentional quota/backoff and idle waits must still notice owner loss.
        # These checks neither call the provider nor refresh acquisition progress.
        while not self.stop_event.is_set():
            remaining=until-time.time()
            if remaining<=0:return
            self.runtime.writer_check(store,1.0)
            if self.stop_event.wait(min(5.0,remaining)):return

    def _writer(self):
        store = None
        try:
            self.health.begin_operation('database')
            inspection = self.runtime.inspect()
            if inspection['empty']:
                if not self.config.backup_archive:
                    raise LifecycleFailure('missing_backup')
                self.health.update(lifecycle='restoring')
                self.runtime.restore()
                inspection = self.runtime.inspect()
            if inspection.get('empty') or inspection.get('migration_state') != 'ready':
                raise LifecycleFailure('incomplete_restore')
            if inspection.get('revoked'):
                raise LifecycleFailure('canonical_conflict')
            del inspection  # Startup metadata is not part of the serving loop.
            if self.stop_event.is_set():
                return
            store = self.runtime.open_writer(self.health)
            progress_callback = getattr(store, 'progress_callback', None)

            def progress(phase, **fields):
                # Runs on the writer itself: raising unwinds verification
                # cursors/transactions before finally closes its connection.
                # A blocked operation still has the normal shutdown deadline.
                if self.stop_event.is_set():
                    raise _CooperativeStop()
                if progress_callback is not None:
                    progress_callback(phase, **fields)
                if self.stop_event.is_set():
                    raise _CooperativeStop()

            store.progress_callback = progress
            self.health.operation_progress()
            self.runtime.verify(store)
            from db_sync_alt.store.database import PgStore
            if isinstance(store,PgStore):
                self.health.verified_schema(store.state()['schema_version'])
                if getattr(store, 'supports_checkpoints', False):
                    self.health.integrity(**store.integrity())
            if self.stop_event.is_set():
                return
            self.health.end_operation()
            # Resolve the real driver before the reader is granted.
            if self.sync_driver is None:
                self.sync_driver = load_sync_driver(self.config)
            if hasattr(self.sync_driver, 'check_follow_mode'):
                self.sync_driver.check_follow_mode(store)
            self.health.reader_verified(self.runtime.bind_reader(store))
            if self.stop_event.is_set():
                return
            with self._state_lock:
                if self.stop_event.is_set():
                    return
                self.health.update(lifecycle='ready', sync='syncing', serving='unready')
                self.ready.set()
            if hasattr(self.sync_driver, 'start_polling') and not self.stop_event.is_set():
                thread = self.sync_driver.start_polling(store, self.health, self.stop_event,
                                                       lambda error: self._fail(failure_code(error)))
                self._threads.append(thread)
            while not self.stop_event.is_set():
                self.health.heartbeat('sync')
                try:
                    # A missing driver checkpoint must not appear healthy just
                    # because the supervisor still heartbeats. Drivers may
                    # transition this operation explicitly for local work.
                    self.health.begin_operation('provider')
                    self.health.update(sync='syncing')
                    progressed = self.sync_driver.run_once(store, self.health, self.stop_event)
                except TransientSyncFailure as error:
                    self.health.end_operation()
                    if error.retry_at <= time.time():
                        raise LifecycleFailure('internal_error')
                    self.health.set_error(error.code)
                    self.health.pause('quota_wait' if error.code == 'quota_exhausted' else 'backoff',
                                      resume_at=error.retry_at)
                    self._wait_with_writer(store,error.retry_at)
                    continue
                if self.stop_event.is_set():
                    break
                self.health.end_operation()
                if progressed is not False:
                    self.health.record_success()
                self.health.update(sync='idle')
                if getattr(self.sync_driver, '_poller', None) is not None:
                    self.runtime.writer_check(store, 1.0)
                    self.sync_driver.wait_for_observation(timeout=1.0)
                else:
                    self._wait_with_writer(store,time.time()+self.config.policy.poll_seconds)
        except _CooperativeStop:
            # Only this private checkpoint signal denotes an orderly stop.
            # Database/provider/verification errors keep their failure paths.
            if not self.stop_event.is_set():
                self._fail('internal_error')
        except Exception as error:
            self._fail(failure_code(error))
        finally:
            # Poll completion metadata is bound to this owner session. Keep it
            # alive during bounded cooperative poll shutdown; closing it first
            # turns an ordinary in-flight success into false owner-loss failure.
            poller = getattr(self.sync_driver, '_poller', None)
            if poller is not None and poller.thread is not None and poller.thread is not threading.current_thread():
                poller.thread.join(self.config.shutdown_seconds)
            if store is not None:
                try:
                    store.close()
                except Exception:
                    pass

    def _heartbeat(self):
        # Independent of DB observations, which can block on their own bounded
        # driver deadline. Only actual checkpoints refresh operation progress.
        while not self.stop_event.is_set():
            writer = next((t for t in self._threads if t.name == 'pg-writer'), None)
            if writer is not None and writer.is_alive():
                self.health.heartbeat('sync')
            self.stop_event.wait(self.config.policy.heartbeat_seconds)

    def _observer(self):
        while not self.stop_event.is_set():
            if self.ready.is_set():
                try:
                    observation = self.runtime.observe()
                    self.health.database_observed(True)
                    if observation.revoked:
                        self._fail('canonical_conflict')
                        return
                    try:
                        sql_ready, sql_error = self.runtime.reader_check() is True, 'sql_unavailable'
                    except Exception as error:
                        code = failure_code(error)
                        sql_ready, sql_error = False, code if code in {'backend_identity_mismatch', 'invalid_reader_role'} else 'sql_unavailable'
                    with self._state_lock:
                        if self.stop_event.is_set():
                            return
                        self.health.set_coverage(committed=observation.committed,
                                                 served=observation.committed if observation.ready and sql_ready else None,
                                                 witness=observation.witness)
                        self.health.update(serving='ready' if observation.ready and sql_ready else 'unready')
                        if sql_ready:
                            self.health.heartbeat('sql')
                        else:
                            self.health.set_error(sql_error)
                except Exception:
                    with self._state_lock:
                        if self.stop_event.is_set():
                            return
                        self.health.database_observed(False)
                        self.health.update(serving='unready')
            self.stop_event.wait(self.config.policy.heartbeat_seconds)

    def stop(self):
        """Bounded cooperative shutdown; False means writer did not exit in time.

        Never start another writer in this process after False. Process exit is
        required to release a permanently blocked DB/provider session safely.
        """
        deadline = time.monotonic() + self.config.shutdown_seconds
        self.stop_event.set()
        self.ready.clear()
        if not self.failed.is_set():
            self.health.update(lifecycle='stopping', serving='stopped')
        if self.http is not None:
            self.http.shutdown()
            self.http.server_close()
        for thread in self._threads:
            if thread is not threading.current_thread():
                thread.join(max(0, deadline - time.monotonic()))
        complete = not any(thread.is_alive() for thread in self._threads)
        if complete and not self.failed.is_set():
            self.health.update(lifecycle='stopped')
        elif not complete:
            self.failure = self.failure or 'shutdown_timeout'
            self.health.set_error('shutdown_timeout')
        return complete


def main(argv=None):
    parser = argparse.ArgumentParser(prog='db-sync-alt service', description='Run the db-sync-alt service from a configuration file.')
    parser.add_argument('--config', required=True)
    args = parser.parse_args(argv)
    service = None
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        config = ServiceConfig.load(args.config)
        service = Service(config)
        service.start()
        while not stop.wait(0.1) and not service.failed.is_set():
            pass
        clean = service.stop()
        if service.failure or not clean:
            print(json.dumps({'error_code': service.failure or 'shutdown_timeout'}), file=sys.stderr)
            return 75 if service.failure in {'database_unavailable', 'database_timeout'} else 1
        return 0
    except Exception as error:
        if service is not None:
            service.stop()
        print(json.dumps({'error_code': failure_code(error)}), file=sys.stderr)
        return 1

