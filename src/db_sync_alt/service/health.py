# SPDX-License-Identifier: Apache-2.0
"""Process-local monitoring for the PostgreSQL service; no storage/provider imports.

The lifecycle owner supplies validated committed/served coverage and local quota
accounting. Monitoring never fetches chain data. All clock ages/deadlines use
monotonic time; chain timestamps only measure confirmed-to-served lag. Restart
must refresh observations: restored chain coverage is not proof of fresh health.
"""
from dataclasses import dataclass, asdict, replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import ipaddress
import json
import math
import re
import socket
import threading
import time


ERROR_CODES = frozenset({
    'relevance_parent_unsupported', 'relevance_mode_mismatch', 'relevance_coverage_invalid', 'relevance_parent_changed',
    'relevance_inventory_invalid', 'relevance_output_unqualified', 'relevance_profile_address_invalid', 'relevance_work_unit_limit',
    'unsupported_follow_mode', 'completion_recovery_required', 'completion_invalid', 'completion_mismatch',
    'provider_unavailable', 'provider_timeout', 'quota_exhausted',
    'database_unavailable', 'database_timeout', 'canonical_conflict',
    'unsupported_capability', 'invalid_backup', 'invalid_identity',
    'invalid_coverage', 'internal_error', 'writer_busy', 'incomplete_restore',
    'missing_backup', 'sync_unavailable', 'sql_unavailable', 'shutdown_timeout', 'startup_failed', 'storage_low',
    'provider_client_unavailable', 'provider_accounting_unavailable', 'request_budget_exhausted',
    'provider_request_rejected', 'invalid_sync_configuration', 'pending_job_missing', 'invalid_sync_job', 'unsupported_postgres_version',
    'postgres_backup_tool_unavailable', 'postgres_backup_tool_version_mismatch', 'backend_identity_mismatch', 'invalid_reader_role',
    'schema_upgrade_required', 'pending_target_changed', 'pending_suffix_base_changed', 'pending_anchor_changed',
})
LIFECYCLES = frozenset({'starting', 'restoring', 'ready', 'stopping', 'stopped', 'failed', 'halted'})
SYNC_STATES = frozenset({'unknown', 'idle', 'syncing', 'checkpointing', 'degraded', 'backoff', 'quota_wait', 'halted'})
SERVING_STATES = frozenset({'unknown', 'unready', 'ready', 'revoked', 'stopped'})
# Report-only warnings: shown in /status and /health, never a reason for an unhealthy answer.
WARNING_CODES = frozenset({
    'reader_statement_timeout_unset', 'reader_idle_in_transaction_timeout_unset', 'reader_search_path_not_public',
    'reader_connection_limit_unset', 'reader_connection_limit_below_node_pool',
    'node_requirements_unchecked', 'node_security_parameter_mismatch', 'node_configuration_mismatch',
})
READER_SETTINGS = ('statement_timeout', 'idle_in_transaction_session_timeout', 'search_path', 'default_transaction_read_only')


def _number(value, name, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(name + ' must be finite')
    if value < 0 or (positive and value == 0):
        raise ValueError(name + ' must be positive' if positive else name + ' must be nonnegative')


def _integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(name + ' must be a nonnegative integer')


def _public_label(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,79}', value):
        raise ValueError('invalid public identity label')
    return value


def _error(value):
    # Do not preserve exception messages, URLs, paths or unknown text.
    return value if isinstance(value, str) and value in ERROR_CODES else 'internal_error'


@dataclass(frozen=True)
class HealthPolicy:
    poll_seconds: float = 20
    heartbeat_seconds: float = 5
    worker_stall_seconds: float = 120
    observation_max_age_seconds: float = 120
    serving_lag_max_seconds: float = 600
    # None: the network profile's limit, which is below the node's stability margin.
    serving_lag_max_blocks: int | None = None
    provider_operation_seconds: float = 120
    local_operation_seconds: float = 300
    database_max_age_seconds: float = 5

    def __post_init__(self):
        for name, value in asdict(self).items():
            if name == 'serving_lag_max_blocks' and value is None:
                continue
            _number(value, name, positive=True)
            if name == 'serving_lag_max_blocks':
                _integer(value, name)


@dataclass(frozen=True)
class ChainPoint:
    height: int
    hash: str
    timestamp: float | None = None  # UTC Unix seconds, from verified block metadata

    def __post_init__(self):
        _integer(self.height, 'height')
        if not isinstance(self.hash, str) or not re.fullmatch(r'[0-9a-f]{64}', self.hash):
            raise ValueError('block hash must be 64 lowercase hexadecimal characters')
        if self.timestamp is not None:
            _number(self.timestamp, 'timestamp')


@dataclass(frozen=True)
class PendingProgress:
    target: ChainPoint | None = None
    completed: int | None = None
    total: int | None = None

    def __post_init__(self):
        if self.target is not None and not isinstance(self.target, ChainPoint):
            raise ValueError('invalid pending target')
        for name in ('completed', 'total'):
            if getattr(self, name) is not None:
                _integer(getattr(self, name), name)
        if self.completed is not None and self.total is not None and self.completed > self.total:
            raise ValueError('completed exceeds total')


@dataclass(frozen=True)
class QuotaStatus:
    used: int | None = None
    limit: int | None = None
    reset_at: float | None = None

    def __post_init__(self):
        for name in ('used', 'limit'):
            if getattr(self, name) is not None:
                _integer(getattr(self, name), name)
        if self.reset_at is not None:
            _number(self.reset_at, 'reset_at')


@dataclass(frozen=True)
class DatabaseStatus:
    reachable: bool | None = None
    checked_at: float | None = None
    age_seconds: float | None = None
    error_code: str | None = None

    def __post_init__(self):
        if self.reachable is not None and type(self.reachable) is not bool:
            raise ValueError('invalid database reachability')
        for name in ('checked_at', 'age_seconds'):
            if getattr(self, name) is not None:
                _number(getattr(self, name), name)
        if self.error_code is not None and self.error_code not in {'database_timeout', 'database_unavailable'}:
            raise ValueError('invalid database error code')


class HealthState:
    """Thread-safe cached observations; methods never perform I/O.

    Required workers are fixed to ``sync`` and ``sql``. Their owning loops must
    heartbeat even when caught up; a supervisor heartbeat must not reset an
    operation's progress timer. Only actual bounded work checkpoints call
    ``operation_progress``. ``pause`` ends the current operation and records an
    intentional wait; expired waits require the sync loop to run again.
    """
    def __init__(self, *, service_version, schema_version, network, profile,
                 policy=None, monotonic=time.monotonic, wall_clock=time.time):
        self.policy = policy or HealthPolicy()
        if self.policy.serving_lag_max_blocks is None:
            from db_sync_alt.networks import network_profile
            self.policy = replace(self.policy, serving_lag_max_blocks=network_profile(network).serving_lag_max_blocks)
        self._mono, self._wall = monotonic, wall_clock
        self._lock = threading.Lock()
        self._identity = dict(service_version=_public_label(service_version),
                              schema_version=_public_label(str(schema_version)),
                              network=_public_label(network), profile=_public_label(profile))
        self._lifecycle, self._sync, self._serving = 'starting', 'unknown', 'unknown'
        self._head = self._confirmed = self._committed = self._served = self._witness = None
        self._observed_at = self._observed_mono = self._success_at = None
        self._pending, self._quota = PendingProgress(), QuotaStatus()
        self._workers = {'sync': None, 'sql': None}
        self._operation = None
        self._retry_at = self._resume_mono = None
        self._error_code = None
        self._poll_error = None
        self._follow_mode = None
        self._poll_interval = None
        self._poll_timing = None
        self._db = None
        self._reader = None
        self._node_warnings = []
        self._integrity = {'base': None, 'checkpointed_at': None}
        self._relevance = dict(requested='full', effective='full', reason='disabled')

    def relevance_mode(self, enabled, *, effective=None, reason=None):
        if type(enabled) is not bool or effective not in ('full', 'relevance'):
            raise ValueError('invalid relevance mode')
        if reason not in ('disabled','verified','trusted-history','relevance_parent_unsupported','relevance_mode_mismatch','relevance_coverage_invalid','relevance_parent_changed'):
            raise ValueError('invalid relevance reason')
        with self._lock:
            self._relevance = dict(requested='relevance' if enabled else 'full', effective=effective, reason=reason)

    def verified_schema(self, version):
        if type(version) is not int or version not in (1,2,3,4):
            raise ValueError('unsupported schema identity')
        with self._lock:self._identity['schema_version']=str(version)

    def integrity(self, *, base, checkpointed_at):
        """The verified integrity base and when its checkpoint was written (None: no checkpoint yet)."""
        _integer(base, 'integrity base')
        if checkpointed_at is not None:
            _number(checkpointed_at, 'checkpointed_at')
        with self._lock:
            self._integrity = {'base': base, 'checkpointed_at': checkpointed_at}

    def reader_verified(self, report):
        """The reader role midnight-node uses: verified privileges, its settings, what to set."""
        if not isinstance(report, dict) or report.get('privileges') != 'node_relations_only':
            raise ValueError('invalid reader report')
        settings = report.get('settings') or {}
        def value(text):
            # Role settings are operator text: keep simple values, never arbitrary strings.
            return text if text is None or (isinstance(text, str) and re.fullmatch(r'[A-Za-z0-9_.:, "$-]{0,64}', text)) else 'set'
        limit = settings.get('connection_limit')
        if limit is not None and type(limit) is not int:
            raise ValueError('invalid reader connection limit')
        warnings = [w for w in report.get('warnings', []) if w in WARNING_CODES]
        with self._lock:
            self._reader = {'role': _public_label(report['role']), 'privileges': 'node_relations_only',
                            'settings': {**{k: value(settings.get(k)) for k in READER_SETTINGS}, 'connection_limit': limit},
                            'warnings': sorted(warnings)}

    def node_requirements(self, warnings):
        """Report-only result of comparing the node's stated values with the profile."""
        if not isinstance(warnings, list) or any(w not in WARNING_CODES or not w.startswith('node_') for w in warnings):
            raise ValueError('invalid node requirement warnings')
        with self._lock:
            self._node_warnings = sorted(warnings)

    def update(self, *, lifecycle=None, sync=None, serving=None, pending=None, quota=None):
        """Update related local flags atomically; unknown keys/text are rejected."""
        for value, choices in ((lifecycle, LIFECYCLES), (sync, SYNC_STATES), (serving, SERVING_STATES)):
            if value is not None and value not in choices:
                raise ValueError('invalid service state')
        if pending is not None and not isinstance(pending, PendingProgress):
            raise ValueError('invalid pending progress')
        if quota is not None and not isinstance(quota, QuotaStatus):
            raise ValueError('invalid quota status')
        with self._lock:
            for name, value in (('_lifecycle', lifecycle), ('_sync', sync), ('_serving', serving),
                                ('_pending', pending), ('_quota', quota)):
                if value is not None:
                    setattr(self, name, value)

    def set_error(self, code=None):
        with self._lock:
            self._error_code = None if code is None else _error(code)

    def configure_follow(self, mode, interval):
        if mode != 'incremental': raise ValueError('unsupported follow mode')
        _number(interval, 'poll interval', positive=True)
        with self._lock:
            self._follow_mode, self._poll_interval = mode, interval
            self._workers.setdefault('poll', None)

    def poll_timing(self, *, started_at, completed_at, start_gap_seconds, max_gap_seconds, lateness_seconds, missed_ticks):
        for value in (started_at, start_gap_seconds, max_gap_seconds, lateness_seconds):
            _number(value, 'poll timing')
        if completed_at is not None: _number(completed_at, 'poll completion')
        _integer(missed_ticks, 'missed poll ticks')
        with self._lock:
            self._poll_timing = dict(started_at=started_at, completed_at=completed_at,
                                    start_gap_seconds=start_gap_seconds, max_gap_seconds=max_gap_seconds,
                                    lateness_seconds=lateness_seconds, missed_ticks=missed_ticks)

    def poll_error(self, code):
        with self._lock:
            self._poll_error = None if code is None else _error(code)

    def observe_provider(self, head, confirmed_target, *, observed_at=None, observed_mono=None):
        if not isinstance(head, ChainPoint) or not isinstance(confirmed_target, ChainPoint):
            raise ValueError('invalid provider observation')
        if confirmed_target.height > head.height:
            raise ValueError('confirmed target exceeds observed head')
        with self._lock:
            wall, mono = self._wall(), self._mono()
            at = wall if observed_at is None else observed_at
            mt = mono if observed_mono is None else observed_mono
            if (isinstance(at,bool) or isinstance(mt,bool) or not isinstance(at,(int,float)) or not isinstance(mt,(int,float))
                    or not math.isfinite(at) or not math.isfinite(mt) or at > wall or mt > mono):
                raise ValueError('invalid provider observation time')
            if self._observed_mono is not None and mt < self._observed_mono:
                raise ValueError('regressed provider observation')
            self._head, self._confirmed = head, confirmed_target
            self._observed_at, self._observed_mono = at, mt

    def set_coverage(self, *, committed, served, witness):
        """Copy an already-validated DB coverage snapshot, never pending work.

        The lifecycle caller must revoke serving separately on a canonical
        conflict; this module is a monitor, not the SQL publication guard.
        """
        for point in (committed, served, witness):
            if point is not None and not isinstance(point, ChainPoint):
                raise ValueError('invalid coverage point')
        if served is not None and (committed is None or served.height > committed.height):
            raise ValueError('served coverage exceeds committed coverage')
        if served is not None and served.height == committed.height and served.hash != committed.hash:
            raise ValueError('served and committed anchor mismatch')
        with self._lock:
            self._committed, self._served, self._witness = committed, served, witness

    def database_observed(self, reachable):
        """Inject a dependency observation from a bounded lifecycle DB check."""
        if type(reachable) is not bool:
            raise ValueError('invalid database reachability')
        with self._lock:
            self._db = (reachable, self._wall(), self._mono())

    def heartbeat(self, worker):
        if worker not in self._workers:
            raise ValueError('unknown required worker')
        with self._lock:
            self._workers[worker] = (self._wall(), self._mono())

    def begin_operation(self, stage, *, deadline_seconds=None):
        if stage not in {'provider', 'verification', 'database'}:
            raise ValueError('invalid operation stage')
        deadline = deadline_seconds if deadline_seconds is not None else (
            self.policy.provider_operation_seconds if stage == 'provider' else self.policy.local_operation_seconds)
        _number(deadline, 'deadline_seconds', positive=True)
        with self._lock:
            if self._operation is not None:
                raise ValueError('operation already active')
            self._operation = (stage, self._wall(), self._mono(), deadline)
            self._retry_at = self._resume_mono = None

    def operation_progress(self):
        with self._lock:
            if self._operation is None:
                raise ValueError('no active operation')
            stage, _, _, deadline = self._operation
            self._operation = (stage, self._wall(), self._mono(), deadline)

    def end_operation(self):
        with self._lock:
            self._operation = None

    def pause(self, reason, *, resume_at):
        if reason not in {'backoff', 'quota_wait'}:
            raise ValueError('invalid pause reason')
        _number(resume_at, 'resume_at')
        with self._lock:
            delay = resume_at - self._wall()
            if delay <= 0:
                raise ValueError('pause must have a future retry time')
            self._sync, self._retry_at = reason, resume_at
            self._resume_mono = self._mono() + delay
            self._operation = None

    def record_success(self):
        with self._lock:
            self._success_at = self._wall()
            self._error_code = None
            self._retry_at = self._resume_mono = None

    def snapshot(self, database=None):
        """Return a detached, bounded JSON-compatible status snapshot."""
        if database is not None and not isinstance(database, DatabaseStatus):
            raise ValueError('invalid database status')
        with self._lock:
            now = self._mono()
            age = lambda stamp: None if stamp is None else max(0.0, now - stamp)
            point = lambda value: None if value is None else asdict(value)
            if database is None:
                database = DatabaseStatus() if self._db is None else DatabaseStatus(
                    self._db[0], self._db[1], age(self._db[2]),
                    None if self._db[0] else 'database_unavailable')
            observed_age = age(self._observed_mono)
            lag_seconds = lag_blocks = None
            if self._confirmed is not None and self._served is not None:
                lag_blocks = max(0, self._confirmed.height - self._served.height)
                if self._confirmed.timestamp is not None and self._served.timestamp is not None:
                    lag_seconds = max(0, self._confirmed.timestamp - self._served.timestamp)
            workers = {name: {'heartbeat_at': None if value is None else value[0],
                              'heartbeat_age_seconds': None if value is None else age(value[1])}
                       for name, value in self._workers.items()}
            operation = None if self._operation is None else {
                'stage': self._operation[0], 'progress_at': self._operation[1],
                'progress_age_seconds': age(self._operation[2]), 'deadline_seconds': self._operation[3]}
            reasons = []
            if self._relevance['reason'] not in ('disabled', 'verified', 'trusted-history'):
                reasons.append(self._relevance['reason'])
            if self._poll_error is not None:
                reasons.append('provider_poll_' + self._poll_error)
            if self._lifecycle != 'ready':
                reasons.append('lifecycle_' + self._lifecycle)
            if database.reachable is not True:
                reasons.append(database.error_code or ('database_unknown' if database.reachable is None else 'database_unavailable'))
            elif database.age_seconds is None or database.age_seconds > self.policy.database_max_age_seconds:
                reasons.append('database_observation_stale')
            if self._serving != 'ready':
                reasons.append('serving_' + self._serving)
            if self._committed is None or self._served is None or self._witness is None:
                reasons.append('coverage_unknown')
            if self._sync in {'unknown', 'halted'}:
                reasons.append('sync_' + self._sync)
            for worker, value in workers.items():
                if value['heartbeat_age_seconds'] is None:
                    reasons.append('worker_' + worker + '_unknown')
                elif value['heartbeat_age_seconds'] > self.policy.worker_stall_seconds:
                    reasons.append('worker_' + worker + '_stalled')
            if operation is not None and operation['progress_age_seconds'] > operation['deadline_seconds']:
                reasons.append('operation_stalled')
            if self._resume_mono is not None and now > self._resume_mono + self.policy.worker_stall_seconds:
                reasons.append('retry_overdue')
            if observed_age is None:
                reasons.append('provider_observation_unknown')
            elif observed_age > self.policy.observation_max_age_seconds:
                reasons.append('provider_observation_stale')
            if lag_blocks is None:
                reasons.append('serving_block_lag_unknown')
            elif self._confirmed.height < self._served.height:
                reasons.append('provider_behind_served')
            elif lag_blocks > self.policy.serving_lag_max_blocks:
                reasons.append('serving_block_lag_exceeded')
            if lag_seconds is None:
                reasons.append('serving_lag_unknown')
            elif lag_seconds > self.policy.serving_lag_max_seconds:
                reasons.append('serving_lag_exceeded')
            return dict(self._identity, follow_mode=self._follow_mode, poll_interval_seconds=self._poll_interval, poll_timing=None if self._poll_timing is None else dict(self._poll_timing), lifecycle=self._lifecycle, sync=self._sync, serving=self._serving,
                        provider_head=point(self._head), provider_observed_at=self._observed_at,
                        confirmed_target=point(self._confirmed), committed_target=point(self._committed),
                        served_target=point(self._served), witness=point(self._witness),
                        pending=asdict(self._pending), lag={'blocks': lag_blocks, 'seconds': lag_seconds},
                        freshness={'provider_observation_age_seconds': observed_age},
                        last_successful_sync=self._success_at, retry_at=self._retry_at, quota=asdict(self._quota),
                        database=asdict(database), workers=workers, operation=operation, error_code=self._error_code, poll_error_code=self._poll_error,
                        integrity=dict(self._integrity), relevance=dict(self._relevance),
                        reader=None if self._reader is None else json.loads(json.dumps(self._reader)),
                        warnings=([] if self._reader is None else list(self._reader['warnings'])) + list(self._node_warnings),
                        policy=asdict(self.policy), healthy=not reasons, reason_codes=reasons)

    def health(self, database=None):
        snapshot = self.snapshot(database)
        return (200 if snapshot['healthy'] else 503), snapshot


class BoundedDatabaseProbe:
    """Single-flight injected PG check; at most one daemon thread can be blocked.

    ``check(timeout_seconds)`` must only check PG with driver/connect/statement
    timeouts. This outer bound protects HTTP even if that dependency misbehaves.
    Python cannot cancel such a call: a permanently blocked call stays failed
    until it returns or the process restarts, without spawning replacement calls.
    """
    def __init__(self, check, *, timeout_seconds=0.5, cache_seconds=1,
                 monotonic=time.monotonic, wall_clock=time.time):
        _number(timeout_seconds, 'timeout_seconds', positive=True)
        _number(cache_seconds, 'cache_seconds')
        if timeout_seconds > 0.75:
            raise ValueError('database probe timeout must be at most 0.75 seconds')
        self._check, self._timeout, self._cache = check, timeout_seconds, cache_seconds
        self._mono, self._wall = monotonic, wall_clock
        self._lock = threading.Lock()
        self._event = None
        self._result = None
        self._closed = False

    def _run(self, event):
        try:
            reachable = self._check(self._timeout) is True
        except Exception:
            reachable = False
        with self._lock:
            self._result = (reachable, self._wall(), self._mono())
            event.set()

    def check(self):
        with self._lock:
            if self._closed:
                return DatabaseStatus(False, error_code='database_unavailable')
            if self._result is not None and self._mono() - self._result[2] <= self._cache:
                reachable, at, stamp = self._result
                return DatabaseStatus(reachable, at, max(0.0, self._mono() - stamp),
                                      None if reachable else 'database_unavailable')
            if self._event is None or self._event.is_set():
                self._event = threading.Event()
                threading.Thread(target=self._run, args=(self._event,), daemon=True,
                                 name='pg-health-probe').start()
            event = self._event
        if not event.wait(self._timeout):
            return DatabaseStatus(False, error_code='database_timeout')
        with self._lock:
            reachable, at, stamp = self._result
            return DatabaseStatus(reachable, at, max(0.0, self._mono() - stamp),
                                  None if reachable else 'database_unavailable')

    def close(self):
        # Never join an arbitrary dependency here: shutdown must remain bounded.
        with self._lock:
            self._closed = True


class _MonitorHandler(BaseHTTPRequestHandler):
    server_version = 'db-sync-alt-monitor'
    sys_version = ''

    def log_message(self, *_args):
        pass  # Request targets/headers may contain credentials: never echo them.

    def _reply(self, code, payload):
        body = json.dumps(payload, separators=(',', ':'), allow_nan=False).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def send_error(self, code, message=None, explain=None):
        self._reply(code, {'error_code': 'invalid_http_request'})

    def do_GET(self):
        if self.path not in {'/status', '/health'}:
            self._reply(404, {'error_code': 'not_found'})
            return
        try:
            database = None if self.server.probe is None else self.server.probe.check()
            code, snapshot = self.server.state.health(database)
            self._reply(200 if self.path == '/status' else code, snapshot)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            return
        except Exception:
            self._reply(503, {'error_code': 'internal_error'})


class HealthHTTPServer(ThreadingMixIn, HTTPServer):
    """Bounded monitoring listener; caller runs serve_forever/shutdown.

    Default binding is loopback. External binds require explicit opt-in. Client
    sockets time out and concurrent request threads are capped; excess clients
    are closed immediately instead of consuming an unbounded worker queue.
    """
    daemon_threads = True
    block_on_close = False
    request_queue_size = 16

    def __init__(self, state, *, host='127.0.0.1', port=0, probe=None,
                 allow_external=False, max_requests=16, socket_timeout=0.75, request_timeout=1.75):
        if not ipaddress.ip_address(host).is_loopback and not allow_external:
            raise ValueError('external HTTP bind requires explicit opt-in')
        if ':' in host:
            raise ValueError('use an IPv4 HTTP bind')
        _integer(max_requests, 'max_requests')
        _number(socket_timeout, 'socket_timeout', positive=True)
        _number(request_timeout, 'request_timeout', positive=True)
        if max_requests < 1 or socket_timeout > 1 or request_timeout > 1.75:
            raise ValueError('invalid HTTP request bounds')
        self.state, self.probe = state, probe
        self._slots = threading.BoundedSemaphore(max_requests)
        self._socket_timeout = socket_timeout
        self._request_timeout = request_timeout
        super().__init__((host, port), _MonitorHandler)

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(self._socket_timeout)
        return sock, address

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except Exception:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        # A per-read socket timeout alone can be evaded by slow-drip headers.
        # This absolute deadline also closes such clients. Timer count is
        # bounded by the same request semaphore as the handler threads.
        def expire():
            try:
                request.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        timer = threading.Timer(self._request_timeout, expire)
        timer.daemon = True
        timer.start()
        try:
            super().process_request_thread(request, client_address)
        finally:
            timer.cancel()
            self._slots.release()

    def handle_error(self, request, client_address):
        pass  # No raw exception/address logging on the monitoring surface.

    def server_close(self):
        if self.probe is not None:
            self.probe.close()
        super().server_close()
