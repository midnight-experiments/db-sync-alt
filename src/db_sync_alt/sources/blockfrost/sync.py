# SPDX-License-Identifier: Apache-2.0
"""Durably budgeted Blockfrost continuation into the fenced PostgreSQL store.

Only the accounted client of the profile's network sends authenticated requests. Tests
inject a deterministic accounted client. A job reservation commits before every
attempt, including metrics/failures; restarts and day changes cannot reset an
acquisition budget. Polling has an explicit independent UTC-day budget.
"""
import datetime as dt
import hashlib
import math
import logging
import re
import time
import uuid

from db_sync_alt.resources import profile_dir
from db_sync_alt.sources.blockfrost.adapter import (OPENAPI, ProviderClient, block_row, update)
from db_sync_alt.contract import Profile, Refusal, canonical
from db_sync_alt.service.health import ChainPoint, PendingProgress, QuotaStatus
from db_sync_alt.errors import LifecycleFailure, TransientSyncFailure, CooperativeStop
from db_sync_alt.verify.completion import COMPLETION_ERRORS, validate_completion


FATAL_CONFLICTS = frozenset({
    'committed_source_conflict', 'provider_handoff_fact_conflict',
    'provider_origin_conflict', 'provider_origin_asset_conflict',
    'provider_inherited_epoch_conflict', 'immutable_fact_conflict',
    'canonical_block_conflict', 'historical_selected_event_changed',
    'historical_state_changed', 'historical_epoch_changed',
    'conflicting_inherited_membership',
})
UNSUPPORTED = frozenset({'unsupported_nonempty_candidates', 'unsupported_candidate_activity',
                         'unsupported_relevant_invalid_transaction', 'provider_relevant_invalid_transaction',
                         'unsupported_capabilities'})
EPOCH_WAIT = frozenset({'provider_epoch_not_ready', 'provider_epoch_end_not_qualified',
                         'provider_epoch_changed'})
# Fail-closed pending anchor refusals are reported under their own code.
ANCHOR_REFUSALS = frozenset({'pending_target_changed', 'pending_suffix_base_changed',
                             'pending_anchor_changed'})


# Persist only explicitly reviewed machine codes; never exception text or paths.
FAILURE_CODES = frozenset({
    'anchor_bounds_mismatch',
    'anchor_mismatch',
    'canonical_block_conflict',
    'canonical_conflict',
    'cbor_body_hash_mismatch',
    'cbor_datum_constructor',
    'cbor_datum_shape',
    'cbor_datum_tag',
    'cbor_datum_trailing_bytes',
    'cbor_depth_limit',
    'cbor_duplicate_input',
    'cbor_duplicate_map_key',
    'cbor_envelope_arity',
    'cbor_input_set',
    'cbor_input_shape',
    'cbor_input_tag',
    'cbor_invalid_chunks',
    'cbor_invalid_hash',
    'cbor_invalid_hex',
    'cbor_invalid_integer',
    'cbor_invalid_tag',
    'cbor_invalid_utf8',
    'cbor_item_limit',
    'cbor_mint_shape',
    'cbor_nonminimal_integer',
    'cbor_output_shape',
    'cbor_reserved_encoding',
    'cbor_size_limit',
    'cbor_trailing_bytes',
    'cbor_truncated',
    'cbor_unsupported_body',
    'cbor_unsupported_envelope',
    'cbor_unsupported_map_key',
    'cbor_unsupported_simple',
    'checkpoint_gap_or_overlap',
    'checkpoint_incomplete',
    'committed_source_conflict',
    'completion_invalid',
    'completion_mismatch',
    'completion_recovery_required',
    'conflicting_inherited_membership',
    'contradictory_ordinary_spend',
    'dependency_generation_invalid',
    'destination_not_empty',
    'destination_reader_already_exists',
    'epoch_anchor_mismatch',
    'epoch_bounds_mismatch',
    'epoch_incomplete',
    'epoch_receipt_missing',
    'follow_requires_moving_confirmed_target',
    'generation_missing',
    'generation_not_canonical_ready',
    'historical_epoch_changed',
    'historical_selected_event_changed',
    'historical_state_changed',
    'immutable_fact_conflict',
    'immutable_generation',
    'incomplete_owned_destination',
    'interrupted',
    'invalid_anchor_set',
    'invalid_bounds',
    'invalid_coverage',
    'invalid_destination_service',
    'invalid_follow_bounds',
    'invalid_identity',
    'invalid_inheritance_receipt',
    'invalid_lineage',
    'invalid_numeric_domain',
    'invalid_open_mode',
    'invalid_parent',
    'invalid_provider_path',
    'invalid_request_budget',
    'invalid_start_cursor',
    'invalid_sync_configuration',
    'invalid_sync_job',
    'invalid_timestamp',
    'invalid_witness',
    'manifest_contract_mismatch',
    'metadata_gap_or_parent_conflict',
    'metadata_incomplete',
    'nested_store_transaction',
    'no_confirmed_progress',
    'no_ready_generation',
    'nonlinear_lineage',
    'pending_job_missing',
    'pending_anchor_changed',
    'pending_or_conflicting_lineage',
    'pending_suffix_base_changed',
    'pending_target_changed',
    'prerequisites_incomplete',
    'profile_fingerprint_mismatch',
    'provider_accounting_unavailable',
    'provider_ambiguous_input_role',
    'provider_bad_address',
    'provider_bad_amounts',
    'provider_bad_asset',
    'provider_bad_block',
    'provider_bad_input',
    'provider_bad_input_role',
    'provider_bad_inputs',
    'provider_bad_inventory',
    'provider_bad_page',
    'provider_block_decode_limit',
    'provider_block_transaction_limit',
    'provider_client_unavailable',
    'provider_completeness_receipt_missing',
    'provider_conflicting_input_rows',
    'provider_datum_hash_mismatch',
    'provider_datum_semantics_mismatch',
    'provider_duplicate_asset',
    'provider_duplicate_or_invalid_stake',
    'provider_duplicate_transaction',
    'provider_empty_epoch_unqualified',
    'provider_epoch_anchor_mismatch',
    'provider_epoch_changed',
    'provider_epoch_end_not_qualified',
    'provider_epoch_not_ready',
    'provider_epoch_size_limit',
    'provider_epoch_total_mismatch',
    'provider_handoff_fact_conflict',
    'provider_inherited_epoch_conflict',
    'provider_inline_datum_hash_mismatch',
    'provider_input_membership_mismatch',
    'provider_invalid_hex',
    'provider_invalid_integer',
    'provider_invalid_pool',
    'provider_invalid_pool_checksum',
    'provider_invalid_pool_padding',
    'provider_metadata_gap',
    'provider_metadata_missing',
    'provider_metadata_parent_conflict',
    'provider_missing_datum',
    'provider_missing_datum_field',
    'provider_missing_input_membership',
    'provider_missing_lovelace',
    'provider_missing_outputs',
    'provider_negative_quantity',
    'provider_nonempty_handoff_unqualified',
    'provider_origin_asset_conflict',
    'provider_origin_block_mismatch',
    'provider_origin_conflict',
    'provider_origin_hash_mismatch',
    'provider_origin_missing',
    'provider_output_count_mismatch',
    'provider_output_role_mismatch',
    'provider_page_limit',
    'provider_profile_mismatch',
    'provider_receipt_unit_limit',
    'provider_relevant_invalid_transaction',
    'provider_request_failed_counted',
    'provider_request_rejected',
    'provider_response_size_limit',
    'provider_target_mismatch',
    'provider_timeout',
    'provider_transaction_count_mismatch',
    'provider_transaction_position_mismatch',
    'provider_unavailable',
    'provider_utxo_hash_mismatch',
    'publication_parent_changed',
    'publication_state_requires_fenced_transition',
    'quota_exhausted',
    'receipt_source_kind_mismatch',
    'request_budget_exhausted',
    'resume_adapter_mismatch',
    'resume_target_mismatch',
    'selected_asset_outside_profile',
    'semantic_digest_mismatch',
    'serving_connection_not_idle',
    'serving_unready_or_revoked',
    'shared_client_unavailable',
    'sql_acquisition_contract_required',
    'sql_completeness_receipt_missing',
    'storage_integrity_failed',
    'sync_job_budget_reset',
    'sync_job_reopen_refused',
    'sync_job_requires_durable_autocommit',
    'sync_job_rollover_refused',
    'target_not_confirmed',
    'transaction_coverage_invalid',
    'unclassified_failure',
    'unconfirmed_target',
    'unknown_fact_table',
    'unowned_database',
    'unsupported_candidate_activity',
    'unsupported_capabilities',
    'unsupported_capability',
    'unsupported_cursor',
    'unsupported_database_identity',
    'unsupported_format',
    'unsupported_network',
    'unsupported_nonempty_candidates',
    'unsupported_postgres_version',
    'unsupported_profile',
    'unsupported_relevant_invalid_transaction',
    'unsupported_source',
    'unsupported_source_kind',
    'untrusted_profile_configuration',
    'untrusted_profile_identity',
    'writer_busy',
    'writer_ownership_required',
    'wrong_provider_network',
}) | FATAL_CONFLICTS | UNSUPPORTED | EPOCH_WAIT | COMPLETION_ERRORS
FAILURE_STAGES = frozenset({'unknown', 'poll', 'verification', 'genesis', 'anchors',
    'handoff', 'metadata', 'metadata.commit', 'events', 'events.inventory',
    'events.transaction', 'events.commit', 'epochs', 'epochs.commit',
    'finalization', 'publication', 'completion'})


def safe_failure(value):
    """Bounded public/inspect view; corrupt historical diagnostics never leak."""
    if not isinstance(value, dict) or type(value.get('version')) is not int or value['version'] != 1:
        return None
    if (not isinstance(value.get('code'), str) or not isinstance(value.get('stage'), str)
            or value['code'] not in FAILURE_CODES or value['stage'] not in FAILURE_STAGES
            or type(value.get('attempts')) is not int or not 0 <= value['attempts'] <= 10000
            or type(value.get('at')) not in (int, float) or not 0 <= value['at'] < 2**63 or not math.isfinite(value['at'])):
        return None
    out = {k: value[k] for k in ('version', 'code', 'stage', 'attempts', 'at')}
    for key in ('generation', 'block_height', 'transaction_index', 'epoch'):
        v = value.get(key)
        if type(v) is int and 0 <= v < 2**63:
            out[key] = v
    for key in ('block_hash', 'transaction_hash'):
        v = value.get(key)
        if isinstance(v, str) and re.fullmatch('[0-9a-f]{64}', v):
            out[key] = v
    return out


def point(row):
    return ChainPoint(row['height'], bytes(row['hash']).hex(),
                      dt.datetime.fromisoformat(row['time'].replace('Z', '+00:00')).timestamp())


def next_day(now):
    current = dt.datetime.fromtimestamp(now, dt.timezone.utc)
    return (current.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)).timestamp()


def shared_client(network):
    try:
        from db_sync_alt.sources.blockfrost.provider import configured_client
        client = configured_client(network)
        return client
    except LifecycleFailure:
        raise
    except Exception:
        raise LifecycleFailure('provider_client_unavailable') from None


def local_usage(client):
    """Local accounted metadata only; never /metrics or another HTTP request."""
    try:
        value = client.status()
        return value
    except Exception:
        raise LifecycleFailure('provider_accounting_unavailable') from None


def validate_job(job, *, kind=None, security=None):
    """Fail closed on corrupted counters; never normalize a spent budget.

    An acquisition job is valid only with the network's security parameter:
    its witness is its target + k.
    """
    if not isinstance(job, dict):
        raise LifecycleFailure('invalid_sync_job')
    if (not isinstance(job.get('job_id'), str) or re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', job['job_id']) is None
            or type(job.get('attempts')) is not int or type(job.get('budget')) is not int
            or not 0 <= job['attempts'] <= job['budget'] <= 10000 or job['budget'] < 1
            or job.get('state') not in {'active', 'completed', 'exhausted'}
            or job.get('kind') not in {'poll', 'acquire'}
            or (kind is not None and job['kind'] != kind)):
        raise LifecycleFailure('invalid_sync_job')
    for field in ('created_at', 'reserved_at', 'metrics_at', 'retry_at', 'completed_at'):
        if field in job and (isinstance(job[field], bool) or not isinstance(job[field], (int, float))
                             or not math.isfinite(job[field]) or job[field] < 0):
            raise LifecycleFailure('invalid_sync_job')
    for field in ('failure_count', 'metrics_count'):
        if field in job and (type(job[field]) is not int or job[field] < 0):
            raise LifecycleFailure('invalid_sync_job')
    if job.get('metrics_count', 0) > job['attempts'] or job.get('failure_count', 0) > 20:
        raise LifecycleFailure('invalid_sync_job')
    if job.get('error_code') not in {None, 'provider_unavailable', 'provider_timeout', 'quota_exhausted', 'request_budget_exhausted'}:
        raise LifecycleFailure('invalid_sync_job')
    if job['kind'] == 'acquire':
        if any(type(job.get(field)) is not int or job[field] < 0 for field in ('target', 'witness', 'parent')):
            raise LifecycleFailure('invalid_sync_job')
        if type(security) is not int or job['witness'] != job['target'] + security:
            raise LifecycleFailure('invalid_sync_job')
    return job


class AccountedProvider(ProviderClient):
    def __init__(self, store, key, job, client, health, stop_event, *, network, clock=time.time):
        # The job row carries budget, attempts and identity; there is no per-invocation budget.
        validate_job(job, security=network.security)
        self.network = network
        self.store, self.key, self.job, self.client = store, key, dict(job), client
        self.health, self.stop_event, self.clock = health, stop_event, clock
        self.job_id, self.budget, self.attempts = job['job_id'], job['budget'], job['attempts']
        self.receipts = []
        self.failure_context = {'stage': 'unknown'}
        self.failure_recorded = False

    def set_failure_context(self, stage, **fields):
        # Every stage resets block/transaction fields. Only generation carries.
        generation = fields.get('generation', self.failure_context.get('generation'))
        candidate = dict(version=1, code='unclassified_failure', stage=stage if isinstance(stage, str) and stage in FAILURE_STAGES else 'unknown',
                         attempts=0, at=0, **{k: v for k, v in fields.items() if k in
                         {'block_height', 'block_hash', 'transaction_index', 'transaction_hash', 'epoch'}})
        candidate['generation'] = generation
        for key in ('block_hash', 'transaction_hash'):
            if type(candidate.get(key)) is bytes:
                candidate[key] = candidate[key].hex()
        safe = safe_failure(candidate)
        self.failure_context = {k: v for k, v in safe.items() if k not in {'version', 'code', 'attempts', 'at'}}

    def record_failure(self, error):
        """Called after unit rollback; diagnostics must never replace the error."""
        if self.failure_recorded:
            return
        self.failure_recorded = True
        try:
            code = getattr(error, 'code', None)
            if not isinstance(code, str) or code not in FAILURE_CODES:
                code = 'unclassified_failure'
            record = safe_failure(dict(version=1, code=code, attempts=self.attempts,
                                       at=self.clock(), **self.failure_context))
            if record is None:
                raise ValueError('invalid_diagnostic')
            # Save from the current durable row, not a potentially stale copy.
            saved = self.store.sync_job(self.key)
            if saved is None or saved['job_id'] != self.job_id or saved['attempts'] != self.attempts:
                raise ValueError('diagnostic_job_changed')
            updated = dict(saved, last_failure=record)
            self.store.save_sync_job(self.key, updated)
            self.job = updated
            logging.getLogger(__name__).warning('sync_failure_recorded code=%s stage=%s', record['code'], record['stage'])
        except Exception:
            logging.getLogger(__name__).warning('failure_record_unavailable')

    def save(self):
        self.job['attempts'] = self.attempts
        self.store.save_sync_job(self.key, self.job)

    def _quota(self):
        value = local_usage(self.client)
        used, limit = value.get('budget_used'), value.get('daily_limit')
        if type(used) is not int or used < 0 or type(limit) is not int or not 1 <= limit <= 50000:
            raise LifecycleFailure('provider_accounting_unavailable')
        self.health.update(quota=QuotaStatus(used, limit, next_day(self.clock())))
        return value

    def _wait(self, code, *, minimum=0, quota=False):
        failures = min(20, self.job.get('failure_count', 0) + 1)
        delay = min(300, 5 * 2 ** (failures - 1))
        retry = max(self.clock() + delay, minimum, next_day(self.clock()) if quota else 0)
        self.job.update(failure_count=failures, retry_at=retry, error_code=code)
        self.save()
        raise TransientSyncFailure(code, retry_at=retry)

    def get(self, path):
        return self._get(path, missing_ok=False)

    def get_optional(self, path):
        """Counted GET whose provider HTTP 404 answer is returned as None."""
        return self._get(path, missing_ok=True)

    def _get(self, path, *, missing_ok):
        if self.stop_event.is_set():
            raise Refusal('interrupted')
        if (not isinstance(path, str) or not path.startswith('/') or path.startswith('//')
                or any(c in path for c in ('#', '\\', '\n', '\r'))):
            raise Refusal('invalid_provider_path')
        now = self.clock()
        if self.job.get('retry_at', 0) > now:
            raise TransientSyncFailure(self.job.get('error_code', 'provider_unavailable'),
                                       retry_at=self.job['retry_at'])
        usage = self._quota()
        if usage.get('exhausted') or usage['budget_used'] >= usage['daily_limit']:
            self._wait('quota_exhausted', quota=True)
        cooldown = usage.get('cooldown_until')
        if isinstance(cooldown, (int, float)) and cooldown > now:
            self._wait('provider_unavailable', minimum=cooldown)
        day = dt.datetime.fromtimestamp(now, dt.timezone.utc).date().isoformat()
        last_metrics = self.job.get('metrics_at', 0)
        if path != '/metrics' and (self.job.get('metrics_day') != day or now < last_metrics
                or now - last_metrics >= 600 or self.attempts - self.job.get('metrics_count', 0) >= 1000):
            self.get('/metrics')
        if self.attempts >= self.budget:
            # This job is terminal until explicit operator action. Never mint a
            # replacement acquisition job to evade its cumulative cap.
            self.job.update(state='exhausted', error_code='request_budget_exhausted')
            self.save()
            if self.job.get('kind') == 'poll':
                self._wait('quota_exhausted', quota=True)
            raise LifecycleFailure('request_budget_exhausted')
        self.attempts += 1
        self.job['reserved_at'] = self.clock()
        self.save()  # durable reservation BEFORE shared request, including failure
        self.health.end_operation()
        self.health.begin_operation('provider')
        try:
            result = self.client.get(path, purpose='db-sync-alt:' + self.job_id)
        except Exception as error:
            usage = self._quota()
            text = str(error)  # classify known shared machine text, never serialize it
            status = getattr(error, 'http_status', None)
            kind = getattr(error, 'kind', None)
            if usage.get('exhausted') or usage['budget_used'] >= usage['daily_limit'] or text.startswith('Daily request budget exhausted') or text.startswith('HTTP 402;') or status == 402:
                self._wait('quota_exhausted', quota=True)
            if status in (418, 429) or kind == 'cooldown' or text.startswith(('HTTP 418;', 'HTTP 429;', 'Provider cooldown active', 'Shared rate-limit cooldown')):
                self._wait('provider_unavailable', minimum=max(self.clock() + 60, usage.get('cooldown_until') or 0))
            if missing_ok and (status == 404 or text.startswith('HTTP 404;')):
                return None
            if (status == 404 or text.startswith('HTTP 404;')) and path.startswith('/epochs/'):
                raise Refusal('provider_epoch_not_ready') from None
            if status in (500,502,503,504) or kind in ('timeout','connection') or text.startswith(('HTTP 500;', 'HTTP 502;', 'HTTP 503;', 'HTTP 504;', 'URLError;', 'TimeoutError;', 'ConnectionError;')):
                self._wait('provider_timeout' if kind == 'timeout' or 'Timeout' in text else 'provider_unavailable')
            # Authentication, invalid JSON, unexpected HTTP statuses and unknown
            # errors are not blindly retried and never expose raw exception text.
            raise LifecycleFailure('provider_request_rejected') from None
        finally:
            self.health.end_operation()
            self.health.begin_operation('verification')
        self._quota()
        encoded = canonical(result).encode()
        if len(encoded) > 32 * 1024 * 1024:
            raise Refusal('provider_response_size_limit')
        self.receipts.append({'endpoint': path, 'sha256': hashlib.sha256(encoded).hexdigest(),
                              'bytes': len(encoded), 'attempt': self.attempts,
                              'at': dt.datetime.fromtimestamp(self.clock(), dt.timezone.utc).isoformat()})
        if len(self.receipts) > 20000:
            raise Refusal('provider_receipt_unit_limit')
        if path == '/metrics':
            self.job.update(metrics_day=day, metrics_at=self.clock(), metrics_count=self.attempts)
        self.job.update(retry_at=0, error_code=None)
        self.save()
        return result

    def drain(self):
        value = {'source': 'blockfrost-cardano', 'shared_profile': self.network.network,
                 'shared_accounting_purpose': 'db-sync-alt:' + self.job_id,
                 'adapter': self.network.adapter, 'openapi': OPENAPI, 'attempts_this_invocation': self.attempts,
                 'attempts_cumulative_job': self.attempts, 'job_budget': self.budget,
                 'requests': self.receipts}
        self.receipts = []
        return value


class PgHooks:
    def __init__(self, health, stop_event):
        self.health, self.stop_event = health, stop_event

    def verify(self, store, generation=None, **kwargs):
        from db_sync_alt.verify.store import verify_store
        self.health.end_operation()
        self.health.begin_operation('verification')
        return verify_store(store, generation, **kwargs)

    def check_space(self, store):
        store.check_space()

    def update(self, **fields):
        # Phase changes are bookkeeping, not useful progress checkpoints.
        if fields.get('phase') == 'verifying':
            self.health.end_operation()
            self.health.begin_operation('verification')

    def sample(self, store, **fields):
        try:
            self.health.operation_progress()
        except ValueError:
            pass
        pending = store.pending()
        if pending:
            ready = store.generation(pending['parent'])
            row = store.block_at(pending['id'], pending['target_height'])
            target = point(row) if row else ChainPoint(pending['target_height'], pending['manifest']['anchors']['target']['hash'])
            completed = max(ready['target_height'], store.last_event_height(pending['id']) or ready['target_height'])
            self.health.update(pending=PendingProgress(target, completed - ready['target_height'],
                                                       pending['target_height'] - ready['target_height']))
        else:
            self.health.update(pending=PendingProgress())

    def stopped(self):
        return self.stop_event.is_set()

    def before_publish(self):
        if self.stopped():
            raise Refusal('interrupted')
        self.health.end_operation()
        self.health.begin_operation('database')


class SyncDriver:
    def __init__(self, config, *, client=None, profile=None, clock=time.time, update_fn=update, fixed_target=None):
        self.config, self._client, self.clock, self.update_fn = config, client, clock, update_fn
        if fixed_target is not None and (type(fixed_target) is not int or fixed_target < 0):
            raise LifecycleFailure('invalid_sync_configuration')
        self.fixed_target = fixed_target
        self._poller = None
        self._follow_sequence = 0
        self._eligible_at = None
        self._checkpoint_due = None
        self.profile = profile or Profile(profile_dir(config.network), config.network)
        self.options = dict(getattr(config, 'sync', {}))
        defaults = {'max_batch_blocks': 100, 'publication_interval_seconds': 60,
                    'acquisition_request_budget': 10000, 'daily_poll_request_budget': 10000,
                    'follow_min_blocks': 1, 'follow_max_wait_seconds': 40,
                    'integrity_checkpoint_interval_seconds': 21600}
        if set(self.options) - set(defaults):
            raise LifecycleFailure('invalid_sync_configuration')
        for key, value in defaults.items():
            self.options.setdefault(key, value)
        for key, low, high in (('max_batch_blocks', 1, 1000), ('publication_interval_seconds', 1, 1800),
                               ('acquisition_request_budget', 1, 10000), ('daily_poll_request_budget', 1, 10000),
                               ('follow_min_blocks', 1, 100), ('follow_max_wait_seconds', 1, 120),
                               ('integrity_checkpoint_interval_seconds', 600, 604800)):
            if type(self.options[key]) is not int or not low <= self.options[key] <= high:
                raise LifecycleFailure('invalid_sync_configuration')

    def check_follow_mode(self, store):
        if not getattr(store, 'supports_incremental', False):
            raise LifecycleFailure('unsupported_follow_mode')

    def start_polling(self, store, health, stop_event, on_fatal):
        from db_sync_alt.sources.blockfrost.poller import HeadPoller, PollBudgetStore, owner_binding
        if self._poller is not None:
            raise LifecycleFailure('startup_failed')
        self.check_follow_mode(store)
        if self.options['daily_poll_request_budget'] != 10000:
            raise LifecycleFailure('invalid_sync_configuration')
        health.configure_follow('incremental', self.config.policy.poll_seconds)
        binding = owner_binding(store)
        if self._client is None:
            self._client = shared_client(self.profile.network)
        self._poller = HeadPoller(lambda: PollBudgetStore(self.config.pg_service, binding),
                                 lambda: self._client, health, stop_event,
                                 interval=self.config.policy.poll_seconds, on_fatal=on_fatal,
                                 network=self.profile.network)
        return self._poller.start()

    def wait_for_observation(self, timeout=1):
        if self._poller is not None:
            self._poller.wait(self._follow_sequence, timeout)

    def _job(self, store, key, budget, **fields):
        from db_sync_alt.store.database import PgStore
        policy_poll=fields.get('kind')=='poll' and budget==10000 and isinstance(store,PgStore)
        old = store.sync_job(key)
        if old is not None:
            validate_job(old, kind=fields.get('kind'))
            if old.get('budget') != budget or any(old.get(key) != value for key, value in fields.items()):
                raise LifecycleFailure('invalid_sync_configuration')
            if policy_poll:
                from db_sync_alt.sources.blockfrost.poller import bind_poll_job
                bind_poll_job(store,key,old,clock=self.clock)
            return old
        value = dict(job_id=uuid.uuid4().hex, attempts=0, budget=budget, state='active',
                     created_at=self.clock(), retry_at=0, failure_count=0, **fields)
        if policy_poll:
            from db_sync_alt.sources.blockfrost.poller import bind_poll_job
            bind_poll_job(store,key,value,clock=self.clock)
        else:store.save_sync_job(key, value)
        return value

    def _provider(self, store, key, job, health, stop_event):
        if self._client is None:
            self._client = shared_client(self.profile.network)
        provider = AccountedProvider(store, key, job, self._client, health, stop_event, clock=self.clock,
                                     network=self.profile.network)
        self._diagnostic_provider = provider
        return provider

    def run_once(self, store, health, stop_event):
        self._diagnostic_provider = None
        from db_sync_alt.store.database import PgStore
        from db_sync_alt.sources.blockfrost.poller import POLL_LOCK
        direct_poll_lock=False
        try:
            if self._poller is None and isinstance(store,PgStore):
                direct_poll_lock=store.db.execute('SELECT pg_try_advisory_lock(%s,%s)',POLL_LOCK).fetchone()[0]
                if not direct_poll_lock:raise LifecycleFailure('writer_busy')
            return self._run_once(store, health, stop_event)
        except Exception as error:
            # The lifecycle owner catches this exact local checkpoint signal.
            # Preserve its rollback/no-diagnostic behavior across driver frames.
            if type(error) is CooperativeStop and stop_event.is_set():
                raise
            # Polling can observe the same cooperative stop as acquisition.
            # Only the exact expected refusal is cancellation; real failures
            # during shutdown retain diagnostics and their original mapping.
            if isinstance(error, Refusal) and error.code == 'interrupted' and stop_event.is_set():
                return
            provider = self._diagnostic_provider
            if provider is not None:
                provider.record_failure(error)
            raise
        finally:
            if direct_poll_lock and not store.db.closed:
                store.db.execute('SELECT pg_advisory_unlock(%s,%s)',POLL_LOCK)

    def _checkpoint(self, store, health):
        """Between publications, once `integrity_checkpoint_interval_seconds` have passed since the
        base's checkpoint: verify the canonical generation in full and make it the integrity base,
        so a restart walks only what is published after it. Publication waits meanwhile."""
        if not getattr(store, 'supports_checkpoints', False):
            return False
        interval = self.options['integrity_checkpoint_interval_seconds']
        if self._checkpoint_due is None:
            # A base without a checkpoint (a full-certified generation) counts from this writer's start.
            at = store.integrity()['checkpointed_at']
            self._checkpoint_due = (self.clock() if at is None else at) + interval
        if self.clock() < self._checkpoint_due:
            return False
        if store.state()['revoked'] or store.generation()['id'] <= store.integrity()['base']:
            return False
        health.end_operation()
        health.begin_operation('verification')
        health.update(sync='checkpointing')
        try:
            store.write_checkpoint(store.certify_checkpoint())
        except Refusal as error:
            # Something changed the verified contents meanwhile: verify again on the next pass.
            if error.code not in ('integrity_revision_changed', 'checkpoint_uncertified'):
                raise
            return True
        integrity = store.integrity()
        self._checkpoint_due = integrity['checkpointed_at'] + interval
        health.integrity(**integrity)
        health.update(sync='syncing')
        return True

    def _run_once(self, store, health, stop_event):
        if stop_event.is_set():
            return
        ready = store.generation()
        pending = store.pending()
        if pending:
            try:
                validate_completion(store, pending)
            except Refusal as error:
                raise LifecycleFailure(error.code if error.code in COMPLETION_ERRORS else 'invalid_coverage') from None
        active = store.sync_job('provider-active')
        if active is not None:
            validate_job(active, kind='acquire', security=self.profile.security)
            if active['target'] > ready['target_height'] and active['parent'] != ready['id']:
                raise LifecycleFailure('invalid_sync_job')
        # A crash after commit but before job completion must neither republish
        # nor assign another budget to the just-completed fixed target.
        if active and active['target'] <= ready['target_height']:
            if active['state'] != 'completed':
                active.update(state='completed', completed_at=self.clock())
                store.save_sync_job('provider-active', active)
        pending = store.pending()
        if pending and (not active or active['target'] != pending['target_height'] or active['state'] == 'completed'
                        or active['witness'] != pending.get('witness_height')
                        or active['parent'] != pending.get('parent')):
            raise LifecycleFailure('pending_job_missing')
        if active and active['state'] == 'exhausted':
            raise LifecycleFailure('request_budget_exhausted')
        if active and active['state'] != 'completed' and active.get('retry_at', 0) > self.clock():
            raise TransientSyncFailure(active.get('error_code', 'provider_unavailable'), retry_at=active['retry_at'])
        if not pending and self._checkpoint(store, health):
            return False
        observation = None
        if self._poller is not None:
            observation = self._poller.latest()
            if observation is None:
                return False
            if observation.sequence <= self._follow_sequence and (not active or active['state']=='completed'):
                return False
            self._follow_sequence = observation.sequence
            if not observation.fresh(health.policy.observation_max_age_seconds):
                return False
            head, confirmed = block_row(observation.head), block_row(observation.confirmed)
            confirmed_height = confirmed['height']
            if confirmed_height != head['height'] - self.profile.security:
                raise LifecycleFailure('invalid_coverage')
            if confirmed_height < ready['target_height']:
                # Publication may race a poll that began against the previous
                # parent. Do not mark a regressed source observation successful.
                health.poll_error('provider_unavailable')
                raise TransientSyncFailure('provider_unavailable',retry_at=self.clock()+20)
            if self.fixed_target is not None and self.fixed_target > confirmed_height:
                raise LifecycleFailure('unconfirmed_target')
            if confirmed_height == ready['target_height'] and confirmed['hash'].hex() != ready['manifest']['anchors']['target']['hash']:
                store.mark_conflict()
                raise LifecycleFailure('canonical_conflict')
        else:
            day = dt.datetime.fromtimestamp(self.clock(), dt.timezone.utc).date().isoformat()
            poll_key = 'provider-poll-' + day
            poll_job = self._job(store, poll_key, self.options['daily_poll_request_budget'], kind='poll', day=day)
            poll = self._provider(store, poll_key, poll_job, health, stop_event)
            poll.set_failure_context('poll', generation=ready['id'])
            head = block_row(poll.get('/blocks/latest'))
            confirmed_height = head['height'] - self.profile.security
            if self.fixed_target is not None and self.fixed_target > confirmed_height:
                raise LifecycleFailure('unconfirmed_target')
            if confirmed_height < ready['target_height']:
                # A lower head alone is not proof of a canonical conflict; refuse
                # acquisition until a stable provider observation is available.
                poll._wait('provider_unavailable')
            if confirmed_height < 0:
                raise LifecycleFailure('invalid_coverage')
            poll.set_failure_context('poll', generation=ready['id'], block_height=confirmed_height)
            confirmed = block_row(poll.get('/blocks/' + str(confirmed_height)))
            if confirmed['height'] != confirmed_height:
                raise LifecycleFailure('invalid_coverage')
            if confirmed_height == ready['target_height'] and confirmed['hash'].hex() != ready['manifest']['anchors']['target']['hash']:
                store.mark_conflict()
                raise LifecycleFailure('canonical_conflict')
            health.observe_provider(point(head), point(confirmed))
        if confirmed_height == ready['target_height'] and not pending:
            self._eligible_at = None
            return  # Cheap idle: never verify full history or perform handoff.
        if active and active['state'] != 'completed':
            if active['state'] == 'exhausted':
                raise LifecycleFailure('request_budget_exhausted')
            target = active['target']
        else:
            # Near tip use the configured spacing after durable job completion.
            # Catch-up >one batch may continue each poll without the spacing.
            last = active.get('completed_at') if active else None
            lag = confirmed_height - ready['target_height']
            if self._poller is None and last is not None and lag <= self.options['max_batch_blocks'] and self.clock() - last < self.options['publication_interval_seconds']:
                return
            if self._poller is not None and self.fixed_target is None:
                if self._eligible_at is None:
                    self._eligible_at = observation.observed_at
                if (lag < self.options['follow_min_blocks'] and lag < health.policy.serving_lag_max_blocks
                        and self.clock() - self._eligible_at < self.options['follow_max_wait_seconds']):
                    return
            self._eligible_at = None
            target = self.fixed_target if self.fixed_target is not None else min(confirmed_height, ready['target_height'] + self.options['max_batch_blocks'])
            active = dict(job_id=uuid.uuid4().hex, attempts=0,
                          budget=self.options['acquisition_request_budget'], state='active', kind='acquire',
                          parent=ready['id'], target=target, witness=target + self.profile.security,
                          created_at=self.clock(), retry_at=0, failure_count=0)
            store.save_sync_job('provider-active', active)
        if active['budget'] != self.options['acquisition_request_budget']:
            raise LifecycleFailure('invalid_sync_configuration')
        provider = self._provider(store, 'provider-active', active, health, stop_event)
        provider.set_failure_context('verification', generation=pending['id'] if pending else ready['id'])
        hooks = PgHooks(health, stop_event)
        try:
            if self.update_fn is update and getattr(store, 'supports_incremental', False) and (not pending or pending['manifest'].get('integrity', {}).get('version') == 2):
                from db_sync_alt.sources.blockfrost.follow import update_follow
                update_follow(store, provider, self.profile, target, hooks=hooks, observation=observation)
            else:
                self.update_fn(store, provider, self.profile, target, hooks=hooks)
                if getattr(store, 'supports_incremental', False) and self.update_fn is update:
                    store.certify_ready_full()
        except Refusal as error:
            if not (error.code == 'interrupted' and stop_event.is_set()):
                provider.record_failure(error)
            if error.code in COMPLETION_ERRORS:
                raise LifecycleFailure(error.code) from None
            if error.code == 'interrupted' and stop_event.is_set():
                return
            if error.code in FATAL_CONFLICTS | UNSUPPORTED:
                store.mark_conflict()
                raise LifecycleFailure('unsupported_capability' if error.code in UNSUPPORTED else 'canonical_conflict') from None
            if error.code in EPOCH_WAIT:
                provider._wait('provider_unavailable', minimum=self.clock() + 60)
            if error.code in ANCHOR_REFUSALS:
                raise LifecycleFailure(error.code) from None
            raise LifecycleFailure('invalid_coverage') from None
        if stop_event.is_set():
            return
        provider.set_failure_context('completion')
        if store.generation()['target_height'] != target:
            raise LifecycleFailure('invalid_coverage')
        provider.job.update(state='completed', completed_at=self.clock(), retry_at=0, failure_count=0)
        provider.save()
        health.update(pending=PendingProgress())
