# SPDX-License-Identifier: Apache-2.0
"""Sanitized lifecycle and sync failures shared by the service, its workers and the commands."""
import logging
import math


class CooperativeStop(Exception):
    """Owner-requested cancellation at a completed local work checkpoint."""


class LifecycleFailure(Exception):
    def __init__(self, code='internal_error'):
        self.code = code
        super().__init__(code)


class TransientSyncFailure(LifecycleFailure):
    """The sync driver reports an accounted retry/quota pause, without invalidating data.

    `request_budget_exhausted`: an update spent its request budget for this UTC day and continues on the next.
    `database_lock_timeout`: another session held a row or lock longer than this session's `lock_timeout`."""
    def __init__(self, code, *, retry_at):
        if code not in {'provider_unavailable', 'provider_timeout', 'quota_exhausted', 'request_budget_exhausted',
                        'database_lock_timeout'}:
            raise ValueError('invalid_transient_error')
        if isinstance(retry_at, bool) or not isinstance(retry_at, (int, float)) or not math.isfinite(retry_at):
            raise ValueError('invalid_retry_time')
        super().__init__(code)
        self.retry_at = retry_at


# A lock timeout (PostgreSQL `lock_not_available`) in the update or the poll is temporary: a stalled disk can keep a
# shared job row locked longer than the other session's `lock_timeout`. The run rolls back and starts again after
# these pauses (the last one repeats); lock timeouts in a row for LOCK_TIMEOUT_LIMIT_SECONDS fail as a database error.
LOCK_NOT_AVAILABLE = '55P03'
LOCK_TIMEOUT_BACKOFF_SECONDS = (1, 2, 4, 8, 16, 30)
LOCK_TIMEOUT_LIMIT_SECONDS = 300


def lock_timeout(error):
    """A PostgreSQL statement that waited longer than its session's `lock_timeout` (SQLSTATE 55P03)."""
    return type(error).__module__.startswith('psycopg') and getattr(error, 'sqlstate', None) == LOCK_NOT_AVAILABLE


class LockTimeouts:
    """One thread's runs in a row that ended in a lock timeout. Any other ending of a run, a success or another
    wait, ends the series (`reset`)."""
    def __init__(self, clock, worker):
        self.clock, self.worker = clock, worker
        self.first, self.count = None, 0

    def retry(self, error):
        """The pause after `error`, a lock timeout. Raises `error` itself once lock timeouts in a row have lasted
        LOCK_TIMEOUT_LIMIT_SECONDS: the caller then fails as on any database error."""
        now = self.clock()
        if self.first is None:
            self.first = now
        if now - self.first >= LOCK_TIMEOUT_LIMIT_SECONDS:
            logging.getLogger(__name__).warning('database_lock_timeout_persisted worker=%s seconds=%d',
                                                self.worker, now - self.first)
            raise error
        delay = LOCK_TIMEOUT_BACKOFF_SECONDS[min(self.count, len(LOCK_TIMEOUT_BACKOFF_SECONDS) - 1)]
        self.count += 1
        logging.getLogger(__name__).warning('database_lock_timeout_retry worker=%s retry_in_seconds=%d series_seconds=%d',
                                            self.worker, delay, now - self.first)
        return TransientSyncFailure('database_lock_timeout', retry_at=now + delay)

    def reset(self):
        self.first, self.count = None, 0


