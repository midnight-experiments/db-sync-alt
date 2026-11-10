# SPDX-License-Identifier: Apache-2.0
"""Sanitized lifecycle and sync failures shared by the service, its workers and the commands."""
import math


class CooperativeStop(Exception):
    """Owner-requested cancellation at a completed local work checkpoint."""


class LifecycleFailure(Exception):
    def __init__(self, code='internal_error'):
        self.code = code
        super().__init__(code)


class TransientSyncFailure(LifecycleFailure):
    """The sync driver reports an accounted retry/quota pause, without invalidating data.

    `request_budget_exhausted`: an update spent its request budget for this UTC day and continues on the next."""
    def __init__(self, code, *, retry_at):
        if code not in {'provider_unavailable', 'provider_timeout', 'quota_exhausted', 'request_budget_exhausted'}:
            raise ValueError('invalid_transient_error')
        if isinstance(retry_at, bool) or not isinstance(retry_at, (int, float)) or not math.isfinite(retry_at):
            raise ValueError('invalid_retry_time')
        super().__init__(code)
        self.retry_at = retry_at


