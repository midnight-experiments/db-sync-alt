"""Integration tests against one disposable PostgreSQL 17 server.

`DB_SYNC_ALT_TEST_DSN` names a superuser connection on that server; the tests create
and drop their own databases and roles. `tools/dev.py integration` provides it.
"""
import os

if not os.environ.get('DB_SYNC_ALT_TEST_DSN'):
    raise RuntimeError('integration tests need DB_SYNC_ALT_TEST_DSN (a disposable PostgreSQL 17 superuser DSN)')
