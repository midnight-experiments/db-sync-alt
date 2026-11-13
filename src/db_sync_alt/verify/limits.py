"""The statement limit of full-history verification, and the advisory ANALYZE after bulk loads.

Every store session runs with `statement_timeout` 30 s, the limit of node-facing, serving and poller
work. A verification statement whose work grows with the stored history, or with all rows of a
generation, runs under VERIFICATION_TIMEOUT instead: `verification()` sets it with SET LOCAL for that
statement's own transaction (a savepoint inside a caller's transaction) and restores the previous value
when the statement ends, so no other statement runs under it.

VERIFICATION_TIMEOUT: these statements cost about 3 µs per row with a warm cache and up to about 35 µs
per row when every row is a cold index lookup. One statement over 10 M rows at 100 times the warm cost
(300 µs per row) takes 3,000 s; the limit is one hour. It stays bounded, so a runaway plan still ends.

`check`, `check_all` and `any_row` run their statement unprepared, so PostgreSQL plans it with its
parameters every time. A follow repeats the same checks for every update on one connection: psycopg
prepares a statement on its fifth execution, and after five more PostgreSQL may switch to a generic
plan that cannot see the generation number. For `first_generation=$1` that plan reads the whole
table instead of the update's rows through `delta_<table>` (issue 00054).
"""
from contextlib import contextmanager
import json
import logging

import psycopg

VERIFICATION_TIMEOUT = '3600s'
# BULK_TIMEOUT: one-time whole-table DDL of a schema upgrade that cannot be split into smaller statements:
# an index build, a constraint that validates every row, the first membership projection. On the Mainnet
# store (2026-11: about 1 M membership blocks and 2.6 M relevant outputs) each takes seconds to minutes on
# write-limited storage; the limit is one hour, like VERIFICATION_TIMEOUT, so a runaway statement still ends.
# Every other upgrade statement runs in bounded chunks under the store's 30 s session limit (issue 00042).
BULK_TIMEOUT = '3600s'
# ANALYZE samples about 30,000 rows per table whatever its size: seconds on a large store.
ANALYZE_TIMEOUT = '600s'

log = logging.getLogger('db_sync_alt.verification')


@contextmanager
def verification(db, query, params=None):
    """Run the block's statements (one statement, or one named cursor's fetches) under VERIFICATION_TIMEOUT.

    `query` and `params` are the statement the block runs. If PostgreSQL cancels it, its plan and the
    estimated row counts of its relations are logged and the cancellation is raised unchanged."""
    try:
        with db.transaction():
            previous = db.execute("SELECT current_setting('statement_timeout')").fetchone()[0]
            db.execute("SET LOCAL statement_timeout='" + VERIFICATION_TIMEOUT + "'")
            yield
            db.execute("SELECT set_config('statement_timeout',%s,true)", (previous,))
    except psycopg.errors.QueryCanceled:
        _diagnose(db, query, params)
        raise


@contextmanager
def bulk(db):
    """Run the block's statements under BULK_TIMEOUT, inside the caller's transaction (a savepoint), and
    restore the previous limit when they end, so no other statement runs under it."""
    with db.transaction():
        previous = db.execute("SELECT current_setting('statement_timeout')").fetchone()[0]
        db.execute("SET LOCAL statement_timeout='" + BULK_TIMEOUT + "'")
        yield
        db.execute("SELECT set_config('statement_timeout',%s,true)", (previous,))


def check(db, query, params=None):
    """The first row of one verification statement run under VERIFICATION_TIMEOUT (None: no row)."""
    with verification(db, query, params):
        return db.execute(query, params, prepare=False).fetchone()


def check_all(db, query, params=None):
    """Every row of one verification statement run under VERIFICATION_TIMEOUT."""
    with verification(db, query, params):
        return db.execute(query, params, prepare=False).fetchall()


def any_row(db, query, params=None):
    """Whether a violation query returns any row, under VERIFICATION_TIMEOUT.

    Its rows are counted instead of fetching the first. Under LIMIT 1, PostgreSQL expects a filter such as
    `a IS NULL OR b IS DISTINCT FROM c` to match early and plans a nested loop that stops at the first match;
    on a sound store nothing matches, so that loop compares every pair of rows. A count is planned for the
    whole result, with hash or merge joins."""
    counted = 'SELECT count(*) FROM (' + query + ') found'
    with verification(db, counted, params):
        return db.execute(counted, params, prepare=False).fetchone()[0] > 0


def _relations(node):
    own = [(node['Schema'], node['Relation Name'])] if 'Relation Name' in node and 'Schema' in node else []
    return own + [r for child in node.get('Plans', []) for r in _relations(child)]


def _diagnose(db, query, params):
    try:
        with db.transaction():
            plan = db.execute('EXPLAIN (VERBOSE, FORMAT JSON) ' + query, params).fetchone()[0]
            relations = sorted(set(_relations(plan[0]['Plan'])))
            estimates = dict(db.execute(
                "SELECT n.nspname||'.'||c.relname,c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace"
                ' WHERE (n.nspname,c.relname) IN (SELECT * FROM unnest(%s::text[],%s::text[]))',
                ([s for s, _ in relations], [r for _, r in relations])).fetchall())
        log.warning('verification_statement_canceled limit=%s estimates=%s plan=%s', VERIFICATION_TIMEOUT,
                    json.dumps(estimates, sort_keys=True), json.dumps(plan, separators=(',', ':')))
    except Exception:
        log.warning('verification_statement_canceled limit=%s plan_unavailable', VERIFICATION_TIMEOUT)


def analyze_chain(db):
    """ANALYZE every chain table after a bulk load, so verification plans with current statistics.

    Advisory: under its own ANALYZE_TIMEOUT, and a failure is logged, never raised."""
    try:
        with db.transaction():
            db.execute("SET LOCAL statement_timeout='" + ANALYZE_TIMEOUT + "'")
            tables = [row[0] for row in db.execute(
                "SELECT format('%I.%I',n.nspname,c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace"
                " WHERE n.nspname='chain' AND c.relkind IN ('r','p') ORDER BY c.relname")]
            if tables:
                db.execute('ANALYZE ' + ','.join(tables))
    except psycopg.Error as error:
        log.warning('advisory_analyze_failed sqlstate=%s', error.sqlstate)
