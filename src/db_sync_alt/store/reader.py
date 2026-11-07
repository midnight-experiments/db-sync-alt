"""The node's reader role: SELECT on the node-facing relations and EXECUTE on the serving gate, nothing else.

The operator creates the reader login and sets its defaults; db-sync-alt never creates or alters a
role. It grants the reader what midnight-node reads, verifies that the reader can do nothing more
(no internal schema, no TEMP or CREATE, no write, no role membership) and reports the reader's
settings. `tx_metadata` (bridge data, not served) and every internal table stay unreadable. Only the
owner and the granted roles may connect, so a reader cannot see the database while it is empty,
importing or restoring. A restore drops all privileges: grant again afterwards (`db-sync-alt restore
--reader-role` and the service do it once the database is ready).
"""
from psycopg import sql
from db_sync_alt.contract import Refusal
from db_sync_alt.store.database import SCHEMA_VERSION

NODE_RELATIONS = ('block', 'tx', 'tx_out', 'tx_in', 'ma_tx_out', 'multi_asset', 'datum', 'epoch_param', 'epoch_stake', 'pool_hash')
UNGRANTED = ('tx_metadata',)
GATE = 'chain.gate()'
# Role defaults the reader should carry (ALTER ROLE ... SET, by the operator).
SETTINGS = ('statement_timeout', 'idle_in_transaction_session_timeout', 'search_path', 'default_transaction_read_only')
# midnight-node's cardano-db-sync connection pool, plus one connection for the service's own serving check.
NODE_CONNECTIONS = 35
READER_CONNECTIONS = NODE_CONNECTIONS + 1
SYSTEM_SCHEMAS = "n.nspname NOT LIKE 'pg\\_%%' AND n.nspname<>'information_schema'"


def _attributes(db, role):
    row = db.execute("""SELECT r.oid, r.rolcanlogin, r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls,
                               r.oid=d.datdba, r.rolname=current_user
                        FROM pg_roles r, pg_database d WHERE r.rolname=%s AND d.datname=current_database()""", (role,)).fetchone()
    if row is None:
        raise Refusal('reader_role_missing')
    oid, login, privileged, owner, current = row
    if not login or privileged or owner or current:
        raise Refusal('invalid_reader_role')
    return oid


def grant_reader(db, role):
    """Grant `role` read access to the node-facing relations of a database of the current schema."""
    _attributes(db, role)
    if db.execute("SELECT to_regprocedure('chain.gate()')").fetchone()[0] is None or \
            db.execute('SELECT schema_version FROM chain.service_state WHERE id=1').fetchone() != (SCHEMA_VERSION,):
        raise Refusal('reader_requires_current_schema')
    ident, database = sql.Identifier(role), sql.Identifier(db.info.dbname)
    with db.transaction():
        # Connecting is a grant too: until the database serves, a reader cannot connect at all
        # (an empty or half-restored database would show the node no tx_in relation).
        db.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(database))
        db.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(database, ident))
        db.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(ident))
        db.execute(sql.SQL('GRANT SELECT ON {} TO {}').format(
            sql.SQL(',').join(sql.Identifier('public', name) for name in NODE_RELATIONS), ident))
        for name in UNGRANTED:
            db.execute(sql.SQL('REVOKE ALL ON {} FROM {}').format(sql.Identifier('public', name), ident))
        db.execute(sql.SQL('GRANT EXECUTE ON FUNCTION chain.gate() TO {}').format(ident))
    return {'reader': role, 'connect': True, 'relations': ['public.' + name for name in NODE_RELATIONS], 'functions': [GATE]}


def reader_settings(db, role):
    """The reader's role defaults in this database (database-specific values win) and its connection limit."""
    found = {}
    for database, config in db.execute("""SELECT s.setdatabase, s.setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid=s.setrole
            WHERE r.rolname=%s AND s.setdatabase IN (0,(SELECT oid FROM pg_database WHERE datname=current_database()))
            ORDER BY s.setdatabase""", (role,)).fetchall():
        for item in config or []:
            name, _, value = item.partition('=')
            if name in SETTINGS:
                found[name] = value
    limit = db.execute('SELECT rolconnlimit FROM pg_roles WHERE rolname=%s', (role,)).fetchone()[0]
    return {**{name: found.get(name) for name in SETTINGS}, 'connection_limit': None if limit < 0 else limit}


def setting_warnings(settings):
    """Report-only: what the operator should still set on the reader role."""
    out = []
    if not settings.get('statement_timeout') or settings['statement_timeout'] in ('0', '0ms'):
        out.append('reader_statement_timeout_unset')
    if not settings.get('idle_in_transaction_session_timeout') or settings['idle_in_transaction_session_timeout'] in ('0', '0ms'):
        out.append('reader_idle_in_transaction_timeout_unset')
    if (settings.get('search_path') or '').replace(' ', '').replace('"', '') != 'public':
        out.append('reader_search_path_not_public')
    if settings.get('connection_limit') is None:
        out.append('reader_connection_limit_unset')
    elif settings['connection_limit'] < READER_CONNECTIONS:
        out.append('reader_connection_limit_below_node_pool')
    return out


def excess_privileges(db, role):
    """Everything the reader could do beyond reading the node-facing relations; empty when exact."""
    out = []
    if db.execute('SELECT EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=%s)', (role,)).fetchone()[0]:
        out.append('role_membership')
    for privilege in ('CREATE', 'TEMPORARY'):
        if db.execute('SELECT has_database_privilege(%s,current_database(),%s)', (role, privilege)).fetchone()[0]:
            out.append('database_' + privilege.lower())
    for name, usage, create in db.execute('SELECT n.nspname,has_schema_privilege(%s,n.oid,%s),has_schema_privilege(%s,n.oid,%s) FROM pg_namespace n WHERE '
                                          + SYSTEM_SCHEMAS + ' ORDER BY 1', (role, 'USAGE', role, 'CREATE')).fetchall():
        if create or (usage and name != 'public'):
            out.append('schema_' + ('create' if create else 'usage') + ':' + name)
    for name, read, write, columns in db.execute(
            """SELECT n.nspname||'.'||c.relname,
                      CASE WHEN c.relkind='S' THEN has_sequence_privilege(%s,c.oid,'USAGE,SELECT') ELSE has_table_privilege(%s,c.oid,'SELECT') END,
                      CASE WHEN c.relkind='S' THEN has_sequence_privilege(%s,c.oid,'UPDATE')
                           ELSE has_table_privilege(%s,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') END,
                      c.relkind<>'S' AND has_any_column_privilege(%s,c.oid,'INSERT,UPDATE,REFERENCES')
               FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE c.relkind IN ('r','p','v','m','f','S') AND """ + SYSTEM_SCHEMAS + ' ORDER BY 1', (role,) * 5).fetchall():
        node = name in ['public.' + r for r in NODE_RELATIONS]
        if write or columns:
            out.append('write:' + name)
        elif read and not node:
            out.append('read:' + name)
        elif not read and node:
            out.append('missing:' + name)
    functions = [r[0] for r in db.execute("""SELECT n.nspname||'.'||p.proname||'('||pg_get_function_identity_arguments(p.oid)||')' FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE """ + SYSTEM_SCHEMAS + " AND has_function_privilege(%s,p.oid,'EXECUTE') ORDER BY 1", (role,)).fetchall()]
    out += ['execute:' + f for f in functions if f != GATE]
    if GATE not in functions:
        out.append('missing:' + GATE)
    if not db.execute('SELECT has_database_privilege(%s,current_database(),%s)', (role, 'CONNECT')).fetchone()[0]:
        out.append('missing:connect')
    return out


def verify_reader(db, role):
    """Refuse a reader that can do more than the node needs; report its settings and what to set."""
    _attributes(db, role)
    excess = excess_privileges(db, role)
    if excess:
        raise Refusal('reader_privileges_not_exact')
    settings = reader_settings(db, role)
    return {'role': role, 'privileges': 'node_relations_only', 'settings': settings, 'warnings': setting_warnings(settings)}


def main(argv=None):
    import argparse
    import json
    from db_sync_alt.store.database import PgStore
    parser = argparse.ArgumentParser(prog='db-sync-alt grant-reader', description=__doc__)
    parser.add_argument('--service', required=True)
    parser.add_argument('--role', required=True, help='existing login role midnight-node connects with')
    args = parser.parse_args(argv)
    try:
        with PgStore(args.service) as store:
            result = grant_reader(store.db, args.role)
            result['verified'] = verify_reader(store.db, args.role)
        print(json.dumps(result, sort_keys=True))
    except Refusal as error:
        parser.exit(2, json.dumps({'error': error.code}) + '\n')
    except Exception:
        parser.exit(2, '{"error":"grant_reader_failed"}\n')
