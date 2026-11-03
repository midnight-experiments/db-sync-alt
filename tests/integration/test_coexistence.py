"""One PostgreSQL 17 server shared with the other databases of a Midnight deployment.

The test's superuser plays the operator: it creates a cardano-db-sync-shaped database carrying
the node's relation and index names, another application database, and db-sync-alt's roles and
database exactly as the operations guide says. db-sync-alt then builds, upgrades, grants, backs up
and restores as its non-superuser owner. Nothing outside its own database may change: no role, no
server-wide setting, no extension, no object of another database, and each database answers the
node's catalogue checks with its own relations only.
"""
import os
from pathlib import Path
import tempfile
import unittest
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_sync_alt.backup import backup, restore
from db_sync_alt.store import serve
from db_sync_alt.store.database import PgStore, WRITER_LOCK
from db_sync_alt.store.reader import grant_reader, verify_reader
from db_sync_alt.store.upgrade import upgrade_store
from fixtures.generations import make_store

DBSYNC_SCHEMA = Path(__file__).resolve().parents[1] / 'fixtures' / 'data' / 'dbsync_schema.sql'
# The node's index names on a cardano-db-sync database (as db-sync and midnight-node create them).
DBSYNC_INDEXES = {
    'idx_block_block_no': 'block(block_no)', 'idx_tx_block_id': 'tx(block_id)', 'idx_tx_out_tx_id': 'tx_out(tx_id)',
    'idx_tx_out_data_hash': 'tx_out(data_hash)', 'idx_tx_out_address': 'tx_out USING hash (address)',
    'idx_tx_in_tx_in_id': 'tx_in(tx_in_id)', 'idx_tx_in_tx_out_id_tx_out_index': 'tx_in(tx_out_id, tx_out_index)',
    'idx_multi_asset_policy_name': 'multi_asset(policy, name)', 'idx_multi_asset_policy_name_hex': "multi_asset(encode(policy, 'hex'), encode(name, 'hex'))",
    'idx_ma_tx_out_ident': 'ma_tx_out(ident)', 'idx_ma_tx_out_id_ident': 'ma_tx_out(tx_out_id, ident)',
}


class CoexistenceTests(unittest.TestCase):
    def setUp(self):
        self.admin = psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True)
        token = uuid.uuid4().hex[:10]
        self.alt, self.restored, self.dbsync, self.app = ('db_sync_alt_' + token, 'db_sync_alt_restored_' + token,
                                                          'cexplorer_' + token, 'indexer_' + token)
        self.owner, self.reader, self.app_owner = 'db_sync_alt_owner_' + token, 'db_sync_alt_reader_' + token, 'indexer_' + token
        self.databases, self.roles = [], []
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        for name in self.databases:
            self.admin.execute(sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(name)))
        for role in self.roles:
            self.admin.execute(sql.SQL('DROP ROLE IF EXISTS {}').format(sql.Identifier(role)))
        self.admin.close()
        self.tmp.cleanup()

    def dsn(self, database, user=None):
        return make_conninfo(os.environ['DB_SYNC_ALT_TEST_DSN'], dbname=database, **({'user': user} if user else {}))

    def run_sql(self, database, statements, user=None):
        with psycopg.connect(self.dsn(database, user), autocommit=True) as db:
            for statement in statements:
                db.execute(statement)

    def provision(self, database):
        """The operator's database for db-sync-alt: owned by the owner login, closed to PUBLIC."""
        ident = sql.Identifier(database)
        self.databases.append(database)
        self.admin.execute(sql.SQL('CREATE DATABASE {} OWNER {} ALLOW_CONNECTIONS false').format(ident, sql.Identifier(self.owner)))
        self.admin.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(ident))
        self.admin.execute(sql.SQL('ALTER DATABASE {} ALLOW_CONNECTIONS true').format(ident))

    def server_state(self):
        """Everything outside db-sync-alt's databases that it must not change."""
        with psycopg.connect(os.environ['DB_SYNC_ALT_TEST_DSN'], autocommit=True) as db:
            state = {
                'roles': db.execute('SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolcanlogin, rolconnlimit FROM pg_roles ORDER BY 1').fetchall(),
                'memberships': db.execute('SELECT roleid::regrole::text, member::regrole::text FROM pg_auth_members ORDER BY 1, 2').fetchall(),
                'settings': db.execute('SELECT name, reset_val FROM pg_settings ORDER BY 1').fetchall(),
                'file_settings': db.execute('SELECT name, setting FROM pg_file_settings ORDER BY 1, 2').fetchall(),
                'role_settings': db.execute('SELECT setdatabase, setrole::regrole::text, setconfig FROM pg_db_role_setting ORDER BY 1, 2').fetchall(),
                'databases': db.execute("SELECT datname, datdba::regrole::text, datacl::text FROM pg_database WHERE datname NOT LIKE 'db\\_sync\\_alt\\_%%' ORDER BY 1").fetchall(),
            }
        for name in (self.dbsync, self.app):
            with psycopg.connect(self.dsn(name), autocommit=True) as db:
                state[name] = db.execute("""SELECT n.nspname, c.relname, c.relkind, c.relacl::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') ORDER BY 1, 2""").fetchall()
                state[name + ':extensions'] = db.execute('SELECT extname FROM pg_extension ORDER BY 1').fetchall()
        return state

    def node_catalogue(self, database, user):
        """The node's startup checks: one tx_in relation, each of its index names exactly once."""
        with psycopg.connect(self.dsn(database, user), autocommit=True) as db:
            tx_in = db.execute("select count(*) from information_schema.tables where table_name = 'tx_in'").fetchone()[0]
            indexes = {name: db.execute('select schemaname, tablename from pg_indexes where indexname = %s', (name,)).fetchall()
                       for name in serve.NODE_INDEXES}
        return tx_in, indexes

    def test_db_sync_alt_shares_a_postgresql_17_server_with_other_databases(self):
        admin = self.admin
        # The deployment's other databases, as their operators create them.
        for role in (self.owner, self.reader, self.app_owner):
            self.roles.append(role)
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(self.app_owner)))
        for name, owner in ((self.dbsync, None), (self.app, self.app_owner)):
            self.databases.append(name)
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)) + (sql.SQL(' OWNER {}').format(sql.Identifier(owner)) if owner else sql.SQL('')))
        self.run_sql(self.dbsync, [DBSYNC_SCHEMA.read_text()] + [sql.SQL('CREATE INDEX {} ON ' + body).format(sql.Identifier(name)) for name, body in DBSYNC_INDEXES.items()])
        self.run_sql(self.app, ['CREATE TABLE block(id bigint PRIMARY KEY, hash bytea)', 'CREATE TABLE tx_out(id bigint, address text)',
                                'CREATE INDEX idx_tx_out_address ON tx_out(address)'], user=self.app_owner)
        # db-sync-alt's roles and database, as the operator creates them (docs/operations.md).
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(self.owner)))
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN NOINHERIT CONNECTION LIMIT 40').format(sql.Identifier(self.reader)))
        for name, value in (('statement_timeout', '60s'), ('idle_in_transaction_session_timeout', '60s'), ('search_path', 'public')):
            admin.execute(sql.SQL('ALTER ROLE {} SET {} = {}').format(sql.Identifier(self.reader), sql.Identifier(name), sql.Literal(value)))
        self.provision(self.alt)
        self.provision(self.restored)
        self.assertEqual(admin.execute('SELECT rolsuper OR rolcreaterole OR rolcreatedb FROM pg_roles WHERE rolname = %s', (self.owner,)).fetchone()[0], False)
        before = self.server_state()

        # db-sync-alt, as its non-superuser owner: build, upgrade, grant, verify, back up, restore.
        owned = self.dsn(self.alt, self.owner)
        make_store(owned)
        with PgStore(conninfo=owned) as store:
            upgrade_store(store)
        archive, manifest = Path(self.tmp.name) / 'alt.dump', Path(self.tmp.name) / 'alt.manifest.json'
        backup(archive, manifest, conninfo=owned)
        result = restore(archive, manifest, conninfo=self.dsn(self.restored, self.owner), reader_role=self.reader)
        self.assertEqual(result['status'], 'restored')
        with PgStore(conninfo=owned) as store:
            grant_reader(store.db, self.reader)
            for database in (self.alt, self.restored):
                with psycopg.connect(self.dsn(database, self.owner), autocommit=True) as db:
                    self.assertEqual(verify_reader(db, self.reader)['warnings'], [])
            # Advisory locks are local to a database: the writer's keys never block another database.
            with psycopg.connect(self.dsn(self.dbsync), autocommit=True) as other:
                self.assertTrue(other.execute('SELECT pg_try_advisory_lock(%s, %s)', WRITER_LOCK).fetchone()[0])

        after = self.server_state()
        for key in before:
            self.assertEqual(after[key], before[key], key)
        for database in (self.alt, self.restored):
            with psycopg.connect(self.dsn(database, self.owner), autocommit=True) as db:
                self.assertEqual(db.execute('SELECT extname FROM pg_extension ORDER BY 1').fetchall(), [('plpgsql',)])
                self.assertEqual(db.execute("SELECT has_database_privilege('public', current_database(), 'CONNECT')").fetchone()[0], False)

        # Each database answers the node's catalogue checks with its own relations only.
        for database, user, schema in ((self.alt, self.reader, 'serve'), (self.restored, self.reader, 'serve'), (self.dbsync, None, 'public')):
            with self.subTest(database=database):
                tx_in, indexes = self.node_catalogue(database, user)
                self.assertEqual(tx_in, 1)
                for name, rows in indexes.items():
                    self.assertEqual([r[0] for r in rows], [schema], name)
        with psycopg.connect(self.dsn(self.app, self.app_owner), autocommit=True) as db:
            self.assertEqual(db.execute('select count(*) from pg_indexes where indexname = %s', ('idx_tx_out_address',)).fetchone()[0], 1)
        # The node reads db-sync-alt as the reader; nothing internal, and no other role gets in.
        for database in (self.alt, self.restored):
            with psycopg.connect(self.dsn(database, self.reader), autocommit=True) as node:
                self.assertTrue(node.execute('SELECT EXISTS (SELECT 1 FROM tx_in)').fetchone()[0])
                with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                    node.execute('SELECT * FROM chain.service_state')
            with self.assertRaises(psycopg.OperationalError):
                psycopg.connect(self.dsn(database, self.app_owner)).close()


if __name__ == '__main__':
    unittest.main(verbosity=2)
