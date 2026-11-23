# Runs once, when the bundled server initialises an empty data directory: the
# owner login that db-sync-alt runs as, the Midnight node's reader login and
# the database. db-sync-alt itself creates no role and needs no superuser.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
\set owner_password `cat /run/db-sync-alt/owner.password`
\set reader_password `cat /run/db-sync-alt/reader.password`
CREATE ROLE db_sync_alt_owner LOGIN PASSWORD :'owner_password';
CREATE ROLE db_sync_alt_reader LOGIN NOINHERIT CONNECTION LIMIT 40 PASSWORD :'reader_password';
ALTER ROLE db_sync_alt_reader SET statement_timeout = '60s';
ALTER ROLE db_sync_alt_reader SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE db_sync_alt_reader SET search_path = 'public';
ALTER ROLE db_sync_alt_reader SET default_transaction_read_only = on;
CREATE DATABASE db_sync_alt OWNER db_sync_alt_owner ALLOW_CONNECTIONS false;
REVOKE ALL ON DATABASE db_sync_alt FROM PUBLIC;
ALTER DATABASE db_sync_alt ALLOW_CONNECTIONS true;
SQL
