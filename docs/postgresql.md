# PostgreSQL

You provide the PostgreSQL server, version 17 or newer, as you would for
cardano-db-sync. db-sync-alt is tested on PostgreSQL 17, 18 and 19 and refuses
a server older than 17 (`postgresql_17_or_newer_required`). A snapshot file
restores on any of these versions, whichever one wrote it. The database can
live on a server that holds other databases of a Midnight deployment.
db-sync-alt:

- needs a dedicated database that `PUBLIC` cannot connect to, and an owner
  login that owns it;
- needs no superuser, creates no role, installs no extension beyond `plpgsql`
  and changes no server-wide setting;
- reads and changes no other database on the server.

## The database and its logins

As the server's administrator, create the owner login db-sync-alt runs as, the
Midnight node's reader login, and the database:

```sql
CREATE ROLE db_sync_alt_owner LOGIN PASSWORD '<owner password>';
CREATE ROLE db_sync_alt_reader LOGIN NOINHERIT CONNECTION LIMIT 40 PASSWORD '<reader password>';
ALTER ROLE db_sync_alt_reader SET statement_timeout = '60s';
ALTER ROLE db_sync_alt_reader SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE db_sync_alt_reader SET search_path = 'public';
ALTER ROLE db_sync_alt_reader SET default_transaction_read_only = on;
-- Created closed: no other role connects before db-sync-alt has prepared it.
CREATE DATABASE db_sync_alt OWNER db_sync_alt_owner ALLOW_CONNECTIONS false;
REVOKE ALL ON DATABASE db_sync_alt FROM PUBLIC;
ALTER DATABASE db_sync_alt ALLOW_CONNECTIONS true;
```

The owner login must be able to log in and must own the database. It must not
be a superuser or have `CREATEDB`, `CREATEROLE`, `REPLICATION` or `BYPASSRLS`;
a command that writes refuses it otherwise (`invalid_owner_role`). New roles
get none of these unless you add them.

db-sync-alt removes `PUBLIC`'s rights on the database whenever it creates or
restores its layout. Give the database name, the owner login and the password
file to db-sync-alt with `--database` and `--password-file`
([configuration](configuration.md#database-flags)).

## The reader login

`run` at every start, and `grant-reader`, grant the reader login exactly:

- `CONNECT` on the database;
- `USAGE` on schema `public`;
- `SELECT` on the eleven views the node reads (`block`, `tx`, `tx_out`,
  `tx_in`, `ma_tx_out`, `multi_asset`, `datum`, `epoch_param`, `pool_hash`,
  `epoch_stake`, `tx_metadata`);
- `EXECUTE` on the gate function `dsa.gate()`.

Then they check that the login can do nothing more, counting rights it has
through `PUBLIC`: no membership in another role, no `CREATE` or `TEMPORARY` on
the database, no right on another schema, no write or other right on any
relation, no other function. A login with more rights is refused
(`reader_privileges_not_exact`) and the report lists each extra right, for
example `database_temporary`, `schema:dsa` or `relation:serve.tx`. The login
must exist (`reader_role_missing`), be able to log in, have none of the role
attributes above, and differ from the owner login (`invalid_reader_role`).

The check also reads the login's settings and reports what is missing as a
warning in `/status`, without failing `/health`:

| Warning | Set |
| --- | --- |
| `reader_statement_timeout_unset` | `ALTER ROLE … SET statement_timeout` |
| `reader_idle_in_transaction_timeout_unset` | `ALTER ROLE … SET idle_in_transaction_session_timeout` |
| `reader_search_path_not_public` | `ALTER ROLE … SET search_path = 'public'` |
| `reader_connection_limit_unset` | `ALTER ROLE … CONNECTION LIMIT 40` |
| `reader_connection_limit_below_node_pool` | A connection limit of at least 36 |

Midnight node 2.1.101 opens up to 32 connections to its db-sync database, in
six pools; a limit of 36 leaves room for a few more, and 40 for your own
checks.

## TLS

Midnight node 2.1.101 connects to its db-sync database only over TLS and
refuses a server without it. To turn TLS on in a server you administer:

1. Create a certificate authority and a server certificate for the host name
   the node and db-sync-alt use (`db.example` here):

   ```sh
   host=db.example
   openssl req -x509 -newkey rsa:3072 -nodes -days 3650 -subj '/CN=db-sync-alt PostgreSQL CA' \
     -keyout root-ca.key -out root-ca.crt
   openssl req -newkey rsa:3072 -nodes -subj "/CN=$host" -keyout server.key -out server.csr
   printf 'subjectAltName=DNS:%s\nextendedKeyUsage=serverAuth\n' "$host" > server.ext
   openssl x509 -req -in server.csr -CA root-ca.crt -CAkey root-ca.key -CAcreateserial \
     -days 365 -extfile server.ext -out server.crt
   ```

   Keep `root-ca.key` private and off the server. A server reached by IP
   address needs `subjectAltName=IP:<address>` instead. Renew `server.crt`
   before it expires; a reload picks up new files.
2. Copy `server.crt` and `server.key` to the server, owned by the server's
   operating-system user and readable by it only (mode `600`).
3. As the server's administrator, turn TLS on; a reload applies it:

   ```sql
   ALTER SYSTEM SET ssl = 'on';
   ALTER SYSTEM SET ssl_cert_file = '<directory>/server.crt';
   ALTER SYSTEM SET ssl_key_file = '<directory>/server.key';
   SELECT pg_reload_conf();
   ```

4. Give `root-ca.crt` to db-sync-alt (`--tls-root-cert`) and to the node
   (`SSL_ROOT_CERT`, [connect a node](connect-a-node.md)).

db-sync-alt's own connections use TLS too unless the connection string says
`sslmode=disable`, and check the certificate and the host name
([configuration](configuration.md#database-flags)). `/status` warns with
`database_tls_unavailable` when the server offers no TLS.

## Size and connections

Measured database sizes (2026-11-23, release builds, PostgreSQL 17 in a
container):

| Database | Mainnet | Preprod |
| --- | --- | --- |
| Right after restoring a file of a 7-day window | 194 MB | 18 MB |
| While following | 376 MB, holding 25 days of blocks before its first pruning (tables 177 MB, indexes 174 MB) | 20 MB after following for three hours |

The retention counts from when this database first served a block, so a
database that catches up keeps every block it caught up for a whole retention
before it prunes: plan room for that peak. Plan room for the daily snapshot
files too ([snapshots](snapshots.md#what-a-file-contains)).

`run` holds up to three connections as the owner login: one that writes, one
that reads the status every 20 seconds, and one for the running upkeep job.
`max_connections` must cover these, the node's pools and every other database
on the server.

The bundled Compose deployment's server settings, with the reason for each,
are in [`deploy/README.md`](../deploy/README.md#postgresql-settings).
