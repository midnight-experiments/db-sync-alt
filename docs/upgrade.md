# Upgrade from 0.4.x

0.5.0 does not upgrade a 0.4.x database in place. `convert` builds a new
0.5.0 database from it, reading the 0.4.x database through its reader login in
one read-only transaction. The 0.4.x database stays as it was and keeps
serving; it is the way back until you remove it. The node switches by changing
its connection string.

0.4.x was a Python package with a state directory and a service configuration
file. 0.5.0 is one binary or one image, configured by flags and `DSA_`
variables ([configuration](configuration.md)); it reads neither the state
directory nor the configuration file. Its database keeps only the window and
the unspent set ([glossary](glossary.md#chain-and-window)), so it cannot be
turned back into a 0.4.x database.

## Steps

The examples use one PostgreSQL 17 server `db.example` that holds the 0.4.x
database `db_sync_alt` and the new database `db_sync_alt_050`, Mainnet, and
the 0.4.x logins `db_sync_alt_owner` and `db_sync_alt_reader`.

### 1. Back up the 0.4.x database

With the installed 0.4.x release, stop its service, take a backup
(`db-sync-alt backup`), restore it into a scratch database with that release's
`db-sync-alt restore` to prove it, and start the 0.4.x service again. Keep the
backup after the switch. The node reads the 0.4.x database meanwhile.

### 2. Create the new database

As the server's administrator, create an empty database owned by the owner
login ([PostgreSQL](postgresql.md#the-database-and-its-logins)). The 0.4.x
owner and reader logins can be reused:

```sql
CREATE DATABASE db_sync_alt_050 OWNER db_sync_alt_owner ALLOW_CONNECTIONS false;
REVOKE ALL ON DATABASE db_sync_alt_050 FROM PUBLIC;
ALTER DATABASE db_sync_alt_050 ALLOW_CONNECTIONS true;
```

### 3. Convert

Keep the 0.4.x service running. Run `convert` with the new database as
destination and the 0.4.x database, as its reader login, as source:

```sh
db-sync-alt convert --network mainnet \
  --database 'host=db.example dbname=db_sync_alt_050 user=db_sync_alt_owner' \
  --password-file owner.password --tls-root-cert root-ca.crt \
  --source 'host=db.example dbname=db_sync_alt user=db_sync_alt_reader' \
  --source-password-file reader.password --source-tls-root-cert root-ca.crt \
  --capture-source mainnet.capture.json --snapshot mainnet-converted.snapshot \
  --overall-timeout-seconds 14400 --statement-timeout-seconds 600 \
  --source-idle-transaction-timeout-seconds 7200 --idle-transaction-timeout-seconds 7200
```

What `convert` does:

- It opens one `REPEATABLE READ READ ONLY` transaction on the source and reads
  everything from it: positions, rows, supply, and with `--capture-source` the
  answers to the node's queries. It writes nothing to the source. The reader
  login must have exactly the 0.4.x reader grant.
- The target is the source's newest stored block minus k, also when the 0.4.x
  follower is behind the chain. The horizon is the newest source block at
  least `--retention-seconds` (default 7 days) older than the target, or
  `--horizon-block`. A source whose history does not reach that far is cut at
  its oldest continuous block, and the report says so.
- It copies the source tables in pages, checks the copy against the source
  table by table, selects what the keep rule keeps with the original row IDs,
  checks digests, references and supply, and publishes the new database in one
  transaction. A failure leaves the new database empty and closed; run the
  command again.
- With `--snapshot` it also writes a snapshot file of the result. If only that
  step fails, the report is `conversion_completed_snapshot_failed`: the
  database serves; run `snapshot` instead of converting again.

The source transaction stays open for the whole read and is idle while the
destination writes a page, and the destination is idle while the source is
read: set both idle limits long enough. A source login without a statement
limit gets 60 seconds per statement, which the paged reads stay within.
`convert` stops after `--overall-timeout-seconds` (default 3,600).

Measured conversions with a pre-release build, each PostgreSQL in a container
limited to 1 CPU and 1 GiB with throttled disk reads and writes:

| Source | Date | `convert` with `--capture-source` and `--snapshot` | Source transaction open | Temporary space in the new database | New database | Snapshot file |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| The serving Preprod 0.4.9 database | 2026-11-22 | 189 s | 100 s | 299,892,736 bytes | 18,790,067 bytes | 1,516,541 bytes |
| A copy of the Mainnet 0.4.9 database | 2026-11-21 | 6,413 s | 2,019 s | 4,193,312,768 bytes | 203,670,031 bytes | 32,877,530 bytes |

The temporary tables, the selected rows, the write-ahead log and the snapshot
file need disk space at the same time; the command's report lists the
temporary relation sizes.

### 4. Verify, grant the reader and compare

```sh
db-sync-alt verify --network mainnet \
  --database 'host=db.example dbname=db_sync_alt_050 user=db_sync_alt_owner' \
  --password-file owner.password --tls-root-cert root-ca.crt
db-sync-alt grant-reader --network mainnet --role db_sync_alt_reader \
  --database 'host=db.example dbname=db_sync_alt_050 user=db_sync_alt_owner' \
  --password-file owner.password --tls-root-cert root-ca.crt
db-sync-alt compare --network mainnet --a-capture mainnet.capture.json \
  --b 'host=db.example dbname=db_sync_alt_050 user=db_sync_alt_reader' \
  --b-password-file reader.password --b-tls-root-cert root-ca.crt
```

`verify` checks the new database in full ([snapshots](snapshots.md#verify)).
`grant-reader` gives the node's login its rights on it; `run` does this too.
`compare` is optional: it runs every node query against the captured 0.4.x
answers and the new database and reports any difference
([comparison](comparison.md)).

Measured with the same pre-release build: on the Mainnet copy, `verify` took
83 s, `grant-reader` 0.10 s and `compare` with the capture 172 s for 543
position pairs (2026-11-21, containers as above); on Preprod, `compare` with the capture took
41.3 s for 8,137 cases (2026-11-23, release build, PostgreSQL 17 in a container
with 4 CPUs and 4 GiB). Both found no difference.

### 5. Start 0.5.0

Start `run` on the new database ([operations](operations.md)). It needs no
`--snapshot`, because the database is not empty:

```sh
db-sync-alt run --network mainnet \
  --database 'host=db.example dbname=db_sync_alt_050 user=db_sync_alt_owner' \
  --password-file owner.password --tls-root-cert root-ca.crt \
  --provider-key-file blockfrost-mainnet.key --reader db_sync_alt_reader \
  --snapshot-dir /var/lib/db-sync-alt/snapshots
```

It catches up from the target of the conversion. Wait until `/health` answers
200. If 0.4.x and 0.5.0 run on the same host, give one of them another HTTP
port (`--listen`). Measured catch-up: [provider](provider.md#catching-up).

### 6. Switch the node

Point the node's `DB_SYNC_POSTGRES_CONNECTION_STRING` at the new database (only
the database name changes when you reuse the logins) and restart the node
([connect a node](connect-a-node.md)). A node whose position is older than the
new database's horizon needs a newer node snapshot
([limitations](limitations.md)).

### 7. Stop 0.4.x

Once the node follows on the new database, stop the 0.4.x service. Keep its
database and the backup of step 1 until you are sure: to go back, point the
node at the 0.4.x database again and start the 0.4.x service.

## What changes for an operator

| 0.4.x | 0.5.0 |
| --- | --- |
| Python package, a virtual environment, `pg_dump` and `pg_restore` | One binary for Linux x86-64 (glibc 2.39 or newer) or one image; no PostgreSQL client tools |
| State directory and service configuration file | Flags and `DSA_` variables; secrets as file paths |
| `--blockfrost-key-file` | `--provider-key-file` |
| `--database-url`, `--database-user`, `--database-password-file` | `--database` (a connection string without a password) and `--password-file` |
| `--reader-role` | `--reader` |
| `--archive` and `--manifest` on an empty database | `--snapshot` and `--expect-content-digest` |
| `--http-port` (loopback) | `--listen` (an address and port; default `0.0.0.0:18080`) |
| Full history | The window and the unspent set; the rest is deleted |
| `backup` and `restore` of `pg_dump` archives | `snapshot` and `restore` of snapshot files, and daily files written by the service |
| A daily request ceiling of the built-in client | No ceiling: the provider's own refusals are waited out |

## Scripts that read `/status`

These fields keep their names and meanings: `healthy`, `reason_codes`,
`serving`, `sync`, `error_code`, `poll_error_code`, `served_target`,
`provider_head`, `lag`, `freshness.provider_observation_age_seconds` and
`service_version`. A position still has `height`, `hash` and `timestamp`.
`/status` answers 200 in every state, and `/health` answers 200 or 503 with
the same body.

- `quota` is gone: the service no longer has a daily ceiling. The calls of each
  UTC day are in `provider.calls_today` and `provider.calls_previous_days`.
- `relevance` is gone.
- `sync` no longer takes the value `checkpointing`.
- The reason codes of `reason_codes` are those of [/health](operations.md#health).
- Every other field is new; [/status](operations.md#status) lists them.

A script that reads the 0.4.x tables or schemas directly (for example
`chain.service_state`) must read `/status` or `db-sync-alt status` instead:
those tables do not exist in 0.5.0.
