# Operations

The [README](../README.md) has the short path: install, first run, connect a
node. This page has the details for running db-sync-alt directly on a host.
For containers see [Docker / Compose](docker.md); for backups and failures see
[backup and recovery](recovery.md).

## Provision PostgreSQL

You provide the PostgreSQL server, as you would for cardano-db-sync. db-sync-alt
needs PostgreSQL 17, the major version the Midnight node's reference environment
runs its cardano-db-sync database on, so its database can live on the same
PostgreSQL 17 server as the other databases of a Midnight deployment
([share a server](#share-a-postgresql-17-server)). db-sync-alt never creates,
alters or grants a role, and needs no superuser: you create, as the server's
administrator,

- a **dedicated, empty database** that `PUBLIC` cannot connect to. db-sync-alt
  never adopts a database with unrelated objects in it and never reads or
  changes the server's other databases;
- an **owner login** that owns the database: no superuser, no `CREATEROLE`, no
  `CREATEDB`. db-sync-alt runs as this login. It must be allowed to execute
  `pg_control_system()` (allowed to everyone by default);
- a **reader login** for the Midnight node, with its own defaults: a statement
  timeout, an idle-in-transaction timeout, `search_path` `public` and a
  connection limit of at least 36 (node 2.1.101's pools of up to 32
  connections, one for db-sync-alt's serving check and 3 spare) plus headroom.
  Give it nothing else.

```sql
CREATE ROLE db_sync_alt_owner LOGIN PASSWORD '<owner password>';
CREATE ROLE db_sync_alt_reader LOGIN NOINHERIT CONNECTION LIMIT 40 PASSWORD '<reader password>';
ALTER ROLE db_sync_alt_reader SET statement_timeout = '60s';
ALTER ROLE db_sync_alt_reader SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE db_sync_alt_reader SET search_path = 'public';
-- Created closed: no role but the owner connects before db-sync-alt has prepared it.
CREATE DATABASE db_sync_alt OWNER db_sync_alt_owner ALLOW_CONNECTIONS false;
REVOKE ALL ON DATABASE db_sync_alt FROM PUBLIC;
ALTER DATABASE db_sync_alt ALLOW_CONNECTIONS true;
```

New roles get neither `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION` nor
`BYPASSRLS` unless you add them; do not. Keep `PUBLIC` out of the database: a
node that connected to an empty or half-restored database would find no `tx_in`
relation and remember that until it restarts. db-sync-alt removes `PUBLIC`'s
privileges on its database again whenever it creates or restores it.

Once the database serves, db-sync-alt grants the reader `CONNECT`, `USAGE` on
schema `public`, `SELECT` on the ten node-facing relations and `EXECUTE` on the
serving check; it does so at every service start and after every restore. It then
verifies that the reader can do nothing more — no role membership, no `TEMP` or
`CREATE`, no access to its internal schemas, no write, no other function — and
refuses to start otherwise (`invalid_reader_role`). `/status` reports the
reader's settings (`statement_timeout`, `idle_in_transaction_session_timeout`,
`search_path`, `default_transaction_read_only`, connection limit) and names any
that is missing in `warnings`, without failing health.

For `db-sync-alt run` the server must also be reachable on a loopback TCP
address (`127.0.0.1` or `::1`) of the machine that runs db-sync-alt: host names
(including `localhost`), Unix sockets, remote hosts and URL query or TLS options
are refused there. Port 18080 (HTTP status) must be free on loopback, or another port given with
`--http-port`. Plan free space for the database, the snapshot or backups you
restore from, and a 10 GiB reserve on the filesystem of the state directory,
which db-sync-alt checks before writing. Watch the PostgreSQL data volume
yourself.

## Share a PostgreSQL 17 server

db-sync-alt's database can sit on the server that holds the other databases of
a Midnight deployment, for example the cardano-db-sync and midnight-indexer
databases of the node's reference stack:

- Everything it creates lives inside its own database: the schemas `chain`,
  `serve` and `public`, their tables, views, functions and indexes. Relation and
  index names (including the cardano-db-sync names `block`, `tx_in` or
  `idx_tx_out_address` that the node checks) are per database, so a
  cardano-db-sync database next to it keeps its own, and each database answers
  the node's catalogue checks with its own relations only.
- It needs no server-wide setting, no preload library and no extension beyond
  `plpgsql`, creates no role and changes no other database.
- It uses PostgreSQL advisory locks of its own database only (advisory locks
  are local to a database): the two-integer keys (36, 1) for the writer and
  (36, 2) for publication, plus one random key pair whose first integer is not
  36 per running service. They cannot conflict with another database's locks.
- Count the reader's connections (and the owner's few) into the server's
  `max_connections` with everyone else's.

## Node requirements

The node's `CARDANO_SECURITY_PARAMETER` must equal the network's k, and its
Cardano addresses and policies must be the profile's
([connect a Midnight node](connect-a-node.md#node-requirements)). db-sync-alt
cannot read the node's settings. If you give the service the node's values
(`db-sync-alt run --node-security-parameter --node-configuration-dir`, or `node`
in the service configuration), it compares them with the profile at start and
reports a mismatch in `warnings` (`node_security_parameter_mismatch`,
`node_configuration_mismatch`); serving continues:

```json
{"node": {"security_parameter": 2160, "configuration_dir": "/path/to/midnight-node/res/preprod", "bridge_checkpoint": "block:4612000"}}
```

`bridge_checkpoint` is the token bridge's on-chain data checkpoint, `block:<n>`
or `tx:<hash>` ([bridge readiness](connect-a-node.md#bridge-readiness)). Unlike
the other values it can make `/health` fail: while the bridge metadata coverage
does not support it, `/health` answers 503 with `bridge_checkpoint_unsupported`.
Leave it out while the bridge is not active.

`configuration_dir` is the node's network configuration directory (the
`res/<network>` directory of the midnight-node sources or image); db-sync-alt
compares the profile's address and policy files with the files of the same name
there. Without a `node` section, `/status` reports `node_requirements_unchecked`.

## `db-sync-alt run`

`db-sync-alt run` prepares private configuration in a state directory and runs
the service in the foreground. Ctrl-C or SIGTERM stops it cleanly; running the
same command again resumes. Nothing restarts it automatically.

| Option | Default | Purpose |
| --- | --- | --- |
| `--network NAME` | required | `mainnet`, `preprod` or `preview`; the packaged profile of that network ([networks](networks.md)) |
| `--blockfrost-key-file PATH` | | Read the Blockfrost key of that network from a private file (mode `600`); a key of another network is refused |
| `--blockfrost-key` | | Without a value: hidden prompt. Environment alternative: `DBSYNC_BLOCKFROST_KEY` |
| `--database-url URL` | `postgresql://127.0.0.1:5432/db_sync_alt` | The database, as one loopback TCP URL (keep the password out of it) |
| `--database-user NAME` | `db_sync_alt_owner` | The owner login |
| `--database-password-file PATH` | | Read its password from a private file |
| `--database-password` | | Without a value: hidden prompt. Environment alternative: `DBSYNC_DATABASE_PASSWORD` |
| `--reader-role NAME` | `db_sync_alt_reader` | The reader login the Midnight node connects with |
| `--reader-password-file PATH` | | Read its password from a private file (the service checks serving as this login) |
| `--reader-password` | | Without a value: hidden prompt. Environment alternative: `DBSYNC_READER_PASSWORD` |
| `--archive PATH`, `--manifest PATH` | | Initial data for an empty database: a snapshot or another db-sync-alt backup |
| `--state-dir PATH` | `~/.local/share/db-sync-alt/native` | Private state; reuse it on every start |
| `--http-port` | 18080 | Status port on loopback |
| `--node-security-parameter K`, `--node-configuration-dir PATH` | | The Midnight node's k and network configuration directory, compared with the profile and reported in `warnings` ([node requirements](#node-requirements)) |
| `--node-bridge-checkpoint block:N\|tx:HASH` | | The bridge's on-chain data checkpoint; `/health` fails while the bridge metadata coverage does not support it ([bridge readiness](connect-a-node.md#bridge-readiness)) |

For each secret, a file or prompt option wins over the environment variable,
which wins over a value in the URL. Prefer files or prompts: values given on the
command line end up in the process list and shell history. Errors print stable
codes, never the secret or the connection details.

On an empty database without `--archive`/`--manifest`, an interactive run asks
for the two paths. A database that is already ready is resumed, never restored
over. An interrupted or incompatible database is refused; see
[recovery](recovery.md).

### The state directory

It holds the copied Blockfrost key, the libpq service file and passfile of the
owner and reader logins, the service configuration (`service.json`) and the
Blockfrost request ledger (`accounting/`). Keep it, with mode `700`, and reuse
it: db-sync-alt refuses changed identities, logins, passwords or ports against
existing state, and a reader login that was dropped and created again. Do not
edit `service.json`; the launch receipt binds it. Never delete the ledger to get
past a quota.

## Initial data

### From a snapshot or backup

Give the archive and its manifest to `db-sync-alt run` (`--archive`,
`--manifest`) on an empty database. Check the files first: compare their
SHA-256 with the values published next to them (for the release snapshot,
`sha256sum --check SHA256SUMS`). The manifest binds the archive's bytes; restore
checks the profile, the identity and every generation before anything is served.
`pg_restore` 17 must be on `PATH`. Restoring and verifying take a while on a
large history.

### Build initial data from your own db-sync

An alternative that makes no Blockfrost requests. You need:

- cardano-db-sync 13.7.1.0 (schema 15.48.6) or 13.8.0.0 (schema 16.52.6), for the same network (`meta.network_name`) with its full core history:
  `tx_in` and epoch stake kept. It must be a different database from the
  destination; db-sync-alt reads it in read-only transactions.
- An empty PostgreSQL 17 destination, with a private libpq service entry for each
  of the two databases (`PGSERVICEFILE`, `PGPASSFILE`).
- A JSON file stating that the db-sync keeps the history db-sync-alt needs:

  ```json
  {"dbsync_version": "13.7.1.0", "tx_in_retained": true, "epoch_stake_retained": true, "operator_attested": true}
  ```

  For a 13.8.0.0 source, use `"dbsync_version": "13.8.0.0"` instead. It must
  match actual schema 16.52.6. `meta.version` may name the older creating binary;
  it is not the current source version. Check full-history settings, mode
  markers and required history before attesting. Unsupported modes and
  incomplete epochs refuse before publication.

  This statement covers retained semantic history that schema inspection alone
  cannot prove. The adapter checks its required fields and observable source
  properties; unknown attestation fields are refused.

The new generation records the network, the db-sync version and schema, the
retention flags and an opaque fingerprint that binds the build to your source
and destination databases (so an interrupted build never resumes from a
different db-sync). It records no host name, cluster identifier, row ids or
configuration details of your source.

Then, with the service stopped:

```sh
db-sync-alt acquire --source db-sync --network preprod --create \
  --destination-service db_sync_alt --source-service dbsync_readonly \
  --source-attestation ~/.config/db-sync-alt/dbsync-attestation.json
db-sync-alt upgrade --service db_sync_alt
```

`--create` initialises the empty destination (never point it at a database in
use); the two service names are your libpq entries. `--network` selects the
packaged profile; `--profile-dir` may name a copy of it. `acquire` checks the
source's schema, network, history and the confirmed anchors, selects the facts
twice by independent queries, and publishes the first generation only if both
agree. It picks the highest confirmed target unless `--target-height` is given,
works in windows of `--window-blocks` (default 1,000) and checks free space
(`--reserve PATH:BYTES`, BYTES a whole number of bytes, default
`/:10737418240`, 10 GiB). `upgrade` turns the new schema-1
database into schema 5 and builds the served tables. After that, `db-sync-alt
run` with the same database resumes it and follows Blockfrost.

Measured on Preprod (a db-sync on the same machine, PostgreSQL 17 with 1 CPU and
1 GiB, db-sync-alt with 1 CPU), building the 0.1.0 release snapshot: `acquire`
up to block 5,366,232 (868,318 block headers) 17.8 minutes, of which about 5
minutes reading the source; `upgrade` 1.1 minutes; `backup` 2.0 minutes,
170 MB; `restore` with `--reader-role` into a new server 4.2 minutes and
`verify` 1.7 minutes. The restored database is about 750 MB, 214 MB of it the
served tables.

### One update from Blockfrost

`db-sync-alt acquire --source blockfrost --network NAME --destination-service NAME
--target-height N` publishes one update of a ready database up to block `N`
(at most 1,000 blocks above its target) and exits. The running service does this
by itself; the command is for administration with the service stopped.

## Status and stopping

`GET /status` returns the full state (HTTP 200); `GET /health` returns 200 only
when the lifecycle, PostgreSQL, the reader's serving check, the workers,
freshness and lag are all within policy, otherwise 503 with reason codes
([troubleshooting](troubleshooting.md)). After a restart, freshness is unknown
until the first poll. Health requests never call the provider.

`db-sync-alt status --service <name>` prints the same database facts offline,
including `bridge` (the bridge metadata coverage start); add
`--bridge-checkpoint block:<n>` or `tx:<hash>` to check the bridge's on-chain
checkpoint against it ([bridge readiness](connect-a-node.md#bridge-readiness)).

Stop the service before backups, upgrades and other administrative commands;
they take the same writer lock and refuse to run next to it. Stopping the
service does not stop serving: the node keeps reading the last published update
from PostgreSQL, and only freshness falls behind.

## Integrity checkpoints

Every start verifies the database in full from its
[integrity base](glossary.md#integrity-checkpoint): it recomputes the digests of
all stored facts at the base, compares them with the base's checkpoint, and
verifies every update published since the base. Once the start has verified the
canonical generation, that generation becomes the new base, so the next start
walks only what is published after it.

While the service runs, it writes a checkpoint between publications every
`integrity_checkpoint_interval_seconds` of the `sync` section in
`service.json` (default 21,600, 6 hours; 600 to 604,800). It verifies the
canonical generation in full and makes it the base; publication waits meanwhile
(about 1.5 to 2 minutes on Preprod, a few blocks of lag) and `/status` shows
`sync: checkpointing`. `/status` reports the base and when its checkpoint was
written (`integrity`). A database whose base is still its first generation
(right after a restore of the snapshot) has no checkpoint yet; the first one is
written once a newer update is published and the interval has passed, or at the
next start.

Each verification statement whose work grows with the history runs under its
own limit of one hour; every other statement of db-sync-alt keeps 30 seconds.
One hour covers about 10 million rows per statement at 100 times the measured
cost of these checks. The statements that sort or hash the whole history write
temporary files while they run, for example about 440 bytes per block for the
block-header check (375 MB for 890,000 blocks). Keep that much free space for
PostgreSQL's temporary files, and do not set `temp_file_limit` below it. If
PostgreSQL cancels a verification statement, db-sync-alt logs
`verification_statement_canceled` with the statement's plan and its tables'
estimated row counts before it stops.

The served tables are written in ranges of block heights or ids, each in one
statement under the 30-second limit. On a slow disk, or for very dense blocks or
large datums, a range can take longer: db-sync-alt then rolls that statement
back, logs `serve_range_split` with the table and the range, and writes the range
as two halves, down to one block or one id. A publication whose new facts take
longer than the limit in one statement (a long cardano-db-sync update) is
written the same way, in the ranges that hold them, after a
`serve_advance_ranged` log line. The rows are the same. Many such lines mean the
disk is slower than the data needs.

## Provider configuration

`db-sync-alt run` configures the provider itself. The service reads these
variables (the Compose deployment sets them):

| Variable | Purpose |
| --- | --- |
| `DBSYNC_PROVIDER_ADAPTER=portable` | Default: db-sync-alt's own client and request ledger |
| `DBSYNC_PROVIDER_KEY_FILE` | Absolute path of the private key file |
| `DBSYNC_PROVIDER_STATE` | Absolute directory of the request ledger |
| `DBSYNC_PROVIDER_ADAPTER=shared` | Delegate requests and accounting to an external accounting client (not part of this repository) |
| `DBSYNC_SHARED_ROOT`, `DBSYNC_SHARED_PROFILE` | Location of that client and its profile (default: the network name); its API host must be the network's |

The portable client talks only to the Blockfrost HTTPS endpoint of the
configured network, refuses a key whose prefix names another network, refuses
redirects, reserves each attempt before sending it, keeps provider cooldowns,
does not retry behind your back and refuses a corrupt ledger. All programs that
use one key must share one ledger.


### Older stores needing source ordinals

When upgrading a store from before schema 3 whose ledger ordinals are absent,
provide both `--source-service SOURCE` and
`--source-attestation ATTESTATION.json`. The JSON has the same truthful
version/retention fields as `acquire`. Ordinary upgrades whose ordinals already
exist need neither option. Existing `upgrade --source-service SOURCE` commands
must include the attestation; source schema, mode, type and version checks run
before ordinal recovery. The same source options give the upgrade of a database
of 0.3.x or earlier each stored epoch's `max_tx_size` without Blockfrost
requests ([upgrade to schema 5](recovery.md#upgrade-to-schema-5)).
