# db-sync-alt

Cardano data for a Midnight node in a small PostgreSQL database, instead of a
full Cardano node and cardano-db-sync stack.

- **About 13 GiB less memory:** run a full Midnight node without a Cardano node
  and cardano-db-sync, which take about 14 GiB with their PostgreSQL
  ([memory and disk](#memory-and-disk)).
- **About 58 GiB less disk:** only the Cardano data Midnight uses is kept, 1.5 GiB
  instead of 59.1 GiB.
- **Same queries:** the Midnight node runs its usual cardano-db-sync SQL against
  db-sync-alt's PostgreSQL 17 database, with a read-only login.
- **Sources:** follows Cardano through the Blockfrost API; starts from a
  published snapshot or from your own db-sync.
- **Never silently wrong:** every update is verified before it is published; with
  nothing verified published, the node's queries fail instead of returning empty
  answers.
- **Keeps serving:** the node reads the last published update while db-sync-alt
  is stopped or restarting.

> This whole project is vibe coded. If you use it for a real application that
> manages any real money, that money will likely be lost. This project is for
> experimentation and development.

```mermaid
flowchart LR
    blockfrost["Blockfrost API<br/>(Cardano)"] -- new blocks --> follower
    initial["Release snapshot or<br/>your own db-sync"] -. initial data, once .-> follower
    follower["db-sync-alt"] -- checked updates --> postgres[("PostgreSQL 17")]
    node["Midnight node"] -- "db-sync SQL queries<br/>(reader login)" --> postgres
```

## Memory and disk

Measured on a running Preprod deployment on 3 November 2026, with db-sync-alt
0.1.0 and midnight-node 1.0.400. Memory is RAM plus swap plus the inactive file
cache.

| Original full stack | GiB of memory | db-sync-alt stack | GiB of memory |
| --- | ---: | --- | ---: |
| Midnight node | 1.30 | Midnight node | 1.30 |
| Cardano node | 6.70 | db-sync-alt | 0.05 |
| cardano-db-sync | 5.36 | PostgreSQL 17 | 1.24 |
| PostgreSQL 15 | 2.20 | | |
| **Total** | **15.56** | **Total** | **2.59** |

About 13 GiB less memory: the Cardano node, cardano-db-sync and PostgreSQL 15
take 14.26 GiB, db-sync-alt and PostgreSQL 17 take 1.29 GiB. Both stacks run the
same Midnight node process, so the saving does not depend on it; the percentage
(83 % here) does.

| Original full stack | GiB on disk | db-sync-alt stack | GiB on disk |
| --- | ---: | --- | ---: |
| Midnight node (archive) | 339.08 | Midnight node (archive) | 339.08 |
| Cardano node | 20.21 | db-sync-alt | 0.16 |
| cardano-db-sync | 2.97 | PostgreSQL 17 | 1.34 |
| PostgreSQL 15 | 35.95 | | |
| **Total** | **398.20** | **Total** | **340.57** |

About 58 GiB less disk. Data and configuration only, no container images or
logs. The PostgreSQL database grows with each update: on 17 November 2026 the
same deployment's PostgreSQL 17 held 1.89 GiB on disk and used 1.86 GiB of
memory, 1.62 GiB of it inactive file cache.

## Requirements

- Linux, Python 3.11 or newer, Git.
- PostgreSQL 17 and its client tools `pg_dump` and `pg_restore`
  ([provision PostgreSQL](docs/operations.md#provision-postgresql)).
- A Blockfrost API key.
- Initial data: the published Preprod snapshot or your own db-sync (see
  [first run](#first-run)).
- Docker (optional): see [Docker / Compose](docs/docker.md).

## Install

```sh
git clone https://github.com/midnight-experiments/db-sync-alt.git
cd db-sync-alt
python3 -m venv ~/.local/share/db-sync-alt/venv
~/.local/share/db-sync-alt/venv/bin/pip install --require-hashes -r requirements.lock
~/.local/share/db-sync-alt/venv/bin/pip install --no-deps --no-build-isolation .
~/.local/share/db-sync-alt/venv/bin/db-sync-alt --version
```

`requirements.lock` pins every dependency by hash. The examples below call
`db-sync-alt` directly; put `~/.local/share/db-sync-alt/venv/bin` on your `PATH`
or use the full path.

## First run

db-sync-alt never starts from an empty database: it needs verified initial
data, a ready [generation](docs/glossary.md#generation) of Preprod history. There
are two ways to get it.

Both ways read the two database passwords and the Blockfrost key from private
files. Create them, then put one value in each with any editor:

```sh
mkdir -p -m 700 ~/.config/db-sync-alt
for f in owner.password reader.password blockfrost-preprod.key; do
  install -m 600 /dev/null ~/.config/db-sync-alt/$f
done
```

`owner.password` and `reader.password` hold the passwords of the owner and
reader logins created during
[provisioning](docs/operations.md#provision-postgresql); `blockfrost-preprod.key`
holds your Blockfrost API key.

### Option A: the published Preprod snapshot

The 0.1.0 release carries a db-sync-alt backup of Cardano Preprod up to block
5,366,232 of 2 November 2026 (the snapshot), its manifest and a checksum file:

| File | Size |
| --- | ---: |
| `db-sync-alt-0.1.0-preprod-5366232.dump` | 170 MB |
| `db-sync-alt-0.1.0-preprod-5366232.manifest.json` | 0.1 MB |
| `SHA256SUMS` | < 0.1 MB |

The snapshot was built with 0.1.0 from a cardano-db-sync database
([option B](#option-b-build-it-from-your-own-db-sync)), without Blockfrost
requests, on PostgreSQL 17. With 1 CPU and 1 GiB for PostgreSQL, 0.1.0 restored
it in about 4 minutes and verified it in about 2 more; the restored database is
about 750 MB.

Download the three files into one directory, check them, then start db-sync-alt
on your empty database from that directory:

```sh
sha256sum --check SHA256SUMS
db-sync-alt run --network preprod \
  --blockfrost-key-file ~/.config/db-sync-alt/blockfrost-preprod.key \
  --database-url postgresql://127.0.0.1:5432/db_sync_alt \
  --database-user db_sync_alt_owner --database-password-file ~/.config/db-sync-alt/owner.password \
  --reader-role db_sync_alt_reader --reader-password-file ~/.config/db-sync-alt/reader.password \
  --archive db-sync-alt-0.1.0-preprod-5366232.dump \
  --manifest db-sync-alt-0.1.0-preprod-5366232.manifest.json
```

db-sync-alt restores and verifies the snapshot, upgrades it to schema 5 (reading
each stored epoch's maximum transaction size from Blockfrost, 2 requests per
stored epoch: 100 requests for the snapshot's 50 epochs), then serves it and
follows new blocks from Blockfrost; `/health` returns 200 once it has caught up.
It runs in the foreground: Ctrl-C stops it and the same command resumes it. All
options: `db-sync-alt run --help`.

Catching up from the snapshot takes days, and longer the older the snapshot is.
Measured on Preprod up to 17 November 2026: the chain adds about 3,900 blocks a
day, at 3.6 to 4 transactions per block, and with the default settings each
block caught up costs 5.5 to 6 requests (about 1.3 per transaction, plus one per
block with transactions). The tip poll takes about 8,700 of the built-in
client's 50,000 requests a day, so each day between the snapshot and your start
takes 1.2 to 1.3 days to catch up: about 17.5 days on 17 November 2026. When a
day's requests are spent, the service waits for the next UTC day
(`quota_exhausted`). To start closer to the tip, build the initial data from your
own db-sync ([option B](#option-b-build-it-from-your-own-db-sync)), or follow
with a larger Blockfrost plan through an external accounting client
([Blockfrost costs](#blockfrost-costs)).

### Option B: build it from your own db-sync

If you run cardano-db-sync 13.7.1.0 or 13.8.0.0 for Preprod with its retained
core/full history, db-sync-alt can build the initial data from it once:
read-only, without Blockfrost requests, up to its newest confirmed block, so
the catch-up from Blockfrost that follows is short. 0.1.0 built Preprod up to
block 5,366,232 this way in about 18 minutes. Then run `db-sync-alt upgrade` and
`db-sync-alt run` without `--archive`. See
[build initial data from your own db-sync](docs/operations.md#build-initial-data-from-your-own-db-sync).

## Upgrade an existing database

This version needs a schema-5 database. A database of 0.3.x or earlier holds
each epoch's nonce but not the epoch's maximum transaction size, which
midnight-node 2.1.101 reads. Stop the service after a publication
(`db-sync-alt status` shows no pending generation) and stop the node (the
upgrade's final transaction locks the node's `epoch_param` while it verifies the
whole database), then run once, with the service's Blockfrost settings:

```sh
db-sync-alt upgrade --service db_sync_alt
```

The upgrade reads every stored epoch's `max_tx_size` twice from Blockfrost (2
requests per stored epoch; `status` shows the count) and refuses unless both
reads agree; a cardano-db-sync of the network can be given instead with
`--source-service` and `--source-attestation`. Then start the service with this
version, then the node. There is no downgrade: 0.3.x refuses an upgraded database, so take a
`db-sync-alt backup` with the earlier version first if you may need to go back,
and go back with that version's `db-sync-alt restore`. See
[upgrade to schema 5](docs/recovery.md#upgrade-to-schema-5).

## Connect a Midnight node

Point the node's db-sync settings at db-sync-alt's database, with the reader
login:

```sh
DB_SYNC_POSTGRES_CONNECTION_STRING='postgresql://db_sync_alt_reader:<password>@<host>:5432/db_sync_alt'
SSL_ROOT_CERT=/path/to/root-ca.crt
CARDANO_SECURITY_PARAMETER=2160
```

- `<host>` is your PostgreSQL server as the node reaches it; `<password>` is the
  reader login's password you chose when you created it.
- midnight-node 2.1.101 connects only over TLS: it refuses a PostgreSQL server
  without TLS, and `ALLOW_NON_SSL` has no effect. Turn TLS on in your server and
  give the node its CA certificate with `SSL_ROOT_CERT`, so that it checks the
  server's certificate and host name ([TLS](docs/connect-a-node.md#tls)).
- `CARDANO_SECURITY_PARAMETER` must equal the network's security parameter k
  (2160 on Mainnet and Preprod, 432 on Preview), and the node's Cardano
  addresses and policies must be the profile's: db-sync-alt cannot see the
  node's settings.

The reader can read only the relations the node's queries use. Details:
[connect a Midnight node](docs/connect-a-node.md).

## Health and status

```sh
curl -i http://127.0.0.1:18080/health
curl http://127.0.0.1:18080/status
```

`/health` returns 200 when db-sync-alt is ready and up to date and the reader
login reads the node's relations, otherwise 503 with reason codes. `/status`
returns 200 with the full state, healthy or not, including the reader login's
settings, report-only `warnings` and the integrity base (`integrity`: its
generation and when its checkpoint was written). Every 6 hours by default the
service writes an [integrity checkpoint](docs/operations.md#integrity-checkpoints);
publication waits for it and `sync` reads `checkpointing`.
[Troubleshooting](docs/troubleshooting.md) explains every code.

## Status and roadmap

db-sync-alt **0.4.9** is experimental. It serves the SQL queries of **Midnight
node 2.1.101**, including the epoch `max_tx_size` its cost-based cNIGHT
observation reads and, from its coverage start, the token bridge data its bridge
query reads ([bridge readiness](docs/connect-a-node.md#bridge-readiness)).

- **Networks** (`--network`): **Preprod** is supported and tested end to end.
  **Mainnet**: a database built from a real Mainnet cardano-db-sync is followed
  through Blockfrost, and a Midnight node reads it; serving that node at the
  Mainnet chain tip is not tested. **Preview**: the profile is packaged and
  tested on synthetic data only ([networks](docs/networks.md)).
- **PostgreSQL 17**, which you provide, as you would for cardano-db-sync. It is
  the major version the Midnight node's reference environment runs its
  cardano-db-sync database on, so db-sync-alt's database can live on the same
  PostgreSQL 17 server as the other databases of a Midnight deployment
  ([sharing a server](docs/operations.md#share-a-postgresql-17-server)).

See the [compatibility matrix](docs/compatibility.md) and
[limitations](docs/limitations.md).

Planned next:

- a Midnight node served at the Mainnet chain tip;
- Cardano Preview built and followed end to end;
- a bound on how much update history the database keeps.

## Blockfrost costs

Measured at the Preprod chain tip with the default settings, at 3,800 to 3,900
blocks and 10,600 to 17,000 transactions a day
([how it was counted](docs/costs.md#blockfrost-requests)):

| Work | Requests per day |
| --- | ---: |
| Polling the chain tip every 20 seconds | ≈ 8,700 |
| Publishing new blocks (7.3 to 9.5 requests per block) | ≈ 28,600 to 36,800 |
| **Total** | **≈ 36,500 to 45,100** |

A block costs about 1.3 requests per transaction plus one for its transaction
page, and each update about 5 more, so the cost grows with the chain's
transactions. Catching up after a pause costs more, in proportion to the blocks
missed ([from the snapshot](#option-a-the-published-preprod-snapshot)).

The built-in client's daily ceiling is 50,000 requests, and it sends one request
at a time. For a larger Blockfrost plan, db-sync-alt can delegate requests and
accounting to an external client, not part of this repository
(`DBSYNC_PROVIDER_ADAPTER=shared`). Then `provider_concurrency` keeps several
requests of an update in flight and `provider_max_requests_per_hour` paces
them, which shortens a catch-up
([requests in flight](docs/operations.md#requests-in-flight)). The key is read
from a private file; see [costs](docs/costs.md).

## More

[Glossary](docs/glossary.md) ·
[Networks](docs/networks.md) · [Operations](docs/operations.md) · [Backup and recovery](docs/recovery.md) ·
[Contributing](CONTRIBUTING.md) ·
[Security](SECURITY.md) · [Changelog](CHANGELOG.md)

Licensed under [Apache-2.0](LICENSE) · [Third-party notices](THIRD_PARTY_NOTICES.md)
