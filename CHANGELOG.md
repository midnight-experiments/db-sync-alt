# Changelog

## 0.5.1

- **PostgreSQL 17 or newer.** db-sync-alt runs on PostgreSQL 17 and every
  newer major version and refuses a server older than 17
  (`postgresql_17_or_newer_required`). The PostgreSQL tests run on 17, 18 and
  19, and snapshot files move between 17 and each of 18 and 19 with their
  content digests ([PostgreSQL](docs/postgresql.md)). The Compose deployment
  runs PostgreSQL 17.
- **Formats.** The database format (1), the snapshot file format (1) and the
  keep rule (version 2) are those of 0.5.0: this version runs on a 0.5.0
  database and restores a 0.5.0 snapshot file as they are
  ([compatibility](docs/compatibility.md)).
- **Dependencies.** bech32 0.12 and zstd 0.14 in the binary; the PostgreSQL
  17.11 image of the Compose deployment is pinned to its current build.
- **README.** The system requirements are one table of memory and disk per
  network, next to the same figures of a stack with a Cardano node and
  cardano-db-sync; the first start is a list of what `run` does
  ([README](README.md#system-requirements)).
- **Checks.** A Midnight node 2.1.101 on Preprod reads a db-sync-alt database
  and imports blocks at the tip of its chain. A node on Mainnet, and a Cardano
  epoch change while a node reads the database, are not part of the checks
  ([compatibility](docs/compatibility.md#what-the-release-checks-cover)).

## 0.5.0

The first release. db-sync-alt follows Cardano through the Blockfrost API and
serves Midnight node 2.1.101 the cardano-db-sync tables its queries read, from
a small PostgreSQL 17 database that prunes itself.

- **The database.** It keeps the events of the retention (default 7 days after
  this database first served them, at least 24 hours), the unspent outputs of
  the profile's addresses and assets, the governance records and the epoch
  values the node reads, and deletes the rest. The node reads eleven views
  through a read-only login; while the database is not serving, every view
  raises SQLSTATE 55000 instead of returning an empty answer. A different
  `--retention-seconds` is refused unless `--change-retention` is given
  ([measured sizes](README.md#system-requirements), [PostgreSQL](docs/postgresql.md)).
- **Snapshot files.** One file holds a whole database and its content digest.
  `snapshot` writes one, `restore` and `run --snapshot` load one into an empty
  database with `--expect-content-digest`, without PostgreSQL client tools; the
  node gets SQLSTATE 55000 until the restore publishes. With `--snapshot-dir`
  the service writes one file a day and keeps the newest `--snapshot-keep`
  ([snapshot files](docs/snapshots.md)).
- **Following.** Each final block is read once, as transaction CBOR whose
  hashes are checked; spends are matched to the outputs the database holds
  without a request. An update and its pruning are one transaction. Asset
  conservation in every transaction and a supply check every 6 hours close the
  gate when they fail (`asset_conservation`, `supply_mismatch`), as does a
  provider that contradicts a final block twice (`canonical_conflict`).
- **Provider.** Requests per second (default 8, at most 10) and in flight
  (default 4) are flags. A 402 is waited out until the next UTC day, 418 and
  429 until the time the provider names; `/status` shows the calls of each UTC
  day for a week ([provider](docs/provider.md)).
- **Service.** `db-sync-alt run` restores an empty database from a snapshot
  file, grants and checks the node's reader login, follows the chain, keeps
  the database in shape and answers `/health` (200 only when the database
  serves at the tip) and `/status` (one JSON document in every state) on
  `0.0.0.0:18080` by default. Temporary conditions are retried; a database or
  transport failure that lasts longer than `--database-fatal-after-seconds`
  (default 1,800) ends it with exit status 75 ([operations](docs/operations.md)).
- **Configuration.** No configuration file: flags, each also readable from a
  `DSA_` variable; secrets only as file paths. Every value is checked before
  anything connects; an invalid one ends the command with exit status 2 and
  names the flag ([configuration](docs/configuration.md)).
- **Commands.** `run`, `create`, `restore`, `snapshot`, `verify`, `status`,
  `grant-reader`, `compare`, `profile` and `health`.
- **Install.** One binary for Linux x86-64 with glibc 2.39 or newer, and the
  image `ghcr.io/midnight-experiments/db-sync-alt`, with a Compose deployment
  that runs it next to its own PostgreSQL 17 server over TLS
  ([deployment](deploy/README.md)). The release archive and the image carry
  the license texts of every crate in the binary.
- **Networks.** Mainnet and Preprod, with profiles generated from the
  configuration of Midnight node 2.1.101 ([compatibility](docs/compatibility.md)).
- **Checks.** The node's query catalogue returns the same rows from a 0.5.0
  database as from a full-history database of the same chain at the same block.
  The follower ran on real Preprod and Mainnet blocks, and on Preprod its
  database had the content digest of a database built from the full history at
  the same blocks. No Midnight node has run against a 0.5.0 database as part of
  these checks ([compatibility](docs/compatibility.md#what-the-release-checks-cover)).
- **Limitations.** Queries about history older than the window, the
  registered-candidate results of the node's operator RPCs and CLI, and the
  node's genesis subcommands are not supported; stake distributions exist for
  recent epochs only ([limitations](docs/limitations.md)).
