# Changelog

## Unreleased

- **BREAKING: schema 4.** A database needs `db-sync-alt upgrade`, run once with
  the service stopped, before this version serves it; the service refuses an
  older schema (`schema_upgrade_required`). Versions before 0.2.0 refuse a
  schema-4 database (`unsupported_database_identity`), so there is no downgrade:
  keep a `pg_dump` of the stopped database taken before the upgrade to go back.
  The upgrade verifies the whole history once and keeps node queries answering
  from the last published update while it runs.
- **BREAKING:** every new incremental update records the network profile by its
  SHA-256 digest (`identity_sha256`, integrity kind `certified-parent-delta-v2`)
  instead of embedding the profile twice. Once the first such update is written,
  versions before 0.2.0 refuse to start on that database (`completion_invalid`).
- An update's manifest is about 4.5 KB of JSON instead of about 199 KB; on
  Preprod the database grows about 150 MB a day less. Stored updates keep their
  embedded profile (`certified-parent-delta-v1`) and are verified under their own
  rules, in one lineage with the new ones; an update left pending by 0.1.x is
  finished in its own form.
- Integrity checkpoints: after a full verification, db-sync-alt records the
  digests of every fact family and the integrity root of the verified
  generation, and makes it the integrity base. Startup recomputes the base's
  digests, compares them with the checkpoint and verifies only the updates
  published since: its time grows with the stored data, not with the number of
  updates.
  Checkpoints are written after each startup verification, by `upgrade`, by the
  restore of a schema-3 backup, and every `integrity_checkpoint_interval_seconds`
  while the service runs (`sync` section of `service.json`, default 21,600,
  600 to 604,800); publication waits while one is written and `sync` reads
  `checkpointing`.
- `/health` and `/status` report `integrity`: the integrity base and when its
  checkpoint was written.
- `backup` and `verify` verify the canonical generation once, from the integrity
  base: their time grows linearly with the data. Backups of schema 4 use the
  format `db-sync-alt-postgresql-backup-v3` and include the checkpoints. Backups
  of format v2 (schema 3, such as the 0.1.0 Preprod snapshot) still restore, are
  compared generation by generation and are upgraded to schema 4 before the
  reader is granted; the service's first run accepts both formats.
- A generation older than the integrity base is not verified on its own
  (`generation_before_integrity_base`). New refusals for a base whose facts or
  checkpoint changed: `checkpoint_digest_mismatch`, `checkpoint_root_mismatch`,
  `checkpoint_chain_mismatch`.
- `db-sync-alt status` lists one summary row per generation (`id`, `parent`,
  `status`, `source` and the heights `start_height`, `start_index`,
  `target_height`, `witness_height`) and no manifest. Read a manifest with
  `SELECT manifest FROM chain.generations WHERE id = …`.

## 0.1.1

- Startup reads only the database's state row to decide whether to serve, and
  keeps nothing from it. Memory after startup does not grow with the number of
  published updates. On a copy of a Preprod database with 8,061 generations,
  RSS at ready went from 3.99 GiB to 43 MiB, and the startup peak from 7.55 GiB
  to 44 MiB.
- Refusals for empty, importing, restoring, revoked, unowned, corrupt or
  permission-limited databases are unchanged.
- No schema or stored-data change: 0.1.0 and 0.1.1 run on the same database.
  The Preprod snapshot of the 0.1.0 release restores with 0.1.1.

## 0.1.0

Initial release; experimental.

- Serves the Cardano data a Midnight node (SQL queries of node 1.0.400) needs
  from PostgreSQL 17: the node connects directly with a read-only reader login
  and runs its SQL unchanged on cardano-db-sync-shaped relations with
  chain-ordered IDs. Every node-facing relation raises an error instead of
  answering while the database is not serving a verified update; the node keeps
  reading the last published update while db-sync-alt is stopped.
- PostgreSQL 17 provided by the operator, the same major version as the
  cardano-db-sync database of the Midnight node's reference environment; the
  database can share that server. It needs a dedicated database closed to
  `PUBLIC`, a non-superuser owner login without `CREATEROLE` and a reader login;
  db-sync-alt never creates a role, grants the reader exactly what the node
  reads and reports the reader's settings.
- Network profiles for Cardano Mainnet, Preprod and Preview (`--network`): every
  network value, from the network magic and security parameter to the Blockfrost
  host, comes from the packaged profile. Preprod is supported and tested end to
  end; the Mainnet and Preview profiles are packaged but tested on synthetic
  data only.
- Follows the chain through the Blockfrost API: polls the tip every 20 seconds
  and publishes each confirmed update as a verified generation, with a daily
  request ceiling and per-job budgets.
- Builds initial data once from a cardano-db-sync 13.7.1.0 database without
  Blockfrost requests (`db-sync-alt acquire --source db-sync`), writing headers in
  bulk and recording no host details of the source; or restores a snapshot.
  The Preprod snapshot `db-sync-alt-0.1.0-preprod-5366232` (up to block
  5,366,232), built this way, is published with the release.
- PostgreSQL-native backup, restore, full verification and status
  (`db-sync-alt backup|restore|verify|status`); schema 1 → 2 → 3 upgrade.
- `/health` and `/status` HTTP endpoints with stable reason codes.
- One `db-sync-alt` command; foreground launcher (`db-sync-alt run`) and an
  optional Docker Compose deployment whose PostgreSQL is reachable from a
  `node` network.
