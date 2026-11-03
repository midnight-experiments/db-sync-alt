# Changelog

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
