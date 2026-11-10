# Changelog

## Unreleased

- **BREAKING: schema 5, fact format v2 and backup format v4.** Every epoch fact
  carries the epoch's maximum transaction size, served as
  `epoch_param.max_tx_size`. midnight-node 2.1.101 reads
  `MAX(epoch_param.max_tx_size)` of the cursor block's epoch on its cost-based
  cNIGHT observation path and stops cNIGHT observations without it. The same
  format holds the token bridge's metadata (below). The packaged node query
  sources and all network profiles now pin node 2.1.101
  (`86774edf8cc76345a5c3a4221f3fe80c094b1c33`); profile configurations and
  genesis are unchanged.
- Serve the token bridge's data for midnight-node 2.1.101, forward only. Once
  governance sets the bridge's checkpoint, the node runs its bridge query on
  every block import and stops when the answer differs from the block author's;
  before this version, the reader could not read `tx_metadata` at all. Every
  update from Blockfrost now stores the `tx_metadata` of key 6500973 of each
  transaction with an output at the ICS address (one request per such
  transaction, checked against the transaction's own CBOR), keeps every asset
  of the ICS and reserve outputs, and serves them as `tx_metadata` and
  `ma_tx_out` to the reader. Coverage starts with the first update from
  Blockfrost: after the upgraded update for a database of 0.3.x, after the
  bootstrap for a database built from cardano-db-sync (which stores no
  metadata). Nothing older is fetched. `db-sync-alt status`, `/status` and the
  upgrade report `bridge.coverage_start`. State the bridge's on-chain checkpoint
  (`status --bridge-checkpoint`, `run --node-bridge-checkpoint` or
  `node.bridge_checkpoint`) to have it checked: an unsupported checkpoint is
  reported, and `/health` answers 503 `bridge_checkpoint_unsupported`. An
  update from cardano-db-sync holds no metadata: coverage starts again after
  it, and `acquire` reports the coverage it ended and warns
  (`bridge_metadata_coverage_restarted`). Blockfrost's integers beyond 2^53 are stored as Blockfrost rounds them;
  the node reads the same recipient. Answers nested deeper than 4,000 levels
  are cut instead of failing the request, by db-sync-alt's Blockfrost client
  and by the external accounting client of `DBSYNC_PROVIDER_ADAPTER=shared`
  (db-sync-alt gives that client's `json.loads` the same bound): metadata that
  deep, which anyone can put on chain, is stored as NULL and ingest goes on.
- Upgrade an existing database in place, with the service and the node stopped:
  `db-sync-alt upgrade --service <name>`. A database written by 0.3.x or
  earlier holds each epoch's nonce but not its size: the upgrade reads every
  stored epoch's `max_tx_size` twice from Blockfrost with the service's
  Blockfrost settings (2 requests per stored epoch, counted in the same daily
  budget; `db-sync-alt status` shows the count beforehand), or from a
  cardano-db-sync given with `--source-service` and `--source-attestation` (no
  Blockfrost request). Both reads must agree and carry the stored nonce. Answers
  are kept as they arrive, so a rerun continues where it stopped without
  repeating an answered request, unless the attempt spent its own request
  budget (4 per stored epoch) on failing requests. The change commits in one
  transaction that verifies the whole database and, until it commits, locks the
  node's `epoch_param`; any refusal leaves the database as it was. Stop the
  service when `status` shows no pending generation (the upgrade refuses with
  `upgrade_pending_generation` otherwise).
- The packaged profiles' `network.json` now names the node 2.1.101 query pin. A
  copy given with `--profile-dir` must be refreshed from this release, or every
  command refuses it (`untrusted_network_profile`).
- 0.3.x and earlier cannot open an upgraded database: they refuse schema 5 with
  `unsupported_database_identity` (`invalid_identity` in the service). There is
  no downgrade; keep a `pg_dump` of the stopped database to go back.
  Backups are written only of schema-5 databases (format v4). Restoring a v2 or
  v3 backup runs the same upgrade and needs the same source settings, which are
  checked before the destination database is touched.
- Acquisition validates every epoch's size (an integer from 0 to 2,147,483,647,
  zero included, never a default) and keeps the maximum of an epoch's
  cardano-db-sync rows, as the node does. A cardano-db-sync source needs its
  typed `epoch_param.max_tx_size` column. Blockfrost acquisition reads the size
  from the epoch parameters it already requests: no extra request.
- Add optional filtering that can skip unnecessary UTxO and transaction-detail
  requests for unrelated transactions while retaining matching facts. Select it
  with `sync.relevance_enabled` or `--relevance` / `--no-relevance`; it is off by
  default on every network, and health reports the requested/effective modes.
- Filtered history requires a reader that verifies relevance proofs. Retain a
  compatible package with the database and its backups, even after filtering
  is switched off.
- Bound relevance checks at integrity checkpoints and report stored history
  provenance in the `status` field `relevance.history`.
- Acquire complete SQL prerequisite history in bounded, atomic windows with
  resumable contiguous coverage and independent source verification. Existing
  source-query deadlines, identity caps and confirmation rules are unchanged.
- Refuse a conservatively oversized completion receipt before generation or fact
  work. Resume uses the immutable stored window; a CLI override cannot change it.
- **BREAKING reader requirement:** SQL acquisition receipt version 2 requires
  the updated reader/service; the 0.3.0 reader cannot admit it. It is separate from
  the schema, incremental manifest and backup format versions. Supported
  five-field version 1 receipts retain their validation; other adapter-versioned
  receipt field sets refuse.
- Startup verification on a long history no longer times out when PostgreSQL
  misestimates the generation membership: the full metadata check now
  hash-joins materialized blocks.
- Fix Blockfrost acquisition and follow stopping at a block that holds a
  transaction whose metadata nests deeper than about 62 levels or repeats a
  map key. Anyone can submit such a transaction. Transaction auxiliary data is
  now read past without being decoded; malformed transaction bytes still refuse.
- Fix Blockfrost acquisition and follow stopping at a block that holds a
  transaction whose witness set carries Plutus data (a datum or a redeemer)
  nested deeper than about 60 levels or with a repeated map key. Anyone can
  submit such a transaction. The witness set must still be a map; its content
  is now read past without being decoded.
- Fix Blockfrost following stopping for good at an epoch whose stake
  distribution has more than about 499,000 delegated stake addresses
  (Mainnet has more; anyone can register more). The epoch's stake is now read
  per pool, the same per-pool sums cardano-db-sync acquisition stores: the
  current epoch from the pool list, a completed epoch from each pool's history.
  Requests grow with the number of pools (about 1,300 per new Mainnet epoch,
  6,100 when catching up a completed one), never with delegated addresses.
- Fix Blockfrost acquisition and follow stopping at every phase-2-invalid
  transaction in full mode, and at a selected one in relevance mode. Such a
  transaction is accepted on chain whenever a script fails, also without an
  attacker. db-sync-alt now stores it as cardano-db-sync 13.8 does: only its
  collateral inputs are spent and only its collateral return is created (at
  the index after the body outputs); its body inputs, outputs and mint, which
  never took effect, are not stored. One that touches nothing relevant stores
  nothing. A relevant one (a spent collateral or a returned output of the
  profile) is served with `valid_contract` false, from a cardano-db-sync source
  as well as from Blockfrost; this is part of fact format v2 and schema 5,
  which allow `transactions.valid` 0 (the upgrade replaces the check in place;
  an older database holds valid transactions only). Before, such a
  transaction stopped the update, or revoked serving
  (`unsupported_relevant_invalid_transaction`, `invalid_relevant_transaction`).
- Fix relevance mode stopping (`provider_output_cbor_mismatch`) at a valid
  transaction whose collateral return carries a native asset, which Blockfrost
  lists with lovelace only, and at an output to a pointer address encoded with
  redundant zero groups, which the ledger and cardano-db-sync re-serialise
  minimally. Anyone can submit either; full mode was not affected.

## 0.3.0

- Read pending generation status without loading its manifest, while preserving
  fresh ownership, status and revocation checks.
- Accept retained core/full-history cardano-db-sync 13.8.0.0 (schema 16.52.6)
  alongside 13.7.1.0 (schema 15.48.6). Record the actual source schema/version
  separately from the unchanged compact contract and network profiles.
- Check required source types, domains, nullability, mode markers and epoch
  coverage. Unknown versions, unsupported modes and incomplete history refuse.
- **BREAKING CLI change:** `upgrade --source-service SOURCE` also requires
  `--source-attestation FILE`. Supply the pair only when an older compact
  database needs external ledger ordinals. Ordinary local schema upgrades
  remain unchanged. An incomplete pair returns exit code 2 with the JSON code
  `source_attestation_pair_required`; unreadable or invalid JSON attestation
  files return `schema_upgrade_failed` without a traceback.
- Full metadata verification, run at startup and by `verify`, `backup` and
  `upgrade`, materializes the generation's lineage membership before joining it
  to the block headers. On a long history the inlined query could take seconds
  just to plan and exceed the 30-second statement timeout, so the service could
  not start. Delta verification of a new update is unchanged.

## 0.2.0

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
