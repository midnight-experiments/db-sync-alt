# Changelog

## 0.5.0

**Breaking.** 0.5.0 is a new implementation in Rust with a new database
format. It does not open a 0.4.x database, reads no configuration file or
state directory, and its database keeps a retention window instead of the full
history. Upgrade path ([upgrade](docs/upgrade.md)):

1. With 0.4.x, take a backup and check that it restores; keep the 0.4.x
   service running.
2. Create an empty PostgreSQL 17 database and run `db-sync-alt convert` with
   the 0.4.x database, read through its reader login, as the source. The 0.4.x
   database stays as it was.
3. Run `db-sync-alt verify` and `db-sync-alt grant-reader` on the new
   database, and optionally `db-sync-alt compare` against the answers
   `convert --capture-source` saved.
4. Start `db-sync-alt run` on the new database, wait until `/health` answers
   200, and point the node's connection string at it.
5. Stop the 0.4.x service. Its database is the way back until you remove it.

What changes for an operator:

- **Install.** One binary for Linux x86-64 with glibc 2.39 or newer, or the
  image `ghcr.io/midnight-experiments/db-sync-alt`. No Python, no virtual
  environment, no `pg_dump` or `pg_restore`. The release archive and the image
  carry the license texts of every crate in the binary.
- **Configuration.** Flags, each also readable from a `DSA_` variable; secrets
  only as file paths. Every value is checked before anything connects; an
  invalid one ends the command with exit status 2 and names the flag
  ([configuration](docs/configuration.md)).
- **A database that prunes itself.** It keeps the events of the retention
  (default 7 days after this database first served them, at least 24 hours),
  the unspent outputs of the profile's addresses and assets, the governance
  records and the epoch values the node reads, and deletes the rest. A
  different `--retention-seconds` is refused unless `--change-retention` is
  given. Measured right after a restore: 194 MB on Mainnet, 18 MB on Preprod
  ([snapshots](docs/snapshots.md#restore)).
- **Snapshot files** replace backups: `snapshot`, `restore` and `run
  --snapshot` with `--expect-content-digest`. A file holds a whole database and
  its content digest; a restore needs no client tools and leaves the node
  getting SQLSTATE 55000 until it publishes. With `--snapshot-dir` the service
  writes one file a day and keeps the newest `--snapshot-keep`.
- **Following.** Each final block is read once, as transaction CBOR whose
  hashes are checked; spends are matched to held outputs without a request.
  Asset conservation in every transaction and a supply check every 6 hours
  close the gate when they fail (`asset_conservation`, `supply_mismatch`), as
  does a provider that contradicts a final block (`canonical_conflict`). An
  update and its pruning are one transaction.
- **Provider.** No built-in daily ceiling: a 402 is waited out until the next
  UTC day, 418 and 429 until the time the provider names. Requests per second
  (default 8) and in flight (default 4) are flags. `/status` shows the calls of
  each UTC day for a week ([provider](docs/provider.md)).
- **Service.** `/health` and `/status` stay on port 18080, now on
  `0.0.0.0:18080` by default (`--listen`). Temporary conditions are retried;
  a database or transport failure that lasts longer than
  `--database-fatal-after-seconds` (default 1,800) ends the service with exit
  status 75. `db-sync-alt health` is the container health check.
- **`/status`.** `healthy`, `reason_codes`, `serving`, `sync`, `error_code`,
  `poll_error_code`, `served_target`, `provider_head`, `lag`,
  `freshness.provider_observation_age_seconds` and `service_version` keep their
  meaning; `quota`, `relevance` and the `sync` value `checkpointing` are gone;
  the other fields are new ([operations](docs/operations.md#status)).
- **Commands.** `run`, `restore`, `snapshot`, `verify`, `status`, `convert`,
  `compare`, `grant-reader`, `create`, `profile` and `health`. `acquire`,
  `backup` and `upgrade` are gone, and with them the build from an operator's
  cardano-db-sync, the integrity checkpoints and the node-configuration and
  bridge-checkpoint reports.
- **Networks.** Mainnet and Preprod, with profiles generated from Midnight
  node 2.1.101. Preview is not supported.
- **Limitations.** Queries about history older than the window, the
  registered-candidate results of the node's operator RPCs and CLI, and the
  node's genesis subcommands are not supported; stake distributions exist for
  recent epochs only ([limitations](docs/limitations.md)).

## 0.4.9

- An update can keep several Blockfrost requests in flight
  (`sync.provider_concurrency`, 1 to 8, default 1) and pace them
  (`sync.provider_max_requests_per_hour`, 1 to 28,800, absent by default).
  - **Before:** an update sent one request at a time, each after the previous
    answer. On a Mainnet catch-up most of each request's time was Blockfrost's
    answer time (about 0.55 s of about 0.66 s).
  - **Now:** with more than 1, an update sends up to that number minus one
    requests ahead whose paths it already knows (the pages of the blocks it
    sizes, the `/txs/{hash}/utxos` requests a block's selection can make, the
    independent pass's pages and transactions, the per-pool stake requests of an
    epoch), after the same checks and with the job attempt saved first, as for
    any request. It uses and checks the answers in the same order as before, so
    the published data are the same for every setting. A request sent ahead and
    not used stays counted and its answer is dropped. Receipts list the requests
    in the order they were used; their attempt numbers are reservation numbers
    and need not increase along a list. The pace spaces the update's requests,
    sent directly or ahead, at least 3,600 / value seconds apart. The built-in
    client still sends one request at a time.
  - With the defaults an update sends the same requests, in the same order,
    with the same receipts and job saves as 0.4.8.
  - Not breaking: an image swap from 0.4.8; a pending update continues. 0.4.8
    refuses the two new options: remove them from `sync` before starting 0.4.8
    again.
- A PostgreSQL lock timeout in the follower's update or in its tip poll is
  temporary: the follower waits and runs it again.
  - **Before:** when another session held a row the follower writes (its
    update job, the day's poll job or the shared `provider-metrics` row) or the
    serving lock longer than the follower's lock timeout (2 to 5 s for the
    update, 1 s for the poll), for example while a disk stall slowed
    PostgreSQL, the service stopped with `database_unavailable` (exit code 75):
    it stayed down, or a restart repeated its startup verification.
  - **Now:** the update or the poll rolls back and runs again after 1, 2, 4,
    8, 16 and then every 30 seconds; a pending generation resumes where it
    stopped. Meanwhile `/status` reports `database_lock_timeout` (`error_code`
    for the update, with `sync` `backoff`; `poll_error_code` for the poll), each
    retry logs `database_lock_timeout_retry`, and no failure record is written
    to the job. When every run for 300 seconds ends in a lock timeout, the
    service stops as before with `database_unavailable` (exit code 75) and logs
    `database_lock_timeout_persisted`.
  - `acquire --source blockfrost` reports a lock timeout as
    `database_lock_timeout` with exit code 75 (temporary) instead of
    `invalid_configuration_or_database` with exit code 2.
  - Every other database error is handled as before, and the lock timeout
    settings are unchanged.
- No format, schema, manifest or refusal change.
- Upgrading from 0.4.8 is an image swap: no schema, format or backup change. A
  pending generation started on 0.4.8 resumes on 0.4.9, and the reverse. A
  downgrade to 0.4.8 needs the `sync` section without `provider_concurrency`
  and `provider_max_requests_per_hour`.

## 0.4.8

- A relevance update holds as many blocks as its content allows, up to
  `max_batch_blocks` (100), instead of at most two.
  - **Before:** every update with filtering held at most two blocks, the static
    bound that two maximal blocks can never pass the 200,000-key limit. On the
    real Mainnet store each update paid its 6 fixed requests for two blocks,
    and waited for the next 20 s head poll: about 9.9 update requests and 1.0
    poll request per served block (10.9 in all, measured on 0.4.7), and about
    357 served blocks per hour.
  - **Now:** before it declares an update, the follower reads the candidate
    blocks' transaction pages, which the update reads anyway, and bounds each
    block's keys by ⌊(66 × its transaction bytes + 65,536) / 63⌋, the derivation
    the per-block proof limit already uses (0 for an empty block). The update
    holds the longest run whose bounds fit the key limit, and always one block.
    The acquisition pass uses those pages; the independent pass reads its own.
    The first block that does not fit starts the next update without another
    read. No request is added. On the measured Mainnet blocks an update holds
    about 20 blocks.
- `follow_min_blocks` above 2 now sets the number of blocks per update at the
  chain tip with filtering. `deploy/mainnet-relevance-sync.json` (3 blocks,
  60 s) saves about 7,000 requests a day at the Mainnet tip.
- A request that fails in transport now waits and is sent again with the
  shared adapter (`DBSYNC_PROVIDER_ADAPTER=shared`).
  - **Before:** only the external client's `URLError`, `ConnectionError` and
    `TimeoutError` reports counted as transport failures. Any other, such as
    `RemoteDisconnected` when Blockfrost closed the connection without an
    answer, stopped the follower with `provider_request_rejected` until it was
    started again.
  - **Now:** a closed, reset, aborted or refused connection, a broken pipe, an
    incomplete or malformed status line, a TLS or name-resolution failure, any
    other connection error and a timeout (`RemoteDisconnected`,
    `ConnectionResetError`, `ConnectionAbortedError`, `ConnectionRefusedError`,
    `BrokenPipeError`, `IncompleteRead`, `BadStatusLine`, `HTTPException`,
    `SSLError`, `SSLEOFError`, `SSLZeroReturnError`, `gaierror`, `OSError`,
    `timeout`, `TimeoutError`) wait and retry (`provider_unavailable` or
    `provider_timeout`), as the built-in client's do. The attempt stays
    counted. HTTP statuses, invalid answers and unknown failures are handled as
    before.
- A daily request limit up to 10,000,000 is accepted (was at most 50,000), for
  Blockfrost plans with a larger allowance. With the shared adapter the limit
  is the one its client reports for the profile. The built-in client's limit
  stays 50,000. The other quota rules are unchanged: requests stop at the
  limit, and an HTTP 402 ends the day.
- A one-shot `acquire --relevance` target stays at most two blocks after the
  ready one. Resuming a pending update through `acquire` is unchanged.
- No format, schema, manifest or refusal change.
- Upgrading from 0.4.7 is an image swap: no schema, format or backup change. A
  pending generation started on 0.4.7 resumes on 0.4.8. Return to an earlier
  release only at a completed publication, with no update in progress: an
  earlier release declares an unfinished update job's whole target without
  sizing it.

## 0.4.7

- Each relevance update makes one provider request fewer. It takes the witness
  header (the block k above its target) from the header suffix it fetches
  anyway, instead of reading `/blocks/{target+k}` separately first. The
  manifest records the same witness height and hash. The witness observation
  before publication is unchanged.
- The Blockfrost usage check `/metrics` runs once per 10,000 requests counted
  in the key's ledger, instead of at the start of every job.
  - **Before:** each new job (every update and every day's polling) called
    `/metrics` first, and again after 600 s, 1,000 requests or a UTC day
    change. On the Mainnet relevance follow that was one request per update,
    about one request in twenty.
  - **Now:** the database keeps the requests counted since the last check
    (`chain.sync_jobs` row `provider-metrics`), across jobs, restarts and UTC
    days. A new database checks on its first request. Every attempt is still
    counted before it is sent, and quota decisions use that count. A provider
    HTTP 402 still ends the day at once.
- No format, schema, manifest or refusal change.
- Upgrading from 0.4.6 is an image swap: no schema, format or backup change. A
  pending generation started on 0.4.6 resumes on 0.4.7.

## 0.4.6

- Following the chain no longer slows down as the store's history grows: the
  checks of each published update now read the update's own rows.
  - **Before:** on the real Mainnet store, the relevance follow of 0.4.5
    served 37.7 blocks per hour while Mainnet produced 181.5. About 175 s of
    each 191 s update went to checks. The historical selected-event checks
    re-read every block since the store's start, about 1 M blocks. From the
    eleventh update of a process, PostgreSQL also switched the dependency
    checks to generic plans, which scanned whole tables. The update's
    transaction coverage check and the lookup of its last stored header read
    the whole stored block membership.
  - **Now:** each check starts from the rows the update added, through the
    `delta_<table>` indexes. Verification statements are planned with their
    parameters on every run. Membership lookups use the height and hash indexes
    of each part of the membership. A check reads a few dozen buffers however
    long the history is.
- The checks give the same answers and refuse with the same codes. No format,
  schema, manifest or refusal change.
- Upgrading from 0.4.5 is an image swap: no schema, format or backup change. A
  pending generation started on 0.4.5 resumes on 0.4.6.

## 0.4.5

- `db-sync-alt upgrade` of a Mainnet-sized store no longer fails on a slow disk
  while it builds the served tables.
  - **Before:** 0.4.4 wrote `serve.datum` in ranges of 100,000 ids, about
    90 MiB of datum values per statement on Mainnet. On a destination writing
    16 MiB/s, the range of ids 1,000,000–1,099,999 was cancelled by the 30 s
    statement limit after 20 minutes of work, and the upgrade rolled back with
    `schema_upgrade_failed`.
  - **Now:** datums are written in ranges of 10,000 ids (about 9 MiB). Each
    range of every served table runs under a savepoint: a range the limit
    cancels is rolled back and written as two halves, down to one block height
    or one id, with a `serve_range_split` warning. One height or id that still
    cannot be written fails the upgrade as before.
- A publication writes its served rows the same way when one statement would
  pass the limit: a long cardano-db-sync update onto a serving store wrote each
  table's new rows in one statement, which a few days of dense Mainnet blocks
  could take past 30 s. Such a table is now written in the build's ranges that
  hold the new rows, after a `serve_advance_ranged` warning. Ordinary
  publications run the same statements as before.
- The served rows, their order and `serve.verify` are unchanged. No format,
  schema, manifest or refusal change.
- Upgrading from 0.4.4 is an image swap: no schema, format or backup change. A
  pending generation started on 0.4.4 resumes on 0.4.5.

- The epochs phase of a SQL bootstrap no longer slows down as the source tip
  moves past the epochs it reads: each epoch's last block is found among that
  epoch's own blocks. On a throttled source it no longer stops with
  `source_timeout` (0.4.3 did on Mainnet at epoch 647, 19 epochs behind the tip).
- `/status` and `/health` report the package's version as `service_version`.
  0.4.2 and 0.4.3 reported `0.4.1` there, while `db-sync-alt --version` was
  correct. The service and the one-shot Blockfrost acquire take the version
  from the package, and a unit test keeps `pyproject.toml` equal to it.
- The way back from an upgrade is documented as a `db-sync-alt backup` taken
  with the earlier release before the upgrade and restored with that release's
  `db-sync-alt restore`. A copy made with plain `pg_dump` and `pg_restore` into
  a new database or onto another server is refused with `invalid_sync_job`
  until the next UTC day, because each day's polling job is bound to the
  database that created it; troubleshooting names this cause.
- Upgrading from 0.4.3 is an image swap: no schema, format or backup change. A
  pending generation started on 0.4.3 resumes on 0.4.4.

## 0.4.3

- A SQL bootstrap and the upgrade that follows it no longer run any destination statement whose cost grows with the history under the store's 30 s statement limit.
  - **Before:** 0.4.2's Mainnet bootstrap stopped after its selection was staged. It built the selection's index in one statement (about 5 million rows), which was cancelled at 30 s, and acquire exited with `invalid_configuration_or_database`.
  - **Now, the selection:** the lookup key is the session table's primary key, maintained by each bounded COPY, so no index is built afterwards. Its ANALYZE runs under the ANALYZE limit.
  - **Now, the upgrade:** the served tables are built in ranges of 10,000 block heights (or 100,000 ids), each block's transactions and facts found by index. Every whole-table statement that cannot be split runs under a documented limit of its own (`BULK_TIMEOUT`, one hour): the membership projection and its foreign key, the per-family `first_generation` indexes, the ordinal constraints, the served-table indexes, and the schema-5 script. That replaces the earlier 120 s, 600 s and 30 s limits.
  - Facts, digests, receipts, served rows, schema and formats are unchanged; `serve.verify` still compares every served row with the stored facts after the build.
- Upgrading from 0.4.2 is an image swap.

## 0.4.2

- A SQL bootstrap from a Mainnet cardano-db-sync now completes.
  - **Before:** the independent selector chose the relevant outputs of the whole history again for every window. On the 2026-11 Mainnet source that is about 2.6 million outputs, mostly cNIGHT. Each choice took minutes, so the first window was cancelled at the source's 15 s statement limit and the bootstrap stopped with `source_timeout` at any window size.
  - **Now:** the selection is read once for the prerequisite windows and once for the independent verification pass. Each window takes its own rows from the destination. Selector work is proportional to the selection plus the windows.
  - Every source statement is short: id streams of 500 rows, then batches of at most 1,000 ids that are retried in halves if the source cancels one.
  - An epoch's stake distribution is now streamed and summed per pool, instead of one statement grouping about 1.3 million Mainnet rows.
  - Facts, digests, receipts, refusals and the independent verification are unchanged. Nothing is written on the source.
  - See `docs/sql-prerequisites.md` for what to expect: one long read-only snapshot per selection, one more selection after a resumed run, and `ANALYZE` on a restored source.
- Upgrading from 0.4.1 is an image swap: no schema, format or backup change.

## 0.4.1

- Full verification no longer takes quadratic time on stores of tens to
  hundreds of thousands of blocks. Transaction coverage, the served-table and
  served-header comparisons, and the prior-answer and bridge checks of
  selected assets used to ask for a first violation; PostgreSQL then planned
  them as loops that compare every pair of rows when nothing is wrong (seconds
  at 20,000 blocks, minutes at 100,000). They now count violations, which
  PostgreSQL plans with hash or merge joins. Results and refusals are unchanged.

## 0.4.0

- **BREAKING: schema 5, fact format v2 and backup format v4.** Upgrade a
  database of 0.2.x or 0.3.x in place, once, with the service and the node
  stopped: `db-sync-alt upgrade --service <name>`. The upgrade reads every
  stored epoch's parameters twice, from Blockfrost with the service's settings
  (2 requests per stored epoch) or from a cardano-db-sync given with
  `--source-service` and `--source-attestation`. 0.3.x and earlier refuse an
  upgraded database with `unsupported_database_identity`, and there is no
  downgrade: keep a `pg_dump` of the stopped database to go back. A database
  written by an unreleased build of `main` must be rebuilt with 0.4.0.
- Every epoch fact carries the epoch's maximum transaction size, served as
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
- Fix every update stopping at a payment to the registered-candidates address.
  Anyone can make one. Blockfrost acquisition and follow refused the block
  (`unsupported_nonempty_candidates`), verification refused a database holding
  such an output (`unsupported_candidate_activity`) and a cardano-db-sync
  bootstrap refused a source holding one (`candidates_unsupported`); none of
  them refuses now. The address is no longer one of the profile's addresses: an
  output there is kept only when it carries one of the profile's assets, as at
  any other address. midnight-node 2.1.101 selects committees without
  registrations, and a test fails if the packaged node sources query them
  again. The node's registration RPCs and subcommands
  (`sidechain_getRegistrations`, `sidechain_getAriadneParameters`,
  `systemParameters_getAriadneParameters`, `registration-status`,
  `ariadne-parameters`) return no registrations, and fail when an output there
  carries a profile asset and its transaction spends no output db-sync-alt
  holds. Serving registrations is future work (midnight-node#1761). Updates of
  the current format carry the capability `registered_candidates_unserved`;
  updates sealed in the nonce-only format keep `candidates_empty`.
- Fix Blockfrost acquisition and follow stopping at a relevant output (at a
  profile address such as ICS, reserve, cNIGHT mapping or governance, or at any
  address when it carries cNIGHT) whose datum is nested deeper than 64 levels,
  repeats a map key, holds an integer beyond 2^53 (which Blockfrost rounds), or
  has no preimage on chain. Datums are now stored as cardano-db-sync 13.8 stores
  them, from the datum's own CBOR with exact integers: the served `datum.value`
  is NULL when its JSON nests deeper than 2000 levels, and an output whose
  preimage is not on chain keeps its `data_hash` without a `datum` row until a
  transaction carrying the preimage is read. A cardano-db-sync source is
  accepted with such outputs too. Schema 5 makes `datum.value` nullable; see
  [limitations](docs/limitations.md) for the datums db-sync-alt cannot see.
- Fix Blockfrost following stopping for good once one update had spent its
  request budget (10,000 requests by default; `request_budget_exhausted`).
  Anyone can fill an update's blocks with cheap transactions: full mode reads
  one `/txs/{h}/utxos` answer per transaction, relevance filtering two per
  selected one. Each new update is now sized to the transaction counts of its
  blocks: as many blocks as fit three quarters of its budget, at least one.
  With relevance filtering it counts both reads and holds at most two blocks,
  so its 200,000-key limit can no longer be reached
  (`relevance_work_unit_limit`). An update that still spends its budget waits
  for the next UTC day (`quota_wait`) and then continues where it stopped
  under a new job record that carries the earlier count; the exhausted record
  is kept. Every request is still counted before it is sent, no count is
  reset, and the daily request ceiling is unchanged. A filtered update resumed
  in the same process no longer reads its committed blocks again. A one-shot
  `acquire --relevance` target more than two blocks ahead is refused before
  any request (`invalid_sync_configuration`).
- Fix relevance filtering stopping for good at one block whose relevance
  proof passes 2 MiB (`relevance_work_unit_limit`). Anyone can build one: an
  output at the ICS address keeps every asset, so 12 outputs of 1,304
  self-minted assets each in one block (about 275 ADA) give a 2.2 MB proof.
  The bound is now derived from the ledger: one valid block body (Conway's
  `maxBlockBodySize`, 90,112 bytes) yields at most 66 proof bytes per body byte,
  so a block's proof may reach 5.73 MiB. An update with filtering holds at
  most two blocks (was six), so it still never passes its 200,000-key limit.
  Full mode was not affected.
- Fix Blockfrost acquisition and following stopping at a transaction whose
  `/txs/{h}/utxos` answer passes 32 MiB (`provider_request_rejected` with
  db-sync-alt's client, `invalid_coverage` with the shared adapter). Anyone can
  cause it: the answer lists every input with all assets of the output it
  spends, and about 300 reference inputs to outputs of 1,300 assets each
  suffice. Such an answer is now read with each input row's assets cut to the
  profile's (all assets at the ICS and reserve addresses), which is all a
  stored fact depends on; outputs stay whole. db-sync-alt's client reads it
  one row at a time, up to 128 MiB; with the shared adapter the external
  client still reads it whole before db-sync-alt cuts it. Answers up to
  32 MiB are unchanged.
- Full verification no longer fails on a long history because one of its
  checks needs more than 30 seconds. This covers startup, the periodic
  integrity checkpoint, `verify`, `acquire`, `upgrade`, `backup` and `restore`.
  Each verification statement whose work grows with the history runs under
  its own limit of one hour; every other statement keeps 30 seconds. Results
  and refusals are unchanged. `restore`, which had no statement limit at
  all, now bounds these statements too.
- `acquire` from cardano-db-sync, `restore` and `upgrade` analyze the chain
  tables before they verify, so PostgreSQL plans the checks with the loaded
  rows. A failed analysis is logged (`advisory_analyze_failed`) and never
  fails the command.
- When PostgreSQL cancels a verification statement, the log line
  `verification_statement_canceled` gives the statement's plan and its
  tables' estimated row counts. The command then ends as before.

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
