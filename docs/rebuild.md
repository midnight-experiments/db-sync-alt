# Rebuild

`db-sync-alt rebuild` builds a database from the provider alone, without a
snapshot file and without another database, checks it and opens it for the
node. The service `run` then follows the chain from it.

A snapshot file stays the recommended first start
([snapshots](snapshots.md#restore)): a restore sends no request to the provider
and takes seconds, a build sends many requests and takes hours. Use `rebuild`
when:

- no snapshot file of the network is at hand;
- you want to check a published snapshot file against the chain itself: a
  build with the file's horizon and target builds the same blocks, and the two
  content digests can be compared ([check a published file](#check-a-published-file)).

## Build a database

Create the database and its logins as for a restore
([PostgreSQL](postgresql.md#the-database-and-its-logins)), then:

```sh
db-sync-alt rebuild --network mainnet \
  --database 'host=db.example dbname=db_sync_alt user=db_sync_alt_owner' \
  --password-file /etc/db-sync-alt/owner.password \
  --tls-root-cert /etc/db-sync-alt/root-ca.crt \
  --provider-key-file /etc/db-sync-alt/blockfrost-mainnet.key
```

The database must be blank or hold the empty layout that `create` makes; any
other database is refused unchanged (`destination_not_empty`). The build runs
in the foreground and prints its report on standard output when it has
published. Then start the service on the same database, without `--snapshot`:

```sh
db-sync-alt run --network mainnet \
  --database 'host=db.example dbname=db_sync_alt user=db_sync_alt_owner' \
  --password-file /etc/db-sync-alt/owner.password \
  --tls-root-cert /etc/db-sync-alt/root-ca.crt \
  --provider-key-file /etc/db-sync-alt/blockfrost-mainnet.key \
  --reader db_sync_alt_reader \
  --snapshot-dir /var/lib/db-sync-alt/snapshots
```

The flags are in [configuration](configuration.md#rebuild). The build works
for a horizon H and a target T:

- T is `--target-block`, or the provider's tip minus k when the build starts.
  A target above the provider's tip minus k is refused
  (`target_above_provider_final`).
- H is `--horizon-block`, or the newest block at or before T's time minus
  `--window-seconds` (default: the retention, `--retention-seconds`, 7 days).
  `--window-seconds` is from 86,400 to the retention and is not given with
  `--horizon-block` (`window_with_horizon`).
- With `--horizon-block`, the chain time from H to T must be at least 86,400
  seconds and may be longer than the retention: this is how the horizon and
  target of a snapshot file are given. A shorter window, and without
  `--horizon-block` a chain younger than the window, are refused
  (`invalid_window`); a horizon of 0 or above T is refused (`invalid_horizon`).
- The built database stores the retention and serves the blocks from H to T,
  as a restored file does. The plan, the report and the database's origin
  hold the window as the chain time from H to T.

## What it reads

A build first reads the provider's `/genesis`: a provider of another network
ends it with `provider_network_mismatch` before the database changes. Then it
works in phases, in this order. Each phase stores what it read in the database
being built before the next one starts, so a stopped build asks the provider
only for what it does not hold yet.

| Phase | What it reads |
| --- | --- |
| `listing` | The outputs the node needs that are unspent now and were created below H: for each asset unit of the profile (with every unit of the two governance policies, from `/assets/policy/{policy}`) `/assets/{unit}/utxos` by ranges of creation height, read in sequence; for each address of the profile `/addresses/{address}/utxos`, read again until two reads agree, or the union of eight reads. |
| `quantities` | For each asset unit of the listing and each fixed-supply asset, one unit at a time: every page of its mints and burns (`/assets/{unit}/history`), then its quantity (`/assets/{unit}`). A unit's reading is taken when no transaction occurs twice in the history, the history has as many rows as the quantity's answer counts and its amounts add up to the quantity; otherwise the unit is read again, five readings in all, and then the build ends with `provider_mint_history_mismatch`. A unit of the network's table of fixed-supply assets exists on the network: the provider must know it (`provider_unit_unknown`). |
| `since` | The provider's tip, then every transaction from H up to that tip's height that created an output holding a unit read in `quantities` or touched a profile address (`/assets/{unit}/transactions`, `/addresses/{address}/transactions`). When the tip's block has left the chain by the time these lists are read, the address listings and the lists are read again, at most three times (`provider_tip_left_chain`). A transaction that burns all it spends of a unit is in no list of the unit, only in its history: each burn that no list names is placed by its own `/txs/{hash}`. A mint creates an output holding its unit, so a mint from H on is in the unit's list: a mint that no list names is below H. |
| `transactions` | The inputs of those transactions (`/txs/{hash}/utxos`). |
| `addresses` | For each address of the profile, its full transaction list up to H − 1, and the outputs of every transaction in it with the transaction that spent each one (`consumed_by_tx` of `/txs/{hash}/utxos`). |
| `history` | The full transaction lists of the asset units up to H − 1: the block and position of each transaction that created an output unspent at H. |
| `validity` | Whether those transactions passed (`/txs/{hash}`), where their outputs do not already tell. |
| `datums` | A datum the node reads that is not inline in its output (`/scripts/datum/{hash}/cbor`). |
| `carried` | The governance and permissioned-candidates transactions below H that the keep rule keeps, from the lists read backward from H − 1. |
| `epochs` | The epoch values below the window: the newest block (looked for in the epoch and up to fifteen epochs before it) and the parameters of each of the two epochs before H's epoch, and the headers from H − 1 to H − 1 + k. Missing parameters end the build with `provider_epoch_parameters_missing`, a newest block found in none of those epochs with `provider_epoch_blocks_missing`. |
| `base` | No new read: the supply proof (below), then the rows below H, written in one transaction. |
| `window` | Every block from H to T, made final exactly as the service follows the chain, with all its checks; then the stake distributions of the epochs from two before H's through T's. |
| `publish` | The publication (below). |

The outputs unspent at H are the listed outputs and the outputs the
transactions since H spent, without the outputs those transactions created.
A row of `/txs/{hash}/utxos` flagged `collateral` or `reference` is an output
the transaction did not spend; such an output, when the node needs it, is
listed while it is unspent, or it is an input of the transaction that spends
it later. A transaction that the build needs and the provider answers with 404
is asked again as every refused answer is, and then ends the build
(`provider_transaction_unknown`).

What a build reads for the asset units grows with their mints and burns. On
2026-11-25 the Mainnet auth unit had 5,703 mints and burns on 58 history
pages, 723 of them burns, and every other unit of the listing on Mainnet had
one. On Preprod the auth unit had 102 (21 burns), cNIGHT 320 mints on 4 pages,
the bridge unit 81 mints and the others one. Each burn that no list from H on
names is one `/txs/{hash}` request.

## What it proves before the gate opens

The gate stays closed for the whole build. Before it opens:

1. **Every unit read.** For every asset unit with a reading (the
   fixed-supply assets and every unit of the listing), the unspent amount at
   H equals the provider's quantity minus the mints and plus the burns of the
   unit's history whose transactions are from H on, whatever their height.
   Otherwise the build ends with `rebuild_supply_mismatch`, which names the
   unit and the amounts. The supply written is that of the fixed-supply
   assets.
2. **Every output placed.** The transaction that created each output unspent
   at H is in a list below H, and a listed output created from H on is an
   output of a transaction of a list from H on. Otherwise:
   `rebuild_listing_incomplete`.
3. **Each profile address twice.** The outputs unspent at H at each address
   of the profile are derived a second time, from the address's full list
   below H: an output that a transaction of the list created there is unspent
   at H unless a transaction of the same list spent it. The two derivations
   must give the same outputs with the same content. Otherwise:
   `rebuild_address_outputs_mismatch`.
4. **Conservation.** Every block of the window passes the checks of following.
   A transaction that spends more of a profile asset than the database holds
   closes the build's gate with `asset_conservation`.
5. **Publication.** In one transaction: when the window holds no spend, the
   newest relevant spend below H is added (the keep rule keeps it); the keep
   rule is applied once; every block from H to T gets its first-served time;
   `verify` runs with all its checks ([verify](snapshots.md#verify)); the gate
   opens. A fault found by `verify` ends the build with its code and nothing is
   served.

### What rests on the provider

A build checks the provider's answers against each other: the outputs it
lists against the quantities and histories of the units, the lists from H on
against the listings, each profile address's listing against its own history,
and the window against the chain's blocks. An output that every one of these
answers leaves out in the same way, or a mint or burn that both a unit's
history and its quantity leave out, is not found by any check. The endpoint
that lists the unspent outputs of a unit (`/assets/{unit}/utxos`) is not in the
provider's public source, so it is not known how it shows the collateral
return of a transaction that failed phase 2: if it leaves such an output out,
the build ends with `rebuild_supply_mismatch` rather than with a wrong
database. The datums, the places of transactions and their validity are the
provider's answers for each transaction.

For an output that the node needs for its asset alone, the build proves the
asset's total at H, not how the total is shared among the outputs: each
output's address, lovelace, amount of the asset and datum hash are one answer
of the provider, a listing row or an input row, that no other answer is
compared with. The governance and permissioned-candidates transactions kept
below H, the epoch values below the window and the newest spend below H are
each read once. A build with the horizon and target of a published snapshot
file that gives the file's content digest is the one check of everything a
build reads ([check a published file](#check-a-published-file)).

## Progress

`status` prints the progress from the database alone, also while the build
runs on another connection:

```sh
db-sync-alt status --network mainnet --database '<connection string>' \
  --password-file owner.password --tls-root-cert root-ca.crt
```

While a build fills the database, or after it stopped:

- `serving` is `unready`, `position.serving` is `false` and `position.reason`
  is `rebuilding`; `identity.origin.kind` is `rebuilding`.
- `state` is `stopped` with `state_reason` `service_not_running`: `status`
  cannot tell whether a build runs. The build's own record is
  `service_state`.
- Before the window, `service_state.state` is `rebuilding` and
  `service_state.reason` the phase. Its `detail` holds `phase`,
  `provider_requests` (the requests of the whole build so far, earlier runs
  included), `horizon`, `target`, `remaining` with `transactions_to_read` and
  `window_blocks` (the blocks from the newest stored target to T), and
  `expected_remaining_requests`, the sum of the two: an estimate of one
  request per transaction and per block. A window with many empty blocks
  costs fewer requests, the stake distributions at its end more. The record
  is written at the start of each phase, and again once 30 seconds have
  passed since the last one while a phase stores what it read.
- In the window, `service_state` is the follower's own record, as for the
  service: its `state` and `reason` (for example `catching_up`), and
  `build_target`, T, in its `detail`. `served_target` and `stored_tip` grow
  towards T and T plus k, and `last_update` describes the last update.
- `provider.calls_today` and `provider.calls_previous_days` count the build's
  requests per UTC day. After a stop that the process sees the counts are
  exact; after the process is killed, the requests since the last record (30
  seconds before the window, the follower's own in the window) are not
  counted.
- After the build ended without publishing, `service_state.state` is
  `stopped` and its reason the code that ended it (`stop_requested` after
  SIGINT or SIGTERM); `state_reason` shows the same code.

No process answers `/status` for a database being built: `run` does not
serve it ([stop, continue, discard](#stop-continue-discard)).

The build logs one JSON line per event on standard error: `rebuild_started`
with the horizon, target and tip and whether it continues a build;
`rebuild_phase` at every record above, with the requests so far and the
`detail`; `rebuild_waiting` while a 402, 418 or 429 of the provider is waited
out; `rebuild_listing_moving` when an address's outputs kept changing over
eight reads, whose union is then taken; `rebuild_reading_again` when a unit's
history and quantity did not agree and the unit is read again;
`rebuild_tip_left_chain` when the lists from H on are read again;
`rebuild_listings_discarded` when a refusal makes the next run read the
listings again, with `address_histories` true when the profile addresses'
full lists below H go too; in the window the `update` and
`follower_state` lines of following; `rebuild_published` with the content
digest and the requests. The report on standard output holds the `plan`
(network, horizon, target, tip, the window as the chain time from H to T, the
retention, the `version` that began the build and its `flags`), `resumed`,
the `verified` report of `verify`, `calls` (the requests of the whole build),
`requests` (the same by endpoint family; a request sent again after a failed
attempt counts again), `counts` (what the base holds) and `seconds`.

## Stop, continue, discard

SIGINT or SIGTERM stops the build. It exits with status 1 and the code
`stop_requested`; a build that has not stopped within its grace of 5 seconds
has its statements cancelled ([exit status](operations.md#exit-status)). What
it has stored stays.

The same command continues a stopped build: with the same `--horizon-block`,
`--target-block`, `--window-seconds` and `--retention-seconds`, given or left
out as at the start (`rebuild_plan_mismatch`), and by the same version of
db-sync-alt (`rebuild_version_mismatch`).

A build also ends when the provider's transport fails, the provider answers
5xx or the client refuses an answer for 30 minutes in a row (exit status 1
with the provider's code), and when the database cannot be reached (exit
status 75); the same command continues it. A build that ends with a code of
its own is listed in [troubleshooting](troubleshooting.md#rebuild) with what
to do. A 402, 418 or 429 is waited out until the time the provider names; it
never ends the build.

A database that a build fills never serves until the build has published:

- the node's queries get SQLSTATE 55000 (`db_sync_alt_not_serving`);
- `run` refuses it with exit status 1 (`database_not_serving`, with the reason
  `rebuilding`), also with `--snapshot`, while no build runs on it. Beside a
  running build, `run` waits for the writer lock that the build holds and
  ends with exit status 1 (`writer_busy`) after 120 seconds; when it gets the
  lock in a moment in which the build holds no connection, it refuses the
  database as above and writes nothing;
- `restore` refuses it unchanged (`destination_not_empty`).

To discard a build, drop the database and create it again
([PostgreSQL](postgresql.md#the-database-and-its-logins)).

After `rebuild_supply_mismatch`, `rebuild_listing_incomplete`,
`rebuild_address_outputs_mismatch`, `provider_tip_left_chain` or
`provider_transaction_unknown` before the window, the build discards its
listings, the readings of the units and the lists from H on: the same command
reads them again, and keeps the places, the validity answers and the other
results below H. After `rebuild_listing_incomplete` and
`rebuild_address_outputs_mismatch` it also discards each profile address's
full list below H and the outputs of its transactions, from which the
address's outputs are derived the second time: the answer that missed an
output may be on either side, so the same command reads both again. After
`provider_mint_history_mismatch`, the same command reads the units that have
no reading yet. A check that closes the build's gate in
the window (`asset_conservation` or another) ends the build for that database:
the same command refuses it (`rebuild_refused`); drop the database and build
again.

## Check a published file

Take the horizon and target of a snapshot file from its description, as a
restore of the file reports them (`state.horizon.height` and
`state.target.height`), and build with them into a new database:

```sh
db-sync-alt rebuild --network preprod --database '<connection string>' \
  --password-file owner.password --tls-root-cert root-ca.crt \
  --provider-key-file blockfrost-preprod.key \
  --horizon-block <horizon> --target-block <target>
```

Compare `verified.content_digest` of the build's report with the file's
content digest. When they differ, `verify` on the built database and on a
restore of the file prints the digest and row count of each table
(`families`), which shows where they differ; [`compare`](comparison.md) runs
the node's queries against both. The [limits](#limits) below say when a build
cannot give the file's digest.

## Cost

Measured on Preprod, at 2 requests a second:

- A build with the horizon and target of the Preprod snapshot file of release
  0.5.1 (5,414,627 and 5,449,279: 34,653 blocks, about 8.9 days) gave the
  file's content digest
  `5a5a4636b1e18c2af73b0bd7c71e1398f38a493f055fca83e631225388572b94`. It sent
  26,653 requests in 3 h 46 min: 664 before the window, 108 of them for the
  full lists of the five profile addresses below H and the outputs of their
  103 transactions. `verify` passed, and [`compare`](comparison.md) against a
  restore of the file found no difference.
- A build with the default positions, a 7-day window of 27,296 blocks, sent
  20,331 requests in 2 h 53 min. `verify` passed. `run` followed from the
  built database to the chain's tip, and `compare` against a database that
  had followed the chain found no difference.

Each build was stopped once with SIGTERM and continued with the same command;
its time is that of its two runs together.

Mainnet: a build has not been run. The expected request count for a 7-day
window is an estimate, from Mainnet's counts of outputs and transactions and
the request pattern measured on Preprod: about 270,000 requests, and the
histories of the profile addresses on top (about 5,900 requests for the
mapping address). No duration is given: on Mainnet the unit listing answered
in about 6.5 seconds a request, whatever the range of heights asked, and a
build reads it in sequence (about 2,500 requests for cNIGHT). That is about
two orders of magnitude more requests than restoring a recent snapshot file
and catching up from it: a restore sends no request, and catching up sends
about one request per block ([provider](provider.md#catching-up)).

Check the daily request allowance of your provider plan before a Mainnet
build. When the provider answers 402, the build waits until the next UTC day
and goes on, as the service does
([provider](provider.md#refusals-and-outages)). The pace is
`--requests-per-second`; lower it when the service of another database shares
the key.

## Limits

- **The same horizon and target.** A build's content digest can equal a
  snapshot file's only when the build has the file's horizon and target. It
  also needs the provider to still hold the file's k headers above the target:
  the newest of them may have been the provider's tip when the file was
  written, and a short fork can replace them later. And the database that
  wrote the file must have stored the stake distribution of the target's
  epoch: a build stores the stake of every epoch up to the target's before it
  publishes.
- **Datums revealed after the target.** A datum given only by its hash and
  revealed by a spend after the target: a build holds it, a database that
  followed to the same target does not yet. None was found at the profile
  addresses of Mainnet and Preprod, where the datums read are inline in their
  outputs.
- **What every answer leaves out or states alike.** An output that every
  listing, list and history leaves out alike is not found, and what every
  answer states alike is taken as stated: an output's address, lovelace,
  amount and datum hash where one answer alone gives them, and the values
  read once below H ([what rests on the provider](#what-rests-on-the-provider)).
- **Networks.** Mainnet and Preprod only, as for every command
  ([limitations](limitations.md#networks-and-node-release)).
