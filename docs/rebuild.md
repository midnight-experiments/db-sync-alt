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
- The built database stores the retention and serves the blocks from H to T,
  as a restored file does.

## What it reads

A build first reads the provider's `/genesis`: a provider of another network
ends it with `provider_network_mismatch` before the database changes. Then it
works in phases, in this order. Each phase stores what it read in the database
being built before the next one starts, so a stopped build asks the provider
only for what it does not hold yet.

| Phase | What it reads |
| --- | --- |
| `listing` | The outputs the node needs that are unspent now and were created below H: for each asset unit of the profile (with every unit of the two governance policies, from `/assets/policy/{policy}`) `/assets/{unit}/utxos` by ranges of creation height; for each address of the profile `/addresses/{address}/utxos`, read again until two reads agree. |
| `quantities` | The provider's quantity of each fixed-supply asset (`/assets/{unit}`), with the provider's tip before and after it. |
| `since` | Every transaction from H on that moved a profile asset or touched a profile address (`/assets/{unit}/transactions`, `/addresses/{address}/transactions`). |
| `transactions` | The inputs and outputs of those transactions (`/txs/{hash}/utxos`). |
| `origins` | The outputs that collateral inputs spend, from their own transactions. |
| `history` | The full transaction lists of the assets and addresses up to H − 1: the block and position of each transaction that created an output unspent at H. |
| `validity` | Whether those transactions passed (`/txs/{hash}`), where their outputs do not already tell. |
| `datums` | A datum the node reads that is not inline in its output (`/scripts/datum/{hash}/cbor`). |
| `carried` | The governance and permissioned-candidates transactions below H that the keep rule keeps, from the lists read backward from H − 1. |
| `epochs` | The epoch values below the window: the last block and the parameters of each of the two epochs before H's epoch, and the headers from H − 1 to H − 1 + k. |
| `base` | No new read: the supply proof (below), then the rows below H, written in one transaction. |
| `window` | Every block from H to T, made final exactly as the service follows the chain, with all its checks; then the stake distributions of the epochs from two before H's through T's. |
| `publish` | The publication (below). |

The outputs unspent at H are the listed outputs and the outputs the
transactions since H spent, without the outputs those transactions created.

## What it proves before the gate opens

The gate stays closed for the whole build. Before it opens:

1. **Supply.** For each fixed-supply asset of the profile, the unspent amount
   at H equals the provider's quantity minus the net mint of the transactions
   from H to the provider's tip at which the quantity was read. Otherwise the
   build ends with `rebuild_supply_mismatch`, which names the unit and the
   amounts.
2. **Conservation.** Every block of the window passes the checks of following.
   A transaction that spends more of a profile asset than the database holds
   closes the build's gate with `asset_conservation`.
3. **Publication.** In one transaction: when the window holds no spend, the
   newest relevant spend below H is added (the keep rule keeps it); the keep
   rule is applied once; every block from H to T gets its first-served time;
   `verify` runs with all its checks ([verify](snapshots.md#verify)); the gate
   opens. A fault found by `verify` ends the build with its code and nothing is
   served.

The supply check covers the quantities of the fixed-supply assets. The outputs
at the profile addresses that hold none of them rest on the provider's
listings.

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
  service: `state` `catching_up` or `waiting` with its reason, and
  `build_target`, T, in its `detail`. `served_target` and `stored_tip` grow
  towards T and T plus k, and `last_update` describes the last update.
- `provider.calls_today` and `provider.calls_previous_days` count the build's
  requests per UTC day.
- After the build ended without publishing, `service_state.state` is
  `stopped` and its reason the code that ended it (`stop_requested` after
  SIGINT or SIGTERM); `state_reason` shows the same code.

No process answers `/status` for a database being built: `run` refuses it.

The build logs one JSON line per event on standard error: `rebuild_started`
with the horizon, target and tip and whether it continues a build;
`rebuild_phase` at every record above, with the requests so far and the
`detail`; `rebuild_waiting` while a 402, 418 or 429 of the provider is waited
out; `rebuild_listing_moving` when an address's outputs kept changing over
eight reads, whose union is then taken; in the window the `update` and
`follower_state` lines of following; `rebuild_published` with the content
digest and the requests. The report on standard output holds the `plan`
(network, horizon, target, tip, window and retention), `resumed`, the
`verified` report of `verify`, `calls` (the requests of the whole build) and
`seconds`.

## Stop, continue, discard

SIGINT or SIGTERM stops the build. It exits with status 1 and the code
`stop_requested`; a build that has not stopped within its grace of 5 seconds
has its statements cancelled ([exit status](operations.md#exit-status)). What
it has stored stays.

The same command continues a stopped build.

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
  `rebuilding`), also with `--snapshot`;
- `restore` refuses it unchanged (`destination_not_empty`).

To discard a build, drop the database and create it again
([PostgreSQL](postgresql.md#the-database-and-its-logins)). After
`rebuild_supply_mismatch`, or a check that closed the build's gate in the
window (`asset_conservation`), the database must be dropped and the build
started again: the same command computes the same refusal from the answers it
stored, or refuses the closed build (`rebuild_refused`).

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
the node's queries against both.
