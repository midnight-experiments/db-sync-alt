# Operations

`db-sync-alt run` is the service: one process that follows Cardano through the
provider into the database, keeps the database within its
[retention](glossary.md#chain-and-window), and answers `/health` and `/status`
over HTTP. This page explains what it does and how to watch it. Its flags are
in [configuration](configuration.md#run).

## Start

At start the service:

1. Checks every flag, before anything connects. An invalid value ends it with
   exit status 2.
2. Opens the HTTP listener (`--listen`, default port 18080). From here on
   `/status` answers in every state.
3. Connects to PostgreSQL. While the database cannot be reached the state is
   `waiting` with reason `database_unreachable`; it retries after 1, 2, 4, 8 and
   16 seconds and then every 30 seconds, and ends with exit status 75 once the
   database has stayed unreachable for `--database-fatal-after-seconds`.
4. Takes the [writer lock](glossary.md#database). While another session holds
   it, the state is `waiting` with reason `writer_busy`, retried with backoff;
   after 120 seconds the service ends with exit status 1 (`writer_busy`). The
   same holds when the writer reconnects later. Every session sets the
   server-side TCP keepalive, so the session of a process that is gone ends
   within about a minute and frees the lock
   ([troubleshooting](troubleshooting.md#connecting-to-postgresql)).
5. On an empty database, restores the `--snapshot` file (state `restoring`).
   Without `--snapshot` an empty database is refused (`snapshot_required`).
   What an interrupted restore left (its build, not yet published) is
   restored again from `--snapshot`. A database whose gate is closed is
   refused (`database_not_serving`): see [a closed gate](#a-closed-gate).
6. Compares `--retention-seconds`, when given, with the stored retention
   ([change the retention](#change-the-retention)).
7. Grants the `--reader` login the node's rights and checks that it has no
   others ([PostgreSQL](postgresql.md#the-reader-login)).
8. Follows the chain, reads the database part of the status every 20 seconds on
   a read-only connection of its own, and runs the [upkeep](#upkeep).

A restart does the same. It reads no history: it continues from the stored
target, and the node keeps reading the last update meanwhile.

## States

`/status` names the state in `state` and its reason in `state_reason`. A
waiting state also gives the time the wait ends in `wait_until`.

| `state` | Meaning | `state_reason` |
| --- | --- | --- |
| `restoring` | A snapshot file is loaded into the empty database. The node gets SQLSTATE 55000 meanwhile. | `restoring_snapshot` |
| `catching_up` | More final blocks are available: updates follow each other without a pause. | `starting`, `catching_up` |
| `following` | The database holds every final block the provider has. One poll every `--poll-interval-seconds`. | `following` |
| `waiting` | A condition that ends by itself, or a retry. | see below |
| `stopped` | The service ended, or is not running. | `stop_requested`, the code that ended it, or `service_not_running` |

Reasons of `waiting`:

| `state_reason` | Meaning |
| --- | --- |
| `database_connecting` | The service is connecting to PostgreSQL. |
| `database_unreachable` | PostgreSQL cannot be reached; retried with backoff. |
| `writer_busy` | Another session holds the writer lock; retried with backoff for up to 120 seconds, then the service ends with exit status 1. |
| `quota_exhausted` | The provider answered 402: the key's daily allowance is spent. Waited out until 00:02 UTC of the next day ([provider](provider.md#refusals-and-outages)). |
| `provider_rate_limited` | The provider answered 429. Waited out for the time the provider names. |
| `provider_banned` | The provider answered 418. Waited out for the time the provider names, 1 to 24 hours. |
| `epoch_parameters_pending` | The provider does not have the parameters of the next epoch yet. The update ends before that epoch's first block; asked again after 60 seconds. |
| `target_contradicted` | The provider returned another block at the stored target's height. Read again after 60 seconds; a second contradiction closes the gate (`canonical_conflict`). |
| `provider_chain_shorter` | The provider's chain is shorter than the stored tip, as after a provider-side rollback. Waited out. |
| any other code | A failed step that is retried: a transport failure or 5xx of the provider (`provider_unavailable`, `provider_timeout`), an answer the provider client or the follower refused (for example `provider_invalid_response`, `transaction_count` or `cbor_body_hash_mismatch`), or a database condition. `wait_until` is the next attempt. |

A failed step writes nothing and is retried after 1, 2, 4, 8 and 16 seconds and
then every 30 seconds. A database condition, a transport failure or a provider
5xx that lasts longer than `--database-fatal-after-seconds` ends the service
with exit status 75. An answer that the provider client or the follower refuses
never ends the service: it is retried every 30 seconds, named in
`state_reason`, `error_code` and `last_error`, and `/health` turns 503 once the
database falls behind. The client refuses an answer that is not valid for its
endpoint, larger than the endpoint's limit, a 404 where the endpoint must
answer, or another 4xx or 3xx status
([troubleshooting](troubleshooting.md#answers-the-follower-refuses)).

The database conditions that are retried: a lost connection, a server that is
starting or stopping, too many connections and the other errors of SQLSTATE
class 53 (for example a full disk), a lock timeout (55P03), a statement timeout
(57014), a serialization failure (40001) and a deadlock (40P01). Any other
database error ends the service at once with exit status 1, and its report
names the server's message, table and constraint.

## /health

```sh
curl -i http://127.0.0.1:18080/health
```

`/health` answers 200 when all of these hold, and 503 otherwise:

- the gate is open;
- the state is `following`;
- the served target is at most `--health-max-lag-blocks` (default 9) behind the
  provider's tip minus k;
- the provider's tip was read less than 120 seconds ago;
- the database part of the status was read less than 120 seconds ago;
- the reader login has exactly the node's rights.

The body is the `/status` document. Its `reason_codes` lists every condition
that fails, each with its own code; the list is empty when the answer is 200.

| Reason code | Condition that fails |
| --- | --- |
| `serving_unready` | The database has not served yet: empty, or restoring. |
| `serving_revoked` | A check closed the gate ([a closed gate](#a-closed-gate)). |
| `serving_unknown` | The database has not been read yet. |
| `state_restoring` | The state is `restoring`. |
| `state_catching_up` | The state is `catching_up`. |
| `state_waiting` | The state is `waiting`. |
| `state_stopped` | The state is `stopped`. |
| `serving_block_lag_exceeded` | The served target is further behind than `--health-max-lag-blocks`. |
| `serving_block_lag_unknown` | The provider's tip or the stored tip is not known yet. |
| `provider_observation_stale` | The provider's tip was read too long ago. |
| `provider_observation_unknown` | The provider's tip has not been read since the start. |
| `database_observation_stale` | The database part of the status was read too long ago. |
| `reader_check_failed` | The reader login has other rights than the node's, or lacks some ([PostgreSQL](postgresql.md#the-reader-login)). |
| `reader_check_unknown` | The reader login has not been checked yet. |

`db-sync-alt health` asks `/health` and exits 0 on 200 and 1 otherwise, within
3 seconds. The image's health check uses it.

## /status

```sh
curl -s http://127.0.0.1:18080/status
```

`/status` answers 200 in every state with one JSON document. It never calls the
provider and never runs SQL: it answers from the last document the service
composed. Times are Unix seconds unless stated. A position (`provider_head`,
`stored_tip`, `served_target`, `horizon`) is an object with `height`, `hash`
(lowercase hexadecimal) and `timestamp`. A field that is not known yet is
`null`.

| Field | Meaning |
| --- | --- |
| `service_version` | The version of db-sync-alt. |
| `network` | `mainnet` or `preprod`. |
| `node_release` | The Midnight node release the keep rule is bound to: `2.1.101`. |
| `database_format` | The database format this version reads: `1`. |
| `keep_rule_version` | The keep rule's version: `2`. |
| `generated_at` | When the document was composed. |
| `healthy` | Whether `/health` answers 200. |
| `reason_codes` | Why `/health` answers 503 ([/health](#health)). |
| `state` | `restoring`, `catching_up`, `following`, `waiting` or `stopped` ([states](#states)). |
| `state_reason` | The reason of the state. |
| `wait_until` | When the current wait or retry ends. |
| `serving` | The gate: `ready` (open), `unready` (not served yet), `revoked` (closed by a check) or `unknown` (not read yet). |
| `sync` | The follower's activity: `idle` (at the tip), `syncing` (catching up or restoring), `backoff` (retrying), `quota_wait` (waiting for the next UTC day), `halted` (stopped) or `unknown` (no follower runs). |
| `error_code` | The condition that keeps the service from following: the reason of a `waiting` state, or of a `stopped` state other than a requested stop. |
| `poll_error_code` | The provider's refusal (`quota_exhausted`, `provider_rate_limited`, `provider_banned`) while its wait lasts. |
| `provider_head` | The provider's tip. |
| `provider_observed_at` | When the provider's tip was read. |
| `freshness` | `provider_observation_age_seconds`: seconds since the provider's tip was read. |
| `stored_tip` | The newest stored header: the target plus k. |
| `served_target` | The newest block whose events are served. |
| `horizon` | The oldest block of the window. |
| `lag` | How far the database is behind: the provider's tip minus the stored tip, in `blocks` and `seconds`. It equals how far the served target is behind the provider's tip minus k. |
| `tip_lag` | The provider's tip minus the served target, in `blocks` and `seconds`; at the tip it is about k. |
| `progress` | `blocks_remaining`, `blocks_per_hour` over the last 10 minutes, and `estimated_tip_at`, the expected time of reaching the tip. |
| `last_update` | The last update: `at`, `duration_seconds`, `blocks` made final, `headers` stored, `events_written`, `rows_pruned`, and the `target` and `horizon` after it. |
| `last_error` | The last error: `code`, `at` and `detail`, the full report. |
| `retention` | `seconds`; `horizon_first_served_at`, when the horizon block was first served; `next_horizon_move_at`, the earliest time the horizon moves. |
| `provider` | `utc_day`; `calls_today`; `calls_previous_days`, the seven UTC days before today with their `day` and `requests`; `pace_requests_per_second`; `requests_in_flight`; `last_refusal`, the provider's last 402, 418 or 429 with its `http_status`, `code`, `at` and `until`. |
| `epochs` | `current_epoch`, the epoch of the stored tip; `newest_parameters_epoch`; `newest_stake_epoch`. |
| `integrity` | `unspent_outputs`, as counted by the last supply check; `supply_check`, the last [supply check](#supply-check) with `at`, `passed`, `failure`, `supply` and `unspent_outputs`; `snapshot`, the last [daily snapshot file](#daily-snapshot-files) with `at`, `file`, `content_digest`, `target` and `bytes`; `unspent_at_profile_addresses`, the stored unspent outputs per profile address. |
| `database` | `observed_at` and `observation_age_seconds` of the last read; `size_bytes`; `tls_available`, whether the server offers TLS; `reader_check`, with `role`, `passed`, `error`, `violations` and `warnings`; `index_rebuilds`, each index's size after its last rebuild. |
| `identity` | The stored identity: `database_format`, `network`, `profile_hash`, `keep_rule_version`, `tool_version`, `node_release`, `origin` (where the data came from) and `created_at`. |
| `position` | The stored position row: `serving`, `reason`, target, tip and horizon heights, hashes and times, `retention_seconds` and `updated_at`. |
| `service_state` | The stored state row that the service writes on every state change and with every update: `state`, `reason`, `detail`, `last_update`, `last_error` and their times. |
| `first_observations` | For each rare shape (`stored_pointer_address`, `stored_large_datum`, `stored_bridge_metadata`), the first one the follower stored on this database, with its block, transaction and the provider answers that held it; `null` until one is stored. |
| `capabilities` | What the database serves to node 2.1.101's operator commands: registered-candidate results are `unsupported` ([limitations](limitations.md)). |
| `limitations` | The [limitations](limitations.md), each with a `code` and a `description`. |
| `unsupported_node_statements` | The node's SQL statements a pruned database does not answer completely: those of the node's genesis subcommands. |
| `warnings` | Conditions that do not fail `/health`: `database_tls_unavailable`; the reader warnings `reader_statement_timeout_unset`, `reader_idle_in_transaction_timeout_unset`, `reader_search_path_not_public`, `reader_connection_limit_unset` and `reader_connection_limit_below_node_pool` ([PostgreSQL](postgresql.md#the-reader-login)); `status_read_failed:<code>` while the database part cannot be read; and `upkeep_failed:<job>:<code>` until an upkeep job succeeds again. |
| `health` | The limits of `/health`: `max_lag_blocks`, `provider_observation_max_age_seconds` and `database_observation_max_age_seconds`. |

`/status` and `/health` answer from memory: in a test, 1,000 requests in a row
took 0.23 ms each on average and 1.03 ms at most, and the process's memory did
not grow (2026-11-23, test build, one process in a 4-CPU container).

## Status without the service

```sh
db-sync-alt status --network mainnet --database '<connection string>' \
  --password-file owner.password --reader db_sync_alt_reader
```

`status` prints the same document from the database alone, for example while
the service is stopped. The fields only a running service knows (the
provider's tip, the pace, the progress) are `null`, `healthy` is `false`, and
`state` is `stopped`: with the stored reason when the service recorded a stop,
otherwise `service_not_running`, because the command cannot tell whether a
service runs. The stored row is printed as it is under `service_state`, with
its time.

## Logs

Every line on standard error is one JSON object with an `event` field. Standard
output carries only a command's result.

- Every command logs `started` at its start and `finished` at its end, with
  `outcome` (`success`, `difference`, `failed`, `temporary_failure`,
  `interrupted` or `deadline_exceeded`), the exit status and
  `duration_seconds`. A failure is reported on its own line before the end.
- `run` logs `listening` with its address, `reader_checked` with the reader's
  warnings, and `<operation>_started` and `<operation>_finished` around each
  long operation: `restore`, `supply_check`, `index_rebuild`, `index_leftovers`
  and `snapshot_file`, each end with its `outcome` and `seconds`.
- While catching up, one `update` line at most every 10 seconds; each line
  carries the update's report.
- Conditions: `database_unreachable` and `database_reachable`, `writer_busy`
  and `writer_lock_taken`,
  `follower_retry` with the error and the time of the next attempt,
  `follower_state` on a change of state, `gate_closed` with the check's report,
  `retention_changed`, `status_read_failed` and `status_read_recovered`,
  `stake_stored` and `stake_read_failed`, and `service_stopped` or
  `follower_stopped` with the error that ended the run.

No line holds a password, a provider key, a connection string or a provider
URL.

## Exit status

Every command ends with one of these statuses. `db-sync-alt --help` prints the
same list.

| Status | Meaning |
| --- | --- |
| 0 | success |
| 1 | the operation failed |
| 2 | usage error: an invalid command line or flag value, found before any connection |
| 3 | a check found a difference: compare did not match, or verify or grant-reader --check refused the database |
| 75 | temporary failure: the database could not be reached or is restarting; the same command may succeed later |

On SIGINT, SIGTERM or `--overall-timeout-seconds` a command cancels the
statements of its own database sessions, removes its temporary files and
exits with status 1. A command that changes the database reports
`"published": false` when it stopped before the commit that publishes, and
`"outcome": "unknown"` when that commit had been sent: run `status` and
`verify` to see which state it reached.

`run` is different: it ends with status 0 on SIGINT or SIGTERM, also when the
stop is forced after its grace ([stop and restart](#stop-and-restart)).

### Exit status of `run`

| Status | Meaning |
| --- | --- |
| 0 | stopped by SIGINT or SIGTERM |
| 1 | a condition that retrying cannot cure: a rejected provider key, a database of another network or format, a second writer, a retention that differs from the stored one, a reader with more rights than the node needs, an empty database without --snapshot, a closed gate found at start, or a check that closed it |
| 2 | an invalid flag value, found before any connection |
| 75 | the database stayed unreachable, or the provider's transport failed, for longer than --database-fatal-after-seconds |

A provider answer of 5xx counts as a transport failure. A refusal that names
its own end (402, 418, 429) never ends the service.

## Stop and restart

SIGINT or SIGTERM stops the service, usually within a second: an update in
progress is either committed or absent, never partly written. A stop that
arrives during a long statement (a large update, the supply check, an update
that waits for a lock another session holds, a connection attempt to a server
that does not answer) can take the whole grace of 10 seconds. After the grace
the service cancels its statements and ends with exit status 0 and the report
`{"stopped":"stop_forced_after_grace","grace_seconds":10,"statements_cancelled":true,"partial_update":false}`:
every update is one transaction, so a cancelled one leaves nothing behind.

`docker stop` sends SIGTERM. Give the service more than 10 seconds before a
supervisor kills it: the Compose file sets `stop_grace_period: 15s`. The gate
lives in the database, so the node keeps reading the last update while the
service is stopped, and gets the same answers as before.

## Upkeep

The service keeps the database in shape by itself. No upkeep job holds the
writer's transaction, and a failing job is logged, named in the `warnings` of
`/status` and tried again at its next slot.

### Supply check

At start and every 6 hours, between two updates, the service compares the
unspent amount of each fixed-supply asset (cNIGHT and the other fixed units of
the profile) with the supply recorded in the database. The result is in
`integrity.supply_check`. A difference closes the gate with `supply_mismatch`:
see [a closed gate](#a-closed-gate). `restore`, `snapshot` and `verify` run the
same comparison.

### Index upkeep

Deletes leave free space in the tables, which new rows reuse. Indexes on the
positional IDs only grow at one end and keep sparse pages, so every hour the
service looks at the index sizes and rebuilds the one index that is larger than
twice its size after its last rebuild and larger than 8 MiB, with
`REINDEX INDEX CONCURRENTLY` on a connection of its own. The node keeps
reading meanwhile, and every index name the node checks exists at every
moment. A rebuild interrupted by a stop leaves an invalid copy, which the next
start removes. `database.index_rebuilds` shows each index's size after its last
rebuild.

In a test over thirty days of a generated chain (1,440 blocks a day, an update
and a horizon advance every hour, a retention of one day, release build,
2026-11-23), the database stayed at 1.234 times the size of a fresh restore of
the same content, with no index rebuilt.

### Daily snapshot files

With `--snapshot-dir`, the service writes a snapshot file every day at
`--snapshot-at` (UTC). The file is named
`<network>-<target height>-<first 12 digits of the content digest>.snapshot`,
written under a temporary name and then published. The newest
`--snapshot-keep` files of the network are kept; older ones and leftovers of an
interrupted write are removed. Every file of that name pattern for the network
in `--snapshot-dir` belongs to the rotation, also one the service did not
write, such as a release file put there for the first start: it is removed
once `--snapshot-keep` newer files exist. Keep a copy elsewhere of any file you
want to keep. A failed write is tried again after 10 minutes without touching
the older files. The last file is in `integrity.snapshot`.

The `--snapshot` and `--expect-content-digest` of the first start may stay
set: the file is read only when a restore is needed, so a start on a serving
database does not need it, also after the rotation removed it.

These files are the backups of a pruned database: a database cannot be rebuilt
from the provider's history at a practical cost. Copy them off the host.
Restoring one is described in [snapshots](snapshots.md#restore).

## Change the retention

The retention is stored in the database. `run` refuses a
`--retention-seconds` that differs from it (`retention_mismatch`, exit status
1), so a typo cannot delete a week of events. To change it, start once with
both flags:

```sh
db-sync-alt run … --retention-seconds 172800 --change-retention
```

The new value is stored and logged (`retention_changed`). A smaller value
moves the horizon at the next horizon advance, at most
`--prune-interval-seconds` later. A larger value keeps more from then on: the
horizon stays where it is until its block is older than the new retention.
Events already deleted do not come back. The smallest value is 86,400 seconds
(24 hours).

## A closed gate

Three checks close the gate when they find a fault. The node then gets
SQLSTATE 55000 for every query instead of an answer, `serving` is `revoked`,
and `state_reason` and `last_error` name the check:

| Check | What it found |
| --- | --- |
| `canonical_conflict` | The provider contradicted the stored target twice, 60 seconds apart: a final block the database serves is not on the provider's chain. |
| `asset_conservation` | In a transaction of a new block, the inputs the database holds plus the mint do not equal the outputs for a profile asset: the database misses an output. The report names the transaction and the unit. |
| `supply_mismatch` | The unspent amount of a fixed-supply asset differs from the recorded supply. The report names the unit and both amounts. |

The service stops, and refuses to start on that database
(`database_not_serving`). Nothing in it is repaired in place. Recover by
restoring a snapshot file into an empty database:

1. Keep the closed database for diagnosis, or drop it.
2. Create an empty database for the same owner
   ([PostgreSQL](postgresql.md#the-database-and-its-logins)).
3. Start `run` on it with `--snapshot` set to the newest daily file or a
   published file, and `--expect-content-digest` set to its digest.
4. Point the node at the new database if its name changed
   ([connect a node](connect-a-node.md)).

The restored database catches up from the file's target. Report the fault with
the `last_error` of the closed database.
