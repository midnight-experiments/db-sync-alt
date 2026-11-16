# Provider relevance filtering

Relevance filtering can skip UTxO and transaction-detail requests for unrelated
transactions while keeping the same matching facts. It is **off by default on
every network**. Ambiguous transactions still use the full request path or refuse;
filtering must never turn missing information into evidence that nothing matches.

## Before enabling

Use a dataset with complete supported SQL-root coverage and certified provider
updates, or an explicitly trusted non-Mainnet history as described below. An
upgrade alone does not make arbitrary full-scan history eligible. An unsupported
history refuses with `relevance_parent_unsupported` before provider requests.

Filtering changes neither the schema, the fact format, the backup format nor
the packaged query and profile identities. Filtered history requires a reader that checks
relevance proofs. Keep that compatible package and profile with the database and
its backups, even after switching filtering off.

Before changing modes, stop the writer after a completed publication, check that
the database is unrevoked with no pending update, and take a verified backup.
Preserve the database, package/profile and request-accounting files. A pending
update must finish in its original mode; see [recovery below](#if-a-pending-update-cannot-finish).

## Enable and observe

For the configuration-file service, set a JSON boolean:

```json
{"sync": {"relevance_enabled": true}}
```

A missing option means `false`. Strings, numbers and null are rejected. A new
foreground `run` or Compose deployment accepts `--relevance`; `--no-relevance`
selects full scan. An existing native `run` state binds its original mode: use
[the completed-boundary procedure](#change-mode-at-a-completed-publication-boundary)
to change it instead of editing its launch receipts.

A one-shot Blockfrost update also accepts the option:

```sh
db-sync-alt acquire --source blockfrost --network preprod \
  --destination-service db_sync_alt --target-height HEIGHT --relevance
```

Use your actual private service name and a confirmed target at most two blocks
after the ready one (a further target is refused before any request with
`invalid_sync_configuration`); two blocks always fit one update.

The service sizes each update with filtering by its blocks' own content, before
it declares the update. It reads each candidate block's transactions (the pages
the update reads anyway) and bounds the proof keys the block can add by
⌊(66 × its transaction bytes + 65,536) / 63⌋, or 0 for an empty block. The update
holds the longest run of blocks, up to `max_batch_blocks`, whose bounds sum to at
most the update's 200,000-key limit, and always at least one block. The first
block that does not fit starts the next update, without being read again. One
block's relevance proof may be up to 5.73 MiB, the most a valid block body
(Conway's `maxBlockBodySize` of 90,112 bytes) can produce, so any valid block is
accepted and no update passes the key limit. If governance raises
`maxBlockBodySize`, this bound must follow before such blocks appear. Filtering
applies to Blockfrost following, not SQL bootstrap.

On real Mainnet blocks an update of a catch-up holds about 20 blocks. At the chain
tip an update holds the blocks confirmed since the previous one, so its fixed
requests (about 5) are paid for one or two blocks. For Mainnet with filtering,
`deploy/mainnet-relevance-sync.json` sets `follow_min_blocks` to 3 and
`follow_max_wait_seconds` to 60: updates of about three blocks, about 42,000 to
45,000 requests a day at the tip instead of about 49,000 to 52,000, for up to
about a minute more before a block is served. Both settings stay below Mainnet's
`serving_lag_max_blocks` of 9.

A catch-up spends most of its time waiting for Blockfrost's answers, one after
another. With a shared client that lets several requests of one key be in
flight, `provider_concurrency` 4 keeps up to four of an update's requests in
flight at once, and the published data stay the same
([requests in flight](operations.md#requests-in-flight)). Add a pace that keeps
a whole day under the key's limit with room for its other users, for example
`provider_max_requests_per_hour` 16,000 for a key of 500,000 requests a day
(384,000 update requests and about 8,700 poll requests). At such a pace more than 4
requests in flight gain nothing:

```json
{"sync": {"relevance_enabled": true, "follow_min_blocks": 3, "follow_max_wait_seconds": 60,
          "provider_concurrency": 4, "provider_max_requests_per_hour": 16000}}
```

To return to an earlier release, stop the writer while `/status` shows no update
in progress and `db-sync-alt status` shows no pending generation. An earlier
release declares an unfinished update job's whole target without sizing it.

Check fresh `/health` and `/status` responses. When filtering is active,
`relevance.requested` and `relevance.effective` are both `relevance`; the reason is
`verified` for certified history or `trusted-history` for explicitly trusted
history. With filtering off, the requested and effective modes are `full`.
Also check that health is ready, chain progress advances and request usage keeps
its existing history. A refusal reason such as `relevance_coverage_invalid` or
`relevance_mode_mismatch` requires diagnosis, not deleting rows or resetting usage.

The separate `db-sync-alt status --service NAME` command reports stored provenance
in `relevance.history`. This is not the current filtering mode or a fresh
verification result. It can still say `trusted-history` after a full-scan update;
use the HTTP mode fields to check what the running service is doing.

## Existing non-Mainnet full-scan history

Existing non-Mainnet full-scan history can be marked trusted through the hidden
`_trust-relevance-history --destination-service NAME --network NETWORK` command,
with its writer stopped and no pending update. Mainnet refuses this command.
Preserve the matching package and verified backup before this explicit trust
decision; start the service with filtering on immediately after running it.
A later full-scan publication may be explicitly trusted again at its new ready
generation; repeating the command on the same ready generation refuses.
The history and its certified descendants remain labeled `trusted-history`,
including after checkpoint and restart, rather than independently verified history.

## Change mode at a completed publication boundary

1. Stop the old writer. Run `db-sync-alt status --service NAME` with its original
   private connection files: require a ready, unrevoked database and no pending
   generation. If an update is pending, complete it in its original mode first.
2. Keep PostgreSQL running against that same database and data directory. The
   `service` command does not start PostgreSQL; if the old supervisor stopped it,
   use your established database lifecycle procedure to restart the original
   server. Do not initialize replacement data.
3. Keep the same provider adapter and accounting paths. Shared deployments must
   retain `DBSYNC_PROVIDER_ADAPTER=shared`, `DBSYNC_SHARED_ROOT` and
   `DBSYNC_SHARED_PROFILE`; portable deployments must reuse their original ledger.
   Do not clear counters, rotate credentials to evade limits or run two writers.
4. For a configuration-file service, change only `sync.relevance_enabled` while
   stopped. For a native `run` deployment, leave its launch state untouched and
   copy its service configuration into a new private configuration directory.
   The following example changes only the mode; choose your actual paths:

```sh
set -eu
umask 077
relevance_old_state="$HOME/.local/share/db-sync-alt/native"
relevance_new_dir="$HOME/.config/db-sync-alt/relevance-service"
mkdir -p -m 700 "$(dirname -- "$relevance_new_dir")"
mkdir -m 700 "$relevance_new_dir"  # Must be new.
export PGSERVICEFILE="$relevance_old_state/pg_service.conf"
export PGPASSFILE="$relevance_old_state/pgpass"
export DBSYNC_PROVIDER_KEY_FILE="$relevance_old_state/provider.key"
export DBSYNC_PROVIDER_STATE="$relevance_old_state/accounting"
python3 - "$relevance_old_state/service.json" "$relevance_new_dir/service.json" <<'PYCONFIG'
import json, os, sys
from pathlib import Path
value = json.loads(Path(sys.argv[1]).read_text())
value.setdefault('sync', {})['relevance_enabled'] = True  # False selects full scan.
fd = os.open(sys.argv[2], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as out:
    json.dump(value, out, indent=2)
    out.write('\n')
PYCONFIG
db-sync-alt service --config "$relevance_new_dir/service.json"
```

5. Check the fresh HTTP mode, health, progress and request usage. Reuse these same
   private paths on later starts. Starting the old native `run` again selects its
   original bound mode, so manage that deployment choice explicitly.

To switch off, use `false` or `--no-relevance` at the same completed boundary.
Stored filtered history still needs its proof-capable reader. A later full-scan
publication can make filtering unsupported until valid coverage is re-established;
non-Mainnet history may use the explicit trust procedure above.

## If a pending update cannot finish

Resume with the original mode, profile and accounting. If it continues to refuse,
for example because a selected output cannot be parsed or a work unit exceeds its
bound, stop and keep the failed database and logs. The supported recovery is to
restore the last verified backup into a **new, empty database** using the
[backup and recovery procedure](recovery.md#restore-into-a-new-empty-database).
A database with a pending generation cannot be backed up, so use a backup made
before that update. Retain its matching package/profile and accounting history.
Do not delete pending facts or coverage, edit manifests/launch receipts, reset
usage or use the trust command to bypass a pending refusal.

## Checkpoints and operating limits

Integrity checkpoints retain a relevance anchor after successful qualification,
so later relevance checks walk only the new updates. Constant-size anchor checks
still run, and ordinary integrity verification still checks stored facts.
A checkpoint taken while an update is pending defers the new relevance anchor.

Filtering still reads transaction CBOR and performs independent provider checks.
Polls, selected responses, retries and epochs still consume requests. Preserve
confirmation depth, polling, serving-lag/node-margin guards, quotas and cooldowns.
Synthetic tests and checkpoint anchors do not establish live request cost or
Mainnet throughput.
