# Provider relevance filtering

Relevance filtering is an optional, coverage-gated optimization for incremental
following. It requires complete certified SQL-root coverage; enabling it does not
establish live request, latency or node acceptance.

Filtering defaults **off on every network**, including Mainnet, Preprod and
Preview. Enable it only for a compatible certified dataset; a network name does
not grant a coverage capability. The example
[mainnet-relevance-sync.json](../deploy/mainnet-relevance-sync.json) selects
three confirmed blocks per publication with a 60-second maximum wait. These
batching settings require measured live processing latency and request headroom.
Keep the configured polling, confirmation depth, node margin and serving-lag
guards; changing them is not a remedy for slow processing.

## Upgrade from 0.3.0

This feature keeps fact format 1, compact schema 4, backup format 3 and the
packaged node query/profile identities. An existing schema-4 database needs no
schema migration for relevance filtering. Install a reader implementing both
the prerequisite-receipt and relevance-proof protocols before enabling the
option; keep that reader with any subsequent database or backup containing
optimized history.

1. Stop the writer at a completed publication, preserve its database, matching
   package/profile and request-accounting history, and take a backup v3.
2. Install the prerequisite-window and relevance features, then verify the
   database. Enabling requires complete supported SQL-root coverage and
   relevance-certified provider transitions; an arbitrary full-scan lineage
   does not gain a certificate merely by upgrading.
3. Leave the option off to retain full-scan behavior, or select the strict
   boolean `sync.relevance_enabled` only after the coverage checks succeed.
4. Restart the single writer with the same database and accounting, and check
   the requested/effective mode and fresh `/health` and `/status` observations.

If an update is pending, finish it in its original mode before changing modes.
Disabling filtering does not disable verification of already stored optimized
history. Preserve the original backup/package for recovery rather than deleting
coverage rows or editing immutable manifests.

## Enable and observe

Set a strict JSON boolean in the service configuration:

```json
{"sync": {"relevance_enabled": true}}
```

Missing `relevance_enabled` means `false`. Strings such as `"false"`, numbers and null are rejected. The foreground launcher and Compose initializer accept `--relevance` and `--no-relevance`; a one-shot Blockfrost update uses the same flags:

```bash
db-sync-alt acquire --source blockfrost --network preprod \
  --destination-service db_sync_alt --target-height HEIGHT --relevance
```

Use an actual confirmed target above the current publication, within the existing maximum batch bound. SQL bootstrap itself does not use this filter and does not accept `--relevance`.

Both HTTP status endpoints expose, for example:

```json
{"relevance": {"requested": "relevance", "effective": "relevance", "reason": "verified"}}
```

Disabled mode reports `full/full/disabled`. An enabled unsupported parent reports `relevance/full/relevance_parent_unsupported` and is unready. Corrupt coverage and a conflicting pending mode report `relevance_coverage_invalid` and `relevance_mode_mismatch`. `relevance_parent_changed` means the requested profile no longer matches the certified parent. Do not retry by deleting pending rows or resetting request accounting.

A new pending generation and its coverage row commit together before event acquisition. On resume, the selected mode must match the pending manifest. For an older pending generation that declares a valid certificate but lacks its coverage row, the writer rechecks the exact certified parent, profile, target and current pending manifest, then inserts just the missing row transactionally before provider work. A forged certificate or conflicting row refuses; existing facts, budgets and committed history are preserved. Certificate versions, root generation identifiers and both range bounds must be strict integers; JSON booleans and floating-point substitutes are rejected even when their numeric values compare equal. Resume with the original mode. The native launcher also binds this option in its durable launch identity; changing it in an existing launch state refuses.

Certification binds each manifest through the store's profile-digest API. Both embedded-profile v1 transitions and digest-bound v2 transitions are supported. A fresh writer at an integrity checkpoint still obtains the core VerifiedParent capability and separately checks the retained relevance certificates back to the complete SQL root. The checkpoint alone never proves relevance coverage; a full-scan transition without coverage remains unsupported for enabling the filter.

Certification reuse is scoped to the exact live VerifiedParent capability, profile, database revision and integrity-checkpoint binding, with an integrity-reference check when inheriting a certified ancestor. Revocation, a different writer session or incompatible profile cannot reuse it. Moving the checkpoint invalidates the cached prefix and requires a new relevance proof walk. One cache entry per live writer retains the latest certified prefix; a cold walk retains a single expected root rather than every historical certificate. Core parent-integrity validation still runs. This avoids repeated additional relevance proof walks, not the store's mandatory checks or a fresh writer's initial history validation. Measure the cold lineage walk and steady-state publications separately on the deployed dataset.

## Change mode at a completed publication boundary

The `service` interface can select a different mode at a completed publication boundary. The compatible PostgreSQL server must already be running against the original database. The `service` command does not launch PostgreSQL; if your supervisor stops it with the old service, first bring back that same server/data using your established database lifecycle procedure. Do not initialize another database.

Cleanly stop the old writer after a completed publication, and confirm that its PostgreSQL database is unrevoked with no pending generation. If shutdown leaves a pending update, resume and complete it in its original mode before changing modes. Enabling still requires the supported certified lineage described below: this route does not create missing coverage, and an intervening full-scan publication without a relevance certificate makes the current lineage unsupported for re-enabling. A completed boundary alone is insufficient. Keep all original native launch state and receipts unchanged. Use a separate private service configuration to select the new mode; do not present it to `run` as an altered native launch state.

The following illustrates changing from the native wrapper to the public configuration-file service. Supply your actual existing state directory and a new private configuration path. Preserve your provider adapter choice: shared deployments must retain the same `DBSYNC_PROVIDER_ADAPTER=shared`, `DBSYNC_SHARED_ROOT` and `DBSYNC_SHARED_PROFILE`; portable deployments must retain their original accounting directory. Do not run both writers.

```bash
set -euo pipefail
umask 077
relevance_old_state="$HOME/.local/share/db-sync-alt/native"
relevance_new_dir="$HOME/.config/db-sync-alt/relevance-service"
mkdir -p -m 700 "$(dirname -- "$relevance_new_dir")"
mkdir -m 700 "$relevance_new_dir"  # Must be a new private directory.
export PGSERVICEFILE="$relevance_old_state/pg_service.conf"
export PGPASSFILE="$relevance_old_state/pgpass"
export DBSYNC_PROVIDER_KEY_FILE="$relevance_old_state/provider.key"
export DBSYNC_PROVIDER_STATE="$relevance_old_state/accounting"

# Read-only check after the old writer has stopped. PostgreSQL must be running.
relevance_ready=$(psql 'service=db_sync_alt_owner' -X -v ON_ERROR_STOP=1 -Atc \
  "SELECT NOT revoked AND canonical_ready IS NOT NULL AND NOT EXISTS (SELECT 1 FROM chain.generations WHERE status='pending') FROM chain.service_state WHERE id=1")
[ "$relevance_ready" = t ] || { echo 'Refusing: database is not at a ready publication boundary.' >&2; exit 1; }

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

Check `/health` and `/status` for the requested/effective mode, startup verification, fresh chain progress and unchanged request usage history. The writer lock prevents concurrent owners, and a pending mode mismatch still refuses before provider work. Existing database jobs, original portable ledger or shared ledger are reused; no usage counter is reset. Future starts of this configuration-file service must reuse these same paths. Starting the old native `run` again selects its original bound mode, so manage that deployment choice explicitly.

## Coverage and discovery

Negative filtering requires a session-certified, immutable parent with a sealed direct-SQL acquisition that independently covered every required matching pre-start prerequisite and every event window. Every intervening optimized provider generation must carry matching coverage, completion and independent inventory receipts. With the option off, compatible roots use the accounted full scan without new relevance certificate or lineage work. With the option on, an unsupported root or missing/incompatible/corrupt lineage refuses before provider requests; it never silently falls back. Stored optimized generations still undergo their existing proof validation even when the next update uses full scan. Changing this option does not change the existing dataset's fact contract, profile, reference source, security parameter or confirmation semantics. Both supported SQL acquisition receipt versions are checked under their own complete-coverage rules; a version-2 root requires the prerequisite-window reader.

All transaction CBOR is still decoded and body-hash checked. Exact supported Shelley addresses, native assets/mints and ordinary input outpoints determine relevance. Profile addresses are checksum/network validated. Unknown forms, unsupported address/reference-script encodings and malformed discovery details never become negative evidence: they retain the full provider path or refuse. Relevant invalid transactions, including relevant collateral returns/spends, refuse.

Acquisition and verification independently enumerate fresh provider block inventories and independently discover newly relevant outputs. The second pass fetches fresh selected UTxO/detail/datum responses; it does not consume acquisition-produced cached JSON or candidate-selected transaction lists. Exact supported CBOR output values are compared with the provider response, preventing a provider omission from changing relevance. Ordinary/collateral/reference role and ledger ordinal checks remain in place. Certified parent rows and already independently discovered earlier work units may be reused as immutable prerequisites.

Independent natural-key/hash inventories are checked against persisted facts before publication; missing, changed or extra candidate facts refuse. An interrupted pending generation replays completed blocks through fresh inventories to rebuild both discovery streams. Candidate row presence alone is never evidence that an absent input origin was irrelevant. Memory and receipt bounds fail closed on excessively dense work units.

## Cost and latency qualification

Request models must include both fresh inventory passes, selected UTxO/detail responses, polls, metrics, origins/datums, unknown-form fallbacks, forks, retries, cold handoff and epochs. A NIGHT-only activity proxy is incomplete. Synthetic wall time measures database work but does not prove network latency or live API usage. Respect the shared daily limit and cooldowns; do not reset counters or rotate credentials to evade them.

Measure complete acquisition, independent verification and publication latency
under live load. With an average block interval of 20 seconds and batch size B,
the prior publication's approximate peak lag is
`B + ceil(job_seconds / 20)` blocks; final witness lag is approximately
`ceil(job_seconds / 20)`. This model does not bound bursts or provider RTT.
Preserve the profile's actual lag and node-margin guards and require advancing
near-tip acceptance.

The request model includes the following synthetic scenario expectations;
polling, cold handoff, epochs, retries and unknown-form fallbacks are excluded:

| Blocks per publication | All irrelevant | One relevant transaction per publication | Four relevant transactions per block |
| ---: | ---: | ---: | ---: |
| 1 | 8 | 12 | 24 |
| 3 | 12 | 16 | 60 |
| 6 | 18 | 22 | 114 |

These scenarios account for six fixed job requests, two fresh CBOR inventory
passes per nonempty block and two UTxO plus two transaction-detail reads per
selected transaction. Pagination and other activity can add calls. They are
request-path examples, not a measured daily allowance or latency guarantee.

The global source selector, cold lineage walk, both inventory passes and
selected transaction reads can dominate cost. Their time and request use must
fit the actual service policy before deployment. Synthetic fixtures are useful
for exact request-path checks, but do not establish live throughput or a daily
quota envelope. Epoch acquisition remains part of the full update path and its
cost must fit the same live resource and accounting budget.
