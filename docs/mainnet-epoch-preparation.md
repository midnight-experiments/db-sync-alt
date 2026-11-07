# Mainnet epoch preparation (candidate feature)

This opt-in feature prepares complete current-epoch pool weights privately while ordinary follow continues. The current-epoch collector is qualified against the independently verified epoch666 db-sync13.8 snapshot. It does not change the fact contract, packaged profile, k2160, margin10 or serving lag9. It does not qualify arbitrary historical API acquisition or replace final live Mainnet acceptance.

Enable only on a qualified Mainnet candidate after combined quota and latency admission:

```json
{
  "network": "mainnet",
  "shutdown_seconds": 30,
  "sync": {
    "follow_min_blocks": 3,
    "follow_max_wait_seconds": 60,
    "epoch_preparation": true
  }
}
```

This is a fragment to merge into the private service configuration; existing owner/reader services, storage reserves and source identity remain required. The flag defaults false. Preprod cannot enable it. A30second shutdown budget is required because a provider request may already be in flight. No production service is started by configuration examples or tests.

## Acquisition and completeness

Every pass discovers all registered and retired identities independently. Exact nonnegative active_stake values from registered pools are accumulated. Retired candidates are searched by retirement epoch descending, then raw hash; that is search order only, never an age cutoff. Search stops only when positive weights close the independently fetched epoch total. A deficit, overcount, duplicate, changed inventory/distribution, malformed value or unsupported epoch refuses. Both independently obtained positive maps must match.

`active_stake` is used; changing `live_stake` is irrelevant. Raw zero inventory entries remain evidence, while canonical positive-only weights are explicitly bound to db-sync13.8 nonzero ActiveStake semantics. Current endpoints cannot be relabeled historical. A deep preparation anchor can precede the current epoch while the confirmed target still lags; fresh adoption requires the actual confirmed target to belong to the prepared epoch.

The service stages one bounded request step per60seconds, yielding before HTTP to ordinary requests using the same Mainnet key. Speculative preparation starts only with fresh observations, lag at most1, healthy service and no active writer operation. A30second in-flight call cannot be preempted; tests quantify this blocking. Global rate reservation releases before HTTP, so another Preprod key is unaffected by the Mainnet request's per-key lock.

If startup/catch-up is blocked only on a missing required epoch, an explicit recovery mode permits bounded preparation despite large lag. Prior publication stays unchanged and readiness remains503. This avoids requiring near-tip coverage that cannot exist until the missing epoch is prepared. Polling retains priority, and quota/owner/profile/confirmation checks still apply.

## Durable private state and budgets

Operational format1 uses only existing private `chain.sync_jobs` rows; no schema or immutable metadata guard changes. The `provider-epoch-v1-E` row carries the cumulative job counter. Versioned `epoch-preparation-v1:E:*` rows store bounded compressed/base64 chunks, headers with exact size/hash, request transcripts and chained reservation evidence; every row stays below the existing64KiB limit. Private state is excluded from native node relations and cannot expose future weights.

The entire epoch lifecycle, including metrics, failures, canceled reservations and fresh adoption checks, is capped at8000attempts. A reservation and its audit-chain record commit before the shared client call. Restart and UTC-day changes do not reset the counter. Source database physical identity, profile, source API identity, canonical owner session and a dedicated advisory lock prevent copied/unqualified state or a second worker from minting a budget. Full replay validates records on open/resume and before adoption. An in-memory private collector avoids repeating parsing between unchanged manifest checkpoints; it never replaces independent provider reads.

The8000cap is not a daily allocation. Before each preparation step, remaining shared daily capacity must exceed a forecast of48000ordinary follow requests/day prorated to UTC midnight, plus1000safety reserve. The shared client independently enforces the user's50k/day and8/s limits. This forecast is a conservative implementation policy, not live quota qualification; combined feature workload/spikes/extras must pass admission separately. Preparation can pause rather than exhaust ordinary follow headroom.

Two full epoch transcripts are retained. Old transcript bodies expire while small spent-budget/policy/reservation history and adopted proof bindings remain. Semantic corruption/mismatch refuses; it is not silently re-enumerated under a new budget. Transient provider errors/cooldowns retain counted reservations and durable backoff. Restoring/copying an unfinished stage to a different physical database refuses automatic reuse/rebudgeting; an explicit qualified recovery is required. Already adopted canonical receipts remain verifiable through normal backups.

## Adoption and visibility

A complete stage remains private until the required confirmed target reaches its epoch. Adoption replays persisted source proof, freshly checks current epoch/nonce/total, canonical target/hash/depth and the original immutable anchor, and spends the same epoch budget. The canonical transaction inserts nonce/weights/checkpoint and marks private adoption/job completion atomically. Candidate omission or corruption rolls all of that back. Verification compares actual canonical epoch rows with the independent prepared positive map and retained proof binding.

`/status` exposes a small `epoch_preparation` state with epoch, phase/state, attempts, budget, error and recovery mode. Incomplete required data is `epoch_preparation_not_ready`; prior publication is retained. This status is operational information, not evidence that full live acceptance passed.

## Proven limits

Q4 replay discovers all2669positive groups, including four retired-positive pools, without SQL missing-ID hints. Current registered-only data is incomplete. Public source replay and synthetic slow-call tests are not a real future epoch transition. Historical address enumeration keeps its original1million/page10000/job10000 limits and fails clearly when those limits do not fit. A missed now-historical preparation cannot automatically acquire current values under an old epoch label.

Tests cover full canonical owner/poller/preparer restart from665→666with large initial lag, private future visibility, exact SQL semantic digests, cumulative reservations, copied state, contention, corrupt candidates and fresh adoption changes. Normal and one-hour dense-burst timing models include30second pool calls and ordinary measured endpoint p95 inputs; longer bursts or different live latency can exceed the model and require honest readiness/quota refusal. Final combined candidate and actual live resource/budget/node acceptance remain separate gates.
