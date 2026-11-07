# Mainnet provider relevance candidate

This unreleased candidate adds a coverage-gated optimization to incremental following. It does not qualify Mainnet epoch rollover or replace an end-to-end node test.

The default configuration and existing Preprod deployment are unchanged. Merge the values in [mainnet-relevance-sync.json](../deploy/mainnet-relevance-sync.json) into the isolated Mainnet service configuration: initially publish after three confirmed blocks, or a 60-second maximum wait. Keep polling every20seconds, k2160, node margin10 and serving lag9. Do not change those security/health values to make batching pass. Six-block/120-second batching is a further cost experiment that requires measured live processing latency and request headroom.

## Coverage and discovery

Negative filtering requires a session-certified, immutable parent with a sealed direct-SQL acquisition that independently covered every required matching pre-start prerequisite and every event window. Every intervening optimized provider generation must carry matching coverage, completion and independent inventory receipts. Legacy or unproven roots retain the original accounted full scan; a malformed existing certificate refuses instead of falling back. No fact contract, packaged profile, reference source, k or confirmation semantics change.

All transaction CBOR is still decoded and body-hash checked. Exact supported Shelley addresses, native assets/mints and ordinary input outpoints determine relevance. Profile addresses are checksum/network validated. Unknown forms, unsupported address/reference-script encodings and malformed discovery details never become negative evidence: they retain the full provider path or refuse. Relevant invalid transactions, including relevant collateral returns/spends, refuse.

Acquisition and verification independently enumerate fresh provider block inventories and independently discover newly relevant outputs. The second pass fetches fresh selected UTxO/detail/datum responses; it does not consume acquisition-produced cached JSON or candidate-selected transaction lists. Exact supported CBOR output values are compared with the provider response, preventing a provider omission from changing relevance. Ordinary/collateral/reference role and ledger ordinal checks remain in place. Certified parent rows and already independently discovered earlier work units may be reused as immutable prerequisites.

Independent natural-key/hash inventories are checked against persisted facts before publication; missing, changed or extra candidate facts refuse. An interrupted pending generation replays completed blocks through fresh inventories to rebuild both discovery streams. Candidate row presence alone is never evidence that an absent input origin was irrelevant. Memory and receipt bounds fail closed on excessively dense work units.

## Cost and latency qualification

Request models must include both fresh inventory passes, selected UTxO/detail responses, polls, metrics, origins/datums, unknown-form fallbacks, forks, retries, cold handoff and epochs. A NIGHT-only activity proxy is incomplete. Synthetic wall time measures database work but does not prove network latency or live API usage. Respect the shared daily limit and cooldowns; do not reset counters or rotate credentials to evade them.

Measure complete acquisition, independent verification and final publication time under live load. With20-second average blocks and a batchB, peak lag of the old publication is approximately B+ceil(job_seconds/20); final witness lag is approximately ceil(job_seconds/20). These are estimates, not a bound on Cardano bursts or provider RTT. Keep the actual lag9/margin10 guards and require live near-tip acceptance. Epoch stake preparation remains a separate unresolved qualification; do not run slow unqualified epoch reads inside a critical publication path or replace stake facts with unproven aggregates.


## Measured candidate costs

Warm acquisition requests, including independent verification and publication (polling excluded):

| Blocks per publication | All irrelevant | One relevant transaction/publication | Four relevant transactions/block |
|---:|---:|---:|---:|
| 1 | 8 | 12 | 24 |
| 3 | 12 | 16 | 60 |
| 6 | 18 | 22 | 114 |

These real PostgreSQL/driver fixtures include six fixed requests per job, two fresh CBOR inventories per nonempty block, and two UTxO plus two transaction-detail reads per selected transaction. Synthetic database processing was0.176–0.554seconds through publication; the provider was a fixture with a fixed head.

Using the2026-11-05/06 observed4,287blocks,4,045CBORpages and4,897NIGHT-transaction proxy, plus8,784poll/metric requests, modeled daily requests are62,184/45,036/40,752 for1/3/6-block batches. The latter two leave4,964/9,248 before unknown watch-address traffic, opaque-form fallbacks, origins/datums, epochs, cold handoff, forks and retries. This is not a complete activity upper bound or a50,000/day qualification.

A separate30-request sequential latency sample measured12confirmed transactions and3blocks. End-to-end median/p95 seconds were0.817/1.199 forCBORpages(n4),0.433/0.958 fortransaction details(n12), and0.444/0.562 forUTxOs(n12). The small-sample p95 is the observed maximum, not a statistical upper bound; no retries were made.

Combining those observations with the retained NIGHT burst distribution gives conditional peak-lag violations even at three-block batching; six-block batching is worse. Fixed-call latency and missing activity are still assumptions. Keep the initial three-block/60-second experiment, preserve all lag/security gates, and qualify actual full following before accepting production operation. This feature removes the unconditional transaction-UTxO cost; it does not claim that serial requests can handle every observed burst within the node margin.
