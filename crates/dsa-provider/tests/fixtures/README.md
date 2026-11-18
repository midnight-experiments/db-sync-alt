# Recorded row-replay fixtures

`*-corpus.json.gz` contains public finalized Blockfrost records together with
independently exported node-facing rows and spent-output origins from the
approved historical schema-5 backup. Each JSON bundle records a SHA-256 for
every member. The adjacent manifest records the compressed digest, projection
blocks, support-only blocks and expected row counts.

The normal Rust test verifies the compressed digest, member digests, counts and bundle limits, then runs
the same offline comparison used on the complete local corpus. It compares
positional IDs and selected rows, datum preimage hashes and JSON values, and
compares ordinary finalized transactions with Pallas 0.33.0. Identity sequence
values are represented by their natural asset keys.

The Preprod SQL source is the schema-5 v0.4.3 backup ending at block 5,397,924,
whose approved archive SHA-256 is
`67fadb31e73cb087994acb3bbe32044dc4a485e41a27a189026a97fa25296582`.
The legacy SQL contract is pinned at
`4d7cba2eee66b081c644fa8e2150025baf46a54f`; the target node is 2.1.101 at
`86774edf8cc76345a5c3a4221f3fe80c094b1c33`.

The Preprod subset has six positive row-projection blocks. It compares 20
transactions, 13 with multiple ordinary inputs, and nine collateral-return
bodies. These are valid transactions whose collateral returns have no chain
effect. This subset has zero phase-2-invalid transactions, Byron or pointer
outputs, fallback resolutions or stored bridge metadata rows. Separate adapter
tests cover finalized provider output replies and a native invalid-projection
model. The complete historical corpus supplies broader real-shape coverage.

An older support block remains recorded with its original provenance. Causal
replay uses only the current transaction's bytes, a requested recorded datum
API response or an earlier successfully stored datum. Explicit404 replies are
scoped to their requesting block, so a later spend can discover the preimage. Row assertions apply to
the manifest's `projection_blocks`. The complete local corpus preserves the independent
comparison of the older bridge outputs and their differing asset rows.

Only public ledger responses and node-facing expected rows belong here.
Source archives, source manifests, request counters, operator paths and
credentials are excluded. Arbitrary-input and generic deep-item tests run on
our own parser separately; these bundles contain real finalized transactions.

The transaction reader supports the four-element Alonzo-and-later envelope.
The pinned profiles and this projection corpus start in that period. The
recorded Preprod projection headers span epochs 266 through 324 and timestamps
1769107159 through 1794067347. The comparison report counts Pallas's decoder era
classification separately. A compatible decoder classification establishes
which representation was compared. Historical set/map normalization is proven
by standalone helper vectors and pinned ledger rules, with real occurrence
counts left to the full historical replay. Byron addresses in later-era
outputs are distinct from a Byron-era transaction envelope.

Shared positive datum replies require a capture sidecar that binds the exact
reply digest, datum hash, recorder source and a past response-completion time
or a documented recorder-completion upper bound. Replies recorded for one
specific block can satisfy that block's request. The recorder records new
request/completion times. Reusing a verified positive reply preserves its
original provenance and saves a repeat request.
