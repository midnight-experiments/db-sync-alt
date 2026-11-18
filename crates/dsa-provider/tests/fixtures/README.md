# Recorded row-replay fixtures

`*-corpus.json.gz` contains public finalized Blockfrost records together with
independently exported node-facing rows and spent-output origins from the
approved historical schema-5 backup. Each JSON bundle records a SHA-256 for
every member. The adjacent manifest records the compressed digest, projection
blocks, support-only blocks and expected row counts.

The normal Rust test verifies the member digests and bundle limits, then runs
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

The Preprod subset has six positive row-projection blocks. One older block
supplies original datum bytes only. Row assertions apply to the manifest's
`projection_blocks`; support-only blocks provide preimages without asserting
their row projection. The complete local corpus preserves the independent
comparison of the older bridge outputs and their differing asset rows.

Only public ledger responses and node-facing expected rows belong here.
Source archives, source manifests, request counters, operator paths and
credentials are excluded. Arbitrary-input and generic deep-item tests run on
our own parser separately; these bundles contain real finalized transactions.
