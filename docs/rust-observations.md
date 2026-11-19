The Rust status contract identifies registered-candidate results in operator RPCs
and CLI commands as unsupported for Midnight node 2.1.101. This includes the
registrations shown by Ariadne-parameter calls. That node sets
`registered_candidates = Vec::new()` during block production and import; it uses
permissioned candidates there. A future node that enables registered candidates
requires full coverage of the registered-candidates address and a new keep-rule
version.

Three historical validation cases remain **not proven on real data**: a pointer
address among stored outputs, raw payload above 10,000 bytes among stored datums,
and a positive stored bridge metadatum with key 6500973. Status retains that
explicit distinction for each case. Existing conformance tests support the
implementation but do not replace these real-data proofs.

The first stored-observation interface preserves already received provider
response bodies locally, with their endpoint, block, transaction, source and
SHA256. It does not request the data again. A committed observation is a future
regression input, rather than an automatic independent validation claim. Skipped
rows and failed updates cannot create one; retries and restarts preserve the
first committed record. Credential material, headers and operator paths are not
status or regression data. Release review rechecks each remaining real-data gap.

This document describes the Rust reader/status contract. The follower's update
transaction and HTTP service wiring are separate integration work; the schema
alone does not provide a running service.

The preparatory `dsa-service::observations` adapter stores raw bodies in private
`dsa_follower.raw_chunk` rows of at most 64 KiB. A unique shape key in that
store's `first_witness` table is authoritative. The adapter must run under the
normal writer lock. Collect actual selected INSERT ... RETURNING outcomes in an `InsertedRows`
receipt (ON CONFLICT DO NOTHING returns no old datum). Call `stage_witnesses`
with that receipt in the same transaction, commit SQL, then call `apply_held`.
Discard receipts if their savepoint rolls back. A source or staging error
requires rolling back the entire publication. Dropping an unsuccessful
publication discards its held overlay. Load `status` again after commit and at
restart; derive the pending set from it before requesting ordinary responses
with `fetch_with_evidence`. A-built snapshots have no follower-local witnesses.
The full follower loop, migration invocation and HTTP status wiring remain
pending the A merge and B4/B5 integration gates.

To recover a local future regression input, take a `response_sha256` under the
status observation's `sources`. With an operator reader granted SELECT on the
three private follower tables, run the read-only extractor:

```sh
cargo run --locked -p dsa-service --example extract_observation -- RESPONSE_SHA256 local-response.json
```

Supply `DSA_DATABASE` through the operator environment or password-file settings
and `DSA_TLS_ROOT_CA` when using a private CA. Do not include credentials in the
command or publish these files automatically. The extractor uses read-only
transactions with a 60-second statement limit, reads chunks in order, verifies
the original length and SHA-256, and creates the destination without overwriting
an existing file only after verification. It makes no provider request. The
recovered body includes the original whitespace and unknown JSON fields; it is
not a reserialization. Header and credential fields from the HTTP request or
connection settings never enter these tables or status. Reflected current
provider credentials, including JSON-escaped spellings in discarded fallback
inputs, are rejected before an evidence handle can be published.

A large inline datum from an earlier held output normally already has its stored
datum row: creation and spending apply the same datum-selection predicate. The
caller marks verified existing datum rows with `known_stored_datum`. An abnormal
missing-origin case reports `held_inline_origin_unavailable`; the adapter can
publish the ordinary selected rows without attributing the old preimage to the
spending block or fetching it again. This limitation must be surfaced by B5.

The large-datum criterion measures the hash-verified original CBOR preimage,
strictly above 10,000 bytes. A hash alone is not a preimage. A valid preimage
whose deeply nested JSON rendering is NULL still meets that raw-byte criterion;
the raw provider response remains the regression input. In contrast, a bridge
placeholder with NULL/rejected JSON does not consume the positive bridge slot.
A non-NULL JSON value of 0 does qualify; it is not treated as absence.
