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
