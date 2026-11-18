# Convert a legacy database

`db-sync-alt convert --network mainnet --database '<destination DSN>' --source '<reader DSN>'`
loads an empty operator-owned PostgreSQL17 database from the legacy schema5
public reader views. Source passwords must come from `--source-password-file`;
destination passwords use `--password-file`. TLS is required by default and
verifies the server name. Custom roots use `--source-tls-root-cert` and
`--tls-root-cert`. An explicit `sslmode=disable` permits a local connection.

The source session is read-only by default, with one REPEATABLE READ READ ONLY
transaction covering anchors, selected rows, supply and old-spent state. It
requires the exact legacy reader grant, without TEMP or private chain access.
The target is the newest stored header minus the profile's security parameter,
even when event rows are much older or the legacy follower is behind the chain.
Network identity requires a positive pinned start header or a positive held
asset under a policy exclusive to that network's generated configuration.
Shared Preprod/Preview cNIGHT, auth and mapping units alone are ambiguous and
are refused. Preview is not selectable.

The initial default horizon is the latest source header at or before seven
days before the *source target's timestamp*. It is independent of wall clock
and of a fixed block-rate estimate. `--retention-seconds` accepts durations of
at least86400; `--horizon-block` chooses an explicit height instead. A short
source clamps to its verified beginning, reporting its shorter coverage. A
matched pinned start establishes the legacy continuous suffix, extending into
an adjacent earlier prefix when target-time retention needs it; isolated older
origin and epoch headers remain available for retention selection. Without a
matched anchor, the oldest physical header is the conservative beginning.
An explicit horizon may precede the anchor when its suffix is continuous.
Any hole within the promised horizon-to-tip window is refused. Only that
bounded window is counted for continuity; horizon navigation uses indexed ID
seeks and point reads, without materializing all historical headers.

Each event in [horizon,target] receives first-served time when the conversion
publishes. Header-only rows in (target,tip] do not receive first-served entries.
This gives the full retention period after conversion, including a lagging
source. Rule2 retains only the four exact configured token units and any name
under the two configured governance policies, at every address. Output shells
and data_hash remain; original selected payloads, IDs and pre-filter asset
ordinals are preserved. The six fixed lookup descriptors retain their source
IDs even without output references. Rule1 pruned sources cannot be relabelled.

The source sends one natural-key stream per family. Destination batches use
`--batch-rows` (default4096) and `--batch-bytes` (default4MiB); one larger
supported row travels alone. Source work includes prefix unspent/carry
selection and sorting. Reports include actual source limits, work_mem,
preflight, network, supply and header/horizon read times, each family's
row/byte/batch counts and wall cost, plus destination preparation,
index/analyze/sequence, verification, commit and total finish times. Network
time is included in the reported bounds read time. The transaction can hold back source vacuum while active.

Positive PostgreSQL statement and idle-transaction limits are preserved. A
source reader whose inherited value is0 receives a scoped60s default. The
command's default overall deadline is3600s, controlled by
`--overall-timeout-seconds`; interrupts and deadline expiry cancel this
operation's own queries before closing its connections.
Explicit positive overrides are connection-local:
`--source-statement-timeout-seconds`,
`--source-idle-transaction-timeout-seconds`,
`--statement-timeout-seconds`, and `--idle-transaction-timeout-seconds`.
Source statement time includes destination backpressure. A destination can be
idle while a source sort runs; a caller's short idle limit can therefore end a
conversion. Limits are never silently disabled. Live-source limits still
require the operator's authorization; synthetic timing does not change them.

Source, horizon, destination and optional snapshot path preflights precede
layout or ACL mutations. A serving or occupied destination is preserved.
Loading and publication use one destination transaction; cancellation rolls
back rows and leaves a closed, explicitly owned `convert_failed` layout that
may be retried. A failed restore belongs to restore and is refused by convert.

`--snapshot new-file.dsa` writes a verified snapshot after conversion publishes.
A missing/unwritable parent is refused during preflight. If the path becomes
unusable later, the command reports `conversion_completed_snapshot_failed`,
`converted:true`, both completed digests and recovery instructions. The
database remains serving. Run `snapshot --file` with a usable new path rather
than repeating convert. Provenance contains chain anchors and source kind,
never connection strings, credential files or host names.

Library callers can use `LegacySource::snapshot` and `Store::stage_conversion`
to capture reference answers in the same source transaction. Commit the source
transaction before `StagedConversion::finish` to avoid keeping it idle during
destination index construction and verification.

`--capture-source FILE` preserves the source's exact node-query answers in the
conversion snapshot for a later `compare --a-capture FILE --b '<reader DSN>'`.
See [comparison](rust-comparison.md) for publication/failure ordering and source
advancement. It adds the configured catalogue read workload to conversion;
real-source access budgets must include those reads.
