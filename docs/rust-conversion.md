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

The source sends bounded unique-key binary COPY pages into destination-owned
TEMP tables. Each source page is fully read before any destination write, so
slow destination writes do not keep a source statement active. Both
`--batch-rows` (default 4096, source cap 4096) and `--batch-bytes` (default 4 MiB)
bound pages. A single larger supported row travels alone and remains subject
to the caller's finite timeouts. A conservative SQL byte counter uses one
indexed successor at a time; it may inspect one extra row at a page boundary.
Only binary COPY transports data: numeric values, timestamps, bytea, datum JSON
and SQL NULL versus JSON null are preserved without a Rust value decoder.

All eleven visible families are read in the same source snapshot, with safe
positional target/tip and bridge-window bounds. This is a full-reader operation;
`raw_families` reports its rows, wire bytes, page costs, total and maximum
destination page-write times, temporary relation bytes, and each raw row-framed
binary digest `{rows, sha256}`. These transport hashes verify exported bytes
against the private copy; they do not independently prove source coverage.
Record independently available source counts/anchors and use anchored capture
plus compare for source-answer evidence. It creates no source TEMP objects or grants.
Source observations are captured and committed before the expensive local
finish. The private copy is checked by an independent binary row digest, then
the unchanged A3 predicate is evaluated once and its keys reused for all
families. Natural-key fingerprints, old-spent state, supply and closure are
checked before publication. TEMP data disappears on commit, rollback or a
lost connection; the served layout and snapshot format do not change.

An already granted node reader remains supported during conversion. The
destination uses the same conservative gate-access check and closed-empty
recheck as restore. The data transaction uses SELECT-compatible locks,
rechecks empty and validates matching indexes before staging; conflicts and
invalid indexes are preserved and refused. Matching public indexes remain,
so startup prerequisites that were present stay present. A missing-index plan is carried
through local selection and built before verification and atomic publication.
The raw private TEMP copy retains only each family's primary key, as before;
public secondary indexes are not cloned into it. Gated reads, including prepared
and nested node queries, continue to raise55000 until commit. Maintaining public
indexes during selected-row inserts has a cost; source-free/generated timings
do not substitute for archive qualification.
An auto-created failed empty layout can already lack indexes before a reader is
granted. Those missing prerequisites appear only at successful commit; startup
helpers can fail or wait during that interval. SELECT-compatible load locks
still exclude DML and index DDL for the whole conversion, including a startup
helper attempting to build a missing index. Gated data reads return55000.
`conversion_load_mode` reports the chosen path. Source permissions, its single
snapshot and its statement/idle limits are unchanged; no keep predicate,
positional ID, format, digest or verification rule changes.

Temporary staging, its indexes, local selection spill, the selected database,
WAL, snapshot and restore all need disk headroom. The separately guarded runbook
must verify the actual PostgreSQL storage mounts and free-space floor against
the qualified peak footprint before starting. The tool reports its TEMP relation
sizes; it never creates local root-disk scratch. Hosted data/costs do not by
themselves qualify the actual archive or authorize live reads.

Reports retain selected-family row/byte costs and add raw copy/staging and
local-selection costs, alongside actual source limits, work_mem and destination
index/analyze/sequence, verification, commit and finish times. The source
transaction stays open through all raw staging and source-answer capture,
holding back vacuum until its source commit finishes. It is idle between source
pages while the destination writes, sizes and logs each page; maximum page
write time helps assess the explicit idle-limit margin. No heartbeat extends
the caller's limits.

Positive PostgreSQL statement and idle-transaction limits are preserved. A
source reader whose inherited value is0 receives a scoped60s default. The
command's default overall deadline is3600s, controlled by
`--overall-timeout-seconds`; interrupts and deadline expiry cancel this
operation's own queries before closing its connections. The overall deadline
covers preflight, supply, raw staging, capture, local selection, publication and
the optional post-publication snapshot; it is not a per-source-statement limit.
Explicit positive overrides are connection-local:
`--source-statement-timeout-seconds`,
`--source-idle-transaction-timeout-seconds`,
`--statement-timeout-seconds`, and `--idle-transaction-timeout-seconds`.
A source page is complete before its destination COPY starts. A destination can
still be idle while a source page or source-answer capture runs, and a source
can be idle while a bounded destination page writes. A caller's short idle
limit can therefore still end a conversion. Limits are never silently disabled. Live-source limits still
require the operator's authorization; synthetic timing does not change them.

Source, horizon, destination and optional snapshot path preflights precede
layout or ACL mutations. A serving or occupied destination is preserved.
Loading and publication use one destination transaction. A failure before
publication rolls back its rows and leaves a closed, explicitly owned
`convert_failed` layout that may be retried. Deadline or signal cancellation can
race the publication COMMIT acknowledgement or the optional snapshot, so the
CLI retains its error cause but reports `outcome:unknown` and
`rollback_confirmed:false`. Run `status` and `verify` on that exact destination
before cleanup or retry. If it is serving and verifies, preserve it and write a
fresh snapshot if needed. Never assume rollback or repeat convert/restore into
a serving or nonempty destination. A failed restore belongs to restore and is refused by convert.

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

Supply preflight is a whole-source operation within the same read-only snapshot. It pages the indexed unique ma_tx_out.id in ascending order, at most4096 raw rows before unit filtering, through the target bound. Each page checks only those outputs and their target-bounded spend lookups. Foreign-only pages still advance the cursor; missing fixed units stay zero. Prior totals and page sums are combined as PostgreSQL numeric values passed as text, without Rust integer/float conversion. The source statement and overall limits remain in force. Source reports include raw supply rows, page count and longest page time; structured progress records identify each page and the SQL hash.

The supply phase uses transaction-local planner controls to disable sequential/bitmap scans and enable ordered index/index-only scans; otherwise stale estimates could put a full-range sort below LIMIT. It saves and restores the caller's four planner flags before subsequent family reads or native queries. Failed source statements abort the read-only transaction, whose rollback also reverts LOCAL settings. Statement/idle/overall limits and work_mem are unchanged. Supply totals published to the existing store still satisfy its uint64 domain; internal PostgreSQL numeric page arithmetic is lossless.
