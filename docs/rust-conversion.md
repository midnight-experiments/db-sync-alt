# Convert a 0.4.x database

`db-sync-alt convert --network mainnet --database '<destination DSN>' --source '<reader DSN>'`
loads an empty operator-owned PostgreSQL 17 database from the public reader
views of a serving 0.4.x database (schema 5). Passwords come from
`--source-password-file` and `--password-file`. TLS is required by default and
verifies the server name; custom roots use `--source-tls-root-cert` and
`--tls-root-cert`, and an explicit `sslmode=disable` permits a local
connection.

## The source

The source session is read-only, and one REPEATABLE READ READ ONLY transaction
covers anchors, selected rows, supply and the spent-before-horizon state. The
reader must hold exactly the legacy grant (the eleven gated views and the
legacy gate) without TEMP or access to private tables. The target is the newest
stored header minus the profile's security parameter, even when the newest
events are much older or the 0.4.x follower is behind the chain. The network is
proven by the profile's pinned start header or by a held asset under a policy
that only this network's profile uses; shared Preprod and Preview units alone
are refused as ambiguous.

The default horizon is the newest source header at or before the retention
(seven days by default, `--retention-seconds`, at least 86,400) before the
target's timestamp; it does not depend on the wall clock or a block rate.
`--horizon-block` sets an explicit height. A shorter source is clamped to its
verified continuous beginning and the report says so. A hole anywhere between
the horizon and the tip is refused.

Every height from the horizon to the target counts as first served when the
conversion publishes, so the full retention follows. Headers above the target
get no first-served time.

## The load

The source sends bounded binary COPY pages (`--batch-rows`, default and cap
4,096; `--batch-bytes`, default 4 MiB) into temporary tables of the destination
transaction. Each page is read completely before the destination writes it, so
a slow destination does not keep a source statement running. A single larger
row travels alone. Binary COPY keeps numeric values, timestamps, bytes, JSON
and the difference between SQL NULL and JSON null without decoding them.

The raw copy is checked against the source with a row-framed binary digest per
table. The keep rule is then evaluated once on the copy, in stages, and the
kept rows are inserted with their original IDs, payloads and pre-filter asset
ordinals. Natural-key digests, the spent-before-horizon state, closure and
supply are checked before the gate opens. A mismatch names the family or unit
and both values.

The supply of the fixed units is read in pages of at most 4,096 asset rows by
ascending ID within the same source snapshot. During those pages the source
transaction disables sequential and bitmap scans; the caller's planner settings
are restored before any other read. Totals are summed as PostgreSQL numeric
values.

Loading and publication are one destination transaction that holds the served
tables. A node reader granted before a conversion waits for it (or meets its
own statement limit) and never sees partial data. A failure rolls the load
back and leaves a closed `convert_failed` layout; a later conversion or restore
accepts it, as `convert` accepts the empty layout a failed restore leaves.

## Limits, interruption and output

PostgreSQL's positive statement and idle-transaction limits are kept; a source
reader whose inherited limit is 0 gets 60 seconds for this session. The overall
deadline (`--overall-timeout-seconds`, default 3,600) covers the whole command.
`--source-statement-timeout-seconds`, `--source-idle-transaction-timeout-seconds`,
`--statement-timeout-seconds` and `--idle-transaction-timeout-seconds` override
the limits for this connection only. A source can be idle while a destination
page writes, and the destination while a source page or source capture runs,
so a short idle limit can end a conversion.

A signal or the deadline cancels the command's own statements. Before the
publishing commit was sent, the report says `published: false`: nothing was
published, and the command can be run again. After it was sent, the outcome is
`unknown`; run `status` and `verify` on the destination, and keep it if it
serves and verifies.

`--snapshot new-file.dsa` writes a snapshot after the conversion publishes. If
that fails, the report is `conversion_completed_snapshot_failed` with
`converted: true` and both digests; the database serves, so run `snapshot` with
a new path instead of converting again. The origin records chain anchors and
the source kind, never connection settings.

`--capture-source FILE` stores the source's literal node-query answers from the
same source transaction, for a later `compare --a-capture FILE`; see
[comparison](rust-comparison.md). Library callers use `LegacySource::snapshot`
and `Store::stage_conversion`, and commit the source transaction before
`StagedConversion::finish`.

Temporary tables, their indexes, the selected rows, WAL and any snapshot need
disk space at the same time; the report lists the temporary relation sizes.
