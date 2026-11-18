# Compare the node's database answers

`db-sync-alt compare --network mainnet --a '<reader DSN>' --b '<reader DSN>' --positions auto --horizon-block H`
compares the complete pinned Midnight2.1.101 SELECT catalogue. Both connections
must have the exact public reader grant. The command executes no index creation,
DDL or source writes. Missing startup indexes fail the comparison.

TLS verifies the server identity by default. Use `--a-tls-root-cert` and
`--b-tls-root-cert` for custom roots and `--a-password-file` and
`--b-password-file` for reader passwords. Inline passwords are refused.
An explicit `sslmode=disable` supports local connections. Positive statement
and idle-transaction timeout overrides apply only to these connections;
configured limits otherwise remain in force.

Auto positions include consecutive cursor/reference pairs spread across the
whole window, a spanning pair and every epoch boundary. `--max-pairs` controls
the consecutive sample. An explicit positions JSON file has `horizon`, `target`
and a `pairs` array, each containing `cursor` and `reference` objects with
`block` and `tx_index`. Explicit pairs must stay within the window and order
cursor before reference. All catalogue queries also receive hit/miss inputs;
a deterministic28-byte policy is checked absent on both anchored readers
(with retries on collisions). Its value and actual hit/miss row counts are
reported. Independently exported captures must pin the same missing policy
through `Options::missing_policy`; differing probes are refused. The original26
gated statements/52 executions are accompanied by three
read-only catalogue probes.

Every event and state answer is exact, including NULL, wide integers and JSON,
with the SQL's outer ordering preserved. Unordered results use multisets,
including duplicate multiplicities. Only the two internal bounds helpers may
report valid differing IDs as information. Their before/after anchors and
related output/asset/input bounds are verified independently at each database.
The actual `resolve_ident` lookup IDs are exact. `--verify-unbounded` additionally
checks event answers with bounds0..i64::MAX; use this on generated scratch data
or under an explicit source-access budget. It adds reads.

JSON stdout includes the pinned catalogue, selected units, positions, settings,
query counts, actual per-query/unit row-hit coverage, both destination-order
and unbounded-check flags, and the first differing rows for every mismatch.
Acceptance requires destination B's chain-order check. Source A remains
unscanned globally. A pruned asset table may have leading/interior ordinal gaps;
order verification does not infer dense rank from filtered facts. First-row
previews are capped at4096characters, with full-row SHA256 and an explicit
truncation flag. Whole rows are still compared exactly. Exit0 means a
match,2 means a real answer mismatch and1 means a refusal or query failure.
Rule1 pruned stores and mismatched rule declarations/identity are refused.
Genuine legacy schema5 readers remain supported. Preprod's empty-name cNIGHT
and named NIGHT bridge unit are reported separately.

Each endpoint uses one REPEATABLE READ READ ONLY transaction. A source advancing
between standalone conversions and comparisons changes the unbounded newest
header answer. For an anchored conversion, a coordinator must call the reusable
`compare::capture` on the same still-open source transaction passed to
`Store::stage_conversion`, then compare that capture with the published target.
The newest-header SQL is unchanged. The library accepts captured pairs without
keeping a source transaction open during destination index/verification work.

Capture streams each query and enforces row/UTF8 byte budgets before retaining
its results. Paging accounts for previously retained rows; unbounded validation
compares one page at a time against the native rows. Shared immutable row
storage avoids result-vector clones. Defaults bound retained row text to512MiB
and1million rows per endpoint; these limits are not a measured RSS guarantee.
A7 records actual peak RSS/time for its500+ pairs and whole-window range.
