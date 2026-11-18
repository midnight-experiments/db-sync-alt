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
positive configured limits otherwise remain in force.

Auto positions include consecutive cursor/reference pairs spread across the
whole window, a spanning pair and every epoch boundary. Bounded source reads
also add real transaction pairs for each create/spend unit, auth registration,
mapping deregistration and ICS output class. Both Preprod units receive their
own witnesses. `--max-pairs` controls the uniform sample; up to16 witnesses per
class are additional. These hint reads are counted separately from node queries.
Both registered-candidate SQL variants also execute at every actual epoch-end
header returned by each literal header-query variant, preserving raw results.
The registered-candidate input and actual transaction witness are pinned.
If no transaction with a retained header exists, that probe explicitly reports
hit-unavailable and executes a verified absent32-byte hash rather than calling
zero a successful hit. Event classes with no available events remain quiet. An explicit positions JSON file has `horizon`, `target`
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

For an advancing source, use the normal anchored pair:

```
db-sync-alt convert --network mainnet --database '<destination DSN>' --source '<reader DSN>' --capture-source source.capture.json
db-sync-alt compare --network mainnet --a-capture source.capture.json --b '<converted reader DSN>'
```

Conversion captures all literal source answers, including the unbounded newest
header, in its one source REPEATABLE READ READ ONLY transaction. It commits that
source before file hashing/publication and destination finish. The new file is
bounded, checksum-protected and atomically published before destination serving.
Existing files are refused. If destination finish fails, the file remains a
source observation; it does not claim conversion success. File failure leaves
the staged destination closed. A subsequent conversion uses a new file path.
The capture reader verifies format, rule, profile, catalogue and checksum before
opening B. Positions and the missing-policy probe are carried by the file. No
source DSN, user, hostname or credential path enters the file. The reusable
library still accepts an already-open source transaction for coordinators.

A standalone comparison opens one read-only repeatable snapshot per endpoint.
A fresh source snapshot after advancement must report a real newest-header
difference; that SQL is never rewritten. Source capture files preserve JSON
numbers inside typed JSON/JSONB columns verbatim, since the node can distinguish
`{"int":1}` from `{"int":1.0}`. Only SQL scalar numeric display scale is
normalized. The pinned node type evidence is separate from integer amounts.

Compare and convert default to a3600s overall deadline, configurable with
`--overall-timeout-seconds`. Their source readers set statement/idle limits to60s
only when the inherited value is0; every positive owner limit is preserved and
explicit positive overrides remain connection-local. New sockets default to a
10s connect timeout and30s TCP user timeout, with keepalive idle at most30s.
SIGINT/SIGTERM, deadlines and failures send cancellation to the operation's own
backends through the original TLS configuration before the scoped clients close.
This never cancels another program's query. The cancellation request itself is
bounded by5s per socket, and logs whether it was sent. A7 rehearses release of
this project's read transaction under its actual health stop.

Capture streams each query and enforces row/UTF8 byte budgets before retaining
its results. Paging accounts for previously retained rows; unbounded validation
compares one page at a time against the native rows. Shared immutable row
storage avoids result-vector clones. Defaults bound retained row text to512MiB
and1million rows per endpoint; these limits are not a measured RSS guarantee.
A7 records actual peak RSS/time for its500+ pairs and whole-window range.
