# Compare the node's database answers

`db-sync-alt compare --network mainnet --a '<reader DSN>' --b '<reader DSN>' --positions auto --horizon-block H`
compares the SELECT catalogue of the pinned Midnight 2.1.101 node sources. Both
connections must have the exact public reader grant. The command executes no
index creation, DDL or source writes. Missing startup indexes fail the comparison.

The catalogue (`crates/dsa-store/catalogue.json`) and the assembled bridge
statement (`crates/dsa-store/bridge.json`) are extracted from the vendored node
files under `reference/`; a test requires both files to equal a fresh extraction.
Every SQL-looking string literal of a vendored file is either catalogued or
listed in `crates/dsa-store/sql-outside-catalogue.json` with its reason, and the
reference check of the CI workflow requires the same of every other node source
file. That list names the seven statements of the node's genesis subcommands
(`generate-ics-genesis`, `generate-reserve-genesis`, `generate-genesis-config`,
`verify-cardano-tip-finalized` and the five `verify-*-auth-script` commands).
They read headers and unspent outputs at an arbitrary reference block, which a
pruned database answers incompletely without an error, so these subcommands are
unsupported; `db-sync-alt status` prints them under `unsupported_node_statements`.

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
own witnesses. `--max-pairs` controls the uniform sample; up to 16 witnesses per
class are additional. These hint reads are counted separately from node queries.
Both registered-candidate SQL variants also execute at every actual epoch-end
header returned by each literal header-query variant, preserving raw results.
The registered-candidate input and actual transaction witness are pinned.
If no transaction with a retained header exists, that probe explicitly reports
hit-unavailable and executes a verified absent 32-byte hash rather than calling
zero a successful hit. Event classes with no available events remain quiet. An explicit positions JSON file has `horizon`, `target`
and a `pairs` array, each containing `cursor` and `reference` objects with
`block` and `tx_index`. Explicit pairs must stay within the window and order
cursor before reference. All catalogue queries also receive hit/miss inputs;
a deterministic 28-byte policy is checked absent on both anchored readers
(with retries on collisions). Its value and actual hit/miss row counts are
reported. Independently exported captures must pin the same missing policy
through `Options::missing_policy`; differing probes are refused. The original 26
gated statements/52 executions are accompanied by three
read-only catalogue probes.

Every supported event and state answer is exact, including NULL, wide integers and JSON,
with the SQL's outer ordering preserved. Unordered results use multisets,
including duplicate multiplicities. Only the two internal bounds helpers may
report valid differing IDs as information; unsupported registered-candidate observations use a separate coverage label. Their before/after anchors and
related output/asset/input bounds are verified independently at each database.
The actual `resolve_ident` lookup IDs are exact. `--verify-unbounded` additionally
checks event answers with bounds 0..i64::MAX; use this on generated scratch data
or under an explicit source-access budget. It adds reads.

JSON stdout includes the pinned catalogue, selected units, positions, settings,
query counts, actual per-query/unit row-hit coverage, both destination-order
and unbounded-check flags, and the first differing rows for every mismatch.
Acceptance requires destination B's chain-order check. Source A remains
unscanned globally. A pruned asset table may have leading/interior ordinal gaps;
order verification does not infer dense rank from filtered facts. First-row
previews are capped at 4,096 characters, with full-row SHA256 and an explicit
truncation flag. Whole rows are still compared exactly. Exit 0 means supported-query parity, 2 means a supported answer mismatch and 1 means a refusal or query failure.
Rule 1 pruned stores and mismatched rule declarations/identity are refused.
Genuine legacy schema 5 readers remain supported. Preprod's empty-name cNIGHT
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

Compare and convert default to a 3,600 s overall deadline, configurable with
`--overall-timeout-seconds`. Their source readers set statement/idle limits to 60 s
only when the inherited value is 0; every positive owner limit is preserved and
explicit positive overrides remain connection-local. New sockets default to a
10 s connect timeout and 30 s TCP user timeout, with keepalive idle at most 30 s.
SIGINT/SIGTERM, deadlines and failures send cancellation to the operation's own
backends through the original TLS configuration before the scoped clients close.
This never cancels another program's query. The cancellation request itself is
bounded by 5 s per socket, and logs whether it was sent.

Capture streams each query and enforces row/UTF8 byte budgets before retaining
its results. Paging accounts for previously retained rows; unbounded validation
compares one page at a time against the native rows. Shared immutable row
storage avoids result-vector clones. Defaults bound retained row text to 512 MiB
and 1 million rows per endpoint; these limits are not a measured memory
guarantee.

Node 2.1.101 supplies an empty registered-candidate list during authority selection.
Registered-candidate results in operator RPCs and CLI are unsupported. Both literal
registered-address query variants still execute, and the report retains their full
raw observations and differences with `unsupported_registered_candidates` coverage.
Only that configured-address scope is nonfatal. Other addresses, RPC queries,
permissioned candidates, identifiers and every event/state query keep their exact
checks. Helper ID information remains separate. A node release enabling
registered candidates requires full address coverage under a new keep-rule version.

Capture files of observation format 2 are also read: their omitted candidate
input metadata is recovered only for its known initial and epoch call sites.
Their raw rows and checksums are unchanged; the report states the coverage.
