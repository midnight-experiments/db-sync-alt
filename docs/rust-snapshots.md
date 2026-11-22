# Development Rust snapshots

The reader-compatible cold-restore path in this branch is a prototype. It is not
a qualified release or a claim to the three-minute limit. Stable served parents
and their startup indexes stay available while one private child per family is
loaded and independently verified. The verified build commits with the gate
closed. Publication changes only identity/position/receipt rows; it performs no
public-view or parent-table binding DDL. A stalled row update therefore leaves
new node readers able to evaluate the old closed gate.

`status` reports a destination/input-bound restore receipt. A `ready` build may
retry the exact input, reverify its actual rows and finish without another COPY.
A different input or destination binding is refused. Invalid input rolls back
the build; completed or unrelated nonempty targets remain refused. No retired
generation is created: original empty parents retain the stable interfaces and
exactly one child holds each family's rows. A failed uncommitted build leaves no
private objects; a verified durable build is retained for owned recovery.

Restored heights in the initial horizon-to-target cohort are protected until a
post-publication transaction sets their immutable retention anchor. A delayed
publication never consumes retention during private build time. A lost settlement
acknowledgement does not reset the anchor. Existing databases and later heights
keep their ordinary per-height clocks; consumers must use
`dsa_store::storage::first_served_at`. The anchor is conservative after first
visibility, not an exact commit timestamp. Settlement counts in restore time.

Internal insertion/COPY writers must resolve the single authoritative relation
with `dsa_store::storage::write_relation`; direct insertion into the empty parent
is rejected with `0A000` (distinct from a reader's closed-gate `55000`). Known
earlier prototype guard bodies remain readable/resumable without rewriting them.
PostgreSQL parent constraints do not enforce child uniqueness, and
parent `ON CONFLICT` cannot arbitrate a child's index. `following_transaction`
recovers an interrupted retention settlement before follow/prune work. The
generated `restore_writer_probe` example exercises this actual API with
RETURNING/ON CONFLICT and rolled-back tuples. Follower integration of these
boundaries is still required and is not claimed by the prototype.

Both children and the served inheritance parents are analyzed before build
commit, so node joins receive inherited statistics. Resuming an older owned
build repairs missing inherited statistics without another COPY. A future
follower must maintain these parent statistics as child facts change.

An owner may call `Store::settle_restored_retention` to finish a published
cohort's pending clock even after a later check closed the gate. It preserves
the gate and all data, and never republishes a READY build or refreshes a settled
anchor. `following_transaction` performs pending settlement before its gate
check, so a closed gate cannot permanently strand the clock. The generated API
probe exposes this method with `--settle-only`; it is not a new operator CLI.

Wire format1 carries keep-rule version2 and preserves the selected payloads
and original pre-filter asset ordinals. Rule1 files and databases are refused;
regenerate by converting a genuine legacy schema5 source. No real-network
golden digest is fixed.
The format and natural-key encodings are documented in `dsa-store::snapshot`
and `dsa-chain::digest`.

`snapshot --file FILE` reads one repeatable PostgreSQL state and publishes a new
file without taking the writer lock. Choose a filesystem with roughly twice the
final compressed file size available: compressed staging sections coexist with
the final temporary file. Temporary files are removed on error. An existing
output is refused. If publication succeeds but the parent directory cannot be
synced, the command reports `snapshot_published_durability_uncertain` with
`published: true`: the file exists, but crash durability is uncertain. Do not
overwrite it as an automatic retry.

`restore --file FILE` refuses a serving or nonempty destination without changing
it. An empty or explicitly failed restore stays gated until the entire file,
logical content, references, supply and protected internal state verify. It
checks every compressed section and exact file length before loading, then
checks the compressed bytes actually consumed during COPY again. A failed load
rolls back and may retry the original valid file in the same destination.

After file preflight, restore locks and rechecks its owned empty destination.
It reports `restore_load_mode` with one of two automatic paths. Before any
non-owner has gate EXECUTE and while the gate still belongs to the database
owner, `isolated_bulk` truncates only the twelve empty COPY tables inside the
load transaction. This permits PostgreSQL's bulk COPY WAL optimization when the
server already uses `wal_level=minimal`; other WAL modes keep their normal WAL
policy. Failure rolls back the empty-storage replacement.

If gate access has been granted to another role, PUBLIC or a group, or ownership
differs, the prototype `reader_compatible` retains the empty stable parents and
their matching startup indexes, builds missing prerequisites, and loads the
fresh private children. Conflicting or invalid canonical indexes are refused as
`destination_index_mismatch`. A legitimate node reader keeps getting SQLSTATE
55000 through private load/verification/commit and a stalled gate publication,
including previously prepared queries. Complete checked rows become public
only at the later gate commit. Both paths preserve stable public relation
identities, ACLs, imported IDs and private allocation sequences; no `CASCADE`,
server setting or reader privilege change is used. The compatible prototype's
new internal write-routing and retention contracts are described above. Full
timing still needs measurement rather than a COPY/WAL-only prediction.

Product `grant-reader` takes the same session writer lock and cannot attach a
reader during restore. Concurrent manual owner DDL or GRANT does not follow that
protocol. Restore refuses a serving/nonempty target before either load path.
If gate access changed while waiting for load locks, restore refuses as
`gate_access_changed` before COPY; retry selects the path for the new grants.

Both commands preserve PostgreSQL's configured statement and idle transaction
timeouts by default. File preflight runs outside the load transaction. Each COPY,
index build and verification statement must finish within the owner's limits.
For a known long operation an operator can explicitly use
`--statement-timeout-seconds 300 --idle-transaction-timeout-seconds 300`;
each option accepts 1–86,400 seconds and applies only to this connection.
The software does not silently disable timeouts or alter role/server defaults.
Choose limits appropriate for the file and storage; real-network size and
restore-time claims require the conversion measurements.

The content digest hashes PostgreSQL 17's deterministic JSONB text one row at a
time, including full datum/metadata JSON and exact wide integers. Integral
numeric-domain amounts use scale-zero decimal text. Flat bounded provenance
retains the original source kind, network, target, newest header and creation
time; repeated restores replace only the latest snapshot links instead of
embedding earlier descriptions. Sparse consumed-before-horizon markers have
their own natural-key state digest and remain internal; public output payloads
are unchanged.
