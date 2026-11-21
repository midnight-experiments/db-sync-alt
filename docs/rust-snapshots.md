# Development Rust snapshots

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

After file preflight, restore locks and rechecks its owned empty destination,
then truncates only the twelve empty COPY tables inside the load transaction.
This preserves relation identities, ACLs and allocation sequences; no `CASCADE`
or server setting is used. It permits PostgreSQL's bulk COPY WAL optimization
when the server already uses `wal_level=minimal`; other WAL modes retain their
normal durability policy. Failure rolls back the empty-storage replacement.
Readers of the load tables can wait on these locks; no partial rows are served,
and the gate remains closed until the final verified commit.

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
