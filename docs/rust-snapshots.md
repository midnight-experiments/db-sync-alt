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

After file preflight, restore locks and rechecks its owned empty destination.
It reports `restore_load_mode` with one of two automatic paths. Before any
non-owner has gate EXECUTE and while the gate still belongs to the database
owner, `isolated_bulk` truncates only the twelve empty COPY tables inside the
load transaction. This permits PostgreSQL's bulk COPY WAL optimization when the
server already uses `wal_level=minimal`; other WAL modes keep their normal WAL
policy. Failure rolls back the empty-storage replacement.

If gate access has been granted to another role, PUBLIC or a group, or ownership
differs, `reader_compatible` retains empty storage and matching secondary indexes,
builds missing indexes and uses locks compatible with SELECT. Conflicting or
invalid named indexes are refused as `destination_index_mismatch`. A legitimate
node reader keeps getting SQLSTATE55000 while loading and verification run,
including queries prepared earlier; complete checked rows appear only at commit.
This path writes normal row WAL and may take longer. Both paths preserve
relation identities, ACLs, private allocation sequences and atomic publication;
no `CASCADE`, server setting or reader privilege change is used.

Product `grant-reader` takes the same session writer lock and cannot attach a
reader during restore. Concurrent manual owner DDL or GRANT does not follow that
protocol. Restore refuses a serving/nonempty target before either load path.
The180-second target applies to both modes; a measured miss remains a miss.

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
