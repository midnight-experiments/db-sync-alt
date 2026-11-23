# Snapshots, restore and verification

## The snapshot file

A snapshot file holds one consistent state of a serving database: a
description and one compressed section per table. The description binds the
network, profile hash, keep-rule version 2, node and tool release, the origin
of the data, the horizon, target and tip, per-family digests, the content and
state digests, the supply of the fixed units, and the size and hash of every
section. Each section is a zstd-compressed PostgreSQL binary COPY stream: the
eleven served tables and the outputs consumed before the horizon. First-served
times and provider call counts belong to an instance and are not in the file.
The format is documented in `dsa-store::snapshot`, the natural-key encodings in
`dsa-chain::digest`.

The content digest covers only what the node reads, by natural key, never by
row ID. It hashes PostgreSQL 17's deterministic JSONB text row by row,
including full datum and metadata JSON and exact wide integers; integral
amounts use scale-zero decimal text. The outputs consumed before the horizon
have their own state digest. The origin is a flat, bounded record: the original
source kind, network, target, newest header and creation time, plus the
digests of the latest snapshot.

`snapshot --file FILE` reads one REPEATABLE READ snapshot without taking the
writer lock and publishes a new file; an existing file is refused. It needs
about twice the final file size in free space while it runs. Temporary files
are removed on error. If the file is published but its directory cannot be
synced, the command reports `snapshot_published_durability_uncertain` with
`published: true`; the file exists and must not be overwritten by a retry.

## Restore

`restore --file FILE [--expect-content-digest DIGEST]` loads a file into an
empty database:

1. The file is opened and checked before the database changes: description,
   format, network, profile hash, keep-rule version, node release, the expected
   content digest if given, the exact file length and every section hash. A
   wrong path or a file of another network, format or rule version leaves the
   database exactly as it was.
2. A blank database receives the closed layout. A database that serves or holds
   a row, or that has an object of its own in `serve`, is refused unchanged. An
   empty layout left by a failed restore or conversion is accepted.
3. One build transaction renames the empty tables' schema `serve` to
   `dsa_retired` and their indexes to `retired_*`, creates new tables in
   `serve`, loads every section (re-hashing the bytes actually read), creates
   the indexes, analyzes, and verifies digests, supply and structure against
   the description. It commits with the gate closed. The public views still
   read the empty tables, so node readers keep receiving SQLSTATE 55000 for the
   whole load, and the node finds exactly one index for each name it looks up.
4. A short publication transaction locks the eleven views without waiting,
   points them at the new tables, drops the empty ones, records the
   first-served time of every height from the horizon to the target, and opens
   the gate. While a reader holds a view lock (an open cursor, or a transaction
   idle after reading a view), publication retries every 100 ms for up to 60
   seconds. A query that arrives during publication waits for it, then reads
   the data.

A failure before the build commits rolls it back. If a restore stops after the
build committed and before publication, the database is not serving and still
answers 55000; the next restore with any valid file undoes the build and loads
again. A restore stopped by a signal before its publishing commit was sent
reports `published: false`; after that commit was sent the outcome is
`unknown`, and `status` and `verify` tell whether it published.

Restore and conversion take the session writer lock, as `grant-reader` does. A
writer that just failed may still be rolling back on the server, so a held lock
is retried for three seconds before `writer_busy`.

Both commands keep PostgreSQL's configured statement and idle-transaction
limits. For a long operation choose them explicitly, for example
`--statement-timeout-seconds 300 --idle-transaction-timeout-seconds 300` (1 to
86,400 seconds, this connection only).

## Verification

`verify` checks a serving database in one REPEATABLE READ snapshot: positional
IDs, referential closure, header continuity from the horizon to the tip, the
token rule, datums and descriptors, the outputs consumed before the horizon,
the supply of every fixed unit, and the 31 indexes (each exists once under its
name among the database's permanent relations, in `serve`, with its exact
definition, valid and ready). It then checks that the database holds exactly
the keep set: no row of any served table lies outside the pruned form of the
keep rule, every height from the horizon to the target has a first-served
time, and every epoch of that range has its parameters. It reports the digests.

Restore and conversion run the same checks except the exactness pass before
they publish, which keeps the extra full read out of the timed load.

## The horizon

Every height from the horizon H to the target T has one first-served time,
set when a restore or conversion publishes and when the follower serves a new
height. `Store::horizon_candidate` returns the lowest height whose first-served
time is younger than the retention, never above T or below H.
`advance_horizon` moves H inside the caller's transaction: it records the kept
outputs whose spend now falls below H, deletes from every served table the rows
the pruned form of the keep rule no longer selects, drops markers and
first-served times that no longer apply, and stores the new horizon. It
requires an epoch on the new horizon's header and refuses
(`horizon_header_incomplete`) if a header that would fall below H lacks an
epoch or a slot.
