# Snapshot files

A snapshot file holds one whole database: every row the node can read, the
outputs spent before the horizon that the keep rule needs, and a description
that binds them. A new database starts from one, and a pruned database is
backed up as one: it cannot be rebuilt from the provider's history at a
practical cost.

## Where files come from

- **The service's daily files.** With `--snapshot-dir`, `run` writes one file a
  day and keeps the newest few ([operations](operations.md#daily-snapshot-files)).
- **The releases.** One file per network is attached to each release, with its
  content digest in the release notes.
- **`db-sync-alt snapshot`** on any serving database.

Anyone with a database of the same network can check a file: restore it and
compare its content digest with the digest of their own database at the same
blocks, or run [`compare`](comparison.md) against it.

## What a file contains

The file starts with the bytes `DSASNAP\0`, the format number (1), the length
and SHA-256 of the description, and the description in JSON. Then follow one
section per table: the eleven served tables and the outputs spent before the
horizon, each a zstd-compressed PostgreSQL binary COPY stream.

The description holds:

- the network, the profile hash, the keep-rule version (2), the node release
  (`2.1.101`), the database format (1) and the version of db-sync-alt that
  wrote it;
- the origin of the data, with the target and tip of the database it came
  from;
- the horizon, target and tip, each with height, hash and time, and the
  retention;
- a digest per table, the content digest, the state digest, and the recorded
  supply of each fixed-supply asset;
- for each section, its table, row count, compressed and plain size and
  SHA-256.

First-served times and provider call counts belong to one database and are not
in the file: a restore counts every block of the window as first served at the
restore.

The content digest covers what the node can read, by natural key, never by row
ID. A file from a followed database and a restore of that file give the same
content digest at the same horizon, target and tip. The
state digest covers the outputs spent before the horizon.

Measured sizes, from conversions of the real databases with a pre-release
build:

| File | Horizon | Target | Size |
| --- | ---: | ---: | ---: |
| Mainnet, converted 2026-11-21 | 14,130,822 | 14,160,893 | 32,877,530 bytes |
| Preprod, converted 2026-11-22 | 5,414,627 | 5,441,843 | 1,516,541 bytes |

## Restore

```sh
db-sync-alt restore --network mainnet \
  --database 'host=db.example dbname=db_sync_alt user=db_sync_alt_owner' \
  --password-file owner.password --tls-root-cert root-ca.crt \
  --file mainnet-<target>-<digest>.snapshot \
  --expect-content-digest <the file's content digest>
```

Take the content digest from where the file was published (the release notes,
or `integrity.snapshot` of the database that wrote it), not from the file
itself.

`run --snapshot FILE --expect-content-digest DIGEST` does the same on its first
start. A restore:

1. Opens the file and checks it before the database changes: the description,
   the format, the network, the profile hash, the keep-rule version, the node
   release, `--expect-content-digest` when given, the file's exact length and
   every section's SHA-256. A wrong path or a file of another network, format
   or rule version leaves the database as it was.
2. Takes the writer lock. The database must be blank (the layout is created)
   or hold the empty layout that a failed restore leaves. A
   database that serves, holds a row, or has an object of its own in schema
   `serve` is refused unchanged (`destination_not_empty`).
3. Builds the tables in one transaction: loads every section, re-hashing the
   bytes actually read, creates the indexes, analyzes, and checks the digests,
   the supply and the structure against the description. The gate stays
   closed: a node that connects meanwhile gets SQLSTATE 55000 at once, and
   finds exactly one index for each name it looks up.
4. Publishes in a short second transaction: points the node's views at the new
   tables, records the first-served time of every block from the horizon to
   the target, and opens the gate. While a reader holds a view (an open cursor
   or an idle transaction), publication retries every 100 ms for up to 60
   seconds.

If a restore stops after the build and before publication, the database is not
serving and still answers 55000; the next restore, with the same or another
valid file, undoes the build and loads again. A restore stopped by a signal
reports `"published": false` before its publishing commit was sent and
`"outcome": "unknown"` after: run `status` and `verify` to see which state it
reached.

Measured restores, with the content digest equal to the recorded one each
time. PostgreSQL 17 ran in a container with 4 CPUs, 4 GiB and NVMe writes
capped at 48 MiB/s, on a 16-core x86-64 Linux host:

| File | Build | Date | Restore | Database after the restore |
| --- | --- | --- | ---: | ---: |
| Mainnet, target 14,160,893 | release, pre-release commit | 2026-11-23 | 19.9 s | 194 MB |
| Preprod, target 5,397,924 | release, pre-release commit | 2026-11-23 | 0.70 s | 18 MB |

After a restore the database follows from the file's target: a file that is a
day old costs a day of catching up ([provider](provider.md#catching-up)).

## Write a file

```sh
db-sync-alt snapshot --network mainnet --database '<connection string>' \
  --password-file owner.password --file mainnet.snapshot
```

`snapshot` reads one consistent state of a serving database in a single
read-only transaction, without the writer lock, so updates continue. It checks
the state as a restore does, writes each section into a temporary file beside
the output, and publishes the complete file under its name. An existing file
is refused. It needs free space for about twice the file's size while it runs,
and removes its temporary files on any error. If the file was published but its
directory could not be synced, the report is
`snapshot_published_durability_uncertain` with `"published": true`: the file
exists; do not write over it.

## Verify

```sh
db-sync-alt verify --network mainnet --database '<connection string>' \
  --password-file owner.password
```

`verify` checks a serving database in one consistent read: the positional IDs,
that every row refers to rows that exist, the headers from the horizon to the
tip without a gap, the asset rule, the datums and asset descriptors, the
outputs spent before the horizon, the supply of every fixed-supply asset, and
the indexes (each exists once under its name, in `serve`, with its exact
definition, valid and ready). It then checks that the database holds exactly
what the keep rule selects, that every block from the horizon to the target
has a first-served time, and that every epoch of the window has its
parameters. It prints the digests and exits 0, or names the first fault and
exits 3.

A restore runs the same checks before it publishes, except the last three,
which read the whole database again.

Measured on 2026-11-23 with an unoptimised build, on the PostgreSQL container
above: `verify` took 9.4 s on the restored Mainnet file and 0.33 s on the
restored Preprod file.
