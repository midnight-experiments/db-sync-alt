# Compare two databases

`db-sync-alt compare` runs the SQL queries of Midnight node 2.1.101 against
two databases, A and B, at cursor and reference positions across the window,
and reports every row that differs. Use it to check a converted database
against its 0.4.x source, a restored file against the database that wrote it,
or two databases of the same network at the same target.

```sh
db-sync-alt compare --network mainnet --a-capture mainnet.capture.json \
  --b 'host=db.example dbname=db_sync_alt_050 user=db_sync_alt_reader' \
  --b-password-file reader.password --b-tls-root-cert root-ca.crt
```

Each side is read as a reader login, with exactly the node's rights, in one
read-only consistent read. The command writes nothing. Its flags are in
[configuration](configuration.md#compare).

## Side A from a capture

A database that keeps following moves between a conversion and a comparison,
so its answers would differ for that reason alone. `convert --capture-source
FILE` saves the 0.4.x database's answers to the node's queries, read in the
same transaction as the conversion; `compare --a-capture FILE` reads them as
side A. The capture carries its positions, and is refused when its network,
keep rule, profile, query catalogue or checksum does not match. It holds no
connection string, user or host name.

## What is compared

- The queries are the pinned catalogue of the node's SQL statements, extracted
  from the node's sources under `reference/`. The node's genesis subcommands
  are not in it ([limitations](limitations.md)).
- Positions: with `--positions auto`, consecutive cursor and reference pairs
  spread across the whole window (`--max-pairs`, default 500), a pair that
  spans it, every epoch boundary, and real transactions of each kind (cNIGHT
  creates and spends, registrations, deregistrations, ICS outputs). A JSON file
  of positions can be given instead: `horizon`, `target` and `pairs`, each pair
  with a `cursor` and a `reference` of `block` and `tx_index`.
- Every event and state answer must be equal row by row, including NULL, wide
  integers and JSON, in the order the SQL sets; unordered answers are compared
  as multisets. The result columns are compared by name and PostgreSQL type,
  because the node decodes each column by its type.
- Side B repeats every event query with unbounded IDs and must return the same
  events; `--a-verify-unbounded` does the same on side A, which reads A's whole
  history.
- The two internal bound queries of the node may return other IDs on a pruned
  database. Their guarantees are checked on each side, and differing IDs are
  reported as information, not as differences.
- The registered-candidate queries run at the configured address and their
  rows are reported under `unsupported_registered_candidates`; a difference
  there does not fail the comparison ([limitations](limitations.md)).

## The result

`compare` prints one JSON report. `matched: true` and exit status 0 mean that
every supported query returned the same rows on both sides. `matched: false`
and exit status 3 mean at least one difference: the report lists, per query,
each position whose rows differ (or each row present on one side only), up to
`--max-differing-rows` rows per query; `differing_rows` counts them all and
`rows_truncated` says when the list was cut. Column differences are listed in
`column_differences`, unbounded-query differences in `unbounded_differences_b`.
Exit status 1 is a refusal or a query that failed, 75 a database that could not
be reached.

A difference means the two databases do not give the node the same answer at
that position: do not switch a node to B until it is understood.

Measured: on Preprod, the database restored from an older file and followed
over 43,919 real blocks matched the answers captured from the serving 0.4.9
database at the same target: 8,137 cases, 19,478 query executions, no
difference, 41.3 s (2026-11-23, release build of a pre-release commit,
PostgreSQL 17 in a container with 4 CPUs and 4 GiB).
