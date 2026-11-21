# Retained generated restore-only pair (prepared; not dispatched)

The existing `a7-whole-generated` workflow now accepts a manually supplied,
full `restore_pair_candidate` SHA. A push of this preparation does not trigger
qualification. The existing push-on-`config.json` full conversion path is unchanged.
Wait for the A executor's exact reviewed candidate and green normal CI, and the
coordinator's dispatch instruction. No candidate has been selected here.

The predecessor is c384207b9bffaffb7995d26a815bdfb333b97555. Both executables
are built from clean exact public checkouts in the pinned Rust image, identified
by SHA-256, and their actual version/restore/verify/status help is checked before
starting PostgreSQL. No test runner, source population or conversion is involved.

The input is the unchanged generated `converted.snapshot` from run44724041124:
18,523,014 file bytes, SHA-256
`a1643f0c1f1eea0efd18d39d556b2776af107331c0de7b405739fbf1125cf2e1`,
77,564,270 plain COPY bytes in twelve sections. All original artifact manifest
entries and the snapshot descriptor hash are checked. The normal restore checks
the compressed/plain section bindings. The retained descriptor and original
conversion supply the expected eleven family hashes, protected state, positions,
supply and content/state digests. The snapshot remains an output artifact.
If that existing GitHub artifact has expired, stop with the download failure;
reuse the independently sealed portable copy through a separately coordinated
upload. Never regenerate a source merely to satisfy this harness.

Each variant gets a fresh owned PG17.11 Docker container/data volume, one CPU,
1GiB RAM, actual read4MiB/s/write2MiB/s device caps with positive direct-IO
probes, and the existing archival server configuration: shared/maintenance64MiB,
parallel workers0, `wal_level=minimal`, `max_wal_senders=0`, `archive_mode=off`,
fsync/synchronous_commit/full_page_writes on. Default `wal_skip_threshold`,
checkpoint and memory GUCs are read back; none is tuned between variants.
The former hosted128MiB shared buffer/30s checkpoint override is not used.
Read-only observation logging matches the archival launcher: duration250ms,
IO timing, lock waits, and bind-parameter logging disabled. No ports are published.

Private statement/TCP timeouts are600s, idle timeout7200s, finite pair deadline
7200s with bounded command/drain waits. There is no source/reader connection or
change to their caps. The actual CLI's numeric socket limit event is preserved.

For each variant preserve raw status/stdout/stderr before JSON parsing, exact
restore phase events, internal verify, and a separately timed normal `verify`.
Both must match all expected logical fields and publish a serving state. Report
restore plus independent verify against180s even when functionality passes.
Physical measurements include per-device cgroup IO deltas (never summed across
mapped/backing devices), WAL insert/flush LSN, `pg_stat_wal`, checkpointer/IO and
database statistics, PGDATA/WAL allocated bytes and sampled active queries/waits.
Raw PostgreSQL logs and >=250ms statement durations expose expensive verify
queries; shorter executions are not individually timed. No EXPLAIN ANALYZE or
additional copy of verification SQL changes the measured workload.

A bounded final `CHECKPOINT` is measured separately after the two SC commands to
expose delayed writeback. It is outside the180s elapsed number; it cannot make
that criterion pass. Statistics and IO include ordinary server/background work.
WAL stats reset epochs are retained, so incompatible counters cannot silently be
subtracted. LSN/pg_stat_wal are generated-byte counters, not physical disk writes.

This single predecessor-first/candidate-second comparison is not order-balanced
or a cold-cache speedup proof. Fresh PGDATA does not reset the host page cache.
Generated data and hosted storage differ from the archived dataset. No result
proves the original failure cause or actual Mainnet performance. Clean up only
exact owned containers/volumes/network, and retain generated evidence even on
failure. Never upload operator evidence, archives, keys, DSNs or host paths.

PostgreSQL17 documents that minimal-WAL COPY can benefit from CREATE/TRUNCATE in
the same transaction; it also documents the commit-time `wal_skip_threshold`
choice between WAL and file fsync. These justify the proposed experiment, not a
prediction that COPY is the complete restore bottleneck:

- https://www.postgresql.org/docs/17/populate.html#POPULATE-COPY-FROM
- https://www.postgresql.org/docs/17/runtime-config-wal.html#GUC-WAL-SKIP-THRESHOLD
