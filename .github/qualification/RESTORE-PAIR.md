# Retained generated restore-only pair (prepared; not dispatched)

The existing `a7-whole-generated` workflow now accepts a manually supplied,
full `restore_pair_candidate` SHA. A push of this preparation does not trigger
qualification. The existing push-on-`config.json` full conversion path is unchanged.
Wait for the A executor's exact reviewed candidate and green normal CI, and the
coordinator's dispatch instruction. No candidate has been selected here.

The separately authorized `owner_work_mem_pair` mode keeps both executables at
the same c384 predecessor (supply that SHA as the candidate input). Fresh targets
receive an explicit owner-only4MiB versus128MiB work_mem setting. The owner reads
back exact GUCs before each restore; all other settings, physical limits and input
bytes stay equal. This is a controlled diagnostic setting variant, not a product
patch or default/cap change. It is still not dispatched. The actual source-free
ABBA verification study is a separate operator-owned experiment.

Run44933813334 earned the4MiB restore and normal verify, then failed only in a
post-verify observation that decoded psql `t/f` as JSON. Its original reports,
timings and artifact seal are retained. `restore_pair_continue_run=44933813334`
validates that exact prior artifact and logical reports and executes only128MiB.
The rebuilt executable must have the same SHA-256 as the original4MiB executable.
The fixed JSON observation is exercised on the fresh empty PG before COPY.
This continuation uses another hosted runner; report it as two observations,
not an isolated paired setting speedup. The first variant's final checkpoint
drain was not measured; do not invent it or repeat an earned baseline to fill it.

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
In the matched product-pair mode both private owners explicitly use128MiB
work_mem. This mode checks that both targets have identical observed GUCs and
device topology on the same runner; it preserves the earlier4/128 observations
as separate evidence. The setting-pair mode still compares4 versus128MiB.
Work_mem is allocated per executor operation,
and hash_mem_multiplier and multiple operators/clients can multiply it;128MiB
does not bound total memory. The1GiB cgroup cap remains enforced and OOM failure
must be reported. No recommendation for a concurrent production setting follows.
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
