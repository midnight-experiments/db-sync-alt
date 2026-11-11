# Bounded SQL prerequisites and receipt compatibility

New direct-SQL generations use `acquisition.version=2`. They acquire the complete history before the network's start block using the configured `window_blocks` (integer 1–10,000, default 1,000), then bisect a window on the existing `source_timeout` or `work_unit_too_dense` refusal. A single irreducible block still refuses and cannot publish. The source's 100,000-ID per-family cap, 15-second statement timeout, 1-second lock timeout, 60-second idle timeout, read-only repeatable-read snapshots and anchor checks are unchanged. This does not filter historical data or reset ordinals.

Each inclusive prerequisite chunk uses a distinct `prerequisites/<lo>:<hi>` unit. Its facts and receipt commit in the same destination transaction. Receipts bind the immutable source identity, generation, profile, anchors and acquisition settings. Successful empty windows are also receipted. Resume validates the committed contiguous prefix from 0 before doing further work; gaps, overlap, invalid types/bounds/units or mismatched bindings refuse rather than silently skipping or relabeling data.

A separate `prerequisites/complete` seal is created only after exact coverage through start−1. It includes the count and digest of the canonical ordered chunk receipts. Full chunks without that seal remain incomplete; a completion label without valid full chunks is invalid. Structure verification and SQL finalization independently validate coverage. Fact counts in chunks describe work performed, not necessarily unique semantic facts: dependency closure can legitimately revisit an immutable natural fact.

Crash before commit rolls back both the chunk facts and receipt. Crash after commit preserves both; resume skips its valid prefix and preserves stable fact IDs. Final independent verification still freshly queries the SQL source, checks every expected fact and ordinal, and rejects omitted or extra candidate facts before immutable completion/publication. Candidate inventory is not reused as independent truth.

Incremental children inherit certified prerequisite facts and create no new prerequisite chunks. A start cursor at block 0 has no prerequisite range or receipt. Facts before a cursor's transaction index within the start block retain the existing event/dependency behavior; chunking changes only heights strictly before the start block.

## Compatibility and recovery

**New version 2 SQL generations require the updated reader/service.** Old readers accept only acquisition.version=1 when admitting SQL completion and reject version 2 ready generations. This is an explicit data-receipt compatibility change; deployment/release notes must call it out. Network profiles, fact contract, PostgreSQL schema, native node query interface and SQL adapter identity are unchanged.

For an existing 0.3.0 dataset, install the updated reader/service before creating version 2 generations. Keep matching software and a backup when planning a rollback; the 0.3.0 reader cannot admit new version 2 SQL completions.

A valid version 1 whole-range `prerequisites/complete` receipt remains accepted without rewriting its generation manifest or attestation. Its exact fields are `lo`, `hi`, `read_only`, `rows` and `source_kind`; bounds, types and source are validated. An interrupted version 1 generation with that complete receipt can continue metadata/events and seal under its original identity. Incremental and block-zero work remains resumable. Older admitted pre-adapter histories retain their previous receipt semantics.

A pending version 1 root that needs prerequisites but lacks a valid whole-range receipt refuses `prerequisites_v1_pending_requires_recovery`. Adapter-versioned version 1 prerequisite receipts must contain exactly the five fields listed above; any other field set refuses `prerequisites_receipt_invalid`. Preserve the original 0.3.0 database, backup and matching software. Fresh-destination reacquisition requires the retained independently verified SQL source: after checking capacity and recovery evidence, acquire into a fresh compact destination with that source. If the source is unavailable, preserve the existing database, backup and matching software; this feature provides no source-free recovery. Do not rewrite the immutable manifest, remove fields from a receipt or invent completion evidence.

**Breaking reader requirement:** prerequisite version 2 requires this feature's reader in addition to any base release. It is independent of incremental delta version 2, schema version 4 and backup version 3. A release number alone does not establish support; check the actual reader's admitted SQL acquisition versions before upgrading or rolling back.

## Completion receipt size

The final SQL receipt retains each configured interval for independent verification.
Before generation creation, fact acquisition or range allocation, an arithmetic
upper bound reserves those intervals, the fixed anchors, every required epoch and
the inventory's fixed hash/count widths under the existing 2 MiB receipt limit.
Interval endpoint widths use the largest height, so the bound is conservative.
Small windows remain supported for histories that fit; no network-specific
minimum window is imposed. Adaptive splits do not add final receipt intervals.

`sql_completion_receipt_too_large` means the effective window cannot be admitted.
For a fresh destination choose a larger supported window (at most 10,000) and
retry. On resume the immutable stored window always applies; a CLI override
cannot repair an oversized pending generation created by an earlier writer.
Retain that database and its backup, check capacity, then acquire into a fresh
destination using an admissible window and the verified source. Do not edit
the stored window, discard history or raise the completion-size limit.

## Selecting the relevant history once per pass

The independent address/asset selector covers the whole history: every output holding a profile asset or at
a profile address, every input spending one, and every transaction minting a profile asset. A bootstrap
reads it **once per pass**, not once per window. The prerequisite windows of the main pass read it once,
and the independent verification pass reads it once more, fresh. Each window then takes its own rows. The
selector work of a bootstrap is therefore proportional to the selection plus the windows, whatever
`window_blocks` is.

Every source statement stays bounded:
- The selection is read as id streams of 500 rows per statement.
- Ids are resolved to their block heights in statements of at most 1,000 ids. A statement the source cancels
  is retried as two halves, down to one id.
- An epoch's stake distribution is streamed and summed per pool in the same way.

Nothing is written on the source. The selection is kept in a session-local table of the destination.

What an operator should expect:
- **One read-only snapshot per selection.** On the Mainnet db-sync 13.8 source of 2026-11 (about 2.6 million
  relevant outputs, mostly cNIGHT), with the source limited to 8 MiB/s reads, one selection held one snapshot
  for about 25 minutes. On a live cardano-db-sync, that snapshot holds back vacuum on the source for the same
  time; schedule the bootstrap accordingly. A source with unthrottled storage needs minutes.
- **An interrupted run selects again when it resumes.** The session-local table does not survive the
  process, so a resumed run pays one more selection: about 25 minutes on that source. Committed windows
  are not repeated.
- **Run `ANALYZE` on a restored source first.** A db-sync restored from a snapshot or a selective restore may
  lack planner statistics. Without them PostgreSQL plans full table scans for the selection and the windows.
  A live db-sync is analyzed by autovacuum.

The windows' own statements keep their limits: at most 100,000 ids of one family per window, bisected on
`source_timeout` or `work_unit_too_dense`. If even a single-block window fails, refuse and request a
separately scoped optimization. Fixture results are not Mainnet throughput estimates.
