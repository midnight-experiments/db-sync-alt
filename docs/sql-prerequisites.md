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

A pending version 1 root that needs prerequisites but lacks a valid whole-range receipt refuses `prerequisites_v1_pending_requires_recovery`. Adapter-versioned v1 receipts from earlier writers with an extra `snapshot` field intentionally refuse `prerequisites_receipt_invalid`, even when the extra value looks valid. They are not silently treated as the supported five-field form. Preserve the original 0.3.0 database, backup and matching software. Fresh-destination reacquisition requires the retained independently verified SQL source: after checking capacity and recovery evidence, acquire into a fresh compact destination with that source. If the source is unavailable, preserve the existing database, backup and matching software; this feature provides no source-free recovery. Do not rewrite the immutable manifest, remove fields from a receipt or invent completion evidence.

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

## Performance remains a measured gate

The global independent address/asset `MATERIALIZED` selection is unchanged. More windows can repeat its fixed cost, so chunking is not proof that a real Mainnet unit fits its deadline. After the actual source is loaded, indexed and analyzed, benchmark normal and independent selectors/facts in representative prerequisite/event windows, preserving all timeouts/caps and independent semantics. Select an admitted supported window before starting a generation; resume retains its stored setting. If even a single-block selector fails because global materialization dominates, refuse and request a separately scoped optimization. Fixture results are not Mainnet throughput estimates.
