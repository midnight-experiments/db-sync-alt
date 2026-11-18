//! Snapshot container and restore protocol, development format v1.
//!
//! This format is unreleased; Q18 does not freeze an asset projection or a real
//! network golden digest. A snapshot preserves the complete selected rows and
//! original asset ordinals, independently of how the producer selected them.
//!
//! File: eight-byte magic `DSASNAP\0`, u32 BE format number, u64 BE description
//! byte length, SHA-256(description), then the UTF-8 JSON description. Description
//! fields bind network/profile/node/rule/tool, H/T/P(height/hash/time), retention,
//! provenance, canonical content and protected-state digests, natural-key family
//! counts/hashes, supply
//! quantities, and ordered section names/counts/compressed+plain sizes/hashes.
//! Each section is one independently zstd-compressed PostgreSQL17 binary COPY
//! stream. Sections cover all eleven served tables and sparse internal
//! consumed-before-horizon state. Supply is protected in the description and
//! canonical digest. First-served timestamps are deliberately reset on restore;
//! provider accounting belongs to the instance and is not imported.
//!
//! Snapshot opens exactly one REPEATABLE READ READ ONLY transaction, verifies
//! serving state and integrity in that snapshot, and streams COPY per family to
//! temporary compressed sections beside the requested output. It takes no writer
//! lock. Only after all hashes/sizes are known is a new complete file atomically
//! published; an existing file is never overwritten. Every temporary is removed
//! on error. Compression memory is bounded; whole tables are never buffered.
//!
//! Restore validates the destination before any destructive change: an existing
//! serving/nonempty database is preserved and refused. An empty database gets
//! the layout without secondary indexes and a closed gate; an explicitly owned
//! failed restore may retry. Identity/state are committed closed before ingest.
//! A writer-locked transaction streams bounded decompression into binary COPY,
//! checks exact sizes/hashes/trailing EOF, creates indexes, analyzes, initializes
//! private allocation sequences above imported identity IDs, verifies closure,
//! positional ranges, header continuity, supply, sparse markers and digest, then
//! publishes state/gate in that same transaction. Any failure rolls back imported
//! rows and leaves the committed failed-restore state closed and retryable.
//!
//! Sparse state stores the retained outpoint ID; presence means consumed before
//! H, derived inside the same source snapshot. It stores no spending height. Its
//! separate natural-key state_digest uses creator hash/index instead of numeric
//! ID, preserving the eleven served families' node-content digest definition.
//! Verification rejects missing/non-old outputs or contradictory window spends.
//! It is never exposed through public views, and consumed_by_tx_id stays NULL.
//! This bit survives snapshots so an old spent carry output cannot become an
//! unspent K3 output when its spend is no longer served and H advances again.
