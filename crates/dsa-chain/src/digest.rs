//! Canonical content digest, development format v1 (not a released file contract).
//!
//! Each family is a stream of JSON arrays in its natural-key order. SQL supplies
//! byte strings as lowercase hex, integral amounts as decimal strings, and
//! timestamps as integer microseconds since the PostgreSQL epoch. SQL NULL is
//! JSON null. Object keys are recursively sorted; arrays retain order. Numbers
//! retain their exact decimal representation, never passing through an f64.
//!
//! Natural keys: block(height,hash); tx(hash); output(creator hash,index);
//! input(spender hash,origin hash,index); asset(creator hash,index,policy,name);
//! datum(hash); epoch_param(epoch); pool(hash); stake(epoch,pool hash);
//! metadata(tx hash,key). Identity IDs and their foreign-key encodings are
//! replaced with these keys. Full supplied payloads remain; no asset is filtered
//! or renumbered. Positional transaction/output coordinates are preserved;
//! input/asset ordinals are included for window rows and omitted for old carry
//! rows as the keep-rule digest specifies. The physical snapshot still preserves
//! every original ID and ordinal, including carry rows.
//!
//! Sparse consumed-before-horizon state is a separate protected family keyed by
//! (creator hash,index); row presence is the bit. Its natural-key state_digest is
//! bound by snapshot integrity separately from the eleven-family node-content
//! digest (FR-044). No unobserved historical spending height must be reproduced.
//! Supply quantities are keyed by exact unit and protected in the description.
//! Operational first-served timestamps, provider call counts, database allocation
//! sequences and provenance do not change logical content.
//!
//! A family SHA-256 hashes `DSA-family-v1\0`, its UTF-8 name framed by a u64
//! big-endian length, then each canonical row framed by its u64 byte length.
//! The root hashes `DSA-content-v1\0` and each lexicographically ordered family
//! name, u64 row count and 32-byte family hash, each field length-framed. Empty
//! families participate. The internal family has its own equally framed root.
//! This is independent of row identity allocation and
//! binary COPY/compression ordering. Q18's projection decision is deliberately
//! upstream: the digest consumes the selected rowset verbatim.
