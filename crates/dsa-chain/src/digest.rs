//! Canonical content digest, development format v1 (not a released file contract).
//!
//! Each family is a stream of PostgreSQL17 JSONB array text in natural-key order. SQL supplies
//! byte strings as lowercase hex, integral amounts as scale-zero decimal strings, and
//! timestamps as integer microseconds since the PostgreSQL epoch. SQL NULL is
//! JSON null. PostgreSQL's deterministic JSONB text fixes object-key order;
//! arrays retain order and arbitrary JSON numbers retain exact decimal text.
//! Rust hashes text bytes directly, without parsing nested datum/metadata JSON,
//! recursion limits or an f64. Memory holds one supported PostgreSQL row, not a
//! table; no artificial depth/number cap is added to PostgreSQL's own limits.
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
//! name, u64 row count and lowercase 64-byte hex family hash, each length-framed. Empty
//! families participate. The internal family has its own equally framed root.
//! This is independent of row identity allocation and
//! binary COPY/compression ordering. Rule-2 token selection precedes hashing;
//! the digest consumes that selected rowset verbatim, without renumbering.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FamilyDigest {
    pub rows: u64,
    pub sha256: String,
}

pub struct FamilyHasher {
    hash: Sha256,
    rows: u64,
}

fn framed(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

impl FamilyHasher {
    pub fn new(name: &str) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"DSA-family-v1\0");
        framed(&mut hash, name.as_bytes());
        Self { hash, rows: 0 }
    }

    pub fn row(&mut self, canonical_jsonb_text: &[u8]) {
        framed(&mut self.hash, canonical_jsonb_text);
        self.rows += 1;
    }

    pub fn finish(self) -> FamilyDigest {
        FamilyDigest {
            rows: self.rows,
            sha256: format!("{:x}", self.hash.finalize()),
        }
    }
}

pub fn root(families: &BTreeMap<String, FamilyDigest>) -> String {
    let mut hash = Sha256::new();
    hash.update(b"DSA-content-v1\0");
    for (name, family) in families {
        framed(&mut hash, name.as_bytes());
        framed(&mut hash, &family.rows.to_be_bytes());
        // Hash bytes represented by the lowercase SHA-256 hex are unambiguous;
        // the representation is framed exactly like every other root field.
        framed(&mut hash, family.sha256.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
