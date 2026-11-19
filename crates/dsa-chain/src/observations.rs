//! Candidate facts are produced only by the successful sealed row projection.
//! They are not committed observations until the SQL publication succeeds.
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Shape {
    StoredPointerAddress,
    StoredLargeDatum,
    StoredBridgeMetadata,
}
impl Shape {
    pub const ALL: [Self; 3] = [
        Self::StoredPointerAddress,
        Self::StoredLargeDatum,
        Self::StoredBridgeMetadata,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StoredPointerAddress => "stored_pointer_address",
            Self::StoredLargeDatum => "stored_large_datum",
            Self::StoredBridgeMetadata => "stored_bridge_metadata",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    BlockCbor,
    Datum(String),
    OutputFallback(String),
    HeldInline,
}
#[derive(Clone, Debug)]
pub struct Fact {
    pub shape: Shape,
    pub transaction_hash: String,
    pub transaction_index: u16,
    pub output_index: Option<u16>,
    pub datum_hash: Option<String>,
    pub raw_datum_bytes: Option<usize>,
    pub metadata_key: Option<u64>,
    pub source: Source,
}
