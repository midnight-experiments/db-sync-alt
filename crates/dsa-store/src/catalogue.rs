//! The SQL of the pinned node, as committed in `catalogue.json` and
//! `bridge.json`, and the node SQL the catalogue does not hold, listed with
//! its reason in `sql-outside-catalogue.json`. The test `tests/catalogue.rs`
//! extracts all three from the vendored node sources and requires the
//! committed files to equal the extraction.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::LazyLock};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Source {
    pub file: String,
    pub line: usize,
    pub function: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Statement {
    pub id: String,
    pub name: String,
    pub sql: String,
    pub sha256: String,
    pub kind: String,
    pub group: String,
    pub parameters: Vec<String>,
    pub sources: Vec<Source>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Catalogue {
    pub node_commit: String,
    pub files: BTreeMap<String, String>,
    pub entries: Vec<Statement>,
    pub candidate_addresses: BTreeMap<String, String>,
}

/// The node's bridge statement for enabled inputs, a block checkpoint and no
/// row limit, assembled from its QueryBuilder template. `{}` marks the block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bridge {
    pub source: Source,
    pub sql: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// A vendored template assembled into a compared statement.
    Assembled,
    /// Issued on the node's db-sync connection by a subcommand that a pruned
    /// database answers incompletely.
    Unsupported,
    /// Issued against a database that is not cardano-db-sync.
    OtherDatabase,
}

/// One SQL literal of the node sources that the catalogue does not hold.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Outside {
    pub file: String,
    pub line: usize,
    pub sha256: String,
    pub class: Class,
    pub subcommands: Vec<String>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OutsideCatalogue {
    pub node_commit: String,
    pub statements: Vec<Outside>,
}

/// The committed catalogue text. Source captures record its SHA-256.
pub const TEXT: &str = include_str!("../catalogue.json");
pub const BRIDGE_TEXT: &str = include_str!("../bridge.json");
pub const OUTSIDE_TEXT: &str = include_str!("../sql-outside-catalogue.json");

static CATALOGUE: LazyLock<Catalogue> =
    LazyLock::new(|| serde_json::from_str(TEXT).expect("committed catalogue"));
static BRIDGE: LazyLock<Bridge> =
    LazyLock::new(|| serde_json::from_str(BRIDGE_TEXT).expect("committed bridge statement"));
static OUTSIDE: LazyLock<OutsideCatalogue> =
    LazyLock::new(|| serde_json::from_str(OUTSIDE_TEXT).expect("committed outside list"));

pub fn catalogue() -> &'static Catalogue {
    &CATALOGUE
}

pub fn sha256() -> String {
    format!("{:x}", Sha256::digest(TEXT))
}

pub fn bridge() -> &'static Bridge {
    &BRIDGE
}

/// The bridge statement with its checkpoint block; the integer is the only
/// value inserted into the text, as in the node.
pub fn bridge_sql(block: i32) -> String {
    BRIDGE.sql.replacen("{}", &block.to_string(), 1)
}

pub fn outside() -> &'static OutsideCatalogue {
    &OUTSIDE
}

/// The node statements a pruned database answers incompletely, with the node
/// subcommands that issue them.
pub fn unsupported() -> impl Iterator<Item = &'static Outside> {
    OUTSIDE
        .statements
        .iter()
        .filter(|statement| statement.class == Class::Unsupported)
}
