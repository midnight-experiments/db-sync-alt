//! Chain profiles and positional identifiers shared by the store and follower.

pub mod assets;
pub mod cbor;
pub mod checks;
pub mod datum;
pub mod digest;
pub mod ids;
pub mod inventory;
pub mod keep;
pub mod metadata;
pub mod profiles;
pub mod rows;
pub mod transaction;

/// The version shared by the binary and its database metadata.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod capabilities;
