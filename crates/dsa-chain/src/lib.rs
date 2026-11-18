//! Chain profiles and positional identifiers shared by the store and follower.

pub mod digest;
pub mod ids;
pub mod keep;
pub mod profiles;

/// The version shared by the binary and its database metadata.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
