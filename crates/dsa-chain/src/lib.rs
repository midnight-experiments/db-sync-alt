//! Chain profiles, positional identifiers, transaction decoding and the
//! one-pass block function shared by the store and the follower.

pub mod assets;
pub mod block;
pub mod capabilities;
pub mod cbor;
pub mod datum;
pub mod digest;
pub mod ids;
pub mod keep;
pub mod metadata;
pub mod observation;
pub mod profiles;
pub mod transaction;

/// The version shared by the binary and its database metadata.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
