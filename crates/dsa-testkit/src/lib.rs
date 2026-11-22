//! Test support shared by the workspace's tests: a builder for Conway
//! transactions whose hash is computed from the body, and a fake provider
//! that serves a scripted chain with switchable faults. Never a dependency
//! of the binary.

pub mod chain;
pub mod server;
pub mod tx;
