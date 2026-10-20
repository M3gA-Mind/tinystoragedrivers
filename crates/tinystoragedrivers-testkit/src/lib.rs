//! Test support for storage-port consumers.
//!
//! Use this crate only from development dependencies. It provides deterministic
//! clocks and shared document, stream, validation, and watch checks.

mod conformance;
pub use conformance::{documents, seal, semantics, streams, validated, watch};
mod clock;

pub use clock::TestClock;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "engine_tests.rs"]
mod engine_tests;
