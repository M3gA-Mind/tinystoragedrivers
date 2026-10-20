//! Test support for storage-port consumers.
//!
//! Use this crate only from development dependencies. It provides deterministic
//! fault schedules and clocks, plus shared conformance checks for real engines.

pub mod conformance;
mod fault;
pub use conformance::{documents, queue, semantics, streams, validated, watch};
mod clock;
mod no_native_ttl;

pub use clock::TestClock;
pub use fault::{Failing, FaultPlan};
pub use no_native_ttl::NoNativeTtl;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "engine_tests.rs"]
mod engine_tests;
