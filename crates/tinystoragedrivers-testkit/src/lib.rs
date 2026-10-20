//! Test support for storage-port consumers.
//!
//! Use this crate only from development dependencies. It provides deterministic
//! fault schedules and clocks, plus shared conformance checks for real engines.

pub mod conformance;
mod fault;
pub use conformance::{
    blobs, documents, kv, queue, seal, secrets, semantics, streams, validated, watch,
};
mod memory;
pub use memory::{MemoryBackend, memory_backend, memory_storage};
mod clock;
mod decorators;
mod no_native_ttl;
mod port_adapters;

pub use clock::TestClock;
pub use decorators::{ConflictOnce, Slow};
pub use fault::{Failing, FaultPlan};
pub use no_native_ttl::NoNativeTtl;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "engine_tests.rs"]
mod engine_tests;

#[cfg(test)]
#[path = "decorators_tests.rs"]
mod decorator_tests;
