//! Reusable behavioral checks for individual storage ports.

#![allow(
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    reason = "conformance failures are reported by assertions"
)]

use std::fmt::Debug;

use tinystoragedrivers_types::{Capability, ErrorKind, Result};

pub mod documents;
pub mod queue;
pub mod streams;
pub mod validated;

fn unique(label: &str) -> String {
    format!("{label}_{}", ulid::Ulid::new().to_string().to_lowercase())
}

fn ok<T>(result: Result<T>, check: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{check}: unexpected error {error}"),
    }
}

fn fails<T: Debug>(result: Result<T>, kind: ErrorKind, check: &str) {
    match result {
        Ok(value) => panic!("{check}: expected {kind}, got Ok({value:?})"),
        Err(error) => assert_eq!(error.kind(), kind, "{check}: {error}"),
    }
}

fn empty<T: Debug>(items: &[T], check: &str) {
    assert_eq!(items.len(), 0, "{check}: expected no items, got {items:?}");
}

fn has(capabilities: tinystoragedrivers_types::Capabilities, capability: Capability) -> bool {
    capabilities.contains(capability)
}
