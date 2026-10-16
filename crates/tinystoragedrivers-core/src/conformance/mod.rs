//! The behavioral suite every driver must pass.
//!
//! A driver's tests call [`run`] with a freshly opened backend. Each check uses
//! collection, stream and key names unique to the run, so the suite can point
//! at a long-lived database (a shared `MongoDB` in CI) without cleanup races.
//! Checks for an optional [`Capability`] assert the documented
//! [`ErrorKind::Unsupported`] when the driver lacks it.
//!
//! Failures panic with a message naming the check, which is how a Rust test
//! reports them.

#![allow(
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    reason = "the conformance suite reports a failed check by panicking inside the driver's test"
)]

mod blobs;
mod documents;
mod fencing;
mod isolation;
mod ledger;
mod maintenance;
mod purge;
mod purge_history;
mod schema;
mod semantics;
mod streams;
mod validated;
mod watch;

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::error::{ErrorKind, Result};
use crate::scope::Scope;

/// Run the port checks (documents, validated batches, watch, streams, blobs)
/// against one already-scoped handle.
///
/// This is the part of [`run`] that needs nothing from the backend itself, so
/// a client that cannot choose scopes, databases or fences (a storage-bus
/// client bound by its host) runs it directly. The scope, database, purge and
/// fencing checks stay in [`run`].
pub async fn run_ports(storage: &ScopedStorage) {
    documents::run(storage).await;
    ledger::run(storage).await;
    semantics::run(storage).await;
    schema::run(storage).await;
    validated::run(storage).await;
    watch::run(storage).await;
    streams::run(storage).await;
    blobs::run(storage).await;
}

/// Run every check against `backend`.
///
/// The backend must accept [`Scope::local`] and at least one other scope
/// (`conformance-b`) unless `single_scope` is set, in which case the isolation
/// checks are skipped (a single-operator SQLite file).
///
/// # Panics
///
/// On the first check the driver fails, naming it. That is the suite's
/// purpose: it is called from a driver's tests.
pub async fn run(backend: &dyn StorageBackend, single_scope: bool) {
    let local = backend
        .for_scope(&Scope::local())
        .expect("for_scope(local) must succeed");
    run_ports(&local).await;
    if single_scope {
        let other = Scope::new("conformance-b").expect("valid scope");
        assert!(
            backend.for_scope(&other).is_err(),
            "single-scope backend accepted a foreign scope"
        );
    } else {
        isolation::run(backend).await;
        validated::isolation(backend).await;
        purge::run(backend).await;
        purge_history::run(backend).await;
        maintenance::run(backend).await;
    }
    isolation::databases(backend).await;
    fencing::run(backend, single_scope).await;
}

/// A name unique to this process and call, safe as a collection, stream or
/// blob key segment.
fn unique(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    // `RandomState` is seeded from the OS per process, so two processes with
    // the same PID (separate containers) still get different names.
    let salt = RandomState::new().build_hasher().finish();
    format!(
        "conf_{label}_{salt:016x}_{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Unwrap a port result, naming the check on failure.
fn ok<T>(result: Result<T>, check: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{check}: unexpected error {error}"),
    }
}

/// Assert a port result failed with `kind`.
fn fails<T: std::fmt::Debug>(result: Result<T>, kind: ErrorKind, check: &str) {
    match result {
        Ok(value) => panic!("{check}: expected {kind}, got Ok({value:?})"),
        Err(error) => assert_eq!(error.kind(), kind, "{check}: {error}"),
    }
}

/// Assert a listing came back empty, showing what it held otherwise.
fn empty<T: std::fmt::Debug>(items: &[T], check: &str) {
    assert_eq!(items.len(), 0, "{check}: expected nothing, got {items:?}");
}

/// Whether the handles' driver claims `capability`.
fn has(storage: &ScopedStorage, capability: Capability) -> bool {
    storage.documents().capabilities().contains(capability)
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
