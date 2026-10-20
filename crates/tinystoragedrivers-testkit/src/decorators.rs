//! Small hooks for hand-written port adapters used in failure and race tests.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tinystoragedrivers_types::{ErrorKind, Result, StorageError};

/// A wrapper carrying a delay hook for handwritten test adapters.
///
/// Call [`Self::before`] at the start of an operation whose timing should be
/// slowed. Keeping the wait at the adapter boundary makes it clear which
/// operations participate in a timing test.
#[derive(Clone, Debug)]
pub struct Slow<T> {
    /// Wrapped adapter state.
    pub inner: T,
    delay: Duration,
}

impl<T> Slow<T> {
    /// Wraps `inner` and delays each operation that calls [`Self::before`].
    #[must_use]
    pub fn new(inner: T, delay: Duration) -> Self {
        Self { inner, delay }
    }

    /// Waits for the configured delay before an adapter operation.
    pub async fn before(&self) {
        tokio::time::sleep(self.delay).await;
    }

    /// Runs a future after the configured delay.
    pub async fn run<F: Future>(&self, operation: F) -> F::Output {
        self.before().await;
        operation.await
    }
}

/// A wrapper that returns one conflict the first time its adapter checks it.
#[derive(Clone, Debug)]
pub struct ConflictOnce<T> {
    /// Wrapped adapter state.
    pub inner: T,
    pending: Arc<AtomicBool>,
}

impl<T> ConflictOnce<T> {
    /// Wraps `inner` with one pending conflict.
    #[must_use]
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            pending: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Returns a conflict once, then succeeds for all later checks.
    ///
    /// # Errors
    /// Returns `Conflict` on the first call only.
    pub fn check(&self) -> Result<()> {
        if self.pending.swap(false, Ordering::SeqCst) {
            Err(StorageError::new(
                ErrorKind::Conflict,
                "injected one-time conflict",
            ))
        } else {
            Ok(())
        }
    }

    /// Injects one conflict before running `operation`, then delegates later
    /// calls without interference.
    ///
    /// # Errors
    /// Returns the injected conflict on the first call or the operation's
    /// result on later calls.
    pub async fn call<R>(&self, operation: impl Future<Output = Result<R>>) -> Result<R> {
        self.check()?;
        operation.await
    }
}
