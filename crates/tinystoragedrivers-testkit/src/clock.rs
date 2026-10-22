//! A manually advanced clock for deterministic expiry and lease tests.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Cloneable millisecond clock that advances only when instructed.
#[derive(Clone, Debug)]
pub struct TestClock(Arc<AtomicU64>);

impl TestClock {
    /// Creates a clock at `now_ms`.
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self(Arc::new(AtomicU64::new(now_ms)))
    }

    /// Reads the current time in milliseconds.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }

    /// Sets the clock to an exact timestamp.
    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }

    /// Advances time and returns the new timestamp.
    #[must_use]
    pub fn advance(&self, millis: u64) -> u64 {
        let mut current = self.0.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_add(millis);
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }
}
