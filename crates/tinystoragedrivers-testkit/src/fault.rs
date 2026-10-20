//! Deterministic operation failure schedules shared by port decorators.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use tinystoragedrivers_types::{ErrorKind, StorageError};

/// A reusable schedule that injects one typed failure into a matching call.
///
/// Calls are numbered from one. An operation-specific schedule matches only
/// the supplied operation label; an unscoped schedule counts every call.
#[derive(Clone, Debug)]
pub struct FaultPlan {
    inner: Arc<FaultState>,
}

#[derive(Debug)]
struct FaultState {
    calls: AtomicUsize,
    rule: Mutex<Option<FaultRule>>,
}

#[derive(Clone, Debug)]
struct FaultRule {
    at: usize,
    operation: Option<String>,
    kind: ErrorKind,
}

impl FaultPlan {
    /// Creates an inactive failure schedule.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FaultState {
                calls: AtomicUsize::new(0),
                rule: Mutex::new(None),
            }),
        }
    }

    /// Fails the `nth` matching call with `kind`.
    pub fn fail_nth(&self, nth: usize, kind: ErrorKind) {
        self.install(nth, None, kind);
    }

    /// Fails the `nth` call to `operation` with `kind`.
    pub fn fail_operation_nth(&self, operation: impl Into<String>, nth: usize, kind: ErrorKind) {
        self.install(nth, Some(operation.into()), kind);
    }

    /// Clears the active rule and resets the call counter.
    pub fn clear(&self) {
        self.inner.calls.store(0, Ordering::SeqCst);
        if let Ok(mut rule) = self.inner.rule.lock() {
            *rule = None;
        }
    }

    /// Records a call and returns its injected error when the schedule matches.
    ///
    /// # Errors
    /// Returns the scheduled typed error when this call matches the rule.
    pub fn check(&self, operation: &str) -> tinystoragedrivers_types::Result<()> {
        let Ok(rule) = self.inner.rule.lock() else {
            return Ok(());
        };
        let Some(rule) = rule.as_ref() else {
            return Ok(());
        };
        if rule
            .operation
            .as_deref()
            .is_some_and(|expected| expected != operation)
        {
            return Ok(());
        }
        let call = self.inner.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == rule.at {
            Err(StorageError::new(
                rule.kind,
                format!("injected failure for {operation} call {call}"),
            ))
        } else {
            Ok(())
        }
    }

    fn install(&self, at: usize, operation: Option<String>, kind: ErrorKind) {
        self.inner.calls.store(0, Ordering::SeqCst);
        if let Ok(mut rule) = self.inner.rule.lock() {
            *rule = Some(FaultRule {
                at: at.max(1),
                operation,
                kind,
            });
        }
    }
}

impl Default for FaultPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// A value paired with an injectable failure schedule.
///
/// Port decorators use this shared wrapper so multiple primitive handles can
/// observe the same deterministic fault plan.
#[derive(Clone, Debug)]
pub struct Failing<T> {
    /// Wrapped value.
    pub inner: T,
    /// Shared fault schedule.
    pub faults: FaultPlan,
}

impl<T> Failing<T> {
    /// Wraps `inner` with an inactive schedule.
    #[must_use]
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            faults: FaultPlan::new(),
        }
    }

    /// Wraps `inner` with a caller-configured schedule.
    #[must_use]
    pub fn with_plan(inner: T, faults: FaultPlan) -> Self {
        Self { inner, faults }
    }
}
