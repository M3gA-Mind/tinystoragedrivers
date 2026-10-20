//! In-memory document and stream engine.

mod documents;
mod state;
mod streams;

use std::sync::{Arc, Mutex, MutexGuard};

use tinystoragedrivers_types::{Result, Scope, StorageError};

pub use documents::MemoryDocuments;
use state::DbState;
pub use streams::MemoryStreams;

/// Cloneable in-memory database shared by scope-bound document and stream
/// adapters.
#[derive(Clone)]
pub struct MemoryConnection {
    state: Arc<Mutex<DbState>>,
    clock: crate::Clock,
}

impl MemoryConnection {
    /// Create an in-memory database using the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(crate::system_clock))
    }

    /// Create an in-memory database with an injected Unix-millisecond clock.
    #[must_use]
    pub fn with_clock(clock: crate::Clock) -> Self {
        Self {
            state: Arc::new(Mutex::new(DbState::default())),
            clock,
        }
    }

    /// Bind document and stream ports to `scope` over this same database.
    #[must_use]
    pub fn for_scope(&self, scope: Scope) -> MemoryStorage {
        MemoryStorage {
            db: Arc::new(MemoryDb::new(
                Arc::clone(&self.state),
                Arc::clone(&self.clock),
            )),
            scope,
        }
    }
}

impl Default for MemoryConnection {
    fn default() -> Self {
        Self::new()
    }
}

/// Scope-bound memory document and stream handles.
#[derive(Clone)]
pub struct MemoryStorage {
    db: Arc<MemoryDb>,
    scope: Scope,
}

impl MemoryStorage {
    /// Return this storage's document handle.
    #[must_use]
    pub fn documents(&self) -> Arc<dyn tinystoragedrivers_api::DocumentStore> {
        Arc::new(MemoryDocuments::new(
            Arc::clone(&self.db),
            self.scope.clone(),
            Arc::clone(&self.db.clock),
        ))
    }

    /// Return this storage's stream handle.
    #[must_use]
    pub fn streams(&self) -> Arc<dyn tinystoragedrivers_api::StreamStore> {
        Arc::new(MemoryStreams::new(
            Arc::clone(&self.db),
            self.scope.clone(),
            Arc::clone(&self.db.clock),
        ))
    }
}

pub(crate) struct MemoryDb {
    state: Arc<Mutex<DbState>>,
    clock: crate::Clock,
}

impl std::fmt::Debug for MemoryConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryConnection").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStorage")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for MemoryDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryDb").finish_non_exhaustive()
    }
}

impl MemoryDb {
    fn new(state: Arc<Mutex<DbState>>, clock: crate::Clock) -> Self {
        Self { state, clock }
    }
    fn lock(&self) -> Result<MutexGuard<'_, DbState>> {
        self.state
            .lock()
            .map_err(|_| StorageError::backend("memory storage lock poisoned"))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
