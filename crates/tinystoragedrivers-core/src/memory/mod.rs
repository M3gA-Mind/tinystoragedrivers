//! The in-memory driver: the reference implementation of every port.
//!
//! It provides every [`Capability`](crate::Capability), including
//! [`Maintenance`](crate::Capability::Maintenance) (it has nothing to reclaim,
//! so `maintain` and `checkpoint` succeed and report zero),
//! [`Fencing`](crate::Capability::Fencing) on every write (the fence is
//! checked under the same lock as the write), keeps nothing across process
//! restarts, and is what tests and stateless embedders run on. Because
//! its semantics are the simplest correct reading of the port docs, the
//! conformance suite is written against it first and every other driver must
//! agree with it.

mod blobs;
mod documents;
mod purge;
mod state;
mod streams;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{
    Checkpoint, MaintenanceBudget, MaintenanceReport, ScopedStorage, StorageBackend,
    validate_database,
};
use crate::capabilities::Capabilities;
use crate::error::{Result, StorageError};
use crate::fence::Fence;
use crate::scope::Scope;

pub use blobs::MemoryBlobs;
pub use documents::MemoryDocuments;
pub use streams::MemoryStreams;

use state::DbState;

/// Milliseconds since the Unix epoch. The memory driver reads time through
/// this so tests can drive document expiry deterministically.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// One in-memory database.
#[derive(Debug, Default)]
pub(crate) struct MemoryDb {
    state: Mutex<DbState>,
    gate: Arc<Mutex<()>>,
}

impl MemoryDb {
    fn lock(&self) -> Result<MemoryGuard<'_>> {
        let gate = self
            .gate
            .lock()
            .map_err(|_| StorageError::backend("memory root lock poisoned"))?;
        let state = self.state.lock().map_err(|_| {
            StorageError::backend("memory storage lock poisoned by a panicked writer")
        })?;
        Ok(MemoryGuard { state, _gate: gate })
    }
    fn guard(&self, state: &DbState, fence: Option<&MemoryFence>, now: u64) -> Result<()> {
        match fence {
            None => Ok(()),
            Some(binding) if std::ptr::eq(self, binding.db.as_ref()) => {
                state.guard(Some(&binding.fence), now)
            }
            Some(binding) => {
                let guard_state = binding
                    .db
                    .state
                    .lock()
                    .map_err(|_| StorageError::backend("memory guard lock poisoned"))?;
                guard_state.guard(Some(&binding.fence), now)
            }
        }
    }
}

#[derive(Debug)]
struct MemoryGuard<'a> {
    state: MutexGuard<'a, DbState>,
    _gate: MutexGuard<'a, ()>,
}
impl std::ops::Deref for MemoryGuard<'_> {
    type Target = DbState;
    fn deref(&self) -> &DbState {
        &self.state
    }
}
impl std::ops::DerefMut for MemoryGuard<'_> {
    fn deref_mut(&mut self) -> &mut DbState {
        &mut self.state
    }
}
#[derive(Debug)]
pub(super) struct MemoryFence {
    fence: Fence,
    db: Arc<MemoryDb>,
}

/// An in-memory [`StorageBackend`].
///
/// ```
/// use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
///
/// let storage = MemoryStorage::new();
/// let alice = storage.for_scope(&Scope::new("alice")?)?;
/// assert_eq!(alice.driver(), "memory");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct MemoryStorage {
    db: Arc<MemoryDb>,
    root: Arc<MemoryDb>,
    databases: Arc<Mutex<BTreeMap<String, Arc<MemoryDb>>>>,
    clock: Clock,
    existing: bool,
}

impl MemoryStorage {
    /// An empty backend reading the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(system_clock))
    }

    /// An empty backend reading time from `clock`.
    #[must_use]
    pub fn with_clock(clock: Clock) -> Self {
        let root = Arc::new(MemoryDb::default());
        Self {
            db: Arc::clone(&root),
            root,
            databases: Arc::default(),
            clock,
            existing: false,
        }
    }

    fn handles(&self, scope: &Scope, fence: Option<&Arc<MemoryFence>>) -> ScopedStorage {
        self.handles_generation(scope, fence, crate::ScopeGeneration::default())
    }
    fn handles_generation(
        &self,
        scope: &Scope,
        fence: Option<&Arc<MemoryFence>>,
        generation: crate::ScopeGeneration,
    ) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(
                MemoryDocuments::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fence.cloned(),
                    self.existing,
                )
                .with_generation(generation),
            ),
            Arc::new(
                MemoryStreams::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fence.cloned(),
                )
                .with_generation(generation),
            ),
            Arc::new(
                MemoryBlobs::new(
                    Arc::clone(&self.db),
                    scope.clone(),
                    Arc::clone(&self.clock),
                    fence.cloned(),
                )
                .with_generation(generation),
            ),
        )
        .with_purge(Arc::new(purge::MemoryPurge {
            db: Arc::clone(&self.db),
            scope: scope.clone(),
            generation,
            clock: Arc::clone(&self.clock),
            fence: fence.cloned(),
        }))
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for MemoryStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryStorage").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl StorageBackend for MemoryStorage {
    fn driver(&self) -> &'static str {
        "memory"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
            .without(crate::Capability::Watch)
            .without(crate::Capability::PresignedUrls)
            .without(crate::Capability::StreamingBlobs)
    }

    async fn prepare_schema(&self) -> Result<()> {
        if self.existing {
            return Err(StorageError::unsupported(
                crate::Capability::ExistingSchema,
                "existing-schema handles cannot prepare internal schema",
            ));
        }
        Ok(())
    }

    /// Nothing to reclaim: memory is freed as records go.
    async fn maintain(&self, _: MaintenanceBudget) -> Result<MaintenanceReport> {
        Ok(MaintenanceReport::default())
    }

    /// Nothing to flush: there is no log.
    async fn checkpoint(&self, _: Checkpoint) -> Result<()> {
        Ok(())
    }

    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        self.db
            .lock()?
            .scopes(prefix)
            .into_iter()
            .map(Scope::new)
            .collect()
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        let mut storage = self.clone();
        storage.existing = true;
        Ok(Arc::new(storage))
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(MemoryFence {
                fence: fence.clone(),
                db: Arc::clone(&self.db),
            })),
        ))
    }

    fn for_scope_fenced_in_database(
        &self,
        scope: &Scope,
        fence: &Fence,
        guard_database: &crate::GuardDatabase,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        guard_database.validate()?;
        let db = match guard_database {
            crate::GuardDatabase::Current => Arc::clone(&self.db),
            crate::GuardDatabase::Default => Arc::clone(&self.root),
            crate::GuardDatabase::Named(name) => self.named(name)?,
        };
        Ok(self.handles(
            scope,
            Some(&Arc::new(MemoryFence {
                fence: fence.clone(),
                db,
            })),
        ))
    }

    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: crate::ScopeGeneration,
    ) -> Result<ScopedStorage> {
        Ok(self.handles_generation(scope, None, generation))
    }

    fn for_scope_fenced_in_database_generation(
        &self,
        scope: &Scope,
        generation: crate::ScopeGeneration,
        fence: &Fence,
        guard_database: &crate::GuardDatabase,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        guard_database.validate()?;
        let db = match guard_database {
            crate::GuardDatabase::Current => Arc::clone(&self.db),
            crate::GuardDatabase::Default => Arc::clone(&self.root),
            crate::GuardDatabase::Named(name) => self.named(name)?,
        };
        Ok(self.handles_generation(
            scope,
            Some(&Arc::new(MemoryFence {
                fence: fence.clone(),
                db,
            })),
            generation,
        ))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        validate_database(name)?;
        let db = self.named(name)?;
        Ok(Arc::new(Self {
            db,
            root: Arc::clone(&self.root),
            databases: Arc::clone(&self.databases),
            clock: Arc::clone(&self.clock),
            existing: self.existing,
        }))
    }
}

impl MemoryStorage {
    fn named(&self, name: &str) -> Result<Arc<MemoryDb>> {
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| StorageError::backend("memory database registry lock poisoned"))?;
        if self.existing {
            return databases
                .get(name)
                .cloned()
                .ok_or_else(|| StorageError::not_found("named database does not exist"));
        }
        Ok(Arc::clone(databases.entry(name.to_owned()).or_insert_with(
            || {
                Arc::new(MemoryDb {
                    state: Mutex::default(),
                    gate: Arc::clone(&self.root.gate),
                })
            },
        )))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;

#[cfg(test)]
#[path = "generation_tests.rs"]
mod generation_tests;

#[cfg(test)]
#[path = "blob_ttl_tests.rs"]
mod blob_ttl_tests;
