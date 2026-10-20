//! Real six-port in-memory backend for consumer tests.

use std::collections::HashMap;
use std::sync::Mutex;

use tinystoragedrivers_api::{Storage, StorageBackend};
use tinystoragedrivers_document::memory::MemoryConnection;
use tinystoragedrivers_types::{Capabilities, Result, Scope, StorageError};

/// Create a real in-memory bundle of all six storage ports for one scope.
#[must_use]
pub fn memory_storage(scope: &Scope) -> Storage {
    memory_storage_with_connection(&MemoryConnection::new(), scope)
}

fn memory_storage_with_connection(connection: &MemoryConnection, scope: &Scope) -> Storage {
    Storage::new(
        connection.for_scope(scope.clone()).documents(),
        connection.for_scope(scope.clone()).streams(),
        std::sync::Arc::new(tinystoragedrivers_kv::MemoryKv::new(scope.clone())),
        std::sync::Arc::new(tinystoragedrivers_blob::MemoryBlobStore::new(scope.clone())),
        std::sync::Arc::new(tinystoragedrivers_queue::MemoryQueue::new()),
        std::sync::Arc::new(tinystoragedrivers_secrets::MemorySecrets::new()),
    )
}

/// A scope-caching backend that opens the real memory engines for every port.
#[derive(Debug, Default)]
pub struct MemoryBackend {
    documents: MemoryConnection,
    scopes: Mutex<HashMap<String, Storage>>,
}

impl MemoryBackend {
    /// Create an empty backend whose document/stream engine is shared across
    /// scope-bound handles.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl StorageBackend for MemoryBackend {
    fn capabilities(&self) -> Capabilities {
        // Optional capabilities are reported by each primitive handle.
        Capabilities::none()
    }

    fn for_scope(&self, scope: &Scope) -> Result<Storage> {
        let mut scopes = self
            .scopes
            .lock()
            .map_err(|_| StorageError::backend("test memory backend lock poisoned"))?;
        Ok(scopes
            .entry(scope.as_str().to_owned())
            .or_insert_with(|| memory_storage_with_connection(&self.documents, scope))
            .clone())
    }
}

/// Construct a new backend that caches one real six-port bundle per scope.
#[must_use]
pub fn memory_backend() -> MemoryBackend {
    MemoryBackend::new()
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
