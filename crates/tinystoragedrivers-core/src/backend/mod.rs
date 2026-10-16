//! A connected backend and the scoped port handles it hands out.
//!
//! A host opens one [`StorageBackend`] at boot (from a URL, through the
//! `tinystoragedrivers` facade) and asks it for a [`ScopedStorage`] per tenant.
//! [`StorageBackend::database`] splits one backend into independent named
//! databases: separate files under a SQLite directory, a collection prefix on
//! `MongoDB`, a separate map in memory. Owners use it to keep their data apart
//! (`approvals`, `sessions`, `flows`) without inventing collection prefixes.

mod composite;
mod maintenance;
mod purge;
pub use composite::{BackendBlobs, BlobBackend, CompositeStorage};
pub use maintenance::{Checkpoint, MaintenanceBudget, MaintenanceReport};
pub use purge::{
    PurgeMode, PurgeReceipt, PurgeRequest, PurgeStage, PurgeStatus, ScopeGeneration, ScopePurge,
};

use std::fmt;
use std::sync::Arc;

use crate::blob::BlobStore;
use crate::capabilities::{Capabilities, Capability};
use crate::document::DocumentStore;
use crate::error::{Result, StorageError};
use crate::fence::Fence;
use crate::scope::Scope;
use crate::stream::StreamStore;

/// A connected storage backend.
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync + fmt::Debug {
    /// Driver name, for logs: `"memory"`, `"sqlite"`, `"mongodb"`, `"file"`.
    fn driver(&self) -> &'static str;

    /// Native driver supplying this backend's documents and streams.
    ///
    /// Ordinary backends report their own driver. Compositions and wrappers
    /// must preserve the originating record driver rather than a blob driver
    /// or wrapper label. Hosts can use this identity for their coordination
    /// policy; it does not grant fencing or prove any optional capability.
    /// This is immutable metadata and must not open scopes or perform I/O.
    fn document_driver(&self) -> &'static str {
        self.driver()
    }

    /// Optional abilities every handle from this backend has.
    fn capabilities(&self) -> Capabilities;

    /// Return native handles that use existing schema without declaring or repairing it.
    ///
    /// The restriction survives named databases and native fenced construction.
    ///
    /// # Errors
    ///
    /// Unsupported when this backend cannot prevent implicit schema initialization.
    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        Err(StorageError::unsupported(
            Capability::ExistingSchema,
            "this backend cannot restrict schema initialization",
        ))
    }

    /// Prepare the native internal tables and indexes under trusted bootstrap authority.
    ///
    /// This does not declare application document collections. Restricted backends refuse.
    ///
    /// # Errors
    ///
    /// Unsupported by default, or the native preparation error.
    async fn prepare_schema(&self) -> Result<()> {
        Err(StorageError::unsupported(
            Capability::ExistingSchema,
            "this backend cannot explicitly prepare internal schema",
        ))
    }

    /// Port handles bound to `scope`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the
    /// backend only serves particular scopes (a single-operator SQLite file
    /// serves only [`Scope::local`]), or a backend error.
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage>;

    /// Port handles bound to `scope` whose writes land only while `fence`
    /// holds (see [`Fence`]). Requires [`Capability::Fencing`].
    ///
    /// Every write through the returned handles reads the fence's guard
    /// document (in this backend and named database, in the fence's own
    /// scope) in the same atomic step as the write, before the write's own
    /// precondition. When the guard is absent, expired, or does not match the
    /// fence's filter, the write fails with
    /// [`ErrorKind::Fenced`](crate::ErrorKind::Fenced) and changes nothing,
    /// even if it would have changed nothing anyway.
    ///
    /// The fenced writes are the document port's `put`, `delete`,
    /// `delete_where`, `claim`, `atomic_batch` and `drop_collection`, the
    /// stream port's `append`, `append_batch`, `append_batch_if_offset`, `truncate_before` and
    /// `delete_stream`, and the blob port's `put` and `delete`. Reads and
    /// `ensure_collection` are not fenced. A driver that cannot make one of
    /// these writes atomic with the check refuses it with
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported)`(Fencing)`
    /// rather than run it unfenced; each driver's README says which, if any.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::Fencing`] (the default),
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an
    /// invalid fence or a scope the backend does not serve, or a backend
    /// error.
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        let _ = (scope, fence);
        Err(StorageError::unsupported(
            Capability::Fencing,
            "this storage driver cannot fence writes",
        ))
    }

    /// Bind an explicitly issued native data-access generation.
    ///
    /// # Errors
    /// Unsupported nonzero generations on unknown drivers.
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<ScopedStorage> {
        if generation == ScopeGeneration::default() {
            return self.for_scope(scope);
        }
        purge::unsupported_generation(scope, generation)
    }

    /// Bind a generation and a fence together.
    ///
    /// # Errors
    /// Unsupported generation/fencing or an invalid fence.
    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        if generation == ScopeGeneration::default() {
            return self.for_scope_fenced(scope, fence);
        }
        purge::unsupported_generation(scope, generation)
    }

    /// Reclaim the lapsed blobs of `scope` (see [`BlobStore::sweep_expired`])
    /// and return how many went.
    ///
    /// This is the hook a host schedules when it holds only an
    /// `Arc<dyn StorageBackend>`. It reaches the scope's blob view, so a
    /// backend without expiring blobs answers `0`.
    ///
    /// # Errors
    ///
    /// The scope's [`for_scope`](Self::for_scope) error, or a backend error.
    async fn sweep_expired_blobs(&self, scope: &Scope) -> Result<u64> {
        self.for_scope(scope)?.blobs().sweep_expired().await
    }

    /// Give back the space the backend no longer needs, within `budget`.
    ///
    /// This is the hook a host schedules when it holds only an
    /// `Arc<dyn StorageBackend>`. It acts on the whole backend, not on one
    /// scope. Today it only reclaims space; seal and retention policies run
    /// from the same call in a later release, so read the
    /// [`MaintenanceReport`] by field rather than assuming it stays one number.
    ///
    /// Requires [`Capability::Maintenance`]. A host that only wants the space
    /// back may treat the refusal as "nothing to do".
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::Maintenance`] (the default), or a backend error.
    async fn maintain(&self, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
        let _ = budget;
        Err(maintenance::unsupported_maintenance())
    }

    /// Flush the backend's write-ahead log into its main storage.
    ///
    /// Requires [`Capability::Maintenance`]. See [`Checkpoint`] for the modes.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::Maintenance`] (the default),
    /// [`ErrorKind::Unavailable`](crate::ErrorKind::Unavailable) when a
    /// [`Checkpoint::Truncate`] could not finish because a reader or writer
    /// held the log (retry), or a backend error.
    async fn checkpoint(&self, mode: Checkpoint) -> Result<()> {
        let _ = mode;
        Err(maintenance::unsupported_maintenance())
    }

    /// The scopes holding any stored record, whose text starts with `prefix`,
    /// sorted by byte order.
    ///
    /// A scope counts when it has a document (a tombstone left by a delete
    /// included), a stream or a blob. `prefix` is literal text, not a pattern:
    /// an empty prefix lists every scope, and `"u:ann"` matches `"u:ann"` and
    /// `"u:ann/agent:x"` but not `"u:bob"`. A host drives a tenant purge by
    /// listing the tenant's scope and everything under it.
    ///
    /// Requires [`Capability::ScopeEnumeration`].
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) without
    /// [`Capability::ScopeEnumeration`] (the default), or a backend error.
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let _ = prefix;
        Err(maintenance::unsupported_scopes())
    }
}

/// The ports of one backend, bound to one scope.
#[derive(Clone)]
pub struct ScopedStorage {
    scope: Scope,
    driver: &'static str,
    documents: Arc<dyn DocumentStore>,
    streams: Arc<dyn StreamStore>,
    blobs: Arc<dyn BlobStore>,
    purge: Option<Arc<dyn ScopePurge>>,
}

impl ScopedStorage {
    /// Assemble handles a driver opened for `scope`.
    #[must_use]
    pub fn new(
        scope: Scope,
        driver: &'static str,
        documents: Arc<dyn DocumentStore>,
        streams: Arc<dyn StreamStore>,
        blobs: Arc<dyn BlobStore>,
    ) -> Self {
        Self {
            scope,
            driver,
            documents,
            streams,
            blobs,
            purge: None,
        }
    }

    /// Attach the driver's native scope reset port.
    #[must_use]
    pub fn with_purge(mut self, purge: Arc<dyn ScopePurge>) -> Self {
        self.purge = Some(purge);
        self
    }

    /// Purge this addressed scope. A root reset must cover its complete owner
    /// database manifest and persist every issued generation before reopening.
    ///
    /// # Errors
    /// Unsupported on unknown adapters; otherwise the native reset's error.
    pub async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        match &self.purge {
            Some(port) => port.purge(request).await,
            None => Err(StorageError::unsupported(
                Capability::ScopePurge,
                "this driver cannot purge a complete scope",
            )),
        }
    }

    /// Read a durable reset receipt without erasing data.
    ///
    /// # Errors
    /// Invalid operation identity, unsupported port or native status error.
    pub async fn purge_status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        PurgeRequest {
            operation_id: operation_id.into(),
            mode: PurgeMode::Atomic,
        }
        .validate()?;
        match &self.purge {
            Some(port) => port.status(operation_id).await,
            None => Err(StorageError::unsupported(
                Capability::ScopePurge,
                "this driver cannot read purge status",
            )),
        }
    }

    /// The scope every handle is bound to.
    #[must_use]
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// The driver that opened these handles.
    #[must_use]
    pub fn driver(&self) -> &'static str {
        self.driver
    }

    /// The document port.
    #[must_use]
    pub fn documents(&self) -> &Arc<dyn DocumentStore> {
        &self.documents
    }

    /// The stream port.
    #[must_use]
    pub fn streams(&self) -> &Arc<dyn StreamStore> {
        &self.streams
    }

    /// The blob port.
    #[must_use]
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }
}

impl fmt::Debug for ScopedStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedStorage")
            .field("scope", &self.scope)
            .field("driver", &self.driver)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
