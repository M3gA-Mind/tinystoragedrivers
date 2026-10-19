//! A connected backend that supplies independently addressable blob handles.

use std::sync::Arc;

use crate::{BlobStore, Capabilities, Capability, Result, Scope, StorageBackend};

/// A connected backend that provides only scoped blob handles.
pub trait BlobBackend: Send + Sync + std::fmt::Debug {
    /// Optional blob abilities supplied by every handle.
    fn capabilities(&self) -> Capabilities;

    /// Return the blob handle bound to `scope`.
    ///
    /// # Errors
    /// Unsupported scope or backend failure.
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>>;
}

/// Use the blob port of an existing complete backend independently.
#[derive(Debug)]
pub struct BackendBlobs(Arc<dyn StorageBackend>);

impl BackendBlobs {
    /// Wrap an existing backend's blob port.
    #[must_use]
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self(backend)
    }
}

impl BlobBackend for BackendBlobs {
    fn capabilities(&self) -> Capabilities {
        let source = self.0.capabilities();
        let mut caps = Capabilities::none();
        for cap in [
            Capability::StreamingBlobs,
            Capability::PresignedUrls,
            Capability::BlobTtl,
        ] {
            if source.contains(cap) {
                caps = caps.with(cap);
            }
        }
        caps
    }

    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        Ok(self.0.for_scope(scope)?.blobs().clone())
    }
}
