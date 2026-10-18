//! Compose document/stream storage with an independently scoped blob driver.

use crate::{
    Blob, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability, Checkpoint, Fence,
    MaintenanceBudget, MaintenanceReport, PurgeMode, PurgeReceipt, PurgeRequest, PurgeStatus,
    Result, Scope, ScopeGeneration, ScopePurge, ScopedStorage, StorageBackend, StorageError,
    async_trait,
};
use std::{ops::Range, sync::Arc, time::Duration};

/// A connected backend that provides only scoped blob handles.
#[async_trait]
pub trait BlobBackend: Send + Sync + std::fmt::Debug {
    /// Optional blob abilities supplied by every unfenced handle.
    fn capabilities(&self) -> Capabilities;
    /// Blob handle bound to one tenant.
    ///
    /// # Errors
    ///
    /// Unsupported scope or backend failure.
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>>;
    /// Erase only this scope's blobs. Implementations must not touch records.
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        let _ = scope;
        Err(StorageError::unsupported(
            Capability::ScopePurge,
            "blob backend cannot purge a scope",
        ))
    }
    /// Enumerate scopes containing blobs.
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let _ = prefix;
        Err(StorageError::unsupported(
            Capability::ScopeEnumeration,
            "blob backend cannot enumerate scopes",
        ))
    }
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

#[async_trait]
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
        for cap in [Capability::ScopePurge, Capability::ScopeEnumeration] {
            if source.contains(cap) {
                caps = caps.with(cap);
            }
        }
        caps
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        Ok(self.0.for_scope(scope)?.blobs().clone())
    }
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        let blobs = self.for_scope(scope)?;
        for item in blobs.list("").await? {
            blobs.delete(&item.key).await?;
        }
        Ok(())
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let mut scopes = Vec::new();
        for scope in self.0.scopes(prefix).await? {
            if !self.for_scope(&scope)?.list("").await?.is_empty() {
                scopes.push(scope);
            }
        }
        Ok(scopes)
    }
}

/// Documents and streams from one backend, blobs from another.
///
/// Upload through an unfenced scope first, then commit its reference through
/// a fenced document handle. Fenced blob mutations always fail rather than
/// silently omit the document fence; reads remain available.
#[derive(Debug)]
pub struct CompositeStorage {
    records: Arc<dyn StorageBackend>,
    blobs: Arc<dyn BlobBackend>,
}

impl CompositeStorage {
    /// Compose already connected backends.
    #[must_use]
    pub fn new(records: Arc<dyn StorageBackend>, blobs: Arc<dyn BlobBackend>) -> Self {
        Self { records, blobs }
    }

    fn combine(&self, records: &ScopedStorage, fenced: bool) -> Result<ScopedStorage> {
        let scope = records.scope().clone();
        let blobs = self.blobs.for_scope(&scope)?;
        let blobs: Arc<dyn BlobStore> = Arc::new(GenerationBlobs {
            inner: if fenced {
                Arc::new(FencedBlobs(blobs))
            } else {
                blobs
            },
            records: records.clone(),
        });
        let mut combined = ScopedStorage::new(
            scope,
            "composite",
            records.documents().clone(),
            records.streams().clone(),
            blobs,
        );
        if self.records.capabilities().contains(Capability::ScopePurge)
            && self.blobs.capabilities().contains(Capability::ScopePurge)
        {
            combined = combined.with_purge(Arc::new(CompositePurge {
                records: records.clone(),
                blobs: Arc::clone(&self.blobs),
                scope: records.scope().clone(),
                coordinator: self.records.for_scope(&coordinator_scope()?)?,
            }));
        }
        Ok(combined)
    }
}

/// Couple independent blob access to the records handle's native admission
/// check, so a generation revoked by purge revokes old blob handles too.
#[derive(Debug)]
struct GenerationBlobs {
    inner: Arc<dyn BlobStore>,
    records: ScopedStorage,
}

impl GenerationBlobs {
    async fn admit(&self) -> Result<()> {
        self.records
            .documents()
            .get("__tsd_admission", "probe")
            .await?;
        Ok(())
    }
}

#[async_trait]
impl BlobStore for GenerationBlobs {
    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn put(&self, key: &str, bytes: Vec<u8>, ct: Option<&str>) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner.put(key, bytes, ct).await
    }
    async fn put_stream(
        &self,
        key: &str,
        reader: crate::BlobReader,
        len: u64,
        ct: Option<&str>,
    ) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner.put_stream(key, reader, len, ct).await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        ct: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner.put_expiring(key, bytes, ct, ttl).await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: crate::BlobReader,
        len: u64,
        ct: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.admit().await?;
        self.inner
            .put_stream_expiring(key, reader, len, ct, ttl)
            .await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.admit().await?;
        self.inner.get(key).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        self.admit().await?;
        self.inner.get_stream(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.admit().await?;
        self.inner.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.admit().await?;
        self.inner.head(key).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        self.admit().await?;
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.admit().await?;
        self.inner.list(prefix).await
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.admit().await?;
        self.inner.presign_get(key, ttl).await
    }
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        self.admit().await?;
        self.inner.presign_put(key, ttl).await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        self.admit().await?;
        self.inner.sweep_expired().await
    }
    async fn list_dir(&self, dir: &str) -> Result<Vec<crate::TreeEntry>> {
        self.admit().await?;
        self.inner.list_dir(dir).await
    }
    async fn put_tree(&self, dir: &str, files: Vec<crate::TreeFile>) -> Result<Vec<BlobMeta>> {
        self.admit().await?;
        self.inner.put_tree(dir, files).await
    }
    async fn delete_tree(&self, dir: &str) -> Result<u64> {
        self.admit().await?;
        self.inner.delete_tree(dir).await
    }
}

#[derive(Debug)]
struct CompositePurge {
    records: ScopedStorage,
    blobs: Arc<dyn BlobBackend>,
    scope: Scope,
    coordinator: ScopedStorage,
}

const COMPOSITE_PURGE_COLLECTION: &str = "__tsd_composite_purges";
const COMPOSITE_PURGE_SCOPE: &str = "__tsd_composite_purge_coordinator";

fn coordinator_scope() -> Result<Scope> {
    Scope::new(COMPOSITE_PURGE_SCOPE)
}

fn coordinator_id(scope: &Scope) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(scope.as_str().len() * 2);
    for byte in scope.as_str().bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
fn coordinator_operation_id(scope: &Scope, operation: &str) -> String {
    format!("{}_{}", coordinator_id(scope), operation)
}

impl CompositePurge {
    async fn read(&self) -> Result<Option<crate::Versioned<serde_json::Value>>> {
        self.coordinator
            .documents()
            .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
            .await?;
        self.coordinator
            .documents()
            .get(COMPOSITE_PURGE_COLLECTION, &coordinator_id(&self.scope))
            .await
    }
    async fn write(
        &self,
        current: Option<&crate::Versioned<serde_json::Value>>,
        value: serde_json::Value,
    ) -> Result<()> {
        let precondition = current.map_or(crate::Precondition::Absent, crate::Versioned::unchanged);
        self.coordinator
            .documents()
            .put(
                COMPOSITE_PURGE_COLLECTION,
                &coordinator_id(&self.scope),
                value,
                precondition,
            )
            .await?;
        Ok(())
    }
    fn receipt(value: &serde_json::Value) -> Option<PurgeReceipt> {
        serde_json::from_value(value.get("receipt")?.clone()).ok()
    }
    async fn save_history(&self, receipt: &PurgeReceipt) -> Result<()> {
        let docs = self.coordinator.documents();
        let id = coordinator_operation_id(&self.scope, &receipt.operation_id);
        let previous = docs.get(COMPOSITE_PURGE_COLLECTION, &id).await?;
        docs.put(
            COMPOSITE_PURGE_COLLECTION,
            &id,
            serde_json::json!({"receipt": receipt}),
            previous
                .as_ref()
                .map_or(crate::Precondition::Absent, crate::Versioned::unchanged),
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl ScopePurge for CompositePurge {
    async fn purge(&self, request: &PurgeRequest) -> Result<PurgeReceipt> {
        request.validate()?;
        if request.mode == PurgeMode::Atomic {
            return Err(StorageError::unsupported(
                Capability::AtomicScopePurge,
                "independent record and blob stores cannot be purged atomically",
            ));
        }
        if self.scope == coordinator_scope()? {
            return Err(StorageError::invalid_input(
                "reserved composite purge coordinator scope",
            ));
        }
        if let Some(history) = self
            .coordinator
            .documents()
            .get(
                COMPOSITE_PURGE_COLLECTION,
                &coordinator_operation_id(&self.scope, &request.operation_id),
            )
            .await?
            && let Some(receipt) = Self::receipt(&history.doc)
        {
            return Ok(receipt);
        }
        let mut current = self.read().await?;
        if let Some(value) = &current {
            let prior = Self::receipt(&value.doc).ok_or_else(|| {
                StorageError::backend("invalid composite purge coordinator record")
            })?;
            if prior.operation_id == request.operation_id && prior.status == PurgeStatus::Complete {
                return Ok(prior);
            }
            if prior.status == PurgeStatus::Pending && prior.operation_id != request.operation_id {
                return Err(StorageError::conflict("another composite purge is pending"));
            }
            if prior.status == PurgeStatus::Complete && prior.operation_id != request.operation_id {
                self.save_history(&prior).await?;
                let next = PurgeReceipt {
                    operation_id: request.operation_id.clone(),
                    generation: prior.generation,
                    status: PurgeStatus::Pending,
                    stage: crate::PurgeStage::Blobs,
                };
                self.write(Some(value), serde_json::json!({"receipt": next}))
                    .await?;
                current = self.read().await?;
            }
        } else {
            let receipt = PurgeReceipt {
                operation_id: request.operation_id.clone(),
                generation: ScopeGeneration(0),
                status: PurgeStatus::Pending,
                stage: crate::PurgeStage::Blobs,
            };
            self.write(None, serde_json::json!({"receipt": receipt}))
                .await?;
            current = self.read().await?;
        }
        let value = current.as_ref().ok_or_else(|| {
            StorageError::backend("composite purge coordinator record disappeared")
        })?;
        let mut receipt = Self::receipt(&value.doc)
            .ok_or_else(|| StorageError::backend("invalid composite purge coordinator record"))?;
        if receipt.stage == crate::PurgeStage::Blobs {
            self.blobs.purge_scope(&self.scope).await?;
            receipt.stage = crate::PurgeStage::Records;
            self.write(Some(value), serde_json::json!({"receipt": receipt}))
                .await?;
        }
        let record_receipt = self.records.purge(request).await?;
        receipt.generation = record_receipt.generation;
        receipt.stage = record_receipt.stage;
        receipt.status = record_receipt.status;
        let current = self.read().await?;
        let current = current.as_ref().ok_or_else(|| {
            StorageError::backend("composite purge coordinator record disappeared")
        })?;
        self.write(Some(current), serde_json::json!({"receipt": receipt}))
            .await?;
        if receipt.status == PurgeStatus::Complete {
            self.save_history(&receipt).await?;
        }
        Ok(receipt)
    }

    async fn status(&self, operation_id: &str) -> Result<Option<PurgeReceipt>> {
        let Some(value) = self.read().await? else {
            return Ok(None);
        };
        if let Some(receipt) =
            Self::receipt(&value.doc).filter(|receipt| receipt.operation_id == operation_id)
        {
            return Ok(Some(receipt));
        }
        let history = self
            .coordinator
            .documents()
            .get(
                COMPOSITE_PURGE_COLLECTION,
                &coordinator_operation_id(&self.scope, operation_id),
            )
            .await?;
        Ok(history.and_then(|record| Self::receipt(&record.doc)))
    }
}

#[async_trait]
impl StorageBackend for CompositeStorage {
    fn driver(&self) -> &'static str {
        "composite"
    }
    fn document_driver(&self) -> &'static str {
        self.records.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        let mut caps = self
            .records
            .capabilities()
            .without(Capability::ExistingSchema)
            .without(Capability::ScopePurge)
            .without(Capability::AtomicScopePurge)
            // Blobs live in an independent store, so the records backend
            // cannot tell which scopes hold only blobs.
            .without(Capability::ScopeEnumeration)
            .without(Capability::PresignedUrls)
            .without(Capability::StreamingBlobs)
            .without(Capability::BlobTtl);
        for cap in [
            Capability::StreamingBlobs,
            Capability::PresignedUrls,
            Capability::BlobTtl,
        ] {
            if self.blobs.capabilities().contains(cap) {
                caps = caps.with(cap);
            }
        }
        if self.records.capabilities().contains(Capability::ScopePurge)
            && self.blobs.capabilities().contains(Capability::ScopePurge)
        {
            caps = caps.with(Capability::ScopePurge);
        }
        if self
            .records
            .capabilities()
            .contains(Capability::ScopeEnumeration)
            && self
                .blobs
                .capabilities()
                .contains(Capability::ScopeEnumeration)
        {
            caps = caps.with(Capability::ScopeEnumeration);
        }
        caps
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        if scope.as_str() == COMPOSITE_PURGE_SCOPE {
            return Err(StorageError::invalid_input(
                "reserved composite purge coordinator scope",
            ));
        }
        self.combine(&self.records.for_scope(scope)?, false)
    }
    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        self.combine(&self.records.for_scope_fenced(scope, fence)?, true)
    }
    fn for_scope_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
    ) -> Result<ScopedStorage> {
        if scope.as_str() == COMPOSITE_PURGE_SCOPE {
            return Err(StorageError::invalid_input(
                "reserved composite purge coordinator scope",
            ));
        }
        if !self.records.capabilities().contains(Capability::ScopePurge)
            || !self.blobs.capabilities().contains(Capability::ScopePurge)
        {
            let _ = (scope, generation);
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "composite backend cannot bind scope generations",
            ));
        }
        self.combine(
            &self.records.for_scope_generation(scope, generation)?,
            false,
        )
    }
    fn for_scope_fenced_generation(
        &self,
        scope: &Scope,
        generation: ScopeGeneration,
        fence: &Fence,
    ) -> Result<ScopedStorage> {
        if scope.as_str() == COMPOSITE_PURGE_SCOPE {
            return Err(StorageError::invalid_input(
                "reserved composite purge coordinator scope",
            ));
        }
        if !self.records.capabilities().contains(Capability::ScopePurge)
            || !self.blobs.capabilities().contains(Capability::ScopePurge)
        {
            let _ = (scope, generation, fence);
            return Err(StorageError::unsupported(
                Capability::ScopePurge,
                "composite backend cannot bind scope generations",
            ));
        }
        self.combine(
            &self
                .records
                .for_scope_fenced_generation(scope, generation, fence)?,
            true,
        )
    }
    /// Maintains the records backend; the blob backend is a separate store a
    /// host maintains on its own.
    async fn maintain(&self, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
        self.records.maintain(budget).await
    }
    async fn checkpoint(&self, mode: Checkpoint) -> Result<()> {
        self.records.checkpoint(mode).await
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        let mut scopes = self.records.scopes(prefix).await?;
        scopes.extend(self.blobs.scopes(prefix).await?);
        scopes.retain(|scope| scope.as_str() != COMPOSITE_PURGE_SCOPE);
        scopes.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        scopes.dedup();
        Ok(scopes)
    }
}

#[derive(Debug)]
struct FencedBlobs(Arc<dyn BlobStore>);

fn unfenceable() -> StorageError {
    StorageError::unsupported(
        Capability::Fencing,
        "independent blob writes cannot join the document fence",
    )
}

#[async_trait]
impl BlobStore for FencedBlobs {
    fn max_key_len(&self) -> usize {
        self.0.max_key_len()
    }
    fn capabilities(&self) -> Capabilities {
        // Presigned PUT cannot enforce the document fence.
        self.0
            .capabilities()
            .without(Capability::ScopePurge)
            .without(Capability::AtomicScopePurge)
            .without(Capability::PresignedUrls)
            .without(Capability::BlobTtl)
    }
    async fn put(&self, _: &str, _: Vec<u8>, _: Option<&str>) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn put_expiring(
        &self,
        _: &str,
        _: Vec<u8>,
        _: Option<&str>,
        _: Duration,
    ) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn put_stream_expiring(
        &self,
        _: &str,
        _: BlobReader,
        _: u64,
        _: Option<&str>,
        _: Duration,
    ) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn put_stream(
        &self,
        _: &str,
        _: BlobReader,
        _: u64,
        _: Option<&str>,
    ) -> Result<BlobMeta> {
        Err(unfenceable())
    }
    async fn delete(&self, _: &str) -> Result<bool> {
        Err(unfenceable())
    }
    async fn presign_put(&self, _: &str, _: Duration) -> Result<String> {
        Err(unfenceable())
    }
    async fn sweep_expired(&self) -> Result<u64> {
        Err(unfenceable())
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.0.get(key).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        self.0.get_stream(key).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.0.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.0.head(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.0.list(prefix).await
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.0.presign_get(key, ttl).await
    }
}

#[cfg(test)]
#[path = "composite_tests.rs"]
mod tests;
