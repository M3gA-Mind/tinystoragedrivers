//! Stable bundle of scope-bound primitive handles.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_types::{
    Blob, BlobMeta, Capabilities, Capability, CollectionSpec, Cursor, DeadLetter, Delivery,
    Enqueued, Filter, NewMessage, Page, Precondition, Query, QueueFeatures, QueueSpec, QueueStats,
    ReadCheck, Receipt, Result, Retry, Scope, SearchHit, Sort, StorageError, StreamEntry,
    StreamInfo, TreeEntry, TreeFile, Version, Versioned, WatchOptions, WriteOp, WriteOptions,
    WriteResult,
};
use zeroize::Zeroizing;

use crate::{
    BlobReader, BlobStore, BlobStream, DocumentStore, DocumentWatch, KvStore, QueueStore,
    SecretStore, StreamStore,
};

/// A configured backend that opens scope-bound storage handles.
pub trait StorageBackend: Send + Sync + fmt::Debug {
    /// Optional abilities shared by handles from this backend.
    fn capabilities(&self) -> Capabilities;

    /// Open storage handles bound to `scope`.
    ///
    /// # Errors
    /// Returns an error when the backend cannot serve the scope or open its
    /// primitive handles.
    fn for_scope(&self, scope: &Scope) -> Result<Storage>;
}

/// Cloneable bundle of all primitive handles for one scope.
#[derive(Clone)]
pub struct Storage {
    documents: Arc<dyn DocumentStore>,
    streams: Arc<dyn StreamStore>,
    kv: Arc<dyn KvStore>,
    blobs: Arc<dyn BlobStore>,
    queues: Arc<dyn QueueStore>,
    secrets: Arc<dyn SecretStore>,
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Storage").finish_non_exhaustive()
    }
}

impl Storage {
    /// Construct a bundle from scope-bound primitive handles.
    #[must_use]
    pub fn new(
        documents: Arc<dyn DocumentStore>,
        streams: Arc<dyn StreamStore>,
        kv: Arc<dyn KvStore>,
        blobs: Arc<dyn BlobStore>,
        queues: Arc<dyn QueueStore>,
        secrets: Arc<dyn SecretStore>,
    ) -> Self {
        Self {
            documents,
            streams,
            kv,
            blobs,
            queues,
            secrets,
        }
    }

    /// Access the document handle.
    #[must_use]
    pub fn documents(&self) -> &Arc<dyn DocumentStore> {
        &self.documents
    }
    /// Access the stream handle.
    #[must_use]
    pub fn streams(&self) -> &Arc<dyn StreamStore> {
        &self.streams
    }
    /// Access the key/value handle.
    #[must_use]
    pub fn kv(&self) -> &Arc<dyn KvStore> {
        &self.kv
    }
    /// Access the blob handle.
    #[must_use]
    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }
    /// Access the queue handle.
    #[must_use]
    pub fn queues(&self) -> &Arc<dyn QueueStore> {
        &self.queues
    }
    /// Access the secret handle.
    #[must_use]
    pub fn secrets(&self) -> &Arc<dyn SecretStore> {
        &self.secrets
    }

    /// Ensure a collection through the scope-bound document port.
    ///
    /// # Errors
    /// Returns the error from the document handle.
    pub async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        self.documents.ensure_collection(spec).await
    }

    /// Construct a stable-shape bundle whose six ports all report unconfigured.
    #[must_use]
    pub fn unsupported() -> Self {
        Self::new(
            Arc::new(UnsupportedDocuments),
            Arc::new(UnsupportedStreams),
            Arc::new(UnsupportedKv),
            Arc::new(UnsupportedBlobs),
            Arc::new(UnsupportedQueues),
            Arc::new(UnsupportedSecrets),
        )
    }
}

macro_rules! unavailable {
    ($cap:ident) => {
        StorageError::unsupported(Capability::$cap, "primitive is not configured")
    };
}

#[derive(Debug)]
struct UnsupportedDocuments;
#[derive(Debug)]
struct UnsupportedStreams;
#[derive(Debug)]
struct UnsupportedKv;
#[derive(Debug)]
struct UnsupportedBlobs;
#[derive(Debug)]
struct UnsupportedQueues;
#[derive(Debug)]
struct UnsupportedSecrets;

#[async_trait]
impl DocumentStore for UnsupportedDocuments {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    async fn watch(&self, _: &str, _: &Filter, _: &WatchOptions) -> Result<Box<dyn DocumentWatch>> {
        Err(unavailable!(Documents))
    }
    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        Err(unavailable!(Documents))
    }
    async fn validate_collection_spec(&self, _: &CollectionSpec) -> Result<()> {
        Err(unavailable!(Documents))
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<Versioned<Value>>> {
        Err(unavailable!(Documents))
    }
    async fn put(&self, _: &str, _: &str, _: Value, _: Precondition) -> Result<Version> {
        Err(unavailable!(Documents))
    }
    async fn put_with(
        &self,
        _: &str,
        _: &str,
        _: Value,
        _: Precondition,
        _: WriteOptions,
    ) -> Result<Version> {
        Err(unavailable!(Documents))
    }
    async fn delete(&self, _: &str, _: &str, _: Precondition) -> Result<bool> {
        Err(unavailable!(Documents))
    }
    async fn query(&self, _: &str, _: &Query) -> Result<Page<Versioned<Value>>> {
        Err(unavailable!(Documents))
    }
    async fn count(&self, _: &str, _: &Filter) -> Result<u64> {
        Err(unavailable!(Documents))
    }
    async fn delete_where(&self, _: &str, _: &Filter) -> Result<u64> {
        Err(unavailable!(Documents))
    }
    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        Err(unavailable!(Documents))
    }
    async fn atomic_batch(&self, _: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        Err(unavailable!(Documents))
    }
    async fn atomic_batch_checked(
        &self,
        _: Vec<ReadCheck>,
        _: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        Err(unavailable!(Documents))
    }
    async fn search(&self, _: &str, _: &str, _: usize) -> Result<Vec<SearchHit>> {
        Err(unavailable!(Documents))
    }
    async fn drop_collection(&self, _: &str) -> Result<()> {
        Err(unavailable!(Documents))
    }
    async fn collections(&self, _: &str) -> Result<Vec<String>> {
        Err(unavailable!(Documents))
    }
}

#[async_trait]
impl StreamStore for UnsupportedStreams {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    async fn append(&self, _: &str, _: Value) -> Result<u64> {
        Err(unavailable!(Streams))
    }
    async fn append_batch(&self, _: &str, _: Vec<Value>) -> Result<u64> {
        Err(unavailable!(Streams))
    }
    async fn append_batch_if_offset(&self, _: &str, _: u64, _: Vec<Value>) -> Result<u64> {
        Err(unavailable!(Streams))
    }
    async fn read_window(&self, _: &str, _: u64, _: usize) -> Result<Vec<StreamEntry>> {
        Err(unavailable!(Streams))
    }
    async fn len(&self, _: &str) -> Result<u64> {
        Err(unavailable!(Streams))
    }
    async fn truncate_before(&self, _: &str, _: u64) -> Result<u64> {
        Err(unavailable!(Streams))
    }
    async fn delete_stream(&self, _: &str) -> Result<bool> {
        Err(unavailable!(Streams))
    }
    async fn streams(&self, _: &str) -> Result<Vec<String>> {
        Err(unavailable!(Streams))
    }
    async fn seal(&self, _: &str) -> Result<tinystoragedrivers_types::SealOutcome> {
        Err(unavailable!(Streams))
    }
    async fn delete_sealed_stream(&self, _: &str, _: i64) -> Result<bool> {
        Err(unavailable!(Streams))
    }
    async fn info(&self, _: &str) -> Result<Option<StreamInfo>> {
        Err(unavailable!(Streams))
    }
}

#[async_trait]
impl KvStore for UnsupportedKv {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    async fn get(&self, _: &str) -> Result<Option<Versioned<Value>>> {
        Err(unavailable!(Kv))
    }
    async fn put(&self, _: &str, _: Value) -> Result<Version> {
        Err(unavailable!(Kv))
    }
    async fn cas(&self, _: &str, _: Value, _: Precondition) -> Result<Version> {
        Err(unavailable!(Kv))
    }
    async fn delete(&self, _: &str, _: Precondition) -> Result<bool> {
        Err(unavailable!(Kv))
    }
    async fn list(&self, _: &str, _: Option<Cursor>, _: usize) -> Result<Page<Versioned<Value>>> {
        Err(unavailable!(Kv))
    }
    async fn put_with_ttl(&self, _: &str, _: Value, _: std::time::Duration) -> Result<Version> {
        Err(unavailable!(Kv))
    }
}

#[async_trait]
impl BlobStore for UnsupportedBlobs {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn max_key_len(&self) -> usize {
        0
    }
    async fn put(&self, _: &str, _: Vec<u8>, _: Option<&str>) -> Result<BlobMeta> {
        Err(unavailable!(Blobs))
    }
    async fn put_stream(
        &self,
        _: &str,
        _: BlobReader,
        _: u64,
        _: Option<&str>,
    ) -> Result<BlobMeta> {
        Err(unavailable!(Blobs))
    }
    async fn put_expiring(
        &self,
        _: &str,
        _: Vec<u8>,
        _: Option<&str>,
        _: std::time::Duration,
    ) -> Result<BlobMeta> {
        Err(unavailable!(Blobs))
    }
    async fn put_stream_expiring(
        &self,
        _: &str,
        _: BlobReader,
        _: u64,
        _: Option<&str>,
        _: std::time::Duration,
    ) -> Result<BlobMeta> {
        Err(unavailable!(Blobs))
    }
    async fn get(&self, _: &str) -> Result<Option<Blob>> {
        Err(unavailable!(Blobs))
    }
    async fn get_stream(&self, _: &str) -> Result<Option<BlobStream>> {
        Err(unavailable!(Blobs))
    }
    async fn get_range(&self, _: &str, _: std::ops::Range<u64>) -> Result<Option<Vec<u8>>> {
        Err(unavailable!(Blobs))
    }
    async fn head(&self, _: &str) -> Result<Option<BlobMeta>> {
        Err(unavailable!(Blobs))
    }
    async fn delete(&self, _: &str) -> Result<bool> {
        Err(unavailable!(Blobs))
    }
    async fn list(&self, _: &str) -> Result<Vec<BlobMeta>> {
        Err(unavailable!(Blobs))
    }
    async fn sweep_expired(&self) -> Result<u64> {
        Err(unavailable!(Blobs))
    }
    async fn presign_get(&self, _: &str, _: std::time::Duration) -> Result<String> {
        Err(unavailable!(Blobs))
    }
    async fn presign_put(&self, _: &str, _: std::time::Duration) -> Result<String> {
        Err(unavailable!(Blobs))
    }
    async fn list_dir(&self, _: &str) -> Result<Vec<TreeEntry>> {
        Err(unavailable!(Blobs))
    }
    async fn put_tree(&self, _: &str, _: Vec<TreeFile>) -> Result<Vec<BlobMeta>> {
        Err(unavailable!(Blobs))
    }
    async fn delete_tree(&self, _: &str) -> Result<u64> {
        Err(unavailable!(Blobs))
    }
}

#[async_trait]
impl QueueStore for UnsupportedQueues {
    fn features(&self) -> QueueFeatures {
        QueueFeatures::default()
    }
    async fn ensure_queue(&self, _: &str, _: &QueueSpec) -> Result<()> {
        Err(unavailable!(Queues))
    }
    async fn enqueue(&self, _: &str, _: &[NewMessage]) -> Result<Vec<Enqueued>> {
        Err(unavailable!(Queues))
    }
    async fn receive(&self, _: &str, _: u32, _: u64, _: &str) -> Result<Vec<Delivery>> {
        Err(unavailable!(Queues))
    }
    async fn ack(&self, _: &str, _: &Receipt) -> Result<bool> {
        Err(unavailable!(Queues))
    }
    async fn nack(&self, _: &str, _: &Receipt, _: Retry) -> Result<bool> {
        Err(unavailable!(Queues))
    }
    async fn extend(&self, _: &str, _: &Receipt, _: u64) -> Result<bool> {
        Err(unavailable!(Queues))
    }
    async fn stats(&self, _: &str) -> Result<QueueStats> {
        Err(unavailable!(Queues))
    }
    async fn dead_letters(&self, _: &str, _: usize) -> Result<Vec<DeadLetter>> {
        Err(unavailable!(Queues))
    }
    async fn redrive(&self, _: &str, _: &str) -> Result<bool> {
        Err(unavailable!(Queues))
    }
    async fn purge(&self, _: &str) -> Result<u64> {
        Err(unavailable!(Queues))
    }
}

#[async_trait]
impl SecretStore for UnsupportedSecrets {
    async fn get(&self, _: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        Err(unavailable!(Secrets))
    }
    async fn set(&self, _: &str, _: &[u8]) -> Result<()> {
        Err(unavailable!(Secrets))
    }
    async fn delete(&self, _: &str) -> Result<bool> {
        Err(unavailable!(Secrets))
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        Err(unavailable!(Secrets))
    }
    fn enumerable(&self) -> bool {
        false
    }
    fn backend_name(&self) -> &'static str {
        "unconfigured"
    }
}
