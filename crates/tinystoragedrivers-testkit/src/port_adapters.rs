//! Trait-object decorators for deterministic consumer tests.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use std::ops::Range;
use tinystoragedrivers_api::{
    BlobReader, BlobStore, BlobStream, DocumentStore, DocumentWatch, KvStore, QueueStore,
    SecretStore, StreamStore,
};
use tinystoragedrivers_types::{
    Blob, BlobListPage, BlobMeta, Capabilities, CollectionSpec, Cursor, DeadLetter, Delivery,
    Enqueued, Filter, Page, Precondition, Query, QueueFeatures, QueueSpec, QueueStats, ReadCheck,
    Receipt, Result, Retry, SealOutcome, SearchHit, SecretPrecondition, SecretVersion, Sort,
    StreamEntry, StreamInfo, TreeEntry, TreeFile, Version, Versioned, WatchOptions, WriteOp,
    WriteOptions, WriteResult,
};
use zeroize::Zeroizing;

use crate::{ConflictOnce, Failing, Slow};

#[async_trait]
impl<T: KvStore + ?Sized> KvStore for Failing<T> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        self.call("kv.get", self.inner.get(key)).await
    }
    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.call("kv.put", self.inner.put(key, value)).await
    }
    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.call("kv.cas", self.inner.cas(key, value, expected))
            .await
    }
    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        self.call("kv.delete", self.inner.delete(key, expected))
            .await
    }
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        self.call("kv.list", self.inner.list(prefix, cursor, limit))
            .await
    }
    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        self.call("kv.put_with_ttl", self.inner.put_with_ttl(key, value, ttl))
            .await
    }
}

#[async_trait]
impl<T: QueueStore + ?Sized> QueueStore for Failing<T> {
    fn features(&self) -> QueueFeatures {
        self.inner.features()
    }
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        self.call("queue.ensure_queue", self.inner.ensure_queue(queue, spec))
            .await
    }
    async fn enqueue(
        &self,
        queue: &str,
        messages: &[tinystoragedrivers_types::NewMessage],
    ) -> Result<Vec<Enqueued>> {
        self.call("queue.enqueue", self.inner.enqueue(queue, messages))
            .await
    }
    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>> {
        self.call(
            "queue.receive",
            self.inner.receive(queue, max, visibility_ms, consumer),
        )
        .await
    }
    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool> {
        self.call("queue.ack", self.inner.ack(queue, receipt)).await
    }
    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool> {
        self.call("queue.nack", self.inner.nack(queue, receipt, retry))
            .await
    }
    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool> {
        self.call(
            "queue.extend",
            self.inner.extend(queue, receipt, visibility_ms),
        )
        .await
    }
    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        self.call("queue.stats", self.inner.stats(queue)).await
    }
    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>> {
        self.call("queue.dead_letters", self.inner.dead_letters(queue, limit))
            .await
    }
    async fn redrive(&self, queue: &str, id: &str) -> Result<bool> {
        self.call("queue.redrive", self.inner.redrive(queue, id))
            .await
    }
    async fn purge(&self, queue: &str) -> Result<u64> {
        self.call("queue.purge", self.inner.purge(queue)).await
    }
}

#[async_trait]
impl<T: SecretStore + ?Sized> SecretStore for Failing<T> {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.call("secrets.get", self.inner.get(name)).await
    }
    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        self.call("secrets.get_versioned", self.inner.get_versioned(name))
            .await
    }
    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        self.call("secrets.set", self.inner.set(name, value)).await
    }
    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: SecretPrecondition,
    ) -> Result<SecretVersion> {
        self.call("secrets.set_if", self.inner.set_if(name, value, condition))
            .await
    }
    async fn delete(&self, name: &str) -> Result<bool> {
        self.call("secrets.delete", self.inner.delete(name)).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.call("secrets.list", self.inner.list(prefix)).await
    }
    fn enumerable(&self) -> bool {
        self.inner.enumerable()
    }
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }
}

#[async_trait]
impl<T: StreamStore + ?Sized> StreamStore for Failing<T> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.call("streams.append", self.inner.append(stream, value))
            .await
    }
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.call(
            "streams.append_batch",
            self.inner.append_batch(stream, values),
        )
        .await
    }
    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.call(
            "streams.append_batch_if_offset",
            self.inner.append_batch_if_offset(stream, expected, values),
        )
        .await
    }
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        self.call(
            "streams.read_window",
            self.inner.read_window(stream, from, limit),
        )
        .await
    }
    async fn len(&self, stream: &str) -> Result<u64> {
        self.call("streams.len", self.inner.len(stream)).await
    }
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        self.call(
            "streams.truncate_before",
            self.inner.truncate_before(stream, offset),
        )
        .await
    }
    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        self.call("streams.delete_stream", self.inner.delete_stream(stream))
            .await
    }
    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        self.call("streams.streams", self.inner.streams(prefix))
            .await
    }
    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.call("streams.seal", self.inner.seal(stream)).await
    }
    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        self.call(
            "streams.delete_sealed_stream",
            self.inner.delete_sealed_stream(stream, sealed_ms),
        )
        .await
    }
    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        self.call("streams.info", self.inner.info(stream)).await
    }
}

#[async_trait]
impl<T: DocumentStore + ?Sized> DocumentStore for Failing<T> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        self.call(
            "documents.watch",
            self.inner.watch(collection, filter, options),
        )
        .await
    }
    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        self.call(
            "documents.ensure_collection",
            self.inner.ensure_collection(spec),
        )
        .await
    }
    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        self.call(
            "documents.validate_collection_spec",
            self.inner.validate_collection_spec(required),
        )
        .await
    }
    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        self.call("documents.get", self.inner.get(collection, id))
            .await
    }
    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        self.call(
            "documents.put",
            self.inner.put(collection, id, value, precondition),
        )
        .await
    }
    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        self.call(
            "documents.put_with",
            self.inner
                .put_with(collection, id, value, precondition, options),
        )
        .await
    }
    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        self.call(
            "documents.delete",
            self.inner.delete(collection, id, precondition),
        )
        .await
    }
    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        self.call("documents.query", self.inner.query(collection, query))
            .await
    }
    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        self.call("documents.count", self.inner.count(collection, filter))
            .await
    }
    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        self.call(
            "documents.delete_where",
            self.inner.delete_where(collection, filter),
        )
        .await
    }
    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        self.call(
            "documents.claim",
            self.inner.claim(collection, filter, sort, patch),
        )
        .await
    }
    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.call("documents.atomic_batch", self.inner.atomic_batch(ops))
            .await
    }
    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        self.call(
            "documents.atomic_batch_checked",
            self.inner.atomic_batch_checked(checks, ops),
        )
        .await
    }
    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        self.call(
            "documents.search",
            self.inner.search(collection, text, limit),
        )
        .await
    }
    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.call(
            "documents.drop_collection",
            self.inner.drop_collection(collection),
        )
        .await
    }
    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        self.call("documents.collections", self.inner.collections(prefix))
            .await
    }
}

#[async_trait]
impl<T: BlobStore + ?Sized> BlobStore for Failing<T> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }
    fn supports_sealed_blobs(&self) -> bool {
        self.inner.supports_sealed_blobs()
    }
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.call("blobs.put", self.inner.put(key, bytes, content_type))
            .await
    }
    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.call(
            "blobs.put_stream",
            self.inner.put_stream(key, reader, len, content_type),
        )
        .await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.call(
            "blobs.put_expiring",
            self.inner.put_expiring(key, bytes, content_type, ttl),
        )
        .await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.call(
            "blobs.put_stream_expiring",
            self.inner
                .put_stream_expiring(key, reader, len, content_type, ttl),
        )
        .await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.call("blobs.get", self.inner.get(key)).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        self.call("blobs.get_stream", self.inner.get_stream(key))
            .await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.call("blobs.get_range", self.inner.get_range(key, range))
            .await
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.call("blobs.head", self.inner.head(key)).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        self.call("blobs.delete", self.inner.delete(key)).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.call("blobs.list", self.inner.list(prefix)).await
    }
    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.call(
            "blobs.put_sealed",
            self.inner.put_sealed(key, bytes, content_type),
        )
        .await
    }
    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        self.call("blobs.get_sealed", self.inner.get_sealed(key))
            .await
    }
    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.call("blobs.head_sealed", self.inner.head_sealed(key))
            .await
    }
    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        self.call("blobs.delete_sealed", self.inner.delete_sealed(key))
            .await
    }
    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.call("blobs.list_sealed", self.inner.list_sealed(prefix))
            .await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        self.call(
            "blobs.list_page",
            self.inner.list_page(prefix, after, limit),
        )
        .await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        self.call("blobs.sweep_expired", self.inner.sweep_expired())
            .await
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.call("blobs.presign_get", self.inner.presign_get(key, ttl))
            .await
    }
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        self.call("blobs.presign_put", self.inner.presign_put(key, ttl))
            .await
    }
    async fn list_dir(&self, dir: &str) -> Result<Vec<TreeEntry>> {
        self.call("blobs.list_dir", self.inner.list_dir(dir)).await
    }
    async fn put_tree(&self, dir: &str, files: Vec<TreeFile>) -> Result<Vec<BlobMeta>> {
        self.call("blobs.put_tree", self.inner.put_tree(dir, files))
            .await
    }
    async fn delete_tree(&self, dir: &str) -> Result<u64> {
        self.call("blobs.delete_tree", self.inner.delete_tree(dir))
            .await
    }
}

#[async_trait]
impl<T: KvStore + ?Sized> KvStore for Slow<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        self.run(self.inner.get(key)).await
    }
    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.run(self.inner.put(key, value)).await
    }
    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.run(self.inner.cas(key, value, expected)).await
    }
    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        self.run(self.inner.delete(key, expected)).await
    }
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        self.run(self.inner.list(prefix, cursor, limit)).await
    }
    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        self.run(self.inner.put_with_ttl(key, value, ttl)).await
    }
}

#[async_trait]
impl<T: KvStore + ?Sized> KvStore for ConflictOnce<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        self.inner.get(key).await
    }
    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.check()?;
        self.inner.put(key, value).await
    }
    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.check()?;
        self.inner.cas(key, value, expected).await
    }
    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        self.check()?;
        self.inner.delete(key, expected).await
    }
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        self.inner.list(prefix, cursor, limit).await
    }
    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        self.check()?;
        self.inner.put_with_ttl(key, value, ttl).await
    }
}

#[async_trait]
impl<T: QueueStore + ?Sized> QueueStore for Slow<std::sync::Arc<T>> {
    fn features(&self) -> QueueFeatures {
        self.inner.features()
    }
    async fn ensure_queue(&self, q: &str, s: &QueueSpec) -> Result<()> {
        self.run(self.inner.ensure_queue(q, s)).await
    }
    async fn enqueue(
        &self,
        q: &str,
        m: &[tinystoragedrivers_types::NewMessage],
    ) -> Result<Vec<Enqueued>> {
        self.run(self.inner.enqueue(q, m)).await
    }
    async fn receive(&self, q: &str, n: u32, v: u64, c: &str) -> Result<Vec<Delivery>> {
        self.run(self.inner.receive(q, n, v, c)).await
    }
    async fn ack(&self, q: &str, r: &Receipt) -> Result<bool> {
        self.run(self.inner.ack(q, r)).await
    }
    async fn nack(&self, q: &str, r: &Receipt, retry: Retry) -> Result<bool> {
        self.run(self.inner.nack(q, r, retry)).await
    }
    async fn extend(&self, q: &str, r: &Receipt, v: u64) -> Result<bool> {
        self.run(self.inner.extend(q, r, v)).await
    }
    async fn stats(&self, q: &str) -> Result<QueueStats> {
        self.run(self.inner.stats(q)).await
    }
    async fn dead_letters(&self, q: &str, n: usize) -> Result<Vec<DeadLetter>> {
        self.run(self.inner.dead_letters(q, n)).await
    }
    async fn redrive(&self, q: &str, id: &str) -> Result<bool> {
        self.run(self.inner.redrive(q, id)).await
    }
    async fn purge(&self, q: &str) -> Result<u64> {
        self.run(self.inner.purge(q)).await
    }
}

#[async_trait]
impl<T: QueueStore + ?Sized> QueueStore for ConflictOnce<std::sync::Arc<T>> {
    fn features(&self) -> QueueFeatures {
        self.inner.features()
    }
    async fn ensure_queue(&self, q: &str, s: &QueueSpec) -> Result<()> {
        self.check()?;
        self.inner.ensure_queue(q, s).await
    }
    async fn enqueue(
        &self,
        q: &str,
        m: &[tinystoragedrivers_types::NewMessage],
    ) -> Result<Vec<Enqueued>> {
        self.check()?;
        self.inner.enqueue(q, m).await
    }
    async fn receive(&self, q: &str, n: u32, v: u64, c: &str) -> Result<Vec<Delivery>> {
        self.inner.receive(q, n, v, c).await
    }
    async fn ack(&self, q: &str, r: &Receipt) -> Result<bool> {
        self.check()?;
        self.inner.ack(q, r).await
    }
    async fn nack(&self, q: &str, r: &Receipt, retry: Retry) -> Result<bool> {
        self.check()?;
        self.inner.nack(q, r, retry).await
    }
    async fn extend(&self, q: &str, r: &Receipt, v: u64) -> Result<bool> {
        self.check()?;
        self.inner.extend(q, r, v).await
    }
    async fn stats(&self, q: &str) -> Result<QueueStats> {
        self.inner.stats(q).await
    }
    async fn dead_letters(&self, q: &str, n: usize) -> Result<Vec<DeadLetter>> {
        self.inner.dead_letters(q, n).await
    }
    async fn redrive(&self, q: &str, id: &str) -> Result<bool> {
        self.check()?;
        self.inner.redrive(q, id).await
    }
    async fn purge(&self, q: &str) -> Result<u64> {
        self.check()?;
        self.inner.purge(q).await
    }
}

#[async_trait]
impl<T: SecretStore + ?Sized> SecretStore for Slow<std::sync::Arc<T>> {
    async fn get(&self, n: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.run(self.inner.get(n)).await
    }
    async fn get_versioned(&self, n: &str) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        self.run(self.inner.get_versioned(n)).await
    }
    async fn set(&self, n: &str, v: &[u8]) -> Result<()> {
        self.run(self.inner.set(n, v)).await
    }
    async fn set_if(&self, n: &str, v: &[u8], c: SecretPrecondition) -> Result<SecretVersion> {
        self.run(self.inner.set_if(n, v, c)).await
    }
    async fn delete(&self, n: &str) -> Result<bool> {
        self.run(self.inner.delete(n)).await
    }
    async fn list(&self, p: &str) -> Result<Vec<String>> {
        self.run(self.inner.list(p)).await
    }
    fn enumerable(&self) -> bool {
        self.inner.enumerable()
    }
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }
}

#[async_trait]
impl<T: SecretStore + ?Sized> SecretStore for ConflictOnce<std::sync::Arc<T>> {
    async fn get(&self, n: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.inner.get(n).await
    }
    async fn get_versioned(&self, n: &str) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        self.inner.get_versioned(n).await
    }
    async fn set(&self, n: &str, v: &[u8]) -> Result<()> {
        self.check()?;
        self.inner.set(n, v).await
    }
    async fn set_if(&self, n: &str, v: &[u8], c: SecretPrecondition) -> Result<SecretVersion> {
        self.check()?;
        self.inner.set_if(n, v, c).await
    }
    async fn delete(&self, n: &str) -> Result<bool> {
        self.check()?;
        self.inner.delete(n).await
    }
    async fn list(&self, p: &str) -> Result<Vec<String>> {
        self.inner.list(p).await
    }
    fn enumerable(&self) -> bool {
        self.inner.enumerable()
    }
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }
}

#[async_trait]
impl<T: StreamStore + ?Sized> StreamStore for Slow<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn append(&self, s: &str, v: Value) -> Result<u64> {
        self.run(self.inner.append(s, v)).await
    }
    async fn append_batch(&self, s: &str, v: Vec<Value>) -> Result<u64> {
        self.run(self.inner.append_batch(s, v)).await
    }
    async fn append_batch_if_offset(&self, s: &str, e: u64, v: Vec<Value>) -> Result<u64> {
        self.run(self.inner.append_batch_if_offset(s, e, v)).await
    }
    async fn read_window(&self, s: &str, f: u64, l: usize) -> Result<Vec<StreamEntry>> {
        self.run(self.inner.read_window(s, f, l)).await
    }
    async fn len(&self, s: &str) -> Result<u64> {
        self.run(self.inner.len(s)).await
    }
    async fn truncate_before(&self, s: &str, o: u64) -> Result<u64> {
        self.run(self.inner.truncate_before(s, o)).await
    }
    async fn delete_stream(&self, s: &str) -> Result<bool> {
        self.run(self.inner.delete_stream(s)).await
    }
    async fn streams(&self, p: &str) -> Result<Vec<String>> {
        self.run(self.inner.streams(p)).await
    }
    async fn seal(&self, s: &str) -> Result<SealOutcome> {
        self.run(self.inner.seal(s)).await
    }
    async fn delete_sealed_stream(&self, s: &str, t: i64) -> Result<bool> {
        self.run(self.inner.delete_sealed_stream(s, t)).await
    }
    async fn info(&self, s: &str) -> Result<Option<StreamInfo>> {
        self.run(self.inner.info(s)).await
    }
}

#[async_trait]
impl<T: StreamStore + ?Sized> StreamStore for ConflictOnce<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn append(&self, s: &str, v: Value) -> Result<u64> {
        self.check()?;
        self.inner.append(s, v).await
    }
    async fn append_batch(&self, s: &str, v: Vec<Value>) -> Result<u64> {
        self.check()?;
        self.inner.append_batch(s, v).await
    }
    async fn append_batch_if_offset(&self, s: &str, e: u64, v: Vec<Value>) -> Result<u64> {
        self.check()?;
        self.inner.append_batch_if_offset(s, e, v).await
    }
    async fn read_window(&self, s: &str, f: u64, l: usize) -> Result<Vec<StreamEntry>> {
        self.inner.read_window(s, f, l).await
    }
    async fn len(&self, s: &str) -> Result<u64> {
        self.inner.len(s).await
    }
    async fn truncate_before(&self, s: &str, o: u64) -> Result<u64> {
        self.check()?;
        self.inner.truncate_before(s, o).await
    }
    async fn delete_stream(&self, s: &str) -> Result<bool> {
        self.check()?;
        self.inner.delete_stream(s).await
    }
    async fn streams(&self, p: &str) -> Result<Vec<String>> {
        self.inner.streams(p).await
    }
    async fn seal(&self, s: &str) -> Result<SealOutcome> {
        self.check()?;
        self.inner.seal(s).await
    }
    async fn delete_sealed_stream(&self, s: &str, t: i64) -> Result<bool> {
        self.check()?;
        self.inner.delete_sealed_stream(s, t).await
    }
    async fn info(&self, s: &str) -> Result<Option<StreamInfo>> {
        self.inner.info(s).await
    }
}

#[async_trait]
impl<T: DocumentStore + ?Sized> DocumentStore for Slow<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn watch(&self, c: &str, f: &Filter, o: &WatchOptions) -> Result<Box<dyn DocumentWatch>> {
        self.run(self.inner.watch(c, f, o)).await
    }
    async fn ensure_collection(&self, s: &CollectionSpec) -> Result<()> {
        self.run(self.inner.ensure_collection(s)).await
    }
    async fn validate_collection_spec(&self, s: &CollectionSpec) -> Result<()> {
        self.run(self.inner.validate_collection_spec(s)).await
    }
    async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        self.run(self.inner.get(c, id)).await
    }
    async fn put(&self, c: &str, id: &str, v: Value, p: Precondition) -> Result<Version> {
        self.run(self.inner.put(c, id, v, p)).await
    }
    async fn put_with(
        &self,
        c: &str,
        id: &str,
        v: Value,
        p: Precondition,
        o: WriteOptions,
    ) -> Result<Version> {
        self.run(self.inner.put_with(c, id, v, p, o)).await
    }
    async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
        self.run(self.inner.delete(c, id, p)).await
    }
    async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
        self.run(self.inner.query(c, q)).await
    }
    async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
        self.run(self.inner.count(c, f)).await
    }
    async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
        self.run(self.inner.delete_where(c, f)).await
    }
    async fn claim(
        &self,
        c: &str,
        f: &Filter,
        s: &[Sort],
        p: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        self.run(self.inner.claim(c, f, s, p)).await
    }
    async fn atomic_batch(&self, o: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.run(self.inner.atomic_batch(o)).await
    }
    async fn atomic_batch_checked(
        &self,
        c: Vec<ReadCheck>,
        o: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        self.run(self.inner.atomic_batch_checked(c, o)).await
    }
    async fn search(&self, c: &str, t: &str, l: usize) -> Result<Vec<SearchHit>> {
        self.run(self.inner.search(c, t, l)).await
    }
    async fn drop_collection(&self, c: &str) -> Result<()> {
        self.run(self.inner.drop_collection(c)).await
    }
    async fn collections(&self, p: &str) -> Result<Vec<String>> {
        self.run(self.inner.collections(p)).await
    }
}

#[async_trait]
impl<T: DocumentStore + ?Sized> DocumentStore for ConflictOnce<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    async fn watch(&self, c: &str, f: &Filter, o: &WatchOptions) -> Result<Box<dyn DocumentWatch>> {
        self.inner.watch(c, f, o).await
    }
    async fn ensure_collection(&self, s: &CollectionSpec) -> Result<()> {
        self.check()?;
        self.inner.ensure_collection(s).await
    }
    async fn validate_collection_spec(&self, s: &CollectionSpec) -> Result<()> {
        self.inner.validate_collection_spec(s).await
    }
    async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        self.inner.get(c, id).await
    }
    async fn put(&self, c: &str, id: &str, v: Value, p: Precondition) -> Result<Version> {
        self.check()?;
        self.inner.put(c, id, v, p).await
    }
    async fn put_with(
        &self,
        c: &str,
        id: &str,
        v: Value,
        p: Precondition,
        o: WriteOptions,
    ) -> Result<Version> {
        self.check()?;
        self.inner.put_with(c, id, v, p, o).await
    }
    async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
        self.check()?;
        self.inner.delete(c, id, p).await
    }
    async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
        self.inner.query(c, q).await
    }
    async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
        self.inner.count(c, f).await
    }
    async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
        self.check()?;
        self.inner.delete_where(c, f).await
    }
    async fn claim(
        &self,
        c: &str,
        f: &Filter,
        s: &[Sort],
        p: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        self.check()?;
        self.inner.claim(c, f, s, p).await
    }
    async fn atomic_batch(&self, o: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        self.check()?;
        self.inner.atomic_batch(o).await
    }
    async fn atomic_batch_checked(
        &self,
        c: Vec<ReadCheck>,
        o: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        self.check()?;
        self.inner.atomic_batch_checked(c, o).await
    }
    async fn search(&self, c: &str, t: &str, l: usize) -> Result<Vec<SearchHit>> {
        self.inner.search(c, t, l).await
    }
    async fn drop_collection(&self, c: &str) -> Result<()> {
        self.check()?;
        self.inner.drop_collection(c).await
    }
    async fn collections(&self, p: &str) -> Result<Vec<String>> {
        self.inner.collections(p).await
    }
}

#[async_trait]
impl<T: BlobStore + ?Sized> BlobStore for Slow<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }
    fn supports_sealed_blobs(&self) -> bool {
        self.inner.supports_sealed_blobs()
    }
    async fn put(&self, k: &str, b: Vec<u8>, t: Option<&str>) -> Result<BlobMeta> {
        self.run(self.inner.put(k, b, t)).await
    }
    async fn put_stream(
        &self,
        k: &str,
        r: BlobReader,
        l: u64,
        t: Option<&str>,
    ) -> Result<BlobMeta> {
        self.run(self.inner.put_stream(k, r, l, t)).await
    }
    async fn put_expiring(
        &self,
        k: &str,
        b: Vec<u8>,
        t: Option<&str>,
        d: Duration,
    ) -> Result<BlobMeta> {
        self.run(self.inner.put_expiring(k, b, t, d)).await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.run(
            self.inner
                .put_stream_expiring(key, reader, len, content_type, ttl),
        )
        .await
    }
    async fn get(&self, k: &str) -> Result<Option<Blob>> {
        self.run(self.inner.get(k)).await
    }
    async fn get_stream(&self, k: &str) -> Result<Option<BlobStream>> {
        self.run(self.inner.get_stream(k)).await
    }
    async fn get_range(&self, k: &str, r: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.run(self.inner.get_range(k, r)).await
    }
    async fn head(&self, k: &str) -> Result<Option<BlobMeta>> {
        self.run(self.inner.head(k)).await
    }
    async fn delete(&self, k: &str) -> Result<bool> {
        self.run(self.inner.delete(k)).await
    }
    async fn list(&self, p: &str) -> Result<Vec<BlobMeta>> {
        self.run(self.inner.list(p)).await
    }
    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.run(self.inner.put_sealed(key, bytes, content_type))
            .await
    }
    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        self.run(self.inner.get_sealed(key)).await
    }
    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.run(self.inner.head_sealed(key)).await
    }
    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        self.run(self.inner.delete_sealed(key)).await
    }
    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.run(self.inner.list_sealed(prefix)).await
    }
    async fn list_page(&self, p: &str, a: Option<&str>, l: usize) -> Result<BlobListPage> {
        self.run(self.inner.list_page(p, a, l)).await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        self.run(self.inner.sweep_expired()).await
    }
    async fn presign_get(&self, k: &str, d: Duration) -> Result<String> {
        self.run(self.inner.presign_get(k, d)).await
    }
    async fn presign_put(&self, k: &str, d: Duration) -> Result<String> {
        self.run(self.inner.presign_put(k, d)).await
    }
    async fn list_dir(&self, d: &str) -> Result<Vec<TreeEntry>> {
        self.run(self.inner.list_dir(d)).await
    }
    async fn put_tree(&self, d: &str, f: Vec<TreeFile>) -> Result<Vec<BlobMeta>> {
        self.run(self.inner.put_tree(d, f)).await
    }
    async fn delete_tree(&self, d: &str) -> Result<u64> {
        self.run(self.inner.delete_tree(d)).await
    }
}

#[async_trait]
impl<T: BlobStore + ?Sized> BlobStore for ConflictOnce<std::sync::Arc<T>> {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn max_key_len(&self) -> usize {
        self.inner.max_key_len()
    }
    fn supports_sealed_blobs(&self) -> bool {
        self.inner.supports_sealed_blobs()
    }
    async fn put(&self, k: &str, b: Vec<u8>, t: Option<&str>) -> Result<BlobMeta> {
        self.check()?;
        self.inner.put(k, b, t).await
    }
    async fn put_stream(
        &self,
        k: &str,
        r: BlobReader,
        l: u64,
        t: Option<&str>,
    ) -> Result<BlobMeta> {
        self.check()?;
        self.inner.put_stream(k, r, l, t).await
    }
    async fn put_expiring(
        &self,
        k: &str,
        b: Vec<u8>,
        t: Option<&str>,
        d: Duration,
    ) -> Result<BlobMeta> {
        self.check()?;
        self.inner.put_expiring(k, b, t, d).await
    }
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.check()?;
        self.inner
            .put_stream_expiring(key, reader, len, content_type, ttl)
            .await
    }
    async fn get(&self, k: &str) -> Result<Option<Blob>> {
        self.inner.get(k).await
    }
    async fn get_stream(&self, k: &str) -> Result<Option<BlobStream>> {
        self.inner.get_stream(k).await
    }
    async fn get_range(&self, k: &str, r: Range<u64>) -> Result<Option<Vec<u8>>> {
        self.inner.get_range(k, r).await
    }
    async fn head(&self, k: &str) -> Result<Option<BlobMeta>> {
        self.inner.head(k).await
    }
    async fn delete(&self, k: &str) -> Result<bool> {
        self.check()?;
        self.inner.delete(k).await
    }
    async fn list(&self, p: &str) -> Result<Vec<BlobMeta>> {
        self.inner.list(p).await
    }
    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.check()?;
        self.inner.put_sealed(key, bytes, content_type).await
    }
    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        self.inner.get_sealed(key).await
    }
    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.inner.head_sealed(key).await
    }
    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        self.check()?;
        self.inner.delete_sealed(key).await
    }
    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        self.inner.list_sealed(prefix).await
    }
    async fn list_page(&self, p: &str, a: Option<&str>, l: usize) -> Result<BlobListPage> {
        self.inner.list_page(p, a, l).await
    }
    async fn sweep_expired(&self) -> Result<u64> {
        self.check()?;
        self.inner.sweep_expired().await
    }
    async fn presign_get(&self, k: &str, d: Duration) -> Result<String> {
        self.inner.presign_get(k, d).await
    }
    async fn presign_put(&self, k: &str, d: Duration) -> Result<String> {
        self.check()?;
        self.inner.presign_put(k, d).await
    }
    async fn list_dir(&self, d: &str) -> Result<Vec<TreeEntry>> {
        self.inner.list_dir(d).await
    }
    async fn put_tree(&self, d: &str, f: Vec<TreeFile>) -> Result<Vec<BlobMeta>> {
        self.check()?;
        self.inner.put_tree(d, f).await
    }
    async fn delete_tree(&self, d: &str) -> Result<u64> {
        self.check()?;
        self.inner.delete_tree(d).await
    }
}
