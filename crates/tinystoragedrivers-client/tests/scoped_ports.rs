//! The partial callback profile retains native metadata without a blob probe.
use std::sync::Arc;
use tinystoragedrivers_client::{Admission, BusStoragePorts, Loopback, StorageServer};
use tinystoragedrivers_core::{
    Capability, CollectionSpec, DocumentStore, MemoryStorage, Precondition, Scope, StorageBackend,
    WriteOptions,
};

#[tokio::test]
async fn partial_ports_preserve_metadata_and_native_cas() {
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::local()).unwrap();
    let server = Arc::new(StorageServer::new(storage));
    let ports = BusStoragePorts::connect(Arc::new(Loopback::new(server, Admission::Confidential)))
        .await
        .unwrap();
    let docs = ports.documents();
    assert!(ports.capabilities().contains(Capability::RecordMetadata));
    for cap in [
        Capability::Watch,
        Capability::StreamingBlobs,
        Capability::PresignedUrls,
        Capability::BlobTtl,
    ] {
        assert!(!ports.capabilities().contains(cap));
    }
    docs.ensure_collection(&CollectionSpec::new("cache"))
        .await
        .unwrap();
    let version = docs
        .put_with(
            "cache",
            "key",
            serde_json::json!({"v": 1}),
            Precondition::Absent,
            WriteOptions {
                schema: 7,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let found = docs.get("cache", "key").await.unwrap().unwrap();
    assert_eq!(found.meta.schema, 7);
    assert_eq!(found.version, version);
    assert!(
        docs.put("cache", "key", serde_json::json!({}), Precondition::Absent)
            .await
            .is_err()
    );
}

#[derive(Debug)]
struct WatchTransport(Loopback);
#[async_trait::async_trait]
impl tinystoragedrivers_client::StorageTransport for WatchTransport {
    async fn call(
        &self,
        object: tinystoragedrivers_client::Object,
        selector: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, tinystoragedrivers_client::WireError> {
        if selector == "WatchOpen" {
            return Ok(serde_json::json!({"watch_id":"unexpected"}));
        }
        self.0.call(object, selector, request).await
    }
    async fn put_blob_stream(
        &self,
        head: tinystoragedrivers_bus::PutBlobStreamRequest,
        body: tinystoragedrivers_client::BlobBody,
    ) -> Result<serde_json::Value, tinystoragedrivers_client::WireError> {
        self.0.put_blob_stream(head, body).await
    }
    async fn get_blob_stream(
        &self,
        request: tinystoragedrivers_bus::BlobKeyRequest,
    ) -> Result<
        Option<(
            tinystoragedrivers_bus::BlobStreamHead,
            tinystoragedrivers_client::BlobBody,
        )>,
        tinystoragedrivers_client::WireError,
    > {
        self.0.get_blob_stream(request).await
    }
    fn signals(&self) -> tokio::sync::broadcast::Receiver<tinystoragedrivers_bus::WatchChanged> {
        self.0.signals()
    }
}
#[tokio::test]
async fn partial_ports_refuse_watches_even_on_a_watch_capable_transport() {
    use tinystoragedrivers_core::{Filter, WatchOptions};
    let backend = MemoryStorage::new();
    let server = Arc::new(StorageServer::new(
        backend.for_scope(&Scope::local()).unwrap(),
    ));
    let ports = BusStoragePorts::connect(Arc::new(WatchTransport(Loopback::new(
        server.clone(),
        Admission::Confidential,
    ))))
    .await
    .unwrap();
    assert!(
        ports
            .documents()
            .watch("cache", &Filter::All, &WatchOptions::default())
            .await
            .is_err()
    );
    assert_eq!(server.open_watches(), 0);
}
