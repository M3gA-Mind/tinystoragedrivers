//! The partial callback profile retains native metadata without a blob probe.
use std::sync::Arc;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_busproxy::{Admission, BusStoragePorts, Loopback, StorageServer};
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{
    Capability, CollectionSpec, Filter, Precondition, Scope, WatchOptions, WriteOptions,
};

#[tokio::test]
async fn partial_ports_preserve_metadata_and_native_cas() {
    let storage = memory_storage(&Scope::new("local").unwrap());
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
impl tinystoragedrivers_busproxy::StorageTransport for WatchTransport {
    async fn call(
        &self,
        object: tinystoragedrivers_busproxy::Object,
        selector: &str,
        request: serde_json::Value,
    ) -> Result<serde_json::Value, tinystoragedrivers_busproxy::WireError> {
        if selector == "WatchOpen" {
            return Ok(serde_json::json!({"watch_id":"unexpected"}));
        }
        self.0.call(object, selector, request).await
    }
    async fn put_blob_stream(
        &self,
        head: tinystoragedrivers_bus::PutBlobStreamRequest,
        body: tinystoragedrivers_busproxy::BlobBody,
    ) -> Result<serde_json::Value, tinystoragedrivers_busproxy::WireError> {
        self.0.put_blob_stream(head, body).await
    }
    async fn get_blob_stream(
        &self,
        request: tinystoragedrivers_bus::BlobKeyRequest,
    ) -> Result<
        Option<(
            tinystoragedrivers_bus::BlobStreamHead,
            tinystoragedrivers_busproxy::BlobBody,
        )>,
        tinystoragedrivers_busproxy::WireError,
    > {
        self.0.get_blob_stream(request).await
    }
    fn signals(&self) -> tokio::sync::broadcast::Receiver<tinystoragedrivers_bus::WatchChanged> {
        self.0.signals()
    }
}
#[tokio::test]
async fn partial_ports_refuse_watches_even_on_a_watch_capable_transport() {
    let backend = memory_storage(&Scope::new("local").unwrap());
    let server = Arc::new(StorageServer::new(backend));
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
