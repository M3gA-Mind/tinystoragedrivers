//! End-to-end behavior through an in-process `TinyBus` broker.
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use tinybus::{BusName, Connection, broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_api::{Storage, StorageBackend};
use tinystoragedrivers_testkit::{MemoryBackend, conformance};
use tinystoragedrivers_types::{Result, Scope};

use super::*;

struct Rig {
    bus: MemoryBus,
    _host: Connection,
    _served: ServedStorage,
    bindings: Arc<Mutex<HashMap<BusName, Storage>>>,
    backend: MemoryBackend,
}

async fn rig() -> Rig {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let bindings = Arc::new(Mutex::new(HashMap::<BusName, Storage>::new()));
    let lookup = Arc::clone(&bindings);
    let endpoint = PerCallerEndpoint::new(move |sender| {
        lookup
            .lock()
            .unwrap()
            .get(sender)
            .cloned()
            .map(StorageServer::new)
    });
    let served = serve(&host, Arc::new(endpoint)).await.unwrap();
    Rig {
        bus,
        _host: host,
        _served: served,
        bindings,
        backend: MemoryBackend::new(),
    }
}

impl Rig {
    async fn module(&self, scope: &str) -> (Connection, Storage) {
        let connection = Connection::connect(self.bus.connect().await.unwrap())
            .await
            .unwrap();
        let scope = Scope::new(scope).unwrap();
        self.bindings.lock().unwrap().insert(
            connection.unique_name().unwrap(),
            self.backend.for_scope(&scope).unwrap(),
        );
        let storage = connect_storage(connection.clone()).await.unwrap();
        assert_eq!(
            storage.secrets().get("secret").await.unwrap_err().kind(),
            tinystoragedrivers_types::ErrorKind::Unsupported(
                tinystoragedrivers_types::Capability::Secrets
            )
        );
        (connection, storage)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn all_six_ports_pass_the_shared_conformance_suites_over_loopback() {
    let server = StorageServer::new(tinystoragedrivers_testkit::memory_storage(
        &Scope::new("transport-conformance").unwrap(),
    ));
    let storage = BusStorage::connect(Arc::new(Loopback::new(
        Arc::new(server),
        Admission::Confidential,
    )))
    .await
    .unwrap();
    conformance::documents::run(storage.documents().as_ref()).await;
    conformance::streams::run(storage.streams().as_ref()).await;
    conformance::kv::run(storage.kv().as_ref()).await;
    conformance::blobs::run(storage.blobs().as_ref()).await;
    conformance::queue::run(storage.queues().as_ref()).await;
    conformance::secrets::run(storage.secrets().as_ref()).await;
    conformance::watch::run(storage.documents().as_ref()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_callers_receive_independent_storage_bindings() {
    let rig = rig().await;
    let (_first, first) = rig.module("caller-a").await;
    let (_second, second) = rig.module("caller-b").await;
    first
        .kv()
        .put("private", serde_json::json!("a"))
        .await
        .unwrap();
    assert!(second.kv().get("private").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ordinary_broker_connection_works_without_secret_admission() {
    let rig = rig().await;
    let (_module, storage) = rig.module("ordinary-secret-denial").await;
    assert!(!storage.secrets().enumerable());
    let error = storage.secrets().get("private").await.unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(
            tinystoragedrivers_types::Capability::Secrets
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn broker_forwards_every_non_confidential_primitive_through_the_bound_host() {
    let rig = rig().await;
    let (_module, storage) = rig.module("broker-conformance").await;
    conformance::documents::run(storage.documents().as_ref()).await;
    conformance::streams::run(storage.streams().as_ref()).await;
    conformance::kv::run(storage.kv().as_ref()).await;
    conformance::blobs::run(storage.blobs().as_ref()).await;
    conformance::queue::run(storage.queues().as_ref()).await;
    conformance::watch::run(storage.documents().as_ref()).await;
    assert!(
        storage
            .blobs()
            .get_stream("absent")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        storage.secrets().get("private").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(
            tinystoragedrivers_types::Capability::Secrets
        )
    );
}

struct OneChunk(Option<Bytes>);

impl Stream for OneChunk {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}

async fn collect(mut stream: tinystoragedrivers_api::BlobReader) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        body.extend_from_slice(&chunk?);
    }
    Ok(body)
}

#[tokio::test(flavor = "multi_thread")]
async fn blobs_larger_than_a_frame_stream_in_bounded_chunks_without_byte_loss() {
    let rig = rig().await;
    let (_module, storage) = rig.module("large-blob").await;
    let bytes: Vec<u8> = (0..(20 * 1024 * 1024 + 17))
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    storage
        .blobs()
        .put_stream(
            "large.bin",
            Box::pin(OneChunk(Some(Bytes::copy_from_slice(&bytes)))),
            bytes.len() as u64,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    let streamed = storage
        .blobs()
        .get_stream("large.bin")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(streamed.meta.len, bytes.len() as u64);
    assert_eq!(collect(streamed.reader).await.unwrap(), bytes);
}
