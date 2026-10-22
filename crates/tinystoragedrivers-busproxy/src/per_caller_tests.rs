use super::*;
use crate::Admission;
use crate::Object;
use bytes::Bytes;
use futures_core::Stream;
use serde_json::json;
use std::pin::Pin;
use std::task::{Context, Poll};
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

struct OnceBytes(Option<Bytes>);
impl Stream for OnceBytes {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}
fn reader(bytes: Vec<u8>) -> BlobReader {
    Box::pin(OnceBytes(Some(Bytes::from(bytes))))
}

fn endpoint() -> PerCallerEndpoint<impl Fn(&BusName) -> Option<StorageServer>> {
    let backend = MemoryBackend::new();
    PerCallerEndpoint::new(move |sender| {
        (sender.as_str() == ":1.1")
            .then(|| StorageServer::new(backend.for_scope(&Scope::new("local").unwrap()).unwrap()))
    })
}

fn call(sender: Option<&BusName>) -> StorageCall<'_> {
    StorageCall {
        object: Object::Documents,
        selector: "Capabilities",
        request: serde_json::Value::Null,
        sender,
        admission: Admission::Private,
    }
}

#[tokio::test]
async fn an_unauthenticated_or_unknown_caller_is_denied_on_every_path() {
    let endpoint = endpoint();
    let stranger = BusName::new(":1.2").unwrap();
    for sender in [None, Some(&stranger)] {
        let expected = if sender.is_none() {
            "unauthenticated-caller"
        } else {
            "unknown-caller"
        };
        assert_eq!(
            endpoint.call(call(sender)).await.unwrap_err(),
            HostFailure::Denied(expected)
        );
        let head = PutBlobStreamRequest {
            key: "k".into(),
            len: 0,
            content_type: None,
            ttl_ms: None,
        };
        let body: BlobReader = reader(Vec::new());
        assert_eq!(
            endpoint
                .put_blob_stream(sender, head, body)
                .await
                .unwrap_err(),
            HostFailure::Denied(expected)
        );
        let get = BlobKeyRequest { key: "k".into() };
        assert_eq!(
            endpoint.get_blob_stream(sender, get).await.err(),
            Some(HostFailure::Denied(expected))
        );
    }
    if let Some(sender) = Some(&stranger) {
        assert!(endpoint.watch_signals(sender).is_none());
    }
    assert!(format!("{endpoint:?}").contains("PerCallerEndpoint"));
}

#[tokio::test]
async fn a_known_caller_is_served_from_one_stable_binding() {
    let endpoint = endpoint();
    let known = BusName::new(":1.1").unwrap();
    let reply = endpoint.call(call(Some(&known))).await.unwrap();
    assert!(reply.get("capabilities").is_some());
    // Secrets without a configured store are refused as unsupported.
    let secrets = StorageCall {
        object: Object::Secrets,
        selector: "Get",
        request: json!({"name": "x"}),
        sender: Some(&known),
        admission: Admission::Private,
    };
    assert_eq!(
        endpoint.call(secrets).await.unwrap_err(),
        HostFailure::Storage(tinystoragedrivers_bus::StorageFailure {
            kind: StorageKind::Unsupported
        })
    );
    assert!(endpoint.watch_signals(&known).is_some());
    let head = PutBlobStreamRequest {
        key: "k".into(),
        len: 2,
        content_type: None,
        ttl_ms: None,
    };
    let body: BlobReader = reader(vec![1, 2]);
    let stored = endpoint
        .put_blob_stream(Some(&known), head, body)
        .await
        .unwrap();
    assert_eq!(stored["len"], 2);
    let found = endpoint
        .get_blob_stream(Some(&known), BlobKeyRequest { key: "k".into() })
        .await
        .unwrap();
    assert_eq!(found.unwrap().0.meta.len, 2);
}

#[tokio::test]
async fn a_released_caller_is_rebound_on_its_next_call() {
    let bound = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = Arc::clone(&bound);
    let backend = MemoryBackend::new();
    let endpoint = PerCallerEndpoint::new(move |_: &BusName| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(StorageServer::new(
            backend.for_scope(&Scope::new("local").unwrap()).unwrap(),
        ))
    });
    let caller = BusName::new(":1.1").unwrap();
    endpoint.call(call(Some(&caller))).await.unwrap();
    endpoint.call(call(Some(&caller))).await.unwrap();
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 1);
    let first = endpoint.watch_epoch(&caller);
    assert_eq!(first, endpoint.watch_epoch(&caller), "stable while bound");

    assert!(endpoint.release(&caller));
    assert!(!endpoint.release(&caller), "nothing left to release");
    endpoint.call(call(Some(&caller))).await.unwrap();
    assert_eq!(bound.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_ne!(
        first,
        endpoint.watch_epoch(&caller),
        "a rebind changes the epoch so the host replaces its signal forwarder"
    );
}
