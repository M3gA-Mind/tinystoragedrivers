use super::*;
use serde_json::json;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_client::{Admission, Object};
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn endpoint() -> PerCallerEndpoint<impl Fn(&BusName) -> Option<StorageServer>> {
    let backend = MemoryStorage::new();
    PerCallerEndpoint::new(move |sender| {
        (sender.as_str() == ":1.1")
            .then(|| StorageServer::new(backend.for_scope(&Scope::local()).unwrap()))
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
        let body: BlobReader = Box::pin(std::io::Cursor::new(Vec::new()));
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
    let body: BlobReader = Box::pin(std::io::Cursor::new(vec![1, 2]));
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
