//! The server fails closed and never reads authority from a request.
use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

use super::*;

fn server() -> StorageServer {
    let backend = MemoryStorage::new();
    StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
}

async fn kind(server: &StorageServer, object: &str, selector: &str, request: Value) -> StorageKind {
    server
        .handle(Admission::Confidential, object, selector, request)
        .await
        .unwrap_err()
        .kind
}

#[tokio::test]
async fn unknown_objects_and_selectors_are_refused() {
    let server = server();
    assert_eq!(
        kind(&server, "Nope", "Get", json!({})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(&server, "Documents", "Arbitrary", json!({})).await,
        StorageKind::InvalidInput
    );
    // Streamed transfers are not request/reply selectors.
    assert_eq!(
        kind(&server, "Blobs", "PutStream", json!({"key":"a","len":0})).await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn a_forged_scope_or_database_field_is_a_decode_failure() {
    let server = server();
    for forged in ["scope", "database", "grant", "generation"] {
        let mut request = json!({"collection": "notes", "document_id": "n1"});
        request[forged] = json!("bob");
        assert_eq!(
            kind(&server, "Documents", "Get", request).await,
            StorageKind::InvalidInput,
            "{forged}"
        );
    }
}

#[tokio::test]
async fn secrets_without_a_store_or_confidential_admission_are_unsupported() {
    let server = server();
    let denied = server
        .handle(Admission::Private, "Secrets", "List", json!({"prefix": ""}))
        .await
        .unwrap_err();
    assert_eq!(denied.kind, StorageKind::Unsupported);
    assert_eq!(
        kind(&server, "Secrets", "List", json!({"prefix": ""})).await,
        StorageKind::Unsupported
    );
}

#[tokio::test]
async fn batches_and_inline_blobs_over_their_limits_are_refused() {
    let server = server();
    let ops: Vec<Value> = (0..=MAX_BATCH_OPS_PLUS)
        .map(|i| json!({"op":"put","collection":"c","document_id":format!("d{i}"),"document_json":"{}","precondition":{"kind":"none"}}))
        .collect();
    assert_eq!(
        kind(&server, "Documents", "AtomicBatch", json!({"ops": ops})).await,
        StorageKind::InvalidInput
    );
    let blob = vec![0u8; tinystoragedrivers_bus::MAX_INLINE_BLOB_BYTES + 1];
    assert_eq!(
        kind(&server, "Blobs", "Put", json!({"key":"k","bytes": blob})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Blobs",
            "PresignGet",
            json!({"key":"k","ttl_secs": 3601})
        )
        .await,
        StorageKind::InvalidInput
    );
}

const MAX_BATCH_OPS_PLUS: usize = tinystoragedrivers_bus::MAX_BATCH_OPS;

#[tokio::test]
async fn capabilities_never_list_host_only_entries() {
    let server = server();
    let reply = server
        .handle(Admission::Private, "Documents", "Capabilities", Value::Null)
        .await
        .unwrap();
    let names: Vec<String> = serde_json::from_value(reply["capabilities"].clone()).unwrap();
    for host_only in [
        "fencing",
        "scope_purge",
        "atomic_scope_purge",
        "existing_schema",
    ] {
        assert!(!names.iter().any(|n| n == host_only), "{host_only}");
    }
    let _ = Arc::new(());
}

#[tokio::test]
async fn closing_an_unknown_watch_is_not_found() {
    let server = server();
    assert_eq!(
        kind(
            &server,
            "Documents",
            "WatchClose",
            json!({"watch_id":"w999"})
        )
        .await,
        StorageKind::NotFound
    );
}
