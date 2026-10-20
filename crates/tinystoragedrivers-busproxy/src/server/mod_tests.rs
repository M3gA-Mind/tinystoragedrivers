//! The server fails closed and never reads authority from a request.
use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{Scope, WatchCursor, WatchNotice, WatchReset};

use super::*;

fn server() -> StorageServer {
    StorageServer::new(memory_storage(&Scope::new("local").unwrap()))
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
async fn a_forged_scope_or_grant_field_is_a_decode_failure() {
    let server = server();
    for forged in ["scope", "grant", "generation"] {
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
async fn secrets_require_confidential_admission_and_forward_to_the_bound_store() {
    let server = server();
    let denied = server
        .handle(Admission::Private, "Secrets", "List", json!({"prefix": ""}))
        .await
        .unwrap_err();
    assert_eq!(denied.kind, StorageKind::Unsupported);
    assert_eq!(
        server
            .handle(
                Admission::Confidential,
                "Secrets",
                "List",
                json!({"prefix": ""})
            )
            .await
            .unwrap(),
        json!([])
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
        "existing_schema",
        "maintenance",
        "scope_enumeration",
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

#[tokio::test]
async fn watch_signals_are_cursor_only_and_terminal_resets_release_the_slot() {
    let base = memory_storage(&Scope::new("watch-server").unwrap());
    let source = Arc::new(watching_fake::Source::default());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: source.clone(),
        fail_open: false,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    let mut signals = server.signals.subscribe();
    let opened = server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            json!({"collection":"items","filter":{"op":"all"},"interval_ms":100}),
        )
        .await
        .unwrap();
    let id: String = serde_json::from_value(opened["watch_id"].clone()).unwrap();
    let sender = source.0.lock().unwrap()[0].clone();
    sender
        .send(Ok(Some(WatchNotice::Invalidated {
            cursor: Some(WatchCursor::new(vec![4, 2]).unwrap()),
        })))
        .unwrap();
    let invalidated = signals.recv().await.unwrap();
    assert_eq!(invalidated.watch_id, id);
    assert_eq!(
        invalidated.notice,
        wire::WatchNotice::Invalidated {
            cursor: Some(vec![4, 2])
        }
    );
    sender
        .send(Ok(Some(WatchNotice::ResetRequired {
            reason: WatchReset::ResumeLost,
            source: None,
        })))
        .unwrap();
    let reset = signals.recv().await.unwrap();
    assert_eq!(
        reset.notice,
        wire::WatchNotice::ResetRequired {
            reason: wire::WatchReset::CursorExpired
        }
    );
    tokio::task::yield_now().await;
    assert_eq!(server.open_watches(), 0);
    assert_eq!(
        server
            .handle(
                Admission::Private,
                "Documents",
                "WatchClose",
                json!({"watch_id":id}),
            )
            .await
            .unwrap_err()
            .kind,
        StorageKind::NotFound
    );
}

#[tokio::test]
async fn watch_open_rejects_invalid_input_and_sanitizes_backend_failures() {
    let invalid = server();
    for request in [
        json!({"collection":"items","filter":{"op":"all"},"interval_ms":0}),
        json!({"collection":"items","filter":{"op":"all"},"interval_ms":100,"after":[]}),
    ] {
        assert_eq!(
            invalid
                .handle(Admission::Private, "Documents", "WatchOpen", request)
                .await
                .unwrap_err()
                .kind,
            StorageKind::InvalidInput
        );
    }

    let base = memory_storage(&Scope::new("watch-failed-open").unwrap());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: Arc::new(watching_fake::Source::default()),
        fail_open: true,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    assert_eq!(
        kind(
            &server,
            "Documents",
            "WatchOpen",
            json!({"collection":"items","filter":{"op":"all"},"interval_ms":100}),
        )
        .await,
        StorageKind::Backend
    );
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test]
async fn native_watch_errors_emit_only_a_terminal_backend_category() {
    let base = memory_storage(&Scope::new("watch-error").unwrap());
    let source = Arc::new(watching_fake::Source::default());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: source.clone(),
        fail_open: false,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    let mut signals = server.signals.subscribe();
    let opened = server
        .handle(
            Admission::Private,
            "Documents",
            "WatchOpen",
            json!({"collection":"items","filter":{"op":"all"},"interval_ms":100}),
        )
        .await
        .unwrap();
    let id: String = serde_json::from_value(opened["watch_id"].clone()).unwrap();
    source.0.lock().unwrap()[0]
        .send(Err(tinystoragedrivers_types::StorageError::backend(
            "private diagnostic detail",
        )))
        .unwrap();
    let signal = signals.recv().await.unwrap();
    assert_eq!(signal.watch_id, id);
    assert_eq!(
        signal.notice,
        wire::WatchNotice::ResetRequired {
            reason: wire::WatchReset::Backend
        }
    );
    assert!(!format!("{signal:?}").contains("private diagnostic detail"));
    tokio::task::yield_now().await;
    assert_eq!(server.open_watches(), 0);
}

#[tokio::test]
async fn watch_registry_enforces_its_per_binding_capacity() {
    let base = memory_storage(&Scope::new("watch-capacity").unwrap());
    let docs = Arc::new(watching_fake::Docs {
        inner: base.documents().clone(),
        source: Arc::new(watching_fake::Source::default()),
        fail_open: false,
    });
    let storage = Storage::new(
        docs,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    let request = || json!({"collection":"items","filter":{"op":"all"},"interval_ms":100});
    let mut ids = Vec::new();
    for _ in 0..MAX_WATCHES {
        let opened = server
            .handle(Admission::Private, "Documents", "WatchOpen", request())
            .await
            .unwrap();
        ids.push(opened["watch_id"].as_str().unwrap().to_owned());
    }
    assert_eq!(server.open_watches(), MAX_WATCHES);
    assert_eq!(
        server
            .handle(Admission::Private, "Documents", "WatchOpen", request())
            .await
            .unwrap_err()
            .kind,
        StorageKind::InvalidInput
    );
    for id in ids {
        server
            .handle(
                Admission::Private,
                "Documents",
                "WatchClose",
                json!({"watch_id":id}),
            )
            .await
            .unwrap();
    }
    assert_eq!(server.open_watches(), 0);
}

async fn call(server: &StorageServer, object: &str, selector: &str, request: Value) -> Value {
    server
        .handle(Admission::Private, object, selector, request)
        .await
        .unwrap()
}

#[tokio::test]
async fn put_options_land_as_metadata_that_every_read_returns() {
    let server = server();
    let far = 4_102_444_800_000_i64;
    call(
        &server,
        "Documents",
        "Put",
        json!({"collection":"c","document_id":"a","document_json":"{\"n\":1}",
               "precondition":{"kind":"none"},"schema":3,"expires_ms":far}),
    )
    .await;
    let got = call(
        &server,
        "Documents",
        "Get",
        json!({"collection":"c","document_id":"a"}),
    )
    .await;
    assert_eq!(got["meta"]["schema"], 3);
    assert_eq!(got["meta"]["expires_ms"], far);
    assert!(got["meta"]["created_ms"].as_i64().unwrap() > 0);
    let page = call(
        &server,
        "Documents",
        "Query",
        json!({"collection":"c","query":{}}),
    )
    .await;
    assert_eq!(page["items"][0]["meta"]["schema"], 3);
    let claimed = call(
        &server,
        "Documents",
        "Claim",
        json!({"collection":"c","filter":{"op":"all"},"patch":"{\"n\":2}"}),
    )
    .await;
    assert_eq!(claimed["meta"]["schema"], 3);
}

#[path = "../watching_fake.rs"]
mod watching_fake;

#[tokio::test]
async fn a_put_without_options_is_the_pre_v4_request_and_a_forged_created_ms_is_refused() {
    let server = server();
    call(
        &server,
        "Documents",
        "Put",
        json!({"collection":"c","document_id":"a","document_json":"{}",
               "precondition":{"kind":"none"}}),
    )
    .await;
    assert_eq!(
        kind(
            &server,
            "Documents",
            "Put",
            json!({"collection":"c","document_id":"b","document_json":"{}",
                   "precondition":{"kind":"none"},"created_ms":1}),
        )
        .await,
        StorageKind::InvalidInput
    );
    // Nothing was written by the refused request.
    let got = call(
        &server,
        "Documents",
        "Get",
        json!({"collection":"c","document_id":"b"}),
    )
    .await;
    assert!(got.is_null());
}

#[tokio::test]
async fn seal_closes_a_stream_and_maintenance_selectors_do_not_exist() {
    let server = server();
    call(
        &server,
        "Streams",
        "Append",
        json!({"stream":"s","record_json":"1"}),
    )
    .await;
    let sealed = call(&server, "Streams", "Seal", json!({"stream":"s"})).await;
    assert_eq!(
        sealed,
        json!({"base":0,"len":1,"blob_bytes":sealed["blob_bytes"]})
    );
    assert_eq!(
        kind(
            &server,
            "Streams",
            "Append",
            json!({"stream":"s","record_json":"2"})
        )
        .await,
        StorageKind::Conflict
    );
    assert_eq!(
        kind(&server, "Streams", "Seal", json!({"stream":"never"})).await,
        StorageKind::NotFound
    );
    for selector in ["Maintain", "Checkpoint", "Scopes"] {
        for object in ["Documents", "Streams", "Blobs"] {
            assert_eq!(
                kind(&server, object, selector, json!({})).await,
                StorageKind::InvalidInput
            );
        }
    }
}

#[tokio::test]
async fn info_reports_extent_and_times_of_a_stream_and_null_for_a_missing_one() {
    let server = server();
    assert!(
        call(&server, "Streams", "Info", json!({"stream":"s"}))
            .await
            .is_null()
    );
    call(
        &server,
        "Streams",
        "Append",
        json!({"stream":"s","record_json":"1"}),
    )
    .await;
    let open = call(&server, "Streams", "Info", json!({"stream":"s"})).await;
    assert_eq!(
        (open["base"].clone(), open["len"].clone()),
        (json!(0), json!(1))
    );
    assert!(open["sealed_ms"].is_null());
    assert!(open["last_append_ms"].is_i64());

    call(&server, "Streams", "Seal", json!({"stream":"s"})).await;
    let sealed = call(&server, "Streams", "Info", json!({"stream":"s"})).await;
    assert!(sealed["sealed_ms"].is_i64());
    assert_eq!(sealed["last_append_ms"], open["last_append_ms"]);
}
