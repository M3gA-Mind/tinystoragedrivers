//! High-value wire boundary regressions for paged documents and streams.
use std::sync::Arc;

use serde_json::json;
use std::time::Duration;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_bus::{StorageFailure, StorageKind};
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{
    Capability, CollectionSpec, ErrorKind, Filter, Precondition, Query, Scope,
};

use super::*;

async fn loopback() -> Storage {
    let server = StorageServer::new(memory_storage(&Scope::new("wire-regressions").unwrap()));
    BusStorage::connect(Arc::new(Loopback::new(
        Arc::new(server),
        Admission::Private,
    )))
    .await
    .unwrap()
}

#[tokio::test]
async fn a_byte_capped_query_page_continues_until_the_cursor_is_exhausted() {
    let storage = loopback().await;
    let docs = storage.documents();
    docs.ensure_collection(&CollectionSpec::new("large-pages"))
        .await
        .unwrap();
    let value = "x".repeat(1024 * 1024);
    for index in 0..10 {
        docs.put(
            "large-pages",
            &format!("item-{index:02}"),
            json!({"body": value}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    }
    let mut query = Query::filter(Filter::All).limit(10);
    let mut found = 0;
    loop {
        let page = docs.query("large-pages", &query).await.unwrap();
        assert!(page.items.len() < 10, "the inline byte cap cuts this page");
        found += page.items.len();
        match page.next {
            Some(cursor) => query = query.after(cursor),
            None => break,
        }
    }
    assert_eq!(found, 10);
}

#[tokio::test]
async fn read_window_assembles_short_pages_cut_by_the_byte_cap() {
    let storage = loopback().await;
    let streams = storage.streams();
    let value = "x".repeat(3 * 1024 * 1024);
    for index in 0..5 {
        streams
            .append("large-records", json!({"index": index, "body": value}))
            .await
            .unwrap();
    }
    let records = streams.read_window("large-records", 0, 5).await.unwrap();
    assert_eq!(records.len(), 5, "a short page does not end the stream");
    assert_eq!(records[4].offset, 4);
}

#[tokio::test]
async fn streams_forward_seal_info_and_listing_results() {
    let storage = loopback().await;
    let streams = storage.streams();
    streams.append("seal-me", json!({"value":1})).await.unwrap();
    let initial = streams.info("seal-me").await.unwrap().unwrap();
    assert_eq!(initial.len, 1);
    assert!(initial.sealed_ms.is_none());

    let outcome = streams.seal("seal-me").await.unwrap();
    assert_eq!(outcome.len, 1);
    let sealed = streams.info("seal-me").await.unwrap().unwrap();
    assert!(sealed.sealed_ms.is_some());
    assert!(
        streams
            .streams("seal")
            .await
            .unwrap()
            .contains(&"seal-me".to_owned())
    );
}

#[tokio::test]
async fn an_unreadable_record_is_refused_before_native_append() {
    let storage = loopback().await;
    let streams = storage.streams();
    let oversized = json!("y".repeat(8 * 1024 * 1024));
    assert_eq!(
        streams
            .append("oversized", oversized)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(streams.len("oversized").await.unwrap(), 0);
    let server = StorageServer::new(memory_storage(&Scope::new("wire-raw").unwrap()));
    let failure = server
        .handle(
            Admission::Private,
            "Streams",
            "Append",
            json!({"stream":"raw","record_json":"y".repeat(8 * 1024 * 1024)}),
        )
        .await
        .unwrap_err();
    assert_eq!(
        failure,
        StorageFailure {
            kind: StorageKind::InvalidInput
        }
    );
}

#[tokio::test]
async fn blobs_cover_inline_streaming_listing_range_ttl_and_presign_paths() {
    let storage = loopback().await;
    let blobs = storage.blobs();
    assert!(blobs.capabilities().contains(Capability::StreamingBlobs));
    assert!(!blobs.capabilities().contains(Capability::PresignedUrls));
    assert_eq!(blobs.max_key_len(), 1024);

    let meta = blobs
        .put("ops/small", b"hello world".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    assert_eq!(meta.len, 11);
    assert_eq!(
        blobs
            .head("ops/small")
            .await
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        blobs.get("ops/small").await.unwrap().unwrap().bytes,
        b"hello world"
    );
    assert_eq!(
        blobs.get_range("ops/small", 6..100).await.unwrap().unwrap(),
        b"world"
    );
    assert_eq!(blobs.list("ops/").await.unwrap().len(), 1);
    assert!(blobs.get("ops/missing").await.unwrap().is_none());
    assert!(
        blobs
            .get_range("ops/missing", 0..1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(blobs.get_stream("ops/missing").await.unwrap().is_none());
    let reversed_range = std::ops::Range { start: 9, end: 2 };
    assert!(blobs.get_range("ops/small", reversed_range).await.is_err());
    assert_eq!(
        blobs.list_page("ops/", None, 0).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let page = blobs.list_page("ops/", None, 10).await.unwrap();
    assert_eq!(page.items.len(), 1);

    let payload = vec![0x6b; tinystoragedrivers_bus::MAX_INLINE_BLOB_BYTES + 1];
    blobs.put("ops/large", payload.clone(), None).await.unwrap();
    assert_eq!(
        blobs.get("ops/large").await.unwrap().unwrap().bytes,
        payload
    );
    blobs
        .put_expiring("ops/ttl", vec![1], None, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(
        blobs
            .presign_get("ops/small", Duration::from_secs(10))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    assert_eq!(
        blobs
            .presign_put("ops/small", Duration::from_secs(10))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    assert!(blobs.delete("ops/small").await.unwrap());
    assert!(!blobs.delete("ops/small").await.unwrap());
}
