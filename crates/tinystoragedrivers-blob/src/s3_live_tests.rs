use super::*;

use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::stream;

#[tokio::test]
async fn minio_blob_round_trip_ranges_scopes_and_pages() {
    let Ok(url) = std::env::var("TSD_S3_URL") else {
        eprintln!("minio_blob_round_trip_ranges_scopes_and_pages: TSD_S3_URL is not set; skipping");
        return;
    };
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let scope_name = format!("blob-ci-{}-{nonce}", std::process::id());
    let scope = Scope::new(&scope_name).unwrap();
    let store = S3BlobStore::open(&url, &scope).unwrap();

    let chunks = vec![
        Ok(Bytes::from_static(b"hello ")),
        Ok(Bytes::from_static(b"world")),
    ];
    store
        .put_stream(
            "objects/b",
            Box::pin(stream::iter(chunks)),
            11,
            Some("text/plain"),
        )
        .await
        .unwrap();
    store
        .put("objects/a", b"first".to_vec(), None)
        .await
        .unwrap();

    let blob = store.get("objects/b").await.unwrap().unwrap();
    assert_eq!(blob.bytes, b"hello world");
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        store.get_range("objects/b", 6..11).await.unwrap().unwrap(),
        b"world"
    );

    let first = store.list_page("objects/", None, 1).await.unwrap();
    assert_eq!(first.items[0].meta.key, "objects/a");
    assert!(first.next_after.is_some());
    let second = store
        .list_page("objects/", first.next_after.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(second.items[0].meta.key, "objects/b");
    assert_eq!(second.next_after, None);

    let other_scope_scope = Scope::new("other").unwrap();
    let other_scope = S3BlobStore::open(&url, &other_scope_scope).unwrap();
    assert!(other_scope.get("objects/b").await.unwrap().is_none());

    store
        .put_expiring("objects/expired", vec![1], None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(store.get("objects/expired").await.unwrap(), None);
    assert!(
        store
            .list_page("objects/expired", None, 10)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.meta.key == "objects/expired")
    );
    assert_eq!(store.sweep_expired().await.unwrap(), 1);
}
