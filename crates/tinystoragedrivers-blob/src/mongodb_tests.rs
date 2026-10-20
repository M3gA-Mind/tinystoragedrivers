use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures_util::stream;
use mongodb::bson::oid::ObjectId;
use tinystoragedrivers_core::{Scope as LegacyScope, StorageBackend};
use tinystoragedrivers_types::ErrorKind;

#[tokio::test]
async fn mongodb_blob_streams_scoped_ttl_and_paged_listing() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "mongodb_blob_streams_scoped_ttl_and_paged_listing: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let database = format!("tsd_blob_{}_{}", std::process::id(), ObjectId::new());
    let now = Arc::new(AtomicU64::new(10_000));
    let clock = Arc::clone(&now);
    let connection = MongoConnection::connect(&url, &database)
        .await
        .unwrap()
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let scope = Scope::new("local").unwrap();
    let store = MongoBlobStore::new(connection.clone(), scope);
    let capabilities = store.capabilities();
    assert!(capabilities.contains(Capability::Blobs));
    assert!(capabilities.contains(Capability::BlobTtl));
    assert!(capabilities.contains(Capability::BlobListing));
    assert_eq!(store.max_key_len(), crate::key::MAX_KEY_LEN);
    assert_eq!(store.get("missing").await.unwrap(), None);
    assert_eq!(store.get_range("missing", 0..1).await.unwrap(), None);
    assert_eq!(store.head("missing").await.unwrap(), None);
    assert!(!store.delete("missing").await.unwrap());

    assert_mongo_reads_ranges_and_pages(&store).await;

    let streamed: BlobReader = Box::pin(stream::iter(vec![Ok(bytes::Bytes::from_static(
        b"streamed",
    ))]));
    store
        .put_stream_expiring(
            "streamed",
            streamed,
            8,
            Some("application/octet-stream"),
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(
        store.get("streamed").await.unwrap().unwrap().bytes,
        b"streamed"
    );

    let other_scope = MongoBlobStore::new(connection.clone(), Scope::new("other").unwrap());
    assert_eq!(other_scope.list("").await.unwrap(), Vec::<BlobMeta>::new());

    assert_failed_mongo_uploads_keep_previous_blob(&connection, &store).await;

    store
        .put_expiring("temporary", vec![1], None, Duration::from_millis(10))
        .await
        .unwrap();
    now.store(10_010, Ordering::SeqCst);
    assert!(store.get("temporary").await.unwrap().is_none());
    assert!(
        store
            .list_page("temporary", None, 10)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.meta.key == "temporary")
    );
    assert_eq!(store.sweep_expired().await.unwrap(), 1);
    assert!(store.get("temporary").await.unwrap().is_none());
    assert!(store.delete("prefix/b").await.unwrap());
    assert!(!store.delete("prefix/b").await.unwrap());

    connection.database().drop().await.unwrap();
}

async fn assert_mongo_reads_ranges_and_pages(store: &MongoBlobStore) {
    store.put("prefix/b", b"b".to_vec(), None).await.unwrap();
    store
        .put("prefix/a", b"abcdef".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    assert_eq!(
        store.get("prefix/a").await.unwrap().unwrap().bytes,
        b"abcdef"
    );
    assert_eq!(
        store.get_range("prefix/a", 2..5).await.unwrap().unwrap(),
        b"cde"
    );
    assert_eq!(
        store.get_range("prefix/a", 3..3).await.unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        store
            .head("prefix/a")
            .await
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );

    let first = store.list_page("prefix/", None, 1).await.unwrap();
    assert_eq!(first.items[0].meta.key, "prefix/a");
    assert_eq!(first.items[0].written_at_ms, Some(10_000));
    assert_eq!(first.next_after.as_deref(), Some("prefix/a"));
    let second = store
        .list_page("prefix/", first.next_after.as_deref(), 1)
        .await
        .unwrap();
    assert_eq!(second.items[0].meta.key, "prefix/b");
    assert_eq!(second.next_after, None);
    assert_eq!(store.list("prefix/").await.unwrap().len(), 2);

    store
        .put("prefix.a", b"regex-safe".to_vec(), None)
        .await
        .unwrap();
    let metachar_page = store.list_page("prefix.", None, 10).await.unwrap();
    assert_eq!(metachar_page.items.len(), 1);
    assert_eq!(metachar_page.items[0].meta.key, "prefix.a");
    assert!(
        store
            .list_page("", None, MAX_BLOB_LIST_PAGE_ITEMS + 1)
            .await
            .is_err()
    );
}

async fn assert_failed_mongo_uploads_keep_previous_blob(
    connection: &MongoConnection,
    store: &MongoBlobStore,
) {
    store
        .put("replace", b"original".to_vec(), None)
        .await
        .unwrap();
    let short: BlobReader = Box::pin(stream::iter(vec![Ok(bytes::Bytes::from_static(b"short"))]));
    assert_eq!(
        store
            .put_stream("replace", short, 6, None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store.get("replace").await.unwrap().unwrap().bytes,
        b"original"
    );
    let long: BlobReader = Box::pin(stream::iter(vec![Ok(bytes::Bytes::from_static(b"longer"))]));
    assert_eq!(
        store
            .put_stream("replace", long, 3, None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        store.get("replace").await.unwrap().unwrap().bytes,
        b"original"
    );

    let chunks = connection
        .database()
        .collection::<Document>(&format!("{BUCKET_NAME}.chunks"));
    let chunks_before = chunks.count_documents(doc! {}).await.unwrap();
    let broken: BlobReader = Box::pin(stream::iter(vec![
        Ok(bytes::Bytes::from(vec![7; 512 * 1024])),
        Err(StorageError::backend("input stream failed")),
    ]));
    assert_eq!(
        store
            .put_stream("replace", broken, 1_000_000, None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        store.get("replace").await.unwrap().unwrap().bytes,
        b"original"
    );
    assert_eq!(
        chunks.count_documents(doc! {}).await.unwrap(),
        chunks_before
    );
}

#[tokio::test]
async fn mongodb_reads_blob_written_by_the_v1_driver() {
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        eprintln!(
            "mongodb_reads_blob_written_by_the_v1_driver: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let database = format!("tsd_blob_v1_{}_{}", std::process::id(), ObjectId::new());
    let legacy = tinystoragedrivers_mongodb::MongoStorage::connect(&url, &database)
        .await
        .unwrap();
    let legacy_scope = legacy.for_scope(&LegacyScope::local()).unwrap();
    legacy_scope
        .blobs()
        .put("legacy/key", b"written-by-v1".to_vec(), Some("text/plain"))
        .await
        .unwrap();

    let connection = MongoConnection::connect(&url, &database).await.unwrap();
    let store = MongoBlobStore::new(connection.clone(), Scope::new("local").unwrap());
    let blob = store.get("legacy/key").await.unwrap().unwrap();
    assert_eq!(blob.bytes, b"written-by-v1");
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));
    assert_eq!(blob.meta.len, 13);
    let page = store.list_page("legacy/", None, 10).await.unwrap();
    assert_eq!(page.items[0].meta.key, "legacy/key");
    assert!(page.items[0].written_at_ms.is_some());
    connection.database().drop().await.unwrap();
}
