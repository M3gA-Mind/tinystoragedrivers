use super::*;

use futures_util::stream;
use object_store::memory::InMemory;
use tinystoragedrivers_types::ErrorKind;

fn run<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("in-memory S3 operation unexpectedly pended"),
    }
}

fn store() -> S3BlobStore {
    S3BlobStore::with_store(
        Arc::new(InMemory::new()),
        None,
        "tenant-prefix",
        &Scope::new("local").unwrap(),
    )
    .unwrap()
}

#[test]
fn s3_blob_round_trips_empty_content_and_byte_ranges() {
    let store = store();
    run(store.put("empty", Vec::new(), None)).unwrap();
    assert_eq!(
        run(store.get("empty")).unwrap().unwrap().bytes,
        Vec::<u8>::new()
    );
    run(store.put("data", b"abcdef".to_vec(), Some("text/plain"))).unwrap();
    assert_eq!(run(store.get_range("data", 2..5)).unwrap().unwrap(), b"cde");
    assert_eq!(
        run(store.head("data"))
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
}

#[test]
fn s3_sealed_blobs_use_reserved_namespace_without_exposing_it_to_users() {
    let store = store();
    let key = "_tsd/sealed/events/g0";
    run(store.put_sealed(key, b"log".to_vec(), Some("application/zstd"))).unwrap();
    assert_eq!(run(store.get_sealed(key)).unwrap().unwrap().bytes, b"log");
    assert_eq!(run(store.head_sealed(key)).unwrap().unwrap().len, 3);
    assert_eq!(
        run(store.list_sealed("_tsd/sealed/events/")).unwrap().len(),
        1
    );
    assert!(run(store.list("")).unwrap().is_empty());
    assert!(run(store.list_page("", None, 10)).unwrap().items.is_empty());
    assert_eq!(
        run(store.get(key)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(run(store.delete_sealed(key)).unwrap());
    assert_eq!(run(store.head_sealed(key)).unwrap(), None);
}

#[test]
fn short_or_long_s3_stream_does_not_replace_existing_object() {
    let store = store();
    run(store.put("kept", b"original".to_vec(), None)).unwrap();
    let short: BlobReader = Box::pin(stream::iter(vec![Ok(Bytes::from_static(b"short"))]));
    assert_eq!(
        run(store.put_stream("kept", short, 6, None))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(run(store.get("kept")).unwrap().unwrap().bytes, b"original");
    let long: BlobReader = Box::pin(stream::iter(vec![Ok(Bytes::from_static(b"longer"))]));
    assert_eq!(
        run(store.put_stream("kept", long, 3, None))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(run(store.get("kept")).unwrap().unwrap().bytes, b"original");
}

#[test]
fn failed_multipart_stream_aborts_staging_and_keeps_previous_object() {
    let store = store();
    run(store.put("kept", b"original".to_vec(), None)).unwrap();
    let source_error = StorageError::backend("input stream failed");
    let broken: BlobReader = Box::pin(stream::iter(vec![
        Ok(Bytes::from(vec![7; PART_SIZE])),
        Err(source_error),
    ]));
    assert_eq!(
        run(store.put_stream("kept", broken, (PART_SIZE * 2) as u64, None))
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(run(store.get("kept")).unwrap().unwrap().bytes, b"original");

    let broken: BlobReader = Box::pin(stream::iter(vec![
        Ok(Bytes::from(vec![7; PART_SIZE])),
        Err(StorageError::backend("input stream failed")),
    ]));
    assert_eq!(
        run(store.put_stream("unpublished", broken, (PART_SIZE * 2) as u64, None))
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert!(run(store.get("unpublished")).unwrap().is_none());
}

#[test]
fn s3_page_is_scoped_bytewise_and_includes_expired_objects() {
    let store = store();
    run(store.put("dir/b", b"b".to_vec(), None)).unwrap();
    run(store.put("dir/a", b"a".to_vec(), None)).unwrap();
    let first = run(store.list_page("dir/", None, 1)).unwrap();
    assert_eq!(first.items[0].meta.key, "dir/a");
    assert!(first.items[0].written_at_ms.is_some());
    assert_eq!(first.next_after.as_deref(), Some("dir/a"));
    let second = run(store.list_page("dir/", first.next_after.as_deref(), 1)).unwrap();
    assert_eq!(second.items[0].meta.key, "dir/b");
    assert_eq!(second.next_after, None);
    let other = S3BlobStore::with_store(
        store.store.clone(),
        None,
        "tenant-prefix",
        &Scope::new("other").unwrap(),
    )
    .unwrap();
    assert_eq!(run(other.list_page("", None, 5)).unwrap().items.len(), 0);
}

#[test]
fn s3_page_validates_bounds_and_presigning_requires_a_signer() {
    let store = store();
    assert_eq!(
        run(store.list_page("", None, 0)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        run(store.presign_get("item", Duration::from_secs(30)))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    assert_eq!(
        run(store.presign_put("item", Duration::from_secs(7 * 24 * 60 * 60 + 1)))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput,
        "presigned lifetimes above seven days are rejected"
    );
}

#[test]
fn s3_open_rejects_invalid_urls_and_new_handles_empty_connection_prefix() {
    assert_eq!(
        S3BlobStore::open("https://bucket.invalid", &Scope::new("local").unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );

    let connection = S3Connection::with_store(Arc::new(InMemory::new()), None, "test", "");
    let store = S3BlobStore::new(&connection, &Scope::new("local").unwrap()).unwrap();
    assert!(store.max_key_len() > 0);
    run(store.put("empty-prefix", b"ok".to_vec(), None)).unwrap();
    assert_eq!(
        run(store.get("empty-prefix")).unwrap().unwrap().bytes,
        b"ok"
    );
}

#[test]
fn s3_rejects_prefixes_and_keys_that_cannot_form_object_paths() {
    let connection =
        S3Connection::with_store(Arc::new(InMemory::new()), None, "test", "x".repeat(1024));
    assert_eq!(
        S3BlobStore::new(&connection, &Scope::new("local").unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );

    let store = store();
    assert_eq!(
        run(store.put("bad\0key", Vec::new(), None))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        run(store.head(&"k".repeat(store.max_key_len() + 1)))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn s3_rejects_unaddressable_and_multipart_length_mismatches() {
    let store = store();
    let too_large: BlobReader = Box::pin(stream::empty());
    assert_eq!(
        run(store.put_stream(
            "too-large",
            too_large,
            (PART_SIZE as u64) * 10_000 + 1,
            None,
        ))
        .unwrap_err()
        .kind(),
        ErrorKind::InvalidInput
    );

    let long: BlobReader = Box::pin(stream::iter(vec![Ok(Bytes::from(vec![1; PART_SIZE + 2]))]));
    assert_eq!(
        run(store.put_stream("long-multipart", long, (PART_SIZE + 1) as u64, None,))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(run(store.get("long-multipart")).unwrap(), None);

    let short: BlobReader = Box::pin(stream::iter(vec![Ok(Bytes::from(vec![2; PART_SIZE]))]));
    assert_eq!(
        run(store.put_stream("short-multipart", short, (PART_SIZE + 1) as u64, None,))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(run(store.get("short-multipart")).unwrap(), None);
}

#[test]
fn s3_blob_port_exercises_streams_ttl_listing_and_expiry_sweep() {
    let store = store();
    let capabilities = store.capabilities();
    assert!(capabilities.contains(Capability::Blobs));
    assert!(capabilities.contains(Capability::BlobTtl));
    assert!(capabilities.contains(Capability::StreamingBlobs));
    assert!(capabilities.contains(Capability::BlobListing));
    assert!(!capabilities.contains(Capability::PresignedUrls));
    assert!(store.max_key_len() > 0);

    run(store.put("prefix/b", b"two".to_vec(), None)).unwrap();
    run(store.put_expiring("prefix/a", b"one".to_vec(), None, Duration::from_secs(60))).unwrap();
    let streamed: BlobReader = Box::pin(stream::iter(vec![
        Ok(Bytes::from_static(b"stream")),
        Ok(Bytes::from_static(b"ed")),
    ]));
    run(store.put_stream_expiring(
        "prefix/c",
        streamed,
        8,
        Some("text/plain"),
        Duration::from_secs(60),
    ))
    .unwrap();
    assert_eq!(
        run(store.get("prefix/c")).unwrap().unwrap().bytes,
        b"streamed"
    );
    assert_eq!(
        run(store.head("prefix/c"))
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
    assert_eq!(run(store.list("prefix/")).unwrap().len(), 3);
    assert!(run(store.delete("prefix/b")).unwrap());
    assert!(!run(store.delete("prefix/b")).unwrap());
    assert_eq!(run(store.head("prefix/missing")).unwrap(), None);
    assert_eq!(run(store.get_range("prefix/missing", 0..1)).unwrap(), None);
    assert_eq!(
        run(store.list_page("prefix/", None, MAX_BLOB_LIST_PAGE_ITEMS + 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        run(store.list_page("prefix/", Some("elsewhere"), 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );

    run(store.put_expiring("expired", b"gone".to_vec(), None, Duration::ZERO)).unwrap();
    assert_eq!(run(store.get("expired")).unwrap(), None);
    assert_eq!(run(store.head("expired")).unwrap(), None);
    assert!(!run(store.delete("expired")).unwrap());
    run(store.put_expiring("sweep", b"gone".to_vec(), None, Duration::ZERO)).unwrap();
    assert_eq!(run(store.sweep_expired()).unwrap(), 1);
}

#[test]
fn s3_maps_object_store_errors_and_preserves_optional_metadata() {
    let boxed_error = || -> Box<dyn std::error::Error + Send + Sync> {
        Box::new(std::io::Error::other("injected object-store failure"))
    };
    assert_eq!(
        super::map_error(&object_store::Error::NotFound {
            path: "missing".to_owned(),
            source: boxed_error(),
        })
        .kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        super::map_error(&object_store::Error::AlreadyExists {
            path: "present".to_owned(),
            source: boxed_error(),
        })
        .kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(
        super::map_error(&object_store::Error::Precondition {
            path: "changed".to_owned(),
            source: boxed_error(),
        })
        .kind(),
        ErrorKind::Conflict
    );
    assert_eq!(
        super::map_error(&object_store::Error::Generic {
            store: "test",
            source: boxed_error(),
        })
        .kind(),
        ErrorKind::Backend
    );

    let attributes = super::attributes(None, None, None);
    assert_eq!(
        super::object_meta("empty", 0, &attributes).content_type,
        None
    );
    assert!(!super::is_expired(&attributes, 0));
    assert!(super::is_expired(
        &super::attributes(None, None, Some(10)),
        10
    ));
    assert_eq!(
        run(store().presign_get("key", Duration::ZERO))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn successful_multipart_stream_publishes_complete_content() {
    let store = store();
    let body = vec![3_u8; PART_SIZE + 17];
    let expected = body.clone();
    let chunks: BlobReader = Box::pin(stream::iter(vec![
        Ok(Bytes::from(body[..PART_SIZE - 1].to_vec())),
        Ok(Bytes::from(body[PART_SIZE - 1..].to_vec())),
    ]));
    run(store.put_stream("large", chunks, expected.len() as u64, None)).unwrap();
    assert_eq!(run(store.get("large")).unwrap().unwrap().bytes, expected);
}
