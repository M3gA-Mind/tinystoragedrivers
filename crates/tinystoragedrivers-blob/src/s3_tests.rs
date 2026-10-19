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
    S3BlobStore::with_store(Arc::new(InMemory::new()), None, "tenant-prefix", Scope::local()).unwrap()
}

#[test]
fn s3_blob_round_trips_empty_content_and_byte_ranges() {
    let store = store();
    run(store.put("empty", Vec::new(), None)).unwrap();
    assert_eq!(run(store.get("empty")).unwrap().unwrap().bytes, Vec::<u8>::new());
    run(store.put("data", b"abcdef".to_vec(), Some("text/plain"))).unwrap();
    assert_eq!(run(store.get_range("data", 2..5)).unwrap().unwrap(), b"cde");
    assert_eq!(run(store.head("data")).unwrap().unwrap().content_type.as_deref(), Some("text/plain"));
}

#[test]
fn short_or_long_s3_stream_does_not_replace_existing_object() {
    let store = store();
    run(store.put("kept", b"original".to_vec(), None)).unwrap();
    let short: BlobReader = Box::pin(stream::iter(vec![Ok(Bytes::from_static(b"short"))]));
    assert_eq!(run(store.put_stream("kept", short, 6, None)).unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(run(store.get("kept")).unwrap().unwrap().bytes, b"original");
    let long: BlobReader = Box::pin(stream::iter(vec![Ok(Bytes::from_static(b"longer"))]));
    assert_eq!(run(store.put_stream("kept", long, 3, None)).unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(run(store.get("kept")).unwrap().unwrap().bytes, b"original");
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
    let other = S3BlobStore::with_store(store.store.clone(), None, "tenant-prefix", Scope::new("other").unwrap()).unwrap();
    assert_eq!(run(other.list_page("", None, 5)).unwrap().items.len(), 0);
}

#[test]
fn s3_page_validates_bounds_and_presigning_requires_a_signer() {
    let store = store();
    assert_eq!(run(store.list_page("", None, 0)).unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(run(store.presign_get("item", Duration::from_secs(30))).unwrap_err().kind(), ErrorKind::Unsupported(Capability::PresignedUrls));
}
