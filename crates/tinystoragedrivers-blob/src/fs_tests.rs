use super::*;

use std::sync::atomic::{AtomicU64, Ordering};

static CLOCK: AtomicU64 = AtomicU64::new(500);
fn test_clock() -> u64 { CLOCK.load(Ordering::SeqCst) }

fn run<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("file operation unexpectedly pended"),
    }
}

#[test]
fn file_blob_write_keeps_v1_raw_bytes_and_sidecar_shape() {
    let root = tempfile::tempdir().unwrap();
    let store = FileBlobStore::open_with_clock(root.path(), Scope::local(), test_clock).unwrap();
    run(store.put("dir/a.txt", b"legacy bytes".to_vec(), Some("text/plain"))).unwrap();
    let data = root.path().join("scopes/local/blobs/dir%2Fa%2Etxt");
    let sidecar = root.path().join("scopes/local/blobs/dir%2Fa%2Etxt.meta.json");
    assert_eq!(fs::read(data).unwrap(), b"legacy bytes");
    let json: serde_json::Value = serde_json::from_slice(&fs::read(sidecar).unwrap()).unwrap();
    assert_eq!(json["key"], "dir/a.txt");
    assert_eq!(json["content_type"], "text/plain");
    assert_eq!(json["written_at_ms"], 500);
}

#[test]
fn file_blob_reads_v1_sidecar_without_age_and_pages_expired_records() {
    CLOCK.store(500, Ordering::SeqCst);
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("scopes/local/blobs");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("old"), b"old-body").unwrap();
    fs::write(directory.join("old.meta.json"), br#"{"key":"old","content_type":null}"#).unwrap();
    let store = FileBlobStore::open_with_clock(root.path(), Scope::local(), test_clock).unwrap();
    run(store.put_expiring("p/expired", b"gone".to_vec(), None, Duration::from_millis(1))).unwrap();
    CLOCK.store(502, Ordering::SeqCst);
    assert_eq!(run(store.get("old")).unwrap().unwrap().bytes, b"old-body");
    let page = run(store.list_page("", None, 10)).unwrap();
    let old = page.items.iter().find(|entry| entry.meta.key == "old").unwrap();
    let expired = page.items.iter().find(|entry| entry.meta.key == "p/expired").unwrap();
    assert_eq!(old.written_at_ms, None);
    assert_eq!(expired.written_at_ms, Some(500));
    assert!(!run(store.list("")).unwrap().iter().any(|meta| meta.key == "p/expired"));
    assert_eq!(run(store.sweep_expired()).unwrap(), 1);
}

#[test]
fn file_blob_ranges_and_long_hashed_keys_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let store = FileBlobStore::open(root.path(), Scope::local()).unwrap();
    let key = "k".repeat(220);
    run(store.put(&key, b"abcdef".to_vec(), None)).unwrap();
    assert_eq!(run(store.get_range(&key, 2..5)).unwrap().unwrap(), b"cde");
    assert_eq!(run(store.head(&key)).unwrap().unwrap().key, key);
}
