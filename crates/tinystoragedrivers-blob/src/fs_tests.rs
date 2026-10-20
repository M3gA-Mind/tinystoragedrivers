use super::*;

use tinystoragedrivers_core::{Scope as LegacyScope, StorageBackend};

use std::sync::atomic::{AtomicU64, Ordering};

static CLOCK: AtomicU64 = AtomicU64::new(500);
fn test_clock() -> u64 {
    CLOCK.load(Ordering::SeqCst)
}

fn write_test_clock() -> u64 {
    500
}

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
    let store =
        FileBlobStore::open_with_clock(root.path(), Scope::new("local").unwrap(), write_test_clock)
            .unwrap();
    run(store.put("dir/a.txt", b"legacy bytes".to_vec(), Some("text/plain"))).unwrap();
    let data = root.path().join("scopes/local/blobs/dir%2Fa%2Etxt");
    let sidecar = root
        .path()
        .join("scopes/local/blobs/dir%2Fa%2Etxt.meta.json");
    assert_eq!(fs::read(data).unwrap(), b"legacy bytes");
    let json: serde_json::Value = serde_json::from_slice(&fs::read(sidecar).unwrap()).unwrap();
    assert_eq!(json["key"], "dir/a.txt");
    assert_eq!(json["content_type"], "text/plain");
    assert_eq!(json["written_at_ms"], 500);
}

#[test]
fn file_sealed_blobs_share_the_scope_but_stay_hidden_from_user_listing() {
    let root = tempfile::tempdir().unwrap();
    let store =
        FileBlobStore::open_with_clock(root.path(), Scope::new("local").unwrap(), write_test_clock)
            .unwrap();
    let key = "_tsd/sealed/events/g0";
    run(store.put_sealed(key, b"log".to_vec(), Some("application/zstd"))).unwrap();
    assert_eq!(run(store.get_sealed(key)).unwrap().unwrap().bytes, b"log");
    assert_eq!(run(store.head_sealed(key)).unwrap().unwrap().len, 3);
    assert_eq!(
        run(store.list_sealed("_tsd/sealed/events/")).unwrap().len(),
        1
    );
    assert_eq!(run(store.list("")).unwrap(), Vec::new());
    assert_eq!(
        run(store.list_page("", None, 10)).unwrap().items,
        Vec::new()
    );
    assert_eq!(
        run(store.get(key)).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert!(run(store.delete_sealed(key)).unwrap());
    assert_eq!(run(store.head_sealed(key)).unwrap(), None);
}

#[test]
fn file_blob_reads_v1_sidecar_without_age_and_pages_expired_records() {
    CLOCK.store(500, Ordering::SeqCst);
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("scopes/local/blobs");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("old"), b"old-body").unwrap();
    fs::write(
        directory.join("old.meta.json"),
        br#"{"key":"old","content_type":null}"#,
    )
    .unwrap();
    let store =
        FileBlobStore::open_with_clock(root.path(), Scope::new("local").unwrap(), test_clock)
            .unwrap();
    run(store.put_expiring(
        "p/expired",
        b"gone".to_vec(),
        None,
        Duration::from_millis(1),
    ))
    .unwrap();
    CLOCK.store(502, Ordering::SeqCst);
    assert_eq!(run(store.get("old")).unwrap().unwrap().bytes, b"old-body");
    let page = run(store.list_page("", None, 10)).unwrap();
    let old = page
        .items
        .iter()
        .find(|entry| entry.meta.key == "old")
        .unwrap();
    let expired = page
        .items
        .iter()
        .find(|entry| entry.meta.key == "p/expired")
        .unwrap();
    assert_eq!(old.written_at_ms, None);
    assert_eq!(expired.written_at_ms, Some(500));
    assert!(
        !run(store.list(""))
            .unwrap()
            .iter()
            .any(|meta| meta.key == "p/expired")
    );
    assert_eq!(run(store.sweep_expired()).unwrap(), 1);
}

#[tokio::test]
async fn file_blob_reads_bytes_written_by_the_v1_driver() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = tinystoragedrivers_file::FileStorage::open(dir.path()).unwrap();
    let legacy_scope = LegacyScope::new("local").unwrap();
    legacy
        .for_scope(&legacy_scope)
        .unwrap()
        .blobs()
        .put("legacy/file", b"v1 file bytes".to_vec(), Some("text/plain"))
        .await
        .unwrap();

    let store = FileBlobStore::open(dir.path(), Scope::new("local").unwrap()).unwrap();
    let blob = store.get("legacy/file").await.unwrap().unwrap();
    assert_eq!(blob.bytes, b"v1 file bytes");
    assert_eq!(blob.meta.len, 13);
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));
    assert_eq!(
        store.list_page("legacy/", None, 10).await.unwrap().items[0].written_at_ms,
        None
    );
}

#[test]
fn file_blob_ranges_and_long_hashed_keys_round_trip() {
    let root = tempfile::tempdir().unwrap();
    let store = FileBlobStore::open(root.path(), Scope::new("local").unwrap()).unwrap();
    let key = "k".repeat(220);
    run(store.put(&key, b"abcdef".to_vec(), None)).unwrap();
    assert_eq!(run(store.get_range(&key, 2..5)).unwrap().unwrap(), b"cde");
    assert_eq!(run(store.head(&key)).unwrap().unwrap().key, key);
}

#[test]
fn file_blob_port_covers_empty_reads_listing_and_delete() {
    let root = tempfile::tempdir().unwrap();
    let store = FileBlobStore::open(root.path(), Scope::new("local").unwrap()).unwrap();
    let capabilities = store.capabilities();
    assert!(capabilities.contains(tinystoragedrivers_types::Capability::Blobs));
    assert!(capabilities.contains(tinystoragedrivers_types::Capability::BlobTtl));
    assert!(capabilities.contains(tinystoragedrivers_types::Capability::BlobListing));
    assert_eq!(store.max_key_len(), crate::key::MAX_KEY_LEN);
    assert_eq!(run(store.get("missing")).unwrap(), None);
    assert_eq!(run(store.get_range("missing", 0..1)).unwrap(), None);
    assert_eq!(run(store.head("missing")).unwrap(), None);
    assert!(!run(store.delete("missing")).unwrap());
    assert_eq!(run(store.list("")).unwrap(), Vec::new());
    assert_eq!(
        run(store.list_page("", None, 10)).unwrap().items,
        Vec::new()
    );

    run(store.put("dir/a", b"body".to_vec(), None)).unwrap();
    assert_eq!(run(store.list("dir/")).unwrap().len(), 1);
    assert_eq!(run(store.get_range("dir/a", 1..3)).unwrap().unwrap(), b"od");
    assert!(run(store.delete("dir/a")).unwrap());
    assert!(!run(store.delete("dir/a")).unwrap());
}

#[test]
fn file_blob_rejects_corrupt_metadata_collisions_and_nonregular_paths() {
    let root = tempfile::tempdir().unwrap();
    let store = FileBlobStore::open(root.path(), Scope::new("local").unwrap()).unwrap();
    let base = root.path().join("scopes/local/blobs");
    fs::create_dir_all(&base).unwrap();

    fs::write(base.join("orphan.meta.json"), br#"{"key":"orphan"}"#).unwrap();
    assert_eq!(run(store.head("orphan")).unwrap(), None);

    fs::write(base.join("corrupt.meta.json"), b"not json").unwrap();
    assert!(run(store.head("corrupt")).is_err());

    fs::write(base.join("mismatch.meta.json"), br#"{"key":"different"}"#).unwrap();
    assert!(run(store.head("mismatch")).is_err());

    fs::create_dir(base.join("directory-data")).unwrap();
    fs::write(
        base.join("directory-data.meta.json"),
        br#"{"key":"directory-data"}"#,
    )
    .unwrap();
    assert!(run(store.get("directory-data")).is_err());

    fs::create_dir(base.join("directory-sidecar.meta.json")).unwrap();
    assert!(run(store.head("directory-sidecar")).is_err());
}

#[test]
fn filesystem_scope_encoding_and_cleanup_cover_reserved_names_and_long_components() {
    assert_eq!(super::encode("con"), "%63on");
    assert_eq!(super::encode("COM1"), "%43%4F%4D1");
    assert!(super::is_reserved("nul"));
    assert!(super::is_reserved("com9"));
    assert!(super::is_reserved("lpt3"));
    assert!(!super::is_reserved("comx"));
    assert!(super::dir_components(&"x".repeat(201)).components().count() > 1);

    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("target");
    fs::create_dir(&directory).unwrap();
    assert!(super::remove_if_exists(&directory).is_err());
    assert!(super::remove_if_exists(&directory.join("missing")).is_ok());

    assert_eq!(
        super::write_atomic(&directory, b"cannot replace a directory")
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Backend
    );
}
