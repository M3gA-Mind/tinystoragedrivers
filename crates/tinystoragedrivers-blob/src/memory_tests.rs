use super::*;

use std::sync::atomic::{AtomicU64, Ordering};

static CLOCK: AtomicU64 = AtomicU64::new(100);
fn test_clock() -> u64 {
    CLOCK.load(Ordering::SeqCst)
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("memory operation unexpectedly pended"),
    }
}

#[test]
fn memory_blob_crud_range_and_scope_are_isolated() {
    let store = MemoryBlobStore::with_clock(Scope::new("local").unwrap(), test_clock);
    let other = MemoryBlobStore::with_clock(Scope::new("other").unwrap(), test_clock);
    let meta = run(store.put("dir/a", b"hello".to_vec(), Some("text/plain"))).unwrap();
    assert_eq!(meta.len, 5);
    assert_eq!(run(store.get("dir/a")).unwrap().unwrap().bytes, b"hello");
    assert_eq!(
        run(store.get_range("dir/a", 1..4)).unwrap().unwrap(),
        b"ell"
    );
    assert_eq!(
        run(store.get_range("dir/a", std::ops::Range { start: 4, end: 2 }))
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        run(store.head("dir/a"))
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
    assert_eq!(run(other.get("dir/a")).unwrap(), None);
    assert!(run(store.delete("dir/a")).unwrap());
    assert!(!run(store.delete("dir/a")).unwrap());
}

#[test]
fn paged_listing_is_byte_ordered_includes_expired_until_sweep_and_keeps_unknown_cursor_semantics() {
    CLOCK.store(100, Ordering::SeqCst);
    let store = MemoryBlobStore::with_clock(Scope::new("local").unwrap(), test_clock);
    run(store.put("p/b", b"b".to_vec(), None)).unwrap();
    run(store.put("p/a", b"a".to_vec(), None)).unwrap();
    run(store.put_expiring("p/c", b"c".to_vec(), None, Duration::from_millis(1))).unwrap();
    CLOCK.store(102, Ordering::SeqCst);
    let first = run(store.list_page("p/", None, 2)).unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|entry| entry.meta.key.as_str())
            .collect::<Vec<_>>(),
        ["p/a", "p/b"]
    );
    assert_eq!(first.next_after.as_deref(), Some("p/b"));
    let second = run(store.list_page("p/", first.next_after.as_deref(), 2)).unwrap();
    assert_eq!(second.items[0].meta.key, "p/c");
    assert_eq!(second.items[0].written_at_ms, Some(100));
    assert_eq!(second.next_after, None);
    assert_eq!(run(store.list("p/")).unwrap().len(), 2);
    assert_eq!(run(store.sweep_expired()).unwrap(), 1);
}

#[test]
fn memory_rejects_invalid_keys_and_pagination_arguments() {
    let store = MemoryBlobStore::new(Scope::new("local").unwrap());
    assert_eq!(
        run(store.put("", vec![], None)).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        run(store.list_page("", None, 0)).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        run(store.list_page("p/", Some("other"), 1))
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}

#[test]
fn memory_reports_port_capabilities_and_empty_reads() {
    let store = MemoryBlobStore::new(Scope::new("local").unwrap());
    let capabilities = store.capabilities();
    assert!(capabilities.contains(tinystoragedrivers_types::Capability::Blobs));
    assert!(capabilities.contains(tinystoragedrivers_types::Capability::BlobTtl));
    assert!(capabilities.contains(tinystoragedrivers_types::Capability::BlobListing));
    assert_eq!(store.max_key_len(), crate::key::MAX_KEY_LEN);
    assert_eq!(run(store.get("missing")).unwrap(), None);
    assert_eq!(run(store.get_range("missing", 0..1)).unwrap(), None);
    assert_eq!(run(store.head("missing")).unwrap(), None);
    assert!(!run(store.delete("missing")).unwrap());
    assert_eq!(run(store.list("missing/")).unwrap(), Vec::new());
    assert_eq!(
        run(store.list_page("missing/", None, 1)).unwrap().items,
        Vec::new()
    );
    assert!(run(store.list_page("", None, MAX_BLOB_LIST_PAGE_ITEMS + 1)).is_err());
}

#[test]
fn memory_sealed_blobs_are_scope_bound_and_hidden_from_user_lists() {
    let store = MemoryBlobStore::new(Scope::new("local").unwrap());
    let other = MemoryBlobStore::new(Scope::new("other").unwrap());
    let key = "_tsd/sealed/events/g0";
    assert!(store.supports_sealed_blobs());
    run(store.put_sealed(key, b"sealed".to_vec(), Some("application/zstd"))).unwrap();
    assert_eq!(
        run(store.get_sealed(key)).unwrap().unwrap().bytes,
        b"sealed"
    );
    assert_eq!(run(other.get_sealed(key)).unwrap(), None);
    assert_eq!(run(store.head_sealed(key)).unwrap().unwrap().len, 6);
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
    assert!(!run(store.delete_sealed(key)).unwrap());
}

#[test]
fn poisoned_memory_map_returns_unavailable_from_all_read_and_write_ports() {
    let store = MemoryBlobStore::new(Scope::new("local").unwrap());
    let records = Arc::clone(&store.records);
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = records.lock().unwrap();
        panic!("poison the test store lock");
    }));
    assert!(poisoned.is_err());

    assert!(run(store.get("key")).is_err());
    assert!(run(store.get_range("key", 0..1)).is_err());
    assert!(run(store.head("key")).is_err());
    assert!(run(store.delete("key")).is_err());
    assert!(run(store.list("")).is_err());
    assert!(run(store.list_page("", None, 1)).is_err());
    assert!(run(store.sweep_expired()).is_err());
    assert!(store.store("key", Vec::new(), None, None, false).is_err());
}
