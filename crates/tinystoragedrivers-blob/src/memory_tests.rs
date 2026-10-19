use super::*;

use std::sync::atomic::{AtomicU64, Ordering};

static CLOCK: AtomicU64 = AtomicU64::new(100);
fn test_clock() -> u64 { CLOCK.load(Ordering::SeqCst) }

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
    let store = MemoryBlobStore::with_clock(Scope::local(), test_clock);
    let other = MemoryBlobStore::with_clock(Scope::new("other").unwrap(), test_clock);
    let meta = run(store.put("dir/a", b"hello".to_vec(), Some("text/plain"))).unwrap();
    assert_eq!(meta.len, 5);
    assert_eq!(run(store.get("dir/a")).unwrap().unwrap().bytes, b"hello");
    assert_eq!(run(store.get_range("dir/a", 1..4)).unwrap().unwrap(), b"ell");
    assert_eq!(run(store.head("dir/a")).unwrap().unwrap().content_type.as_deref(), Some("text/plain"));
    assert_eq!(run(other.get("dir/a")).unwrap(), None);
    assert!(run(store.delete("dir/a")).unwrap());
    assert!(!run(store.delete("dir/a")).unwrap());
}

#[test]
fn paged_listing_is_byte_ordered_includes_expired_until_sweep_and_keeps_unknown_cursor_semantics() {
    CLOCK.store(100, Ordering::SeqCst);
    let store = MemoryBlobStore::with_clock(Scope::local(), test_clock);
    run(store.put("p/b", b"b".to_vec(), None)).unwrap();
    run(store.put("p/a", b"a".to_vec(), None)).unwrap();
    run(store.put_expiring("p/c", b"c".to_vec(), None, Duration::from_millis(1))).unwrap();
    CLOCK.store(102, Ordering::SeqCst);
    let first = run(store.list_page("p/", None, 2)).unwrap();
    assert_eq!(first.items.iter().map(|entry| entry.meta.key.as_str()).collect::<Vec<_>>(), ["p/a", "p/b"]);
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
    let store = MemoryBlobStore::default();
    assert_eq!(run(store.put("", vec![], None)).unwrap_err().kind(), tinystoragedrivers_types::ErrorKind::InvalidInput);
    assert_eq!(run(store.list_page("", None, 0)).unwrap_err().kind(), tinystoragedrivers_types::ErrorKind::InvalidInput);
    assert_eq!(run(store.list_page("p/", Some("other"), 1)).unwrap_err().kind(), tinystoragedrivers_types::ErrorKind::InvalidInput);
}
