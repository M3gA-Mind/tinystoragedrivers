use super::*;

use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tinystoragedrivers_connect::sqlite::rusqlite::params;

static CLOCK: AtomicU64 = AtomicU64::new(1_000);
fn test_clock() -> u64 { CLOCK.load(Ordering::SeqCst) }

fn run<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut context) {
        std::task::Poll::Ready(value) => value,
        std::task::Poll::Pending => panic!("SQLite operation unexpectedly pended"),
    }
}

#[test]
fn sqlite_reads_v1_blob_rows_without_age_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let connection = SqliteConnection::open_with_clock(directory.path(), Arc::new(test_clock)).unwrap();
    let blobs = connection.tables().blobs().to_owned();
    connection.run_now(move |db| {
        let sql = format!("INSERT INTO \"{blobs}\" (scope,key,content_type,size,sha256,expires_ms,bytes) VALUES (?1,?2,?3,?4,?5,NULL,?6)");
        db.execute(&sql, params!["local", "legacy", "text/plain", 8_i64, vec![0_u8; 32], b"old-body"]).map_err(sql_error)?;
        Ok(())
    }).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::local());
    let blob = run(store.get("legacy")).unwrap().unwrap();
    assert_eq!(blob.bytes, b"old-body");
    let page = run(store.list_page("", None, 10)).unwrap();
    assert_eq!(page.items[0].written_at_ms, None);
}

#[test]
fn sqlite_updates_blob_and_age_atomically_and_pages_by_key() {
    CLOCK.store(1_000, Ordering::SeqCst);
    let directory = tempfile::tempdir().unwrap();
    let connection = SqliteConnection::open_with_clock(directory.path(), Arc::new(test_clock)).unwrap();
    let store = SqliteBlobStore::new(connection.clone(), Scope::local());
    run(store.put("p/b", b"b".to_vec(), None)).unwrap();
    run(store.put("p/a", b"a".to_vec(), Some("text/plain"))).unwrap();
    let page = run(store.list_page("p/", None, 1)).unwrap();
    assert_eq!(page.items[0].meta.key, "p/a");
    assert_eq!(page.items[0].written_at_ms, Some(1_000));
    assert_eq!(page.next_after.as_deref(), Some("p/a"));
    assert!(run(store.delete("p/a")).unwrap());
    let age_table = connection.tables().blob_written().to_owned();
    let count = connection.run_now(move |db| {
        let sql = format!("SELECT count(*) FROM \"{age_table}\" WHERE scope=?1 AND key=?2");
        db.query_row(&sql, params!["local", "p/a"], |row| row.get::<_, i64>(0)).map_err(sql_error)
    }).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn sqlite_ttl_reads_hide_expired_rows_and_sweeper_removes_them() {
    CLOCK.store(2_000, Ordering::SeqCst);
    let directory = tempfile::tempdir().unwrap();
    let connection = SqliteConnection::open_with_clock(directory.path(), Arc::new(test_clock)).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::local());
    run(store.put_expiring("temporary", vec![1], None, Duration::from_millis(1))).unwrap();
    CLOCK.store(2_002, Ordering::SeqCst);
    assert_eq!(run(store.get("temporary")).unwrap(), None);
    assert_eq!(run(store.list_page("", None, 10)).unwrap().items.len(), 1);
    assert_eq!(run(store.sweep_expired()).unwrap(), 1);
}
