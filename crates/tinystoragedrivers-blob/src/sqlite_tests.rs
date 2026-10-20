use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tinystoragedrivers_connect::sqlite::rusqlite::params;
use tinystoragedrivers_types::ErrorKind;

fn connection_clock(value: u64) -> (Arc<AtomicU64>, tinystoragedrivers_connect::Clock) {
    let now = Arc::new(AtomicU64::new(value));
    let reading = Arc::clone(&now);
    (now, Arc::new(move || reading.load(Ordering::SeqCst)))
}

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
    let (_, clock) = connection_clock(1_000);
    let connection = SqliteConnection::open_with_clock(directory.path(), clock).unwrap();
    let blobs = connection.tables().blobs().to_owned();
    connection.run_now(move |db| {
        let sql = format!("INSERT INTO \"{blobs}\" (scope,key,content_type,size,sha256,expires_ms,bytes) VALUES (?1,?2,?3,?4,?5,NULL,?6)");
        db.execute(&sql, params!["local", "legacy", "text/plain", 8_i64, vec![0_u8; 32], b"old-body"]).map_err(sql_error)?;
        Ok(())
    }).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::new("local").unwrap());
    let blob = run(store.get("legacy")).unwrap().unwrap();
    assert_eq!(blob.bytes, b"old-body");
    let page = run(store.list_page("", None, 10)).unwrap();
    assert_eq!(page.items[0].written_at_ms, None);
}

#[test]
fn sqlite_sealed_blobs_share_the_scope_but_stay_hidden_from_user_listing() {
    let directory = tempfile::tempdir().unwrap();
    let connection = SqliteConnection::open(directory.path()).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::new("local").unwrap());
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
fn sqlite_reads_blob_from_historical_v1_database_fixture() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("storage.db");
    std::fs::write(
        &path,
        include_bytes!("../../tinystoragedrivers-sqlite/tests/fixtures/storage-be94f75.db"),
    )
    .unwrap();
    let (_, clock) = connection_clock(1_000);
    let connection = SqliteConnection::open_with_clock(&path, clock).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::new("local").unwrap());

    let blob = run(store.get("fixture/blob")).unwrap().unwrap();
    assert_eq!(blob.bytes, b"be94f75-preserved-blob");
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));
    let page = run(store.list_page("fixture/", None, 10)).unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].written_at_ms, None);
}

#[test]
fn sqlite_updates_blob_and_age_atomically_and_pages_by_key() {
    let directory = tempfile::tempdir().unwrap();
    let (_, clock) = connection_clock(1_000);
    let connection = SqliteConnection::open_with_clock(directory.path(), clock).unwrap();
    let store = SqliteBlobStore::new(connection.clone(), Scope::new("local").unwrap());
    run(store.put("p/b", b"b".to_vec(), None)).unwrap();
    run(store.put("p/a", b"a".to_vec(), Some("text/plain"))).unwrap();
    let page = run(store.list_page("p/", None, 1)).unwrap();
    assert_eq!(page.items[0].meta.key, "p/a");
    assert_eq!(page.items[0].written_at_ms, Some(1_000));
    assert_eq!(page.next_after.as_deref(), Some("p/a"));
    assert!(run(store.delete("p/a")).unwrap());
    let age_table = connection.tables().blob_written().to_owned();
    let count = connection
        .run_now(move |db| {
            let sql = format!("SELECT count(*) FROM \"{age_table}\" WHERE scope=?1 AND key=?2");
            db.query_row(&sql, params!["local", "p/a"], |row| row.get::<_, i64>(0))
                .map_err(sql_error)
        })
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn sqlite_ttl_reads_hide_expired_rows_and_sweeper_removes_them() {
    let directory = tempfile::tempdir().unwrap();
    let (now, clock) = connection_clock(2_000);
    let connection = SqliteConnection::open_with_clock(directory.path(), clock).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::new("local").unwrap());
    run(store.put_expiring("temporary", vec![1], None, Duration::from_millis(1))).unwrap();
    now.store(2_002, Ordering::SeqCst);
    assert_eq!(run(store.get("temporary")).unwrap(), None);
    assert_eq!(run(store.list_page("", None, 10)).unwrap().items.len(), 1);
    assert_eq!(run(store.sweep_expired()).unwrap(), 1);
}

#[test]
fn sqlite_blob_port_reports_capabilities_and_covers_empty_reads_and_ranges() {
    let directory = tempfile::tempdir().unwrap();
    let (_, clock) = connection_clock(3_000);
    let connection = SqliteConnection::open_with_clock(directory.path(), clock).unwrap();
    let store = SqliteBlobStore::new(connection, Scope::new("local").unwrap());
    let capabilities = store.capabilities();
    assert!(capabilities.contains(Capability::Blobs));
    assert!(capabilities.contains(Capability::BlobTtl));
    assert!(capabilities.contains(Capability::BlobListing));
    assert_eq!(store.max_key_len(), crate::key::MAX_KEY_LEN);

    assert_eq!(run(store.get("missing")).unwrap(), None);
    assert_eq!(run(store.get_range("missing", 0..2)).unwrap(), None);
    assert_eq!(run(store.head("missing")).unwrap(), None);
    assert!(!run(store.delete("missing")).unwrap());
    assert_eq!(run(store.list("missing/")).unwrap(), Vec::new());
    assert_eq!(
        run(store.list_page("missing/", None, 1)).unwrap().items,
        Vec::new()
    );
    assert!(run(store.list_page("", None, MAX_BLOB_LIST_PAGE_ITEMS + 1)).is_err());

    run(store.put("dir/a", b"abcdef".to_vec(), Some("text/plain"))).unwrap();
    run(store.put("dir/b", b"second".to_vec(), None)).unwrap();
    assert_eq!(
        run(store.get_range("dir/a", 2..5)).unwrap().unwrap(),
        b"cde"
    );
    assert_eq!(
        run(store.head("dir/a"))
            .unwrap()
            .unwrap()
            .content_type
            .as_deref(),
        Some("text/plain")
    );
    assert_eq!(run(store.list("dir/")).unwrap().len(), 2);
}

#[test]
fn sqlite_maps_database_errors_to_backend_errors() {
    let directory = tempfile::tempdir().unwrap();
    let (_, clock) = connection_clock(3_000);
    let connection = SqliteConnection::open_with_clock(directory.path(), clock).unwrap();
    let store = SqliteBlobStore::new(connection.clone(), Scope::new("local").unwrap());
    let table = connection.tables().blobs().to_owned();
    connection
        .run_now(move |db| {
            db.execute(&format!("DROP TABLE \"{table}\""), [])
                .map_err(sql_error)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        run(store.get("key")).unwrap_err().kind(),
        ErrorKind::Backend
    );
}
