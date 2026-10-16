//! The pragma plumbing, including the checkpoint that cannot finish.

use std::time::Duration;

use rusqlite::Connection;
use tinystoragedrivers_core::ErrorKind;

use super::*;

fn wal_database(dir: &tempfile::TempDir) -> (std::path::PathBuf, Connection) {
    let file = dir.path().join("m.db");
    let conn = Connection::open(&file).unwrap();
    conn.execute_batch(
        "PRAGMA auto_vacuum = INCREMENTAL; PRAGMA journal_mode = WAL;
         CREATE TABLE t (v TEXT); INSERT INTO t VALUES ('a');",
    )
    .unwrap();
    (file, conn)
}

#[test]
fn a_zero_budget_does_nothing_even_to_a_file_that_needs_converting() {
    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("z.db")).unwrap();
    conn.execute_batch("PRAGMA auto_vacuum = NONE; CREATE TABLE t (v TEXT);")
        .unwrap();
    let report = maintain(&conn, MaintenanceBudget::reclaim_up_to(0)).unwrap();
    assert_eq!(report, MaintenanceReport::default());
    assert_eq!(read(&conn, "auto_vacuum").unwrap(), 0);
}

#[test]
fn a_budget_smaller_than_a_page_reclaims_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (_, conn) = wal_database(&dir);
    conn.execute_batch(
        "CREATE TABLE big (v BLOB); INSERT INTO big VALUES (zeroblob(100000)); DROP TABLE big;",
    )
    .unwrap();
    assert!(read(&conn, "freelist_count").unwrap() > 0);
    let report = maintain(&conn, MaintenanceBudget::reclaim_up_to(1)).unwrap();
    assert_eq!(report.reclaimed_bytes, 0);
}

#[test]
fn a_truncate_checkpoint_blocked_by_a_reader_is_retryable() {
    let dir = tempfile::tempdir().unwrap();
    let (file, writer) = wal_database(&dir);
    writer.busy_timeout(Duration::ZERO).unwrap();
    let reader = Connection::open(&file).unwrap();
    reader
        .execute_batch("BEGIN; SELECT count(*) FROM t;")
        .unwrap();
    writer.execute_batch("INSERT INTO t VALUES ('b')").unwrap();

    let error = checkpoint(&writer, Checkpoint::Truncate).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    // A passive checkpoint never waits, so it is not an error.
    checkpoint(&writer, Checkpoint::Passive).unwrap();

    reader.execute_batch("COMMIT").unwrap();
    checkpoint(&writer, Checkpoint::Truncate).unwrap();
}
