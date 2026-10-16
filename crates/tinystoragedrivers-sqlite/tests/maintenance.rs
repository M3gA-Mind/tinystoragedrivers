//! Space reclamation, WAL checkpoints and scope listing on a real SQLite file.

#![allow(
    clippy::unwrap_used,
    reason = "test helpers report a failed call by panicking"
)]

use std::path::Path;

use rusqlite::Connection;
use serde_json::json;
use tinystoragedrivers_core::{
    Capability, Checkpoint, MaintenanceBudget, Precondition, Scope, StorageBackend,
};
use tinystoragedrivers_sqlite::SqliteStorage;

const DOCS: usize = 400;

fn pragma(file: &Path, name: &str) -> i64 {
    let conn = Connection::open(file).unwrap();
    conn.query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .unwrap()
}

fn size(file: &Path) -> u64 {
    std::fs::metadata(file).unwrap().len()
}

fn wal_size(file: &Path) -> u64 {
    let mut wal = file.as_os_str().to_owned();
    wal.push("-wal");
    std::fs::metadata(wal).map_or(0, |meta| meta.len())
}

/// Fill the local scope with large documents, then delete them all, so the
/// file is mostly free pages.
async fn fill_and_empty(storage: &SqliteStorage) {
    let docs = storage.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let filler = "x".repeat(8 * 1024);
    for n in 0..DOCS {
        docs.put(
            "bulk",
            &format!("doc-{n}"),
            json!({"filler": filler}),
            Precondition::None,
        )
        .await
        .unwrap();
    }
    for n in 0..DOCS {
        docs.delete("bulk", &format!("doc-{n}"), Precondition::None)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn reports_the_maintenance_capabilities() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let caps = storage.capabilities();
    assert!(caps.contains(Capability::Maintenance));
    assert!(caps.contains(Capability::ScopeEnumeration));
}

#[tokio::test]
async fn new_databases_vacuum_incrementally() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    // 2 is INCREMENTAL.
    assert_eq!(pragma(storage.path(), "auto_vacuum"), 2);
}

#[tokio::test]
async fn maintain_shrinks_the_file_after_a_bulk_delete() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    fill_and_empty(&storage).await;
    storage.checkpoint(Checkpoint::Truncate).await.unwrap();
    let before = size(storage.path());
    assert!(before > 2 * 1024 * 1024, "the fixture must grow the file");

    let report = storage
        .maintain(MaintenanceBudget::unlimited())
        .await
        .unwrap();

    let after = size(storage.path());
    assert!(
        after < before / 10,
        "file should shrink: {before} -> {after}"
    );
    assert!(report.reclaimed_bytes >= (before - after) / 2);
    assert!(report.reclaimed_bytes > 0);
    // Nothing left to give back.
    let again = storage
        .maintain(MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!(again.reclaimed_bytes, 0);
}

#[tokio::test]
async fn a_budget_bounds_how_much_one_call_reclaims() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    fill_and_empty(&storage).await;
    let page = u64::try_from(pragma(storage.path(), "page_size")).unwrap();

    let none = storage
        .maintain(MaintenanceBudget::reclaim_up_to(0))
        .await
        .unwrap();
    assert_eq!(none.reclaimed_bytes, 0);
    let small = storage
        .maintain(MaintenanceBudget::reclaim_up_to(10 * page + page / 2))
        .await
        .unwrap();
    assert_eq!(small.reclaimed_bytes, 10 * page);
    let rest = storage
        .maintain(MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert!(rest.reclaimed_bytes > small.reclaimed_bytes);
}

#[tokio::test]
async fn the_first_maintain_converts_a_file_made_without_auto_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("old.db");
    {
        // A file as an earlier release left it: auto_vacuum off.
        let conn = Connection::open(&file).unwrap();
        conn.execute_batch("PRAGMA auto_vacuum = NONE; PRAGMA journal_mode = WAL; CREATE TABLE keep (v TEXT); INSERT INTO keep VALUES ('still here');")
            .unwrap();
    }
    assert_eq!(pragma(&file, "auto_vacuum"), 0);
    let storage = SqliteStorage::open(&file).unwrap();
    assert_eq!(pragma(&file, "auto_vacuum"), 0, "opening must not rewrite");
    fill_and_empty(&storage).await;
    let before = size(&file);

    let report = storage
        .maintain(MaintenanceBudget::unlimited())
        .await
        .unwrap();

    assert_eq!(pragma(&file, "auto_vacuum"), 2);
    assert!(report.reclaimed_bytes > 0);
    assert!(size(&file) < before / 10);
    let kept: String = Connection::open(&file)
        .unwrap()
        .query_row("SELECT v FROM keep", [], |row| row.get(0))
        .unwrap();
    assert_eq!(kept, "still here");
}

#[tokio::test]
async fn checkpoint_truncate_empties_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let docs = storage.for_scope(&Scope::local()).unwrap();
    docs.documents()
        .put("c", "d", json!({"n": 1}), Precondition::None)
        .await
        .unwrap();
    assert!(wal_size(storage.path()) > 0, "writes land in the WAL first");

    storage.checkpoint(Checkpoint::Passive).await.unwrap();
    storage.checkpoint(Checkpoint::Truncate).await.unwrap();

    assert_eq!(wal_size(storage.path()), 0);
    let conn = Connection::open(storage.path()).unwrap();
    let doc: String = conn
        .query_row("SELECT body FROM docs WHERE id = 'd'", [], |row| row.get(0))
        .unwrap();
    assert!(doc.contains("\"n\":1"), "{doc}");
}

#[tokio::test]
async fn scopes_span_the_named_databases_of_a_single_file_separately() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path().join("all.db")).unwrap();
    let flows = storage.database("flows").unwrap();
    let ann = Scope::new("u:ann").unwrap();
    flows
        .for_scope(&ann)
        .unwrap()
        .documents()
        .put("runs", "r", json!({}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(flows.scopes("u:").await.unwrap(), [ann]);
    assert_eq!(storage.scopes("u:").await.unwrap().len(), 0);
}

#[tokio::test]
async fn maintaining_an_existing_schema_handle_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let existing = storage.existing_schema().unwrap();
    existing
        .maintain(MaintenanceBudget::unlimited())
        .await
        .unwrap();
    existing.checkpoint(Checkpoint::Passive).await.unwrap();
    assert_eq!(
        existing.scopes("").await.unwrap(),
        storage.scopes("").await.unwrap()
    );
}
