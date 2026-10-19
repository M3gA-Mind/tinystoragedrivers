//! Existing SQLite files retain their records when opened after policy removal.

use serde_json::json;
use tinystoragedrivers_core::{Scope, StorageBackend};
use tinystoragedrivers_sqlite::SqliteStorage;

#[tokio::test]
async fn opens_be94_storage_and_preserves_documents_streams_blobs_and_legacy_tables() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("storage.db");
    std::fs::write(&path, include_bytes!("fixtures/storage-be94f75.db")).unwrap();

    let storage = SqliteStorage::open(&path).unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    assert_eq!(
        scoped
            .documents()
            .get("fixture", "document")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"source":"be94f75","value":42})
    );
    let events = scoped
        .streams()
        .read_window("fixture.events", 0, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].value, json!({"source":"be94f75","sequence":1}));
    let blob = scoped.blobs().get("fixture/blob").await.unwrap().unwrap();
    assert_eq!(blob.bytes, b"be94f75-preserved-blob");
    assert_eq!(blob.meta.content_type.as_deref(), Some("text/plain"));

    let legacy_policy_rows = storage
        .native()
        .with_connection(|connection| {
            connection.query_row(
                "SELECT a.generation, a.operation, r.generation FROM \
                 _tsd_scope_admissions a JOIN _tsd_purge_receipts r USING \
                 (scope, operation) WHERE a.scope = 'local' AND a.operation = \
                 'historic-reset'",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
        })
        .await
        .unwrap();
    assert_eq!(legacy_policy_rows, (7, "historic-reset".to_owned(), 7));
}
