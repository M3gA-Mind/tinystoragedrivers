//! The scope listing's SQL, against the driver's own tables.

use serde_json::json;
use tinystoragedrivers_core::{Precondition, Scope, StorageBackend};

use crate::SqliteStorage;

#[tokio::test]
async fn lists_each_table_kind_once_in_byte_order() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = |text: &str| Scope::new(text).unwrap();
    for name in ["u:b", "u:a"] {
        let handles = storage.for_scope(&scope(name)).unwrap();
        // Two records of different kinds in one scope must not repeat it.
        handles
            .documents()
            .put("c", "d", json!({}), Precondition::None)
            .await
            .unwrap();
        handles.streams().append("s", json!(1)).await.unwrap();
    }
    let blobs = storage.for_scope(&scope("u:a/agent:x")).unwrap();
    blobs.blobs().put("k", vec![1], None).await.unwrap();

    assert_eq!(
        storage.scopes("u:").await.unwrap(),
        [scope("u:a"), scope("u:a/agent:x"), scope("u:b")]
    );
    assert_eq!(
        storage.scopes("u:a/").await.unwrap(),
        [scope("u:a/agent:x")]
    );
    assert_eq!(storage.scopes("v:").await.unwrap().len(), 0);
}

#[tokio::test]
async fn a_scope_in_the_purge_ledger_alone_is_not_listed() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("u:erased").unwrap();
    let handles = storage.for_scope(&scope).unwrap();
    handles
        .documents()
        .put("c", "d", json!({}), Precondition::None)
        .await
        .unwrap();
    handles
        .purge(&tinystoragedrivers_core::PurgeRequest {
            operation_id: "erase-1".into(),
            mode: tinystoragedrivers_core::PurgeMode::Atomic,
        })
        .await
        .unwrap();
    assert_eq!(storage.scopes("").await.unwrap().len(), 0);
}
