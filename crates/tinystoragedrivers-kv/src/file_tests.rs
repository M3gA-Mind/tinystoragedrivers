//! Behavioral tests for the file KV engine.

use super::*;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::tempdir;
use tinystoragedrivers_types::{ErrorKind, Scope};

#[tokio::test]
async fn values_survive_reopen_and_keep_versions_across_delete() {
    let root = tempdir().unwrap();
    let scope = Scope::new("tenant/a").unwrap();
    let store = FileKv::open(root.path(), &scope).unwrap();
    let first = store.put("key", json!({"x": [1, 2]})).await.unwrap();
    assert!(
        store
            .delete("key", Precondition::Version(first))
            .await
            .unwrap()
    );
    drop(store);
    let reopened = FileKv::open(root.path(), &scope).unwrap();
    assert_eq!(reopened.put("key", json!(null)).await.unwrap(), Version(3));
    assert_eq!(reopened.get("key").await.unwrap().unwrap().doc, json!(null));
}

#[tokio::test]
async fn cas_and_stable_prefix_paging_match_memory_semantics() {
    let root = tempdir().unwrap();
    let store = FileKv::open(root.path(), &Scope::new("tenant").unwrap()).unwrap();
    for key in ["other", "pre/c", "pre/a", "pre/b"] {
        store.put(key, json!(key)).await.unwrap();
    }
    let first = store.list("pre/", None, 2).await.unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["pre/a", "pre/b"]
    );
    let second = store.list("pre/", first.next, 2).await.unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["pre/c"]
    );
    let err = store
        .cas("pre/a", json!(false), Precondition::Absent)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn separately_opened_handles_share_in_process_cas_lock() {
    let root = tempdir().unwrap();
    let scope = Scope::new("tenant").unwrap();
    let first = FileKv::open(root.path(), &scope).unwrap();
    let second = FileKv::open(root.path(), &scope).unwrap();
    let version = first.put("key", json!(1)).await.unwrap();
    assert_eq!(second.get("key").await.unwrap().unwrap().version, version);
    assert!(
        second
            .cas("key", json!(2), Precondition::Version(version))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn concurrent_handles_allow_only_one_matching_version_write() {
    let root = tempdir().unwrap();
    let scope = Scope::new("tenant").unwrap();
    let first = FileKv::open(root.path(), &scope).unwrap();
    let second = FileKv::open(root.path(), &scope).unwrap();
    let version = first.put("key", json!(0)).await.unwrap();
    let (left, right) = tokio::join!(
        first.cas("key", json!(1), Precondition::Version(version)),
        second.cas("key", json!(2), Precondition::Version(version)),
    );
    let loser = match (left, right) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        outcome => panic!("expected one successful CAS, got {outcome:?}"),
    };
    assert_eq!(loser.kind(), ErrorKind::Conflict);
}

#[tokio::test]
async fn adapter_uses_a_supplied_shared_file_connection() {
    let root = tempdir().unwrap();
    let connection = FileConnection::open(root.path()).unwrap();
    let store = FileKv::new(connection.clone(), Scope::new("tenant").unwrap()).unwrap();
    let version = store.put("key", json!("shared root")).await.unwrap();
    assert_eq!(version, Version::FIRST);
    assert_eq!(
        store.get("key").await.unwrap().unwrap().doc,
        json!("shared root")
    );
    assert_eq!(store.connection.root(), connection.root());
}

#[tokio::test]
async fn ttl_is_checked_against_wall_clock() {
    let root = tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let clock_value = Arc::clone(&now);
    let store = FileKv::open_with_clock(
        root.path(),
        &Scope::new("tenant").unwrap(),
        Arc::new(move || clock_value.load(Ordering::SeqCst)),
    )
    .unwrap();
    store
        .put_with_ttl("expiring", json!(1), Duration::from_millis(10))
        .await
        .unwrap();
    assert!(store.get("expiring").await.unwrap().is_some());
    now.fetch_add(10, Ordering::SeqCst);
    assert!(store.get("expiring").await.unwrap().is_none());
}
