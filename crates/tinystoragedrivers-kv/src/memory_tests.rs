//! Behavioral tests for the in-memory KV engine.

use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use tinystoragedrivers_types::ErrorKind;

fn clock() -> (Clock, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000));
    let read = Arc::clone(&now);
    (Arc::new(move || read.load(Ordering::SeqCst)), now)
}

fn store() -> (MemoryKv, Arc<AtomicU64>) {
    let (clock, now) = clock();
    (
        MemoryKv::with_clock(
            tinystoragedrivers_types::Scope::new("tenant").unwrap(),
            clock,
        ),
        now,
    )
}

#[tokio::test]
async fn stores_arbitrary_json_and_versions_updates_and_deletes() {
    let (store, _) = store();
    let first = store.put("state", json!([1, true, null])).await.unwrap();
    assert_eq!(first, Version::FIRST);
    assert_eq!(
        store.get("state").await.unwrap().unwrap().doc,
        json!([1, true, null])
    );
    let changed = store
        .cas("state", json!("replacement"), Precondition::Version(first))
        .await
        .unwrap();
    assert_eq!(changed, Version(2));
    assert!(
        store
            .delete("state", Precondition::Version(changed))
            .await
            .unwrap()
    );
    assert_eq!(store.put("state", json!(false)).await.unwrap(), Version(4));
}

#[tokio::test]
async fn compare_and_swap_reports_conflicts_without_mutating() {
    let (store, _) = store();
    let version = store.put("state", json!(1)).await.unwrap();
    let error = store
        .cas("state", json!(2), Precondition::Absent)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(store.get("state").await.unwrap().unwrap().version, version);
    assert_eq!(store.get("state").await.unwrap().unwrap().doc, json!(1));
}

#[tokio::test]
async fn expired_values_are_absent_and_new_versions_keep_advancing() {
    let (store, now) = store();
    assert_eq!(
        store
            .put_with_ttl("expiring", json!("value"), Duration::from_millis(20))
            .await
            .unwrap(),
        Version::FIRST
    );
    now.fetch_add(20, Ordering::SeqCst);
    assert!(store.get("expiring").await.unwrap().is_none());
    assert_eq!(
        store.put("expiring", json!("new")).await.unwrap(),
        Version(2)
    );
}

#[tokio::test]
async fn listing_is_stable_keyset_paged_and_prefix_scoped() {
    let (store, _) = store();
    for key in ["other/1", "p/3", "p/1", "p/2"] {
        store.put(key, json!(key)).await.unwrap();
    }
    let first = store.list("p/", None, 2).await.unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["p/1", "p/2"]
    );
    let second = store.list("p/", first.next, 2).await.unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["p/3"]
    );
    assert!(second.next.is_none());
    assert!(store.list("p/", None, 0).await.is_err());
}

#[tokio::test]
async fn expired_values_do_not_appear_in_listing() {
    let (store, now) = store();
    store
        .put_with_ttl("p/expired", json!(1), Duration::from_millis(1))
        .await
        .unwrap();
    store.put("p/live", json!(2)).await.unwrap();
    now.fetch_add(1, Ordering::SeqCst);
    let listed = store.list("p/", None, 10).await.unwrap();
    assert_eq!(
        listed
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["p/live"]
    );
}

#[tokio::test]
async fn clones_share_one_scope_and_lock() {
    let (store, _) = store();
    let clone = store.clone();
    let version = store.put("shared", json!(true)).await.unwrap();
    assert_eq!(clone.get("shared").await.unwrap().unwrap().version, version);
}

#[tokio::test]
async fn concurrent_compare_and_swap_allows_only_one_writer() {
    let (store, _) = store();
    let version = store.put("shared", json!(0)).await.unwrap();
    let left = store.clone();
    let right = store.clone();
    let (left, right) = tokio::join!(
        left.cas("shared", json!(1), Precondition::Version(version)),
        right.cas("shared", json!(2), Precondition::Version(version)),
    );
    let loser = match (left, right) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        outcome => panic!("expected one successful CAS, got {outcome:?}"),
    };
    assert_eq!(loser.kind(), ErrorKind::Conflict);
    assert_eq!(
        store.get("shared").await.unwrap().unwrap().version,
        Version(2)
    );
}
