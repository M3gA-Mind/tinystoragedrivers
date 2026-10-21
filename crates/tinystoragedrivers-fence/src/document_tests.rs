use serde_json::json;
use tinystoragedrivers_core::{
    ErrorKind, MemoryStorage, Precondition, Scope, StorageBackend, Version,
};

use crate::{DocumentLeases, LeaseError, LeaseStore, for_scope_fenced};

#[tokio::test]
async fn document_store_tracks_reentrancy_and_fences_after_expired_takeover() {
    let backend = MemoryStorage::new();
    let guard_scope = Scope::new("cluster").unwrap();
    let cluster = backend.for_scope(&guard_scope).unwrap();
    let first = DocumentLeases::new(
        cluster.documents(),
        guard_scope.clone(),
        "profile",
        "node-a",
        100,
    )
    .unwrap()
    .with_endpoint(Some("https://node-a.example".to_owned()));
    let second = DocumentLeases::new(
        cluster.documents(),
        guard_scope.clone(),
        "profile",
        "node-b",
        100,
    )
    .unwrap()
    .with_endpoint(Some("https://node-b.example".to_owned()));

    let initial = first.acquire("alice", 1_000).await.unwrap();
    let holder_view = first.holder("alice").await.unwrap().unwrap();
    assert_eq!(holder_view.holder, "node-a");
    let reentrant = first.acquire("alice", 1_025).await.unwrap();
    assert_eq!(initial.epoch, reentrant.epoch);
    assert_eq!(reentrant.version.0, initial.version.0 + 1);
    assert!(matches!(
        first.renew(&initial, 1_030).await,
        Err(LeaseError::Lost)
    ));
    let held = second.acquire("alice", 1_050).await.unwrap_err();
    let LeaseError::Held(record) = held else {
        panic!("expected lease held")
    };
    assert_eq!(record.endpoint.as_deref(), Some("https://node-a.example"));

    let data_scope = Scope::new("profile-data").unwrap();
    let stale = for_scope_fenced(
        &backend,
        &data_scope,
        &first
            .driver_fence("alice", "node-a", reentrant.epoch)
            .unwrap(),
    )
    .unwrap();
    stale
        .documents()
        .put("notes", "n1", json!({"ok":true}), Precondition::None)
        .await
        .unwrap();
    let takeover = second.acquire("alice", 1_126).await.unwrap();
    assert_eq!(takeover.epoch, reentrant.epoch + 1);
    assert!(takeover.previous_unclean);
    assert_eq!(
        second
            .inspect("alice")
            .await
            .unwrap()
            .unwrap()
            .endpoint
            .as_deref(),
        Some("https://node-b.example")
    );
    assert_eq!(
        stale
            .documents()
            .put("notes", "n2", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        first.inspect("alice").await.unwrap().unwrap().epoch,
        takeover.epoch
    );
}

#[test]
fn constructor_rejects_empty_owner_holder_and_nonpositive_ttl() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let documents = backend.for_scope(&scope).unwrap();

    for (owner, holder, ttl_ms) in [
        ("", "node", 100),
        ("profile", "", 100),
        ("profile", "node", 0),
    ] {
        let error =
            DocumentLeases::new(documents.documents(), scope.clone(), owner, holder, ttl_ms)
                .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn debug_output_identifies_the_document_lease_context() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let documents = backend.for_scope(&scope).unwrap();
    let leases = DocumentLeases::new(documents.documents(), scope, "profile", "node-a", 100)
        .unwrap()
        .with_endpoint(Some("https://node-a.example".to_owned()));

    let debug = format!("{leases:?}");

    assert!(debug.contains("DocumentLeases"));
    assert!(debug.contains("profile"));
    assert!(debug.contains("node-a"));
    assert!(debug.contains("node-a.example"));
}

#[tokio::test]
async fn acquire_and_renew_report_version_space_exhaustion() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let documents = backend.for_scope(&scope).unwrap();
    let leases =
        DocumentLeases::new(documents.documents(), scope, "profile", "node-a", 100).unwrap();
    let grant = leases.acquire("alice", 1_000).await.unwrap();

    leases
        .grants
        .lock()
        .await
        .insert("alice".to_owned(), (grant.epoch, Version(u64::MAX)));
    let error = leases.acquire("alice", 1_025).await.unwrap_err();
    assert!(matches!(error, LeaseError::Storage(_)));

    let overflow = crate::LeaseGrant {
        version: Version(u64::MAX),
        ..grant
    };
    let error = leases.renew(&overflow, 1_050).await.unwrap_err();
    assert!(matches!(error, LeaseError::Storage(_)));
}

#[tokio::test]
async fn release_records_the_released_state_and_allows_a_new_epoch() {
    let backend = MemoryStorage::new();
    let scope = Scope::new("cluster").unwrap();
    let documents = backend.for_scope(&scope).unwrap();
    let leases =
        DocumentLeases::new(documents.documents(), scope, "profile", "node-a", 100).unwrap();
    let grant = leases.acquire("alice", 1_000).await.unwrap();

    leases.release(grant).await.unwrap();

    let released = leases.inspect("alice").await.unwrap().unwrap();
    assert!(released.released);
    let next = leases.acquire("alice", 1_001).await.unwrap();
    assert_eq!(next.epoch, 2);
    assert!(!next.previous_unclean);
}
