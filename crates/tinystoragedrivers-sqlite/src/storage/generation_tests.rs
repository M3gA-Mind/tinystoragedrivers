//! Scope-reset refusal is distinct from native lease loss.
use super::*;
use serde_json::json;
use tinystoragedrivers_core::{
    ErrorKind, Fence, Precondition, PurgeMode, PurgeRequest, PurgeStatus, Scope, StorageBackend,
};

#[tokio::test]
async fn generation_rejection_preserves_native_lease_authority() {
    let root = tempfile::tempdir().unwrap();
    let backend = SqliteStorage::open(root.path().join("generation.db")).unwrap();
    let owner = Scope::new("generation_owner").unwrap();
    let guard_scope = Scope::new("generation_guard").unwrap();
    let guard = backend.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put("leases", "active", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let fence = Fence::epoch(guard_scope, "leases", "active", "epoch", 1);
    let old = backend.for_scope_fenced(&owner, &fence).unwrap();
    old.documents()
        .put("records", "old", json!({}), Precondition::None)
        .await
        .unwrap();
    let receipt = old
        .purge(&PurgeRequest {
            operation_id: "generation_reset".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    assert_eq!(receipt.status, PurgeStatus::Complete);
    let rejected = [
        old.documents()
            .get("records", "old")
            .await
            .unwrap_err()
            .kind(),
        old.documents()
            .put("records", "late", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        old.streams()
            .append("events", json!({}))
            .await
            .unwrap_err()
            .kind(),
        old.blobs()
            .put("late", vec![], None)
            .await
            .unwrap_err()
            .kind(),
    ];
    // The same actual native lease remains valid after the scope reset.
    let fresh = backend
        .for_scope_fenced_generation(&owner, receipt.generation, &fence)
        .unwrap();
    assert!(
        fresh
            .documents()
            .get("records", "late")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fresh.streams().len("events").await.unwrap(), 0);
    assert!(fresh.blobs().get("late").await.unwrap().is_none());
    fresh
        .documents()
        .put("records", "fresh", json!({}), Precondition::None)
        .await
        .unwrap();
    fresh.streams().append("events", json!({})).await.unwrap();
    fresh.blobs().put("fresh", vec![1], None).await.unwrap();
    // Genuine native lease loss still returns Fenced, never a reset refusal.
    guard
        .documents()
        .put("leases", "active", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    let lease_refusal = fresh
        .documents()
        .put("records", "lease_late", json!({}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(lease_refusal.kind(), ErrorKind::Fenced);
    for kind in rejected {
        assert_eq!(kind, ErrorKind::ScopeGenerationRejected);
        assert_ne!(
            kind,
            ErrorKind::Fenced,
            "retired generation must not claim that the still-valid native lease was lost"
        );
    }
}
