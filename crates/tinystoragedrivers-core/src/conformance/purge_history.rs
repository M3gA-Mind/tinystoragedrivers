//! Completed reset identities stay durable across later resets.
use super::{ok, unique};
use crate::{Capability, Precondition, PurgeMode, PurgeRequest, Scope, StorageBackend};
use serde_json::json;

pub(super) async fn run(backend: &dyn StorageBackend) {
    if !backend.capabilities().contains(Capability::ScopePurge) {
        return;
    }
    let mode = if backend
        .capabilities()
        .contains(Capability::AtomicScopePurge)
    {
        PurgeMode::Atomic
    } else {
        PurgeMode::Quiescent
    };
    let owner = Scope::new(unique("reset_history")).expect("valid scope");
    {
        let target = backend;
        let first = PurgeRequest {
            operation_id: unique("first_reset"),
            mode,
        };
        let second = PurgeRequest {
            operation_id: unique("second_reset"),
            mode,
        };
        let original = ok(target.for_scope(&owner), "history scope");
        let first_receipt = ok(original.purge(&first).await, "first reset");
        let next = ok(
            target.for_scope_generation(&owner, first_receipt.generation),
            "next reset generation",
        );
        let second_receipt = ok(next.purge(&second).await, "second reset");
        let current = ok(
            target.for_scope_generation(&owner, second_receipt.generation),
            "replacement generation",
        );
        ok(
            current
                .documents()
                .put(
                    "records",
                    "replacement",
                    json!({"keep":true}),
                    Precondition::Absent,
                )
                .await,
            "replacement document",
        );
        ok(
            current
                .streams()
                .append("events", json!({"keep":true}))
                .await,
            "replacement stream",
        );
        ok(
            current.blobs().put("replacement", vec![7], None).await,
            "replacement blob",
        );
        assert_eq!(
            ok(
                current.purge_status(&first.operation_id).await,
                "historical receipt"
            ),
            Some(first_receipt.clone()),
            "historical receipt must survive later resets"
        );
        assert_eq!(
            ok(current.purge(&first).await, "historical reset retry"),
            first_receipt,
            "historical retry must return original receipt"
        );
        assert!(
            ok(
                current.documents().get("records", "replacement").await,
                "replacement document after retry"
            )
            .is_some()
        );
        assert_eq!(
            ok(
                current.streams().read_window("events", 0, 10).await,
                "replacement stream after retry"
            )
            .len(),
            1
        );
        assert_eq!(
            ok(
                current.blobs().get("replacement").await,
                "replacement blob after retry"
            )
            .expect("replacement blob survives")
            .bytes,
            vec![7]
        );
    }
}
