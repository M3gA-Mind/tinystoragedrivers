//! Read-only dependencies checked atomically with document writes.
use super::{fails, ok, unique};
use crate::{
    Capability, ErrorKind, ExpectedDocument, Precondition, ReadCheck, ScopedStorage, WriteOp,
};
use serde_json::json;

pub(super) async fn run(store: &ScopedStorage) {
    let docs = store.documents();
    if !docs.capabilities().contains(Capability::ValidatedBatch) {
        fails(
            docs.atomic_batch_checked(vec![], vec![]).await,
            ErrorKind::Unsupported(Capability::ValidatedBatch),
            "unsupported checked batch",
        );
        return;
    }
    let deps = unique("dependencies");
    let writes = unique("checked_writes");
    let version = ok(
        docs.put(
            &deps,
            "dep",
            json!({"state": "complete"}),
            Precondition::Absent,
        )
        .await,
        "seed dependency",
    );
    let check = ReadCheck {
        collection: deps.clone(),
        id: "dep".into(),
        expected: ExpectedDocument::Version(version),
    };
    let absent = ReadCheck {
        collection: deps.clone(),
        id: "missing".into(),
        expected: ExpectedDocument::Absent,
    };
    let put = |id: &str| WriteOp::Put {
        collection: writes.clone(),
        id: id.into(),
        doc: json!({"claimed":true}),
        precondition: Precondition::Absent,
    };
    ok(
        docs.atomic_batch_checked(vec![check.clone(), absent.clone()], vec![put("claim")])
            .await,
        "check and claim",
    );
    let unchanged =
        ok(docs.get(&deps, "dep").await, "unchanged dependency").expect("dependency must exist");
    assert_eq!(unchanged.version, version);
    assert_eq!(unchanged.doc, json!({"state":"complete"}));
    ok(
        docs.atomic_batch_checked(vec![check.clone()], vec![]).await,
        "read-only checked batch",
    );
    ok(
        docs.atomic_batch_checked(vec![], vec![]).await,
        "empty checked batch",
    );
    fails(
        docs.atomic_batch_checked(
            vec![ReadCheck {
                expected: ExpectedDocument::Absent,
                ..check.clone()
            }],
            vec![put("absent_failure")],
        )
        .await,
        ErrorKind::Conflict,
        "present dependency fails absence",
    );
    fails(
        docs.atomic_batch_checked(
            vec![ReadCheck {
                expected: ExpectedDocument::Version(version),
                ..absent.clone()
            }],
            vec![put("missing_failure")],
        )
        .await,
        ErrorKind::Conflict,
        "absent dependency fails version",
    );
    rollback(store, &deps, &writes, check, absent).await;
    ttl(store, &writes).await;
}
async fn rollback(
    store: &ScopedStorage,
    deps: &str,
    writes: &str,
    check: ReadCheck,
    absent: ReadCheck,
) {
    let docs = store.documents();
    let put = |id: &str| WriteOp::Put {
        collection: writes.into(),
        id: id.into(),
        doc: json!({}),
        precondition: Precondition::Absent,
    };
    ok(
        docs.atomic_batch_checked(vec![check.clone(), check.clone()], vec![])
            .await,
        "equivalent duplicate checks",
    );
    fails(
        docs.atomic_batch_checked(
            vec![
                check.clone(),
                ReadCheck {
                    expected: ExpectedDocument::Absent,
                    ..check.clone()
                },
            ],
            vec![put("contradictory")],
        )
        .await,
        ErrorKind::Conflict,
        "contradictory duplicate checks",
    );
    fails(
        docs.atomic_batch_checked(vec![check.clone()], vec![put("rolled_back"), put("claim")])
            .await,
        ErrorKind::Conflict,
        "checked writes roll back together",
    );
    assert!(ok(docs.get(writes, "rolled_back").await, "rolled back write").is_none());
    ok(
        docs.put(deps, "dep", json!({"state":"changed"}), Precondition::None)
            .await,
        "dependency takeover",
    );
    fails(
        docs.atomic_batch_checked(vec![check], vec![put("stale")])
            .await,
        ErrorKind::Conflict,
        "stale dependency",
    );
    assert!(ok(docs.get(writes, "stale").await, "stale claim absent").is_none());
    fails(
        docs.atomic_batch_checked(
            vec![ReadCheck {
                collection: "bad collection".into(),
                ..absent
            }],
            vec![put("invalid")],
        )
        .await,
        ErrorKind::InvalidInput,
        "invalid dependency address",
    );
}

async fn ttl(store: &ScopedStorage, writes: &str) {
    let docs = store.documents();
    let put = |id: &str| WriteOp::Put {
        collection: writes.into(),
        id: id.into(),
        doc: json!({}),
        precondition: Precondition::Absent,
    };
    if docs.capabilities().contains(Capability::Ttl) {
        let expired = unique("expired_dependency");
        ok(
            docs.ensure_collection(&crate::CollectionSpec::new(&expired).ttl("expires"))
                .await,
            "dependency TTL declaration",
        );
        let version = ok(
            docs.put(&expired, "dep", json!({"expires":0}), Precondition::None)
                .await,
            "expired dependency seed",
        );
        let check = ReadCheck {
            collection: expired,
            id: "dep".into(),
            expected: ExpectedDocument::Absent,
        };
        ok(
            docs.atomic_batch_checked(vec![check.clone()], vec![put("expired_absent")])
                .await,
            "expired dependency is absent",
        );
        fails(
            docs.atomic_batch_checked(
                vec![ReadCheck {
                    expected: ExpectedDocument::Version(version),
                    ..check
                }],
                vec![put("expired_version")],
            )
            .await,
            ErrorKind::Conflict,
            "expired dependency fails version",
        );
    }
}

/// Read checks remain bound to the handle's scope and named database.
pub(super) async fn isolation(backend: &dyn crate::StorageBackend) {
    if !backend.capabilities().contains(Capability::ValidatedBatch) {
        return;
    }
    let collection = unique("read_isolation");
    let local = ok(
        backend.for_scope(&crate::Scope::local()),
        "local read checks",
    );
    let other = ok(
        backend.for_scope(&crate::Scope::new("checked_other").expect("valid scope")),
        "other read checks",
    );
    ok(
        other
            .documents()
            .put(&collection, "dep", json!({}), Precondition::Absent)
            .await,
        "foreign dependency",
    );
    let absent = ReadCheck {
        collection: collection.clone(),
        id: "dep".into(),
        expected: ExpectedDocument::Absent,
    };
    ok(
        local
            .documents()
            .atomic_batch_checked(vec![absent.clone()], vec![])
            .await,
        "foreign scope stays absent",
    );
}
