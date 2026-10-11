//! Existing-schema admission preserves native identity and data operations.

use super::*;
use crate::{Capability, CollectionSpec, ErrorKind, Filter, GuardDatabase, Precondition};
use serde_json::json;

#[tokio::test]
async fn existing_schema_rejects_malformed_collection_before_missing_declaration() {
    let storage = MemoryStorage::new().existing_schema().unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    assert_eq!(
        scoped
            .documents()
            .get("bad name", "n")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn existing_schema_reuses_declared_database_and_refuses_declaration() {
    let storage = MemoryStorage::new();
    storage.prepare_schema().await.unwrap();
    let declared = storage.database("sessions").unwrap();
    let spec = CollectionSpec::new("notes").ttl("until");
    declared
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    let restricted = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    let docs = restricted
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.validate_collection_spec(&spec).await.unwrap();
    assert_eq!(
        docs.ensure_collection(&spec).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ExistingSchema)
    );
    docs.put("notes", "n", json!({"until": u64::MAX}), Precondition::None)
        .await
        .unwrap();
    assert!(
        declared
            .for_scope(&Scope::local())
            .unwrap()
            .documents()
            .get("notes", "n")
            .await
            .unwrap()
            .is_some()
    );
    docs.drop_collection("notes").await.unwrap();
    docs.validate_collection_spec(&spec).await.unwrap();
}

#[tokio::test]
async fn existing_schema_validates_presence_without_creating_bare_declarations() {
    let storage = MemoryStorage::new();
    let restricted = storage.existing_schema().unwrap();
    let docs = restricted
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("missing"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    assert!(storage.db.lock().unwrap().specs.is_empty());
}

#[tokio::test]
async fn existing_schema_keeps_cross_database_native_fence_after_replacement() {
    let storage = MemoryStorage::new();
    let cluster = Scope::new("cluster").unwrap();
    let profile = Scope::new("alice").unwrap();
    storage
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    storage
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put("leases", "alice", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    storage
        .database("sessions")
        .unwrap()
        .for_scope(&profile)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let fence = Fence::new(cluster.clone(), "leases", "alice", Filter::eq("epoch", 1));
    let restricted = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    let old = restricted
        .for_scope_fenced_in_database(&profile, &fence, &GuardDatabase::Default)
        .unwrap();
    old.documents()
        .put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    storage
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put("leases", "alice", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        old.documents()
            .drop_collection("notes")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        old.streams()
            .append("s", json!({}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        old.blobs()
            .put("b", vec![1], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
}

#[tokio::test]
async fn existing_schema_validates_supersets_and_preserves_metadata() {
    let storage = MemoryStorage::new();
    let spec = CollectionSpec::new("notes")
        .ttl("until")
        .searchable(["body", "title"]);
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&spec).await.unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    actor
        .documents()
        .validate_collection_spec(&CollectionSpec::new("notes").searchable(["title"]))
        .await
        .unwrap();
    for required in [
        CollectionSpec::new("notes").ttl("other"),
        CollectionSpec::new("notes").searchable(["missing"]),
        CollectionSpec::new(""),
    ] {
        assert_eq!(
            actor
                .documents()
                .validate_collection_spec(&required)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(storage.db.lock().unwrap().specs.get("notes"), Some(&spec));
    assert_eq!(
        actor
            .documents()
            .put("unknown", "n", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

#[cfg(feature = "blocking")]
#[test]
fn existing_schema_native_fence_refusal_survives_blocking_bridge() {
    let bridge = crate::Blocking::new().unwrap();
    bridge
        .run(async {
            let storage = MemoryStorage::new();
            let scope = Scope::local();
            let docs = storage.for_scope(&scope).unwrap().documents().clone();
            docs.ensure_collection(&CollectionSpec::new("notes"))
                .await
                .unwrap();
            let fence = Fence::new(scope.clone(), "notes", "absent", Filter::All);
            let actor = storage
                .existing_schema()
                .unwrap()
                .for_scope_fenced(&scope, &fence)
                .unwrap();
            actor
                .documents()
                .put("notes", "n", json!({}), Precondition::None)
                .await
        })
        .unwrap()
        .map_err(|error| assert_eq!(error.kind(), ErrorKind::Fenced))
        .unwrap_err();
}

#[tokio::test]
async fn existing_schema_checked_dependencies_require_declarations() {
    let storage = MemoryStorage::new();
    let docs = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    let check = crate::ReadCheck {
        collection: "missing".into(),
        id: "n".into(),
        expected: crate::ExpectedDocument::Absent,
    };
    assert_eq!(
        docs.atomic_batch_checked(vec![check], Vec::new())
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}
