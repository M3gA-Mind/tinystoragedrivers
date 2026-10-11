//! Restricted native construction does not initialize schema.

use super::*;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, Precondition};

#[tokio::test]
async fn existing_schema_keeps_named_declarations_and_scoped_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    storage.prepare_schema().await.unwrap();
    let named = storage.database("sessions").unwrap();
    let scope = Scope::new("alice").unwrap();
    let other = Scope::new("bob").unwrap();
    let spec = CollectionSpec::new("notes").ttl("until");
    named
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    named
        .for_scope(&other)
        .unwrap()
        .documents()
        .put("notes", "other", json!({}), Precondition::None)
        .await
        .unwrap();
    let restricted = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    let docs = restricted.for_scope(&scope).unwrap().documents().clone();
    docs.validate_collection_spec(&spec).await.unwrap();
    assert!(docs.ensure_collection(&spec).await.is_err());
    docs.put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    docs.drop_collection("notes").await.unwrap();
    docs.validate_collection_spec(&spec).await.unwrap();
    assert!(
        named
            .for_scope(&other)
            .unwrap()
            .documents()
            .get("notes", "other")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn existing_schema_does_not_create_missing_named_database() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let before = std::fs::read_dir(dir.path()).unwrap().count();
    let restricted = storage.existing_schema().unwrap();
    assert_eq!(
        restricted.database("missing").unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), before);
}

#[tokio::test]
async fn existing_schema_requires_persisted_specs_and_refuses_bootstrap() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let restricted = storage.existing_schema().unwrap();
    assert!(restricted.prepare_schema().await.is_err());
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
    assert_eq!(
        docs.put("missing", "n", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
    let spec = CollectionSpec::new("notes")
        .ttl("until")
        .searchable(["body", "title"]);
    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&spec)
        .await
        .unwrap();
    docs.validate_collection_spec(&CollectionSpec::new("notes").searchable(["title"]))
        .await
        .unwrap();
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("notes").ttl("changed"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        docs.validate_collection_spec(&CollectionSpec::new("notes").searchable(["missing"]))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn existing_schema_preserves_cross_database_guard_for_every_port() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let cluster = Scope::new("cluster").unwrap();
    let scope = Scope::new("alice").unwrap();
    let leases = storage.for_scope(&cluster).unwrap().documents().clone();
    leases
        .ensure_collection(&CollectionSpec::new("leases"))
        .await
        .unwrap();
    leases
        .put("leases", "alice", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    storage
        .database("sessions")
        .unwrap()
        .for_scope(&scope)
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let fence = tinystoragedrivers_core::Fence::new(
        cluster,
        "leases",
        "alice",
        tinystoragedrivers_core::Filter::eq("epoch", 1),
    );
    let backend = storage
        .existing_schema()
        .unwrap()
        .database("sessions")
        .unwrap();
    let actor = backend
        .for_scope_fenced_in_database(
            &scope,
            &fence,
            &tinystoragedrivers_core::GuardDatabase::Default,
        )
        .unwrap();
    actor
        .documents()
        .put("notes", "n", json!({}), Precondition::None)
        .await
        .unwrap();
    leases
        .put("leases", "alice", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        actor
            .documents()
            .drop_collection("notes")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .streams()
            .append_batch_if_offset("s", 0, vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        actor
            .blobs()
            .put("b", vec![1], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
}

#[tokio::test]
async fn existing_schema_preserves_corrupt_declaration_bytes_and_source() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    let path = storage
        .db
        .specs_dir()
        .join(format!("{}.json", crate::encode::file_stem("notes")));
    std::fs::write(&path, b"not json").unwrap();
    let actor = storage
        .existing_schema()
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    let error = actor
        .documents()
        .validate_collection_spec(&CollectionSpec::new("notes"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(std::fs::read(path).unwrap(), b"not json");
}

#[tokio::test]
async fn existing_schema_validates_inputs_before_missing_declarations() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path())
        .unwrap()
        .existing_schema()
        .unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    let docs = scoped.documents();
    let invalid_filter = tinystoragedrivers_core::Filter::eq("", 1);
    let invalid_query = tinystoragedrivers_core::Query {
        filter: invalid_filter.clone(),
        ..tinystoragedrivers_core::Query::default()
    };
    let invalid_cursor = tinystoragedrivers_core::Query {
        cursor: Some(tinystoragedrivers_core::Cursor("invalid".into())),
        ..tinystoragedrivers_core::Query::default()
    };
    let errors = [
        docs.get("missing", "").await.unwrap_err(),
        docs.put("missing", "n", json!(null), Precondition::None)
            .await
            .unwrap_err(),
        docs.delete("missing", "", Precondition::None)
            .await
            .unwrap_err(),
        docs.query("missing", &invalid_query).await.unwrap_err(),
        docs.query("missing", &invalid_cursor).await.unwrap_err(),
        docs.count("missing", &invalid_filter).await.unwrap_err(),
        docs.delete_where("missing", &invalid_filter)
            .await
            .unwrap_err(),
        docs.claim("missing", &invalid_filter, &[], &json!({}))
            .await
            .unwrap_err(),
        docs.claim(
            "missing",
            &tinystoragedrivers_core::Filter::All,
            &[],
            &json!(null),
        )
        .await
        .unwrap_err(),
    ];
    for error in errors {
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}
