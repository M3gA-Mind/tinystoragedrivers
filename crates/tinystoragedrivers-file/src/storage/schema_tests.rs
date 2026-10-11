//! Restricted native construction does not initialize schema.

use super::*;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, Precondition};

#[tokio::test]
async fn existing_schema_keeps_named_declarations_and_scoped_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
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
