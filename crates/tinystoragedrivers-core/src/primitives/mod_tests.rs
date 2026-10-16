use super::*;
use crate::document::validate_collection;
use crate::{MemoryStorage, Scope, StorageBackend};

#[test]
fn collection_names_are_valid_and_not_reserved() {
    for spec in collection_specs() {
        validate_collection(&spec.name).unwrap();
        spec.validate().unwrap();
    }
    let names: Vec<_> = collection_specs().into_iter().map(|s| s.name).collect();
    assert_eq!(names, [LEASES, KV, COUNTERS]);
}

#[tokio::test]
async fn ensure_is_idempotent() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    ensure(&storage).await.unwrap();
    ensure(&storage).await.unwrap();
}
