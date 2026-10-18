use super::*;
use crate::{BackendBlobs, MemoryStorage, StorageBackend};

#[tokio::test]
async fn malformed_coordinator_record_fails_closed_before_scope_erasure() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let target = Scope::new("u:corrupt-coordinator").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();
    coordinator
        .documents()
        .put(
            COMPOSITE_PURGE_COLLECTION,
            &coordinator_id(&target).unwrap(),
            serde_json::json!({"not_a_receipt": true}),
            crate::Precondition::Absent,
        )
        .await
        .unwrap();
    let scoped = records.for_scope(&target).unwrap();
    scoped
        .streams()
        .append("preserve", serde_json::json!(1))
        .await
        .unwrap();
    let purge = CompositePurge {
        records: scoped,
        blobs: Arc::new(BackendBlobs::new(blob_storage)),
        scope: target,
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let error = purge
        .purge(&PurgeRequest {
            operation_id: "corrupt".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Backend);
    assert_eq!(
        purge.records.streams().len("preserve").await.unwrap(),
        1,
        "invalid coordination state must not erase tenant records"
    );
}

#[tokio::test]
async fn maximum_scope_and_operation_ids_have_bounded_coordinator_keys() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let scope = Scope::new("x".repeat(crate::MAX_SCOPE_LEN)).unwrap();
    let scoped = records.for_scope(&scope).unwrap();
    let purge = CompositePurge {
        records: scoped,
        blobs: Arc::new(BackendBlobs::new(blob_storage)),
        scope,
        generation: ScopeGeneration(0),
        coordinator: records.for_scope(&coordinator_scope().unwrap()).unwrap(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let operation_id = "o".repeat(128);
    let receipt = purge
        .purge(&PurgeRequest {
            operation_id: operation_id.clone(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(
        purge.status(&operation_id).await.unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        purge
            .purge(&PurgeRequest {
                operation_id,
                mode: PurgeMode::Quiescent,
            })
            .await
            .unwrap(),
        receipt
    );
}
