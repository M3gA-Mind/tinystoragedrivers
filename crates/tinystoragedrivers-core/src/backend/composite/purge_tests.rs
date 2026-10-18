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

#[tokio::test]
async fn completed_history_replay_after_a_later_generation_preserves_replacement_data() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:composite-history-replay").unwrap();
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();

    let first = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs: blobs.clone(),
        scope: scope.clone(),
        generation: ScopeGeneration(0),
        coordinator: coordinator.clone(),
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    let first_request = PurgeRequest {
        operation_id: "history-replay-1".into(),
        mode: PurgeMode::Quiescent,
    };
    let first_receipt = first.purge(&first_request).await.unwrap();

    let second = CompositePurge {
        records: records
            .for_scope_generation(&scope, first_receipt.generation)
            .unwrap(),
        blobs: blobs.clone(),
        scope: scope.clone(),
        generation: first_receipt.generation,
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    blobs
        .for_scope_generation(&scope, first_receipt.generation)
        .unwrap()
        .put("replacement", vec![9], None)
        .await
        .unwrap();
    let second_receipt = second
        .purge(&PurgeRequest {
            operation_id: "history-replay-2".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();

    assert_eq!(
        first.status("history-replay-1").await.unwrap(),
        Some(first_receipt.clone())
    );
    assert_eq!(first.purge(&first_request).await.unwrap(), first_receipt);
    assert_eq!(
        blobs
            .for_scope_generation(&scope, second_receipt.generation)
            .unwrap()
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [9],
        "replaying historical completion must not erase later-generation data"
    );
}

#[tokio::test]
async fn composite_purge_reconciles_native_receipt_without_erasing_new_generation() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(blob_storage));
    let scope = Scope::new("u:composite-native-receipt-recovery").unwrap();
    let operation_id = "native-receipt-recovery";
    let request = PurgeRequest {
        operation_id: operation_id.into(),
        mode: PurgeMode::Quiescent,
    };
    let coordinator = records.for_scope(&coordinator_scope().unwrap()).unwrap();
    coordinator
        .documents()
        .ensure_collection(&crate::CollectionSpec::new(COMPOSITE_PURGE_COLLECTION))
        .await
        .unwrap();
    let legacy_blobs = blobs.for_scope(&scope).unwrap();
    legacy_blobs
        .put("pending-delete", vec![4], None)
        .await
        .unwrap();

    let native_receipt = records
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    let next_generation_blobs = blobs
        .for_scope_generation(&scope, native_receipt.generation)
        .unwrap();
    next_generation_blobs
        .put("replacement", vec![5], None)
        .await
        .unwrap();
    let composite = CompositePurge {
        records: records.for_scope(&scope).unwrap(),
        blobs,
        scope: scope.clone(),
        generation: ScopeGeneration(0),
        coordinator,
        purge_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let receipt = composite.purge(&request).await.unwrap();
    assert_eq!(receipt, native_receipt);
    assert_eq!(receipt.status, crate::PurgeStatus::Complete);
    assert!(legacy_blobs.get("pending-delete").await.unwrap().is_none());
    assert_eq!(
        next_generation_blobs
            .get("replacement")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        [5]
    );
    assert_eq!(composite.status(operation_id).await.unwrap(), Some(receipt));
}
