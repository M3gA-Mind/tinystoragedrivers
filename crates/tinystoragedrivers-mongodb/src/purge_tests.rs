use super::*;
use crate::MongoStorage;
use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, ErrorKind, Precondition, StorageBackend};

async fn live(label: &str) -> Option<(MongoStorage, Scope)> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return None;
    };
    let scope = Scope::new(format!(
        "purge_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
    .unwrap();
    Some((
        MongoStorage::connect(&url, "tsd_test").await.unwrap(),
        scope,
    ))
}

fn port(backend: &MongoStorage, scope: &Scope) -> MongoPurge {
    MongoPurge {
        shared: Arc::clone(&backend.shared),
        scope: scope.clone(),
        generation: ScopeGeneration::default(),
        fenced: false,
        fail_after_chunks: false,
    }
}

#[test]
fn admission_and_receipts_fail_closed_on_malformed_authority() {
    assert!(verify(None, ScopeGeneration::default()).is_ok());
    assert_eq!(
        verify(
            Some(&doc! {"generation": 0_i64, "pending": "false"}),
            ScopeGeneration::default()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Serialization
    );

    assert_eq!(
        verify(None, ScopeGeneration(1)).unwrap_err().kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        verify(
            Some(&doc! {"generation": -1_i64}),
            ScopeGeneration::default()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        verify(
            Some(&doc! {"generation": 0_i64, "pending": true}),
            ScopeGeneration::default()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Fenced
    );
    for record in [
        doc! {},
        doc! {"generation": -1_i64},
        doc! {"generation": 1_i64, "pending": false, "operation": "r", "stage": "records"},
        doc! {"generation": 1_i64, "pending": true, "operation": "r", "stage": "unknown"},
    ] {
        assert_eq!(
            decode(&record).unwrap_err().kind(),
            ErrorKind::Serialization
        );
    }
}

#[tokio::test]
async fn pending_reset_blocks_all_generations_and_forced_off_independent_handles() {
    let Some((backend, scope)) = live("pending").await else {
        return;
    };
    let reset = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "pending_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = reset.begin(&request).await.unwrap();
    assert_eq!(receipt.stage, PurgeStage::Records);
    let independent = live("independent").await.unwrap().0.without_transactions();
    for source in [&backend, &independent] {
        for generation in [ScopeGeneration(0), receipt.generation] {
            let bound = source.for_scope_generation(&scope, generation).unwrap();
            for error in [
                bound.documents().get("records", "k").await.unwrap_err(),
                bound
                    .documents()
                    .put("records", "k", json!({}), Precondition::None)
                    .await
                    .unwrap_err(),
                bound
                    .documents()
                    .ensure_collection(&CollectionSpec::new("records"))
                    .await
                    .unwrap_err(),
                bound
                    .streams()
                    .read_window("events", 0, 0)
                    .await
                    .unwrap_err(),
                bound
                    .streams()
                    .append_batch("events", vec![])
                    .await
                    .unwrap_err(),
                bound.blobs().get("blob").await.unwrap_err(),
                bound.blobs().put("blob", vec![1], None).await.unwrap_err(),
            ] {
                assert_eq!(error.kind(), ErrorKind::Fenced);
            }
        }
    }
    assert_eq!(
        backend
            .sweep_expired(&scope, "records")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    let other = PurgeRequest {
        operation_id: "another".into(),
        ..request.clone()
    };
    assert_eq!(
        reset.purge(&other).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    reset.purge(&request).await.unwrap();
    assert_eq!(
        backend
            .for_scope(&scope)
            .unwrap()
            .documents()
            .get("records", "k")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
}

#[tokio::test]
async fn named_reset_preserves_default_and_reopens_with_issued_generation() {
    let Some((backend, scope)) = live("named").await else {
        return;
    };
    let name = format!("named_{}", scope.as_str());
    let named = MongoStorage {
        shared: backend.named(&name).unwrap(),
        databases: Arc::default(),
    };
    let root = backend.for_scope(&scope).unwrap();
    let target = named.for_scope(&scope).unwrap();
    for bound in [&root, &target] {
        bound
            .documents()
            .put("records", "k", json!({}), Precondition::None)
            .await
            .unwrap();
        bound.streams().append("events", json!(1)).await.unwrap();
        bound
            .blobs()
            .put("blob", vec![2; 600_000], None)
            .await
            .unwrap();
    }
    let request = PurgeRequest {
        operation_id: "named_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = target.purge(&request).await.unwrap();
    let reopened = live("reopen").await.unwrap().0.database(&name).unwrap();
    let fresh = reopened
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    assert!(
        fresh
            .documents()
            .get("records", "k")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fresh.streams().append("events", json!(2)).await.unwrap(), 0);
    assert!(fresh.blobs().get("blob").await.unwrap().is_none());
    assert!(
        root.documents()
            .get("records", "k")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(root.streams().len("events").await.unwrap(), 1);
    assert!(root.blobs().get("blob").await.unwrap().is_some());
}

#[test]
fn namespace_ownership_excludes_metadata_and_sibling_databases() {
    assert_eq!(scope_key(&Scope::local()), "scope:local");
    assert!(owns_records("", "records"));
    assert!(owns_records("", "_tsd_stream_heads"));
    assert!(!owns_records("", "other:records"));
    for metadata in [
        META,
        ADMISSIONS,
        MANIFEST,
        "_tsd_blobs.files",
        "_tsd_blobs.chunks",
        "system.views",
    ] {
        assert!(!owns_records("", metadata));
        assert!(!owns_records("named:", &format!("named:{metadata}")));
    }
    assert!(owns_records("named:", "named:records"));
    assert!(!owns_records("named:", "records"));
    assert!(!owns_records("named:", "sibling:records"));
}

#[tokio::test]
async fn chunk_stage_failure_keeps_manifest_and_blocks_admission_until_resume() {
    let Some((backend, scope)) = live("partial").await else {
        return;
    };
    let bound = backend.for_scope(&scope).unwrap();
    bound
        .blobs()
        .put("blob", vec![5; 600_000], None)
        .await
        .unwrap();
    let files = backend.shared.raw(&format!("{BLOBS}.files"));
    let file = files
        .find_one(doc! {"metadata._scope": scope.as_str()})
        .await
        .unwrap()
        .unwrap();
    let id = file.get("_id").unwrap();
    let mut reset = port(&backend, &scope);
    reset.fail_after_chunks = true;
    let request = PurgeRequest {
        operation_id: "partial_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let error = reset.purge(&request).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    let receipt = reset.status(&request.operation_id).await.unwrap().unwrap();
    assert_eq!(receipt.status, PurgeStatus::Pending);
    assert_eq!(receipt.stage, PurgeStage::Blobs);
    assert_eq!(files.count_documents(doc! {"_id": id}).await.unwrap(), 1);
    assert_eq!(
        backend
            .shared
            .raw(&format!("{BLOBS}.chunks"))
            .count_documents(doc! {"files_id": id})
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        backend
            .shared
            .raw(MANIFEST)
            .count_documents(doc! {"scope": scope.as_str()})
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        backend
            .for_scope_generation(&scope, receipt.generation)
            .unwrap()
            .blobs()
            .get("blob")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    let independent = live("partial_resume").await.unwrap().0;
    let resumed = port(&independent, &scope).purge(&request).await.unwrap();
    assert_eq!(resumed.status, PurgeStatus::Complete);
    assert_eq!(files.count_documents(doc! {"_id": id}).await.unwrap(), 0);
    assert_eq!(
        backend
            .shared
            .raw(MANIFEST)
            .count_documents(doc! {"scope": scope.as_str()})
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn stale_snapshot_writer_cannot_commit_after_reset_admission_closes() {
    let Some((backend, scope)) = live("snapshot").await else {
        return;
    };
    backend
        .shared
        .prepare_validation(Some(&scope))
        .await
        .unwrap();
    let docs =
        crate::documents::MongoDocuments::new(Arc::clone(&backend.shared), scope.clone(), None);
    let mut session = backend.shared.client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    // The admission precheck already passed in this snapshot before reset.
    check(
        &backend.shared,
        &scope,
        ScopeGeneration::default(),
        Some(&mut session),
    )
    .await
    .unwrap();
    let request = PurgeRequest {
        operation_id: "snapshot_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = port(&backend, &scope).begin(&request).await.unwrap();
    let result = docs.hold_validation(&mut session).await;
    session.abort_transaction().await.unwrap();
    assert!(result.unwrap_err().is_retryable());
    let fresh = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    assert_eq!(
        fresh
            .documents()
            .get("records", "late")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    port(&backend, &scope).purge(&request).await.unwrap();
    assert!(
        fresh
            .documents()
            .get("records", "late")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn unsupported_guarantees_never_close_admission_or_erase_data() {
    let Some((backend, scope)) = live("unsupported").await else {
        return;
    };
    let bound = backend.for_scope(&scope).unwrap();
    bound.streams().append("events", json!(1)).await.unwrap();
    let request = PurgeRequest {
        operation_id: "unsupported_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let guard = Scope::new(format!("guard_{}", scope.as_str())).unwrap();
    backend
        .for_scope(&guard)
        .unwrap()
        .documents()
        .put(
            "reset_guards",
            "lease",
            json!({"epoch": 1}),
            Precondition::None,
        )
        .await
        .unwrap();
    let fence = tinystoragedrivers_core::Fence::epoch(guard, "reset_guards", "lease", "epoch", 1);
    let native = backend.for_scope_fenced(&scope, &fence).unwrap();
    assert_eq!(
        native.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        native
            .purge_status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    let mut reset = port(&backend, &scope);
    assert!(reset.status("missing").await.unwrap().is_none());
    reset.fenced = true;
    assert_eq!(
        reset.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        reset
            .status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    let disabled = backend.without_transactions();
    let unsupported = port(&disabled, &scope);
    assert_eq!(
        unsupported.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        unsupported
            .status(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert!(!disabled.capabilities().contains(Capability::ScopePurge));
    assert_eq!(bound.streams().len("events").await.unwrap(), 1);
}

#[tokio::test]
async fn unexpected_late_file_remains_pending_and_replans_after_writer_drain() {
    let Some((backend, scope)) = live("late_blob").await else {
        return;
    };
    let reset = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "late_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    reset.begin(&request).await.unwrap();
    reset.records(&request.operation_id).await.unwrap();
    reset.plan_blobs(&request.operation_id).await.unwrap();
    // Simulate an upload whose owner failed to drain before asserting quiescence.
    let files = backend.shared.raw(&format!("{BLOBS}.files"));
    let id = mongodb::bson::oid::ObjectId::new();
    files.insert_one(doc! {"_id": id, "metadata": {"_scope": scope.as_str()},
        "filename": "late", "length": 0_i64, "chunkSize": 255_000, "uploadDate": mongodb::bson::DateTime::now()})
        .await.unwrap();
    assert_eq!(
        reset
            .erase_blobs(&request.operation_id)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unavailable
    );
    let pending = reset.status(&request.operation_id).await.unwrap().unwrap();
    assert_eq!(pending.status, PurgeStatus::Pending);
    assert_eq!(pending.stage, PurgeStage::BlobManifest);
    assert_eq!(
        reset.purge(&request).await.unwrap().status,
        PurgeStatus::Complete
    );
    assert_eq!(files.count_documents(doc! {"_id": id}).await.unwrap(), 0);
}

#[tokio::test]
async fn reset_between_read_admission_and_payload_rejects_replacement_data() {
    let Some((backend, scope)) = live("read_race").await else {
        return;
    };
    let reader = port(&backend, &scope);
    let request = PurgeRequest {
        operation_id: "read_race_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let outcome: Result<Document> = async {
        admitted!(reader, {
            let receipt = reader.purge(&request).await?;
            backend
                .for_scope_generation(&scope, receipt.generation)?
                .documents()
                .put(
                    "records",
                    "new",
                    json!({"replacement": true}),
                    Precondition::None,
                )
                .await?;
            let record = backend
                .shared
                .raw("records")
                .find_one(doc! {"_scope": scope.as_str(), "_key": "new"})
                .await
                .map_err(errors::failed("test payload read"))?
                .unwrap();
            Ok(record)
        })
    }
    .await;
    assert_eq!(outcome.unwrap_err().kind(), ErrorKind::Fenced);
}

#[tokio::test]
async fn issued_generation_keeps_current_default_and_named_native_guards() {
    use tinystoragedrivers_core::{Fence, GuardDatabase};
    let Some((backend, scope)) = live("guard_generation").await else {
        return;
    };
    let name = format!("guard_target_{}", scope.as_str());
    let target = MongoStorage {
        shared: backend.named(&name).unwrap(),
        databases: Arc::default(),
    };
    let guard_scope = Scope::new(format!("guard_{}", scope.as_str())).unwrap();
    let request = PurgeRequest {
        operation_id: "guard_generation_reset".into(),
        mode: PurgeMode::Quiescent,
    };
    let receipt = target
        .for_scope(&scope)
        .unwrap()
        .purge(&request)
        .await
        .unwrap();
    let named_guard = backend.database("generation_guard_database").unwrap();
    for (index, locator) in [
        GuardDatabase::Current,
        GuardDatabase::Default,
        GuardDatabase::Named("generation_guard_database".into()),
    ]
    .into_iter()
    .enumerate()
    {
        let guard = match &locator {
            GuardDatabase::Current => target.for_scope(&guard_scope).unwrap(),
            GuardDatabase::Default => backend.for_scope(&guard_scope).unwrap(),
            GuardDatabase::Named(_) => named_guard.for_scope(&guard_scope).unwrap(),
        };
        let id = format!("lease_{index}");
        guard
            .documents()
            .put(
                "generation_guards",
                &id,
                json!({"epoch":1}),
                Precondition::None,
            )
            .await
            .unwrap();
        let fence = Fence::epoch(guard_scope.clone(), "generation_guards", &id, "epoch", 1);
        let fenced = target
            .for_scope_fenced_in_database_generation(&scope, receipt.generation, &fence, &locator)
            .unwrap();
        fenced
            .documents()
            .put("records", &id, json!({"live":true}), Precondition::None)
            .await
            .unwrap();
        fenced.streams().append(&id, json!(1)).await.unwrap();
        assert_eq!(
            fenced
                .blobs()
                .put("blob", vec![1], None)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported(Capability::Fencing)
        );
        guard
            .documents()
            .put(
                "generation_guards",
                &id,
                json!({"epoch":2}),
                Precondition::None,
            )
            .await
            .unwrap();
        assert_eq!(
            fenced
                .documents()
                .put("records", &id, json!({"late":true}), Precondition::None)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Fenced
        );
        assert_eq!(
            fenced
                .streams()
                .append(&id, json!(2))
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Fenced
        );
    }
}
