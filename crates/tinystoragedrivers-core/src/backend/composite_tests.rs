//! Composite scope, database and fencing contracts.
use super::*;
use crate::MemoryStorage;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
struct FailPurgeOnce {
    inner: BackendBlobs,
    fail: AtomicBool,
}
#[async_trait]
impl BlobBackend for FailPurgeOnce {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.inner.for_scope(scope)
    }
    async fn purge_scope(&self, scope: &Scope) -> Result<()> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(StorageError::backend("injected blob purge failure"));
        }
        self.inner.purge_scope(scope).await
    }
    async fn scopes(&self, prefix: &str) -> Result<Vec<Scope>> {
        self.inner.scopes(prefix).await
    }
}

#[tokio::test]
async fn independent_ports_pass_conformance_with_safe_fencing() {
    let records = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new())));
    let backend = CompositeStorage::new(records, blobs);
    crate::conformance::run(&backend, false).await;
}

#[tokio::test]
async fn fenced_composite_blobs_allow_reads_but_refuse_every_mutation() {
    let backend = MemoryStorage::new();
    let scope = backend.for_scope(&Scope::local()).unwrap();
    scope
        .blobs()
        .put("existing", vec![1, 2, 3], None)
        .await
        .unwrap();
    let blobs = FencedBlobs(scope.blobs().clone());
    assert_eq!(blobs.max_key_len(), scope.blobs().max_key_len());
    assert!(!blobs.capabilities().contains(Capability::PresignedUrls));
    assert_eq!(
        blobs.get("existing").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
    assert!(blobs.get_stream("existing").await.unwrap().is_some());
    assert_eq!(
        blobs.get_range("existing", 1..3).await.unwrap().unwrap(),
        [2, 3]
    );
    assert!(blobs.head("existing").await.unwrap().is_some());
    assert_eq!(blobs.list("").await.unwrap().len(), 1);
    assert!(
        blobs
            .presign_get("existing", Duration::from_secs(60))
            .await
            .is_err()
    );
    for error in [
        blobs.put("existing", vec![9], None).await.unwrap_err(),
        blobs
            .put_stream("existing", Box::pin(std::io::Cursor::new(vec![9])), 1, None)
            .await
            .unwrap_err(),
        blobs.delete("existing").await.unwrap_err(),
        blobs
            .presign_put("existing", Duration::from_secs(60))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(
            error.kind(),
            crate::ErrorKind::Unsupported(Capability::Fencing)
        );
    }
    assert_eq!(
        scope.blobs().get("existing").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
}

#[derive(Debug)]
struct OverclaimingBlobs(BackendBlobs);
impl BlobBackend for OverclaimingBlobs {
    fn capabilities(&self) -> Capabilities {
        Capabilities::all()
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.0.for_scope(scope)
    }
}

#[derive(Debug)]
struct MaskedRecords(MemoryStorage);
impl StorageBackend for MaskedRecords {
    fn driver(&self) -> &'static str {
        "masked-memory"
    }
    fn document_driver(&self) -> &'static str {
        self.0.document_driver()
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        self.0.for_scope(scope)
    }
}

#[test]
fn blob_backend_cannot_invent_document_watch_or_transaction_capabilities() {
    let records = Arc::new(MaskedRecords(MemoryStorage::new()));
    let blobs = Arc::new(OverclaimingBlobs(BackendBlobs::new(Arc::new(
        MemoryStorage::new(),
    ))));
    let composite = CompositeStorage::new(records.clone(), blobs);
    for cap in [
        Capability::Watch,
        Capability::FullText,
        Capability::ValidatedBatch,
        Capability::Fencing,
    ] {
        assert_eq!(
            composite.capabilities().contains(cap),
            records.capabilities().contains(cap)
        );
    }
    assert!(
        composite
            .capabilities()
            .contains(Capability::StreamingBlobs)
    );
    assert!(composite.capabilities().contains(Capability::PresignedUrls));
}

#[tokio::test]
async fn composite_refuses_purge_and_generation_before_erasing_either_port() {
    use crate::{ErrorKind, PurgeMode, PurgeRequest, ScopeGeneration};
    let records = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(OverclaimingBlobs(BackendBlobs::new(blobs.clone()))),
    );
    let scope = Scope::local();
    let bound = backend.for_scope(&scope).unwrap();
    bound
        .streams()
        .append("retained", serde_json::json!(1))
        .await
        .unwrap();
    bound.blobs().put("retained", vec![1], None).await.unwrap();
    assert!(backend.capabilities().contains(Capability::ScopePurge));
    assert!(
        !backend
            .capabilities()
            .contains(Capability::AtomicScopePurge)
    );
    let error = bound
        .purge(&PurgeRequest {
            operation_id: "reset".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::AtomicScopePurge)
    );
    assert_eq!(
        backend
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap()
            .streams()
            .len("retained")
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        records
            .for_scope(&scope)
            .unwrap()
            .streams()
            .len("retained")
            .await
            .unwrap(),
        1
    );
    assert!(
        blobs
            .for_scope(&scope)
            .unwrap()
            .blobs()
            .get("retained")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn composite_scope_purge_erases_both_ports_and_preserves_siblings() {
    use crate::{PurgeMode, PurgeRequest, PurgeStatus};
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(BackendBlobs::new(blob_storage.clone())),
    );
    assert!(backend.capabilities().contains(Capability::ScopePurge));
    assert!(
        !backend
            .capabilities()
            .contains(Capability::AtomicScopePurge)
    );
    let target = Scope::new("u:purge-target").unwrap();
    let sibling = Scope::new("u:purge-sibling").unwrap();
    for (store, scope) in [(&records, &target), (&records, &sibling)] {
        store
            .for_scope(scope)
            .unwrap()
            .streams()
            .append("s", serde_json::json!(1))
            .await
            .unwrap();
    }
    for scope in [&target, &sibling] {
        blob_storage
            .for_scope(scope)
            .unwrap()
            .blobs()
            .put("b", vec![1], None)
            .await
            .unwrap();
    }
    let bound = backend.for_scope(&target).unwrap();
    let receipt = bound
        .purge(&PurgeRequest {
            operation_id: "reset-1".into(),
            mode: PurgeMode::Quiescent,
        })
        .await
        .unwrap();
    assert_eq!(receipt.status, PurgeStatus::Complete);
    assert_eq!(
        bound.blobs().get("b").await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(
        backend
            .for_scope_generation(&target, receipt.generation)
            .unwrap()
            .streams()
            .len("s")
            .await
            .unwrap(),
        0
    );
    assert!(
        blob_storage
            .for_scope(&target)
            .unwrap()
            .blobs()
            .get("b")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        records
            .for_scope(&sibling)
            .unwrap()
            .streams()
            .len("s")
            .await
            .unwrap(),
        1
    );
    assert!(
        blob_storage
            .for_scope(&sibling)
            .unwrap()
            .blobs()
            .get("b")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn composite_purge_records_pending_external_stage_and_replay_preserves_new_generation() {
    let records = Arc::new(MemoryStorage::new());
    let blob_storage = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(FailPurgeOnce {
        inner: BackendBlobs::new(blob_storage.clone()),
        fail: AtomicBool::new(true),
    });
    let backend = CompositeStorage::new(records, blobs);
    let scope = Scope::new("u:retry-composite").unwrap();
    let old = backend.for_scope(&scope).unwrap();
    old.blobs().put("before", vec![1], None).await.unwrap();
    let request = PurgeRequest {
        operation_id: "retry-composite".into(),
        mode: PurgeMode::Quiescent,
    };
    assert!(old.purge(&request).await.is_err());
    let pending = old.purge_status("retry-composite").await.unwrap().unwrap();
    assert_eq!(pending.status, PurgeStatus::Pending);
    assert_eq!(pending.stage, crate::PurgeStage::Blobs);
    let completed = old.purge(&request).await.unwrap();
    assert_eq!(completed.status, PurgeStatus::Complete);
    let current = backend
        .for_scope_generation(&scope, completed.generation)
        .unwrap();
    current.blobs().put("after", vec![2], None).await.unwrap();
    assert_eq!(
        current.blobs().get("after").await.unwrap().unwrap().bytes,
        [2]
    );
    assert_eq!(
        old.blobs().get("after").await.unwrap_err().kind(),
        crate::ErrorKind::ScopeGenerationRejected
    );
    assert_eq!(current.purge(&request).await.unwrap(), completed);
    assert_eq!(
        current.blobs().get("after").await.unwrap().unwrap().bytes,
        [2]
    );
}

#[test]
fn composite_document_driver_preserves_record_identity_instead_of_blob_or_wrapper_name() {
    let records = Arc::new(MaskedRecords(MemoryStorage::new()));
    let blobs = Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new())));
    let backend = CompositeStorage::new(records, blobs);
    assert_eq!(
        backend.driver(),
        "composite",
        "diagnostic wrapper label remains compatible"
    );
    assert_eq!(
        backend.document_driver(),
        "memory",
        "record coordination must not use composite or blob identity"
    );
}

#[test]
fn native_default_and_erased_nested_composite_database_keep_document_driver() {
    let native: Arc<dyn StorageBackend> = Arc::new(MemoryStorage::new());
    assert_eq!(native.document_driver(), "memory");
    let inner: Arc<dyn StorageBackend> = Arc::new(CompositeStorage::new(
        native,
        Arc::new(BackendBlobs::new(Arc::new(MaskedRecords(
            MemoryStorage::new(),
        )))),
    ));
    let outer: Arc<dyn StorageBackend> = Arc::new(CompositeStorage::new(
        inner,
        Arc::new(BackendBlobs::new(Arc::new(MemoryStorage::new()))),
    ));
    assert_eq!(outer.driver(), "composite");
    assert_eq!(outer.document_driver(), "memory");
    assert_eq!(outer.document_driver(), "memory");
}
