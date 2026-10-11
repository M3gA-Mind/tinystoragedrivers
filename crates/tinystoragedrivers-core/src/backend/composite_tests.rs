//! Composite scope, database and fencing contracts.
use super::*;
use crate::MemoryStorage;

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
struct FullCapabilitiesBlobs(BackendBlobs);
impl BlobBackend for FullCapabilitiesBlobs {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::ScopePurge)
            .with(Capability::AtomicScopePurge)
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        self.0.for_scope(scope)
    }
    fn database(&self, name: &str) -> Result<Arc<dyn BlobBackend>> {
        self.0.database(name)
    }
}

#[tokio::test]
async fn composite_refuses_purge_and_generation_before_erasing_either_port() {
    use crate::{ErrorKind, PurgeMode, PurgeRequest, ScopeGeneration};
    let records = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(MemoryStorage::new());
    let backend = CompositeStorage::new(
        records.clone(),
        Arc::new(FullCapabilitiesBlobs(BackendBlobs::new(blobs.clone()))),
    );
    let scope = Scope::local();
    let bound = backend.for_scope(&scope).unwrap();
    bound
        .streams()
        .append("retained", serde_json::json!(1))
        .await
        .unwrap();
    bound.blobs().put("retained", vec![1], None).await.unwrap();
    for cap in [Capability::ScopePurge, Capability::AtomicScopePurge] {
        assert!(!backend.capabilities().contains(cap));
    }
    let error = bound
        .purge(&PurgeRequest {
            operation_id: "reset".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::ScopePurge));
    assert_eq!(
        backend
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
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
