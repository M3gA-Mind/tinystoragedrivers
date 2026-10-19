use super::*;
use crate::MemoryStorage;

#[tokio::test]
async fn backend_blobs_routes_blob_operations_through_the_requested_scope() {
    let backend = Arc::new(MemoryStorage::new());
    let blobs = BackendBlobs::new(backend.clone());
    let first_scope = Scope::new("first").unwrap();
    let second_scope = Scope::new("second").unwrap();
    let first = blobs.for_scope(&first_scope).unwrap();
    let second = blobs.for_scope(&second_scope).unwrap();

    first
        .put("object", b"first payload".to_vec(), None)
        .await
        .unwrap();
    second
        .put("object", b"second payload".to_vec(), None)
        .await
        .unwrap();

    assert_eq!(
        first.get("object").await.unwrap().unwrap().bytes,
        b"first payload"
    );
    assert_eq!(
        second.get("object").await.unwrap().unwrap().bytes,
        b"second payload"
    );
    assert_eq!(
        backend
            .for_scope(&Scope::local())
            .unwrap()
            .blobs()
            .get("object")
            .await
            .unwrap(),
        None
    );
}

#[test]
fn backend_blobs_projects_only_blob_capabilities() {
    let backend = MemoryStorage::new();
    let wrapped = BackendBlobs::new(Arc::new(backend));
    let caps = wrapped.capabilities();

    assert!(caps.contains(Capability::BlobTtl));
    assert!(!caps.contains(Capability::StreamingBlobs));
    assert!(!caps.contains(Capability::PresignedUrls));
    assert!(!caps.contains(Capability::Transactions));
    assert!(!caps.contains(Capability::Fencing));
}

#[test]
fn backend_blobs_keeps_supported_blob_capabilities_and_drops_database_ones() {
    #[derive(Debug)]
    struct Advertised(MemoryStorage);

    impl StorageBackend for Advertised {
        fn driver(&self) -> &'static str {
            "advertised"
        }

        fn capabilities(&self) -> crate::Capabilities {
            crate::Capabilities::all()
        }

        fn for_scope(&self, scope: &Scope) -> crate::Result<crate::ScopedStorage> {
            self.0.for_scope(scope)
        }
    }

    let wrapped = BackendBlobs::new(Arc::new(Advertised(MemoryStorage::new())));
    let caps = wrapped.capabilities();

    assert!(caps.contains(Capability::StreamingBlobs));
    assert!(caps.contains(Capability::PresignedUrls));
    assert!(caps.contains(Capability::BlobTtl));
    assert!(!caps.contains(Capability::Transactions));
    assert!(!caps.contains(Capability::Fencing));
    assert!(!caps.contains(Capability::Watch));
}
