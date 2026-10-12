//! Client behavior that conformance does not pin: binding and error mapping.
use std::sync::Arc;

use tinystoragedrivers_core::{Capability, ErrorKind, MemoryStorage, Scope, StorageBackend};

use super::*;
use crate::{Admission, Loopback, StorageServer};

async fn bus() -> BusStorage {
    let backend = MemoryStorage::new();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap());
    BusStorage::connect(
        Arc::new(Loopback::new(Arc::new(server), Admission::Private)),
        Scope::local(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_bus_caller_is_bound_to_its_scope_and_cannot_pick_databases_or_fences() {
    let bus = bus().await;
    assert_eq!(bus.driver(), "bus");
    assert!(bus.for_scope(&Scope::local()).is_ok());
    let other = Scope::new("bob").unwrap();
    assert_eq!(
        bus.for_scope(&other).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        bus.database("other").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    for host_only in [
        Capability::Fencing,
        Capability::ScopePurge,
        Capability::ExistingSchema,
    ] {
        assert!(!bus.capabilities().contains(host_only));
    }
    let fence = tinystoragedrivers_core::Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    assert!(bus.for_scope_fenced(&Scope::local(), &fence).is_err());
}

#[tokio::test]
async fn unsupported_refusals_name_the_capability_the_call_needs() {
    let bus = bus().await;
    let storage = bus.for_scope(&Scope::local()).unwrap();
    let expected = Capability::PresignedUrls;
    let error = storage
        .blobs()
        .presign_get("k", std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(expected));
}

/// Blobs that forward everything but cannot expire, like the `MongoDB` driver.
#[derive(Debug)]
struct NoTtl(Arc<dyn tinystoragedrivers_core::BlobStore>);

#[tinystoragedrivers_core::async_trait]
impl tinystoragedrivers_core::BlobStore for NoTtl {
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::BlobMeta> {
        self.0.put(key, bytes, content_type).await
    }
    async fn get(
        &self,
        key: &str,
    ) -> tinystoragedrivers_core::Result<Option<tinystoragedrivers_core::Blob>> {
        self.0.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> tinystoragedrivers_core::Result<Option<Vec<u8>>> {
        self.0.get_range(key, range).await
    }
    async fn head(
        &self,
        key: &str,
    ) -> tinystoragedrivers_core::Result<Option<tinystoragedrivers_core::BlobMeta>> {
        self.0.head(key).await
    }
    async fn delete(&self, key: &str) -> tinystoragedrivers_core::Result<bool> {
        self.0.delete(key).await
    }
    async fn list(
        &self,
        prefix: &str,
    ) -> tinystoragedrivers_core::Result<Vec<tinystoragedrivers_core::BlobMeta>> {
        self.0.list(prefix).await
    }
}

#[tokio::test]
async fn a_host_without_blob_expiry_is_refused_before_any_io() {
    use std::time::Duration;
    use tinystoragedrivers_core::ScopedStorage;

    let backend = MemoryStorage::new();
    let base = backend.for_scope(&Scope::local()).unwrap();
    let storage = ScopedStorage::new(
        Scope::local(),
        "no-ttl",
        base.documents().clone(),
        base.streams().clone(),
        Arc::new(NoTtl(base.blobs().clone())),
    );
    let server = Arc::new(StorageServer::new(storage));
    let bus = BusStorage::connect(
        Arc::new(Loopback::new(server, Admission::Private)),
        Scope::local(),
    )
    .await
    .unwrap();
    let blobs = bus.for_scope(&Scope::local()).unwrap().blobs().clone();
    assert!(!blobs.capabilities().contains(Capability::BlobTtl));
    let expected = ErrorKind::Unsupported(Capability::BlobTtl);
    let ttl = Duration::from_secs(60);
    assert_eq!(
        blobs
            .put_expiring("k", vec![1], None, ttl)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(
        blobs
            .put_stream_expiring("k", Box::pin(std::io::Cursor::new(vec![1])), 1, None, ttl)
            .await
            .unwrap_err()
            .kind(),
        expected
    );
    // The refusal came before any write.
    assert!(base.blobs().get("k").await.unwrap().is_none());
}
