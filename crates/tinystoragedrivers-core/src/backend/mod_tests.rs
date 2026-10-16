//! The scoped handle bundle.

use super::*;
use crate::MemoryStorage;

#[test]
fn scoped_storage_exposes_its_parts() {
    let scope = Scope::new("t1").unwrap();
    let storage = MemoryStorage::new().for_scope(&scope).unwrap();
    assert_eq!(storage.scope(), &scope);
    assert_eq!(storage.driver(), "memory");
    assert!(
        storage
            .documents()
            .capabilities()
            .contains(crate::Capability::Ttl)
    );
    let _ = (storage.streams(), storage.blobs());
    let rendered = format!("{storage:?}");
    assert!(
        rendered.contains("t1") && rendered.contains("memory"),
        "{rendered}"
    );
}

#[tokio::test]
async fn unknown_adapters_refuse_fencing() {
    #[derive(Debug)]
    struct Adapter;
    impl StorageBackend for Adapter {
        fn driver(&self) -> &'static str {
            "unknown"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::none()
        }
        fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
            MemoryStorage::new().for_scope(scope)
        }
    }
    assert_eq!(
        Adapter.prepare_schema().await.unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ExistingSchema)
    );
    assert!(matches!(
        Adapter.existing_schema().unwrap_err().kind(),
        crate::ErrorKind::Unsupported(Capability::ExistingSchema)
    ));
    let fence = Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    assert_eq!(
        Adapter
            .for_scope_fenced(&Scope::local(), &fence)
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::Fencing)
    );
}
