//! Database names and the scoped handle bundle.

use super::*;
use crate::MemoryStorage;

#[test]
fn database_names_validate() {
    assert!(validate_database("approvals").is_ok());
    assert!(validate_database("flows-v2_1").is_ok());
    for bad in ["", "Upper", "dot.ted", "sp ace", &"a".repeat(65)] {
        assert!(validate_database(bad).is_err(), "{bad}");
    }
}

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

#[test]
fn unknown_adapters_refuse_explicit_guard_addresses() {
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
        fn database(&self, _: &str) -> Result<Arc<dyn StorageBackend>> {
            Ok(Arc::new(Adapter))
        }
    }
    let fence = Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    for locator in [
        crate::GuardDatabase::Default,
        crate::GuardDatabase::Named("guards".into()),
    ] {
        assert_eq!(
            Adapter
                .for_scope_fenced_in_database(&Scope::local(), &fence, &locator)
                .unwrap_err()
                .kind(),
            crate::ErrorKind::Unsupported(Capability::CrossDatabaseFencing)
        );
    }
    assert_eq!(
        Adapter
            .for_scope_fenced_in_database(&Scope::local(), &fence, &crate::GuardDatabase::Current)
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        Adapter
            .for_scope_fenced_in_database(
                &Scope::local(),
                &fence,
                &crate::GuardDatabase::Named("invalid space".into())
            )
            .unwrap_err()
            .kind(),
        crate::ErrorKind::InvalidInput
    );
}
