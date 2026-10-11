use super::*;
use crate::{Capabilities, ErrorKind, Fence, GuardDatabase, MemoryStorage, ScopedStorage};

#[test]
fn purge_addresses_and_identities_are_explicit_and_validated() {
    let root = Arc::new(MemoryStorage::new()) as Arc<dyn StorageBackend>;
    assert_eq!(
        DatabaseAddress::Default.resolve(&root).unwrap().driver(),
        "memory"
    );
    assert_eq!(
        DatabaseAddress::Named("sessions".into())
            .resolve(&root)
            .unwrap()
            .driver(),
        "memory"
    );
    assert_eq!(
        DatabaseAddress::Named("Bad Name".into())
            .resolve(&root)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    for id in ["", "space id", "slash/id", &"x".repeat(129)] {
        assert_eq!(
            PurgeRequest {
                operation_id: id.into(),
                mode: PurgeMode::Atomic
            }
            .validate()
            .unwrap_err()
            .kind(),
            ErrorKind::InvalidInput
        );
    }
    for id in ["reset_1", "RESET-2"] {
        let request = PurgeRequest {
            operation_id: id.into(),
            mode: PurgeMode::Quiescent,
        };
        request.validate().unwrap();
        let encoded = serde_json::to_string(&request).unwrap();
        assert_eq!(
            serde_json::from_str::<PurgeRequest>(&encoded).unwrap(),
            request
        );
    }
}

#[derive(Debug)]
struct Legacy;
impl StorageBackend for Legacy {
    fn driver(&self) -> &'static str {
        "legacy"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        MemoryStorage::new().for_scope(scope)
    }
    fn database(&self, _: &str) -> Result<Arc<dyn StorageBackend>> {
        Ok(Arc::new(Self))
    }
}
#[test]
fn unknown_adapters_only_accept_legacy_generation() {
    let scope = Scope::local();
    assert!(
        Legacy
            .for_scope_generation(&scope, ScopeGeneration::default())
            .is_ok()
    );
    assert_eq!(
        Legacy
            .for_scope_generation(&scope, ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    let fence = Fence::epoch(Scope::new("guard").unwrap(), "leases", "l", "epoch", 1);
    assert_eq!(
        Legacy
            .for_scope_fenced_in_database_generation(
                &scope,
                ScopeGeneration::default(),
                &fence,
                &GuardDatabase::Current
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::Fencing)
    );
    assert_eq!(
        Legacy
            .for_scope_fenced_in_database_generation(
                &scope,
                ScopeGeneration(1),
                &fence,
                &GuardDatabase::Default
            )
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
}
