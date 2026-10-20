use super::*;

#[tokio::test]
async fn confidential_secret_calls_round_trip_through_the_bus() {
    use std::sync::Arc;

    use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
    use tinystoragedrivers_secrets::MemorySecrets;

    let backend = MemoryStorage::new();
    let server = crate::StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
        .with_secrets(Arc::new(MemorySecrets::new()));
    let wire = Wire::new(Arc::new(crate::Loopback::new(
        Arc::new(server),
        crate::Admission::Confidential,
    )));
    let secrets = BusSecrets::new(wire, true);

    assert_eq!(secrets.backend_name(), "tinybus");
    assert!(secrets.enumerable());
    assert_eq!(secrets.get("api/token").await.unwrap(), None);
    secrets.set("api/token", b"secret-value").await.unwrap();
    assert_eq!(
        secrets.get("api/token").await.unwrap().unwrap().as_slice(),
        b"secret-value"
    );
    assert_eq!(secrets.list("api/").await.unwrap(), vec!["api/token"]);
    assert!(secrets.delete("api/token").await.unwrap());
    assert!(!secrets.delete("api/token").await.unwrap());
}
