use super::*;
use crate::StorageServer;

#[tokio::test]
async fn confidential_secret_calls_round_trip_through_the_bus() {
    use std::sync::Arc;

    use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
    use tinystoragedrivers_secrets::MemorySecrets;

    let backend = MemoryStorage::new();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
        .with_secrets(Arc::new(MemorySecrets::new()));
    let wire = Wire::new(Arc::new(crate::Loopback::new(
        Arc::new(server),
        crate::Admission::Confidential,
    )));
    let secrets = BusSecrets::new(wire);

    assert_eq!(secrets.backend_name(), "bus");
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

#[test]
fn host_error_categories_are_preserved_without_exposing_diagnostics() {
    use tinystoragedrivers_core::StorageError as HostError;

    let private_diagnostic = "private-token-should-not-cross-the-bus";
    let cases = [
        (
            HostError::not_found(private_diagnostic),
            ErrorKind::NotFound,
        ),
        (HostError::conflict(private_diagnostic), ErrorKind::Conflict),
        (
            HostError::already_exists(private_diagnostic),
            ErrorKind::AlreadyExists,
        ),
        (
            HostError::unsupported(
                tinystoragedrivers_core::Capability::Watch,
                private_diagnostic,
            ),
            ErrorKind::Unsupported(Capability::Secrets),
        ),
        (
            HostError::invalid_input(private_diagnostic),
            ErrorKind::InvalidInput,
        ),
        (
            HostError::serialization(private_diagnostic),
            ErrorKind::Serialization,
        ),
        (HostError::crypto(private_diagnostic), ErrorKind::Crypto),
        (
            HostError::unavailable(private_diagnostic),
            ErrorKind::Unavailable,
        ),
        (HostError::backend(private_diagnostic), ErrorKind::Backend),
    ];

    for (host_error, expected) in cases {
        let mapped = secret_error(&host_error);
        assert_eq!(mapped.kind(), expected);
        assert!(!mapped.to_string().contains(private_diagnostic));
        assert_eq!(
            mapped.message(),
            "the storage host refused the secret operation"
        );
    }
}
