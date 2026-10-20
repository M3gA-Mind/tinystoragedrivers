use super::*;

#[tokio::test]
async fn secret_selectors_forward_each_operation_to_the_confidential_store() {
    use std::sync::Arc;

    use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
    use tinystoragedrivers_secrets::MemorySecrets;

    let backend = MemoryStorage::new();
    let server = StorageServer::new(backend.for_scope(&Scope::local()).unwrap())
        .with_secrets(Arc::new(MemorySecrets::new()));

    let missing: Option<wire::SecretValue> = serde_json::from_value(
        server
            .secrets("Get", serde_json::json!({"name": "api/token"}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(missing.is_none());
    server
        .secrets(
            "Set",
            serde_json::json!({"name": "api/token", "value": [115, 101, 99, 114, 101, 116]}),
        )
        .await
        .unwrap();
    let found: Option<wire::SecretValue> = serde_json::from_value(
        server
            .secrets("Get", serde_json::json!({"name": "api/token"}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(found.unwrap().value, b"secret");
    let listed: Vec<String> = serde_json::from_value(
        server
            .secrets("List", serde_json::json!({"prefix": "api/"}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(listed, vec!["api/token"]);
    let deleted: bool = serde_json::from_value(
        server
            .secrets("Delete", serde_json::json!({"name": "api/token"}))
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(deleted);
}

#[test]
fn native_secret_maps_typed_errors_to_wire_categories() {
    use tinystoragedrivers_api::types::{Capability, StorageError};

    let private_diagnostic = "private-token-must-not-cross-the-wire";
    let cases = [
        (
            StorageError::not_found(private_diagnostic),
            wire::StorageKind::NotFound,
        ),
        (
            StorageError::conflict(private_diagnostic),
            wire::StorageKind::Conflict,
        ),
        (
            StorageError::already_exists(private_diagnostic),
            wire::StorageKind::AlreadyExists,
        ),
        (
            StorageError::unsupported(Capability::Secrets, private_diagnostic),
            wire::StorageKind::Unsupported,
        ),
        (
            StorageError::invalid_input(private_diagnostic),
            wire::StorageKind::InvalidInput,
        ),
        (
            StorageError::serialization(private_diagnostic),
            wire::StorageKind::Serialization,
        ),
        (
            StorageError::crypto(private_diagnostic),
            wire::StorageKind::Crypto,
        ),
        (
            StorageError::unavailable(private_diagnostic),
            wire::StorageKind::Unavailable,
        ),
        (
            StorageError::backend(private_diagnostic),
            wire::StorageKind::Backend,
        ),
    ];

    for (native_error, expected) in cases {
        let failure = native_secret::<()>(Err(native_error)).unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains(private_diagnostic));
    }
}
