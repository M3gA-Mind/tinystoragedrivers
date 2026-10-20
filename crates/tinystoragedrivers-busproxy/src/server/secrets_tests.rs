use super::*;
use tinystoragedrivers_api::types::{Capability, ErrorKind, StorageError};

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
fn secret_store_errors_keep_only_their_supported_wire_categories() {
    let private_diagnostic = "private-token-must-not-cross-the-wire";
    for (kind, expected) in [
        (ErrorKind::NotFound, wire::StorageKind::NotFound),
        (ErrorKind::Conflict, wire::StorageKind::Conflict),
        (ErrorKind::AlreadyExists, wire::StorageKind::AlreadyExists),
        (
            ErrorKind::Unsupported(Capability::Secrets),
            wire::StorageKind::Unsupported,
        ),
        (ErrorKind::InvalidInput, wire::StorageKind::InvalidInput),
        (ErrorKind::Serialization, wire::StorageKind::Serialization),
        (ErrorKind::Crypto, wire::StorageKind::Crypto),
        (ErrorKind::Unavailable, wire::StorageKind::Unavailable),
        (ErrorKind::Backend, wire::StorageKind::Backend),
    ] {
        let failure =
            native_secret::<()>(Err(StorageError::new(kind, private_diagnostic))).unwrap_err();
        assert_eq!(failure.kind, expected);
        assert!(!format!("{failure:?}").contains(private_diagnostic));
    }
}
