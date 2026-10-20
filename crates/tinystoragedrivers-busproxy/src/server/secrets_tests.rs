use super::*;
use tinystoragedrivers_api::types::{Capability, ErrorKind, StorageError};

#[test]
fn secret_store_errors_keep_only_their_supported_wire_categories() {
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
        assert_eq!(
            native_secret::<()>(Err(StorageError::new(kind, "private secret")))
                .unwrap_err()
                .kind,
            expected
        );
    }
}
