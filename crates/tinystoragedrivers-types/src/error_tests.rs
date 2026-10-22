//! Error kinds preserve stable categories and source causes.
use super::*;

#[test]
fn constructors_keep_kind_message_and_source() {
    let error =
        StorageError::unavailable("database busy").with_source(std::io::Error::other("busy"));
    assert_eq!(error.kind(), ErrorKind::Unavailable);
    assert_eq!(error.message(), "database busy");
    assert!(error.is_retryable());
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(
        StorageError::conflict("stale").to_string(),
        "conflict: stale"
    );
    assert!(!StorageError::invalid_input("bad key").is_retryable());
}

#[test]
fn json_failures_map_to_serialization() {
    let error = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
    let storage_error = StorageError::from(error);
    assert_eq!(storage_error.kind(), ErrorKind::Serialization);
}

#[test]
fn every_category_has_stable_display_and_constructor() {
    let cases = [
        (
            ErrorKind::NotFound,
            StorageError::not_found("x"),
            "not found",
        ),
        (ErrorKind::Conflict, StorageError::conflict("x"), "conflict"),
        (
            ErrorKind::AlreadyExists,
            StorageError::already_exists("x"),
            "already exists",
        ),
        (
            ErrorKind::Unsupported(Capability::Watch),
            StorageError::unsupported(Capability::Watch, "x"),
            "unsupported capability watch",
        ),
        (
            ErrorKind::InvalidInput,
            StorageError::invalid_input("x"),
            "invalid input",
        ),
        (
            ErrorKind::Serialization,
            StorageError::serialization("x"),
            "serialization",
        ),
        (ErrorKind::Crypto, StorageError::crypto("x"), "crypto"),
        (
            ErrorKind::Unavailable,
            StorageError::unavailable("x"),
            "unavailable",
        ),
        (ErrorKind::Backend, StorageError::backend("x"), "backend"),
    ];
    for (kind, error, display) in cases {
        assert_eq!(kind.to_string(), display);
        assert_eq!(error.kind(), kind);
    }
}
