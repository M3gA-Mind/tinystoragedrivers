use super::*;
use rusqlite::ffi;

#[test]
fn busy_errors_are_retryable_and_data_free() {
    let failure = rusqlite::Error::SqliteFailure(
        ffi::Error::new(ffi::SQLITE_BUSY),
        Some("private row contents".to_owned()),
    );
    let error = map("write a value", failure);

    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
    assert!(error.to_string().contains("busy"));
    assert!(!error.to_string().contains("private row contents"));
}

#[test]
fn ordinary_sqlite_errors_are_backend_failures() {
    let failure = rusqlite::Error::SqliteFailure(ffi::Error::new(ffi::SQLITE_CONSTRAINT), None);
    let error = during("insert a value")(failure);

    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
    assert!(error.to_string().contains("insert a value"));
}
