//! SQLite error mapping shared by connection setup and primitive engines.

use rusqlite::ErrorCode;
use tinystoragedrivers_types::StorageError;

/// Map a SQLite failure without including query or data contents.
#[must_use]
pub fn map(operation: &'static str, error: rusqlite::Error) -> StorageError {
    let busy = matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    );
    let message = if busy {
        format!("sqlite is busy during {operation}")
    } else {
        format!("sqlite failed during {operation}")
    };
    let mapped = if busy {
        StorageError::unavailable(message)
    } else {
        StorageError::backend(message)
    };
    mapped.with_source(error)
}

/// Bind the operation name for `map_err`.
#[must_use]
pub fn during(operation: &'static str) -> impl Fn(rusqlite::Error) -> StorageError {
    move |error| map(operation, error)
}
