//! Validation, cursors, and precondition checks shared by KV engines.

use std::time::Duration;

use tinystoragedrivers_types::{
    Cursor, CursorPosition, Key, Precondition, Result, StorageError, Version,
};

/// Validate one stored key through the public key vocabulary.
pub(crate) fn validate_key(key: &str) -> Result<()> {
    Key::new(key.to_owned()).map(|_| ())
}

/// Validate a prefix, which may be empty but has the same byte constraints as
/// a complete key.
pub(crate) fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.len() > tinystoragedrivers_types::MAX_KEY_LEN || prefix.contains('\0') {
        return Err(StorageError::invalid_input(
            "key prefix must be at most 512 bytes without NUL",
        ));
    }
    Ok(())
}

/// Decode and check a continuation key bound to this prefix.
pub(crate) fn cursor_key(cursor: &Cursor, prefix: &str) -> Result<String> {
    let fingerprint = prefix_hash(prefix);
    let position = cursor.decode_keyset("kv", fingerprint)?;
    position.validate(&[])?;
    validate_key(&position.id)?;
    if !position.id.starts_with(prefix) {
        return Err(StorageError::invalid_input(
            "cursor was not issued for this key prefix",
        ));
    }
    Ok(position.id)
}

/// Encode a continuation key bound to this prefix.
pub(crate) fn make_cursor(key: &str, prefix: &str) -> Result<Cursor> {
    Cursor::keyset(
        "kv",
        prefix_hash(prefix),
        &CursorPosition {
            id: key.to_owned(),
            values: Vec::new(),
        },
    )
}

/// Stable 64-bit FNV-1a fingerprint for a prefix.
fn prefix_hash(prefix: &str) -> u64 {
    prefix
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

/// Enforce an optimistic write condition against the current live version.
pub(crate) fn check_precondition(expected: Precondition, current: Option<Version>) -> Result<()> {
    let matches = match expected {
        Precondition::None => true,
        Precondition::Absent => current.is_none(),
        Precondition::Version(version) => current == Some(version),
    };
    if matches {
        Ok(())
    } else {
        Err(StorageError::conflict("key value changed"))
    }
}

/// The next revision after the last stored revision, or the first revision.
pub(crate) fn next_version(last: Option<Version>) -> Result<Version> {
    match last {
        None => Ok(Version::FIRST),
        Some(version) => version
            .next()
            .ok_or_else(|| StorageError::backend("key version space is exhausted")),
    }
}

/// Reject a zero-sized page, which cannot produce a continuation position.
pub(crate) fn validate_limit(limit: usize) -> Result<()> {
    if limit == 0 {
        Err(StorageError::invalid_input(
            "key listing limit must be at least 1",
        ))
    } else {
        Ok(())
    }
}

/// Round positive fractional milliseconds up so a value never expires early.
pub(crate) fn duration_millis(ttl: Duration) -> u64 {
    let millis = ttl.as_millis();
    let fractional = u128::from(!ttl.subsec_nanos().is_multiple_of(1_000_000));
    u64::try_from(millis.saturating_add(fractional)).unwrap_or(u64::MAX)
}

/// Convert a version to a signed integer representation used by SQL-backed engines.
#[cfg(any(feature = "sqlite", feature = "mongodb"))]
pub(crate) fn version_to_i64(version: Version) -> Result<i64> {
    i64::try_from(version.0).map_err(|_| StorageError::backend("key version space is exhausted"))
}

/// Convert a stored SQL integer back to a validated version.
#[cfg(feature = "sqlite")]
pub(crate) fn from_sqlite_version(version: i64) -> Result<Version> {
    u64::try_from(version)
        .map(Version)
        .map_err(|_| StorageError::backend("sqlite stored an invalid key version"))
}

/// Error kind helper for tests and engine-specific result mappings.
#[cfg(test)]
pub(crate) fn is_conflict(error: &StorageError) -> bool {
    error.kind() == tinystoragedrivers_types::ErrorKind::Conflict
}

#[cfg(test)]
#[path = "common_tests.rs"]
mod tests;
