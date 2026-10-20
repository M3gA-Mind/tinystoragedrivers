//! Shared validation and ordering rules for blob engines.

use tinystoragedrivers_types::{Result, StorageError, is_reserved_blob_key};

pub(crate) const MAX_KEY_LEN: usize = 1024;

pub(crate) fn validate_key(key: &str) -> Result<()> {
    validate_key_shape(key)?;
    if is_reserved_blob_key(key) {
        return Err(StorageError::invalid_input("invalid blob key"));
    }
    Ok(())
}

pub(crate) fn validate_sealed_key(key: &str) -> Result<()> {
    validate_key_shape(key)?;
    if !is_reserved_blob_key(key) {
        return Err(StorageError::invalid_input(
            "driver-owned blob key is outside the sealed-stream namespace",
        ));
    }
    Ok(())
}

fn validate_key_shape(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > MAX_KEY_LEN
        || key.bytes().any(|b| b == 0 || b.is_ascii_control())
    {
        return Err(StorageError::invalid_input("invalid blob key"));
    }
    Ok(())
}

pub(crate) fn validate_prefix(prefix: &str, after: Option<&str>) -> Result<()> {
    if prefix.len() > MAX_KEY_LEN
        || prefix.bytes().any(|b| b == 0 || b.is_ascii_control())
        || is_reserved_blob_key(prefix)
    {
        return Err(StorageError::invalid_input("invalid blob prefix"));
    }
    if let Some(after) = after {
        validate_key(after)?;
        if !after.starts_with(prefix) {
            return Err(StorageError::invalid_input(
                "blob cursor is outside the requested prefix",
            ));
        }
    }
    Ok(())
}

pub(crate) fn clamp_range(len: usize, start: u64, end: u64) -> Result<std::ops::Range<usize>> {
    if start > end {
        return Err(StorageError::invalid_input(
            "blob range start is after its end",
        ));
    }
    let start = usize::try_from(start).unwrap_or(usize::MAX).min(len);
    let end = usize::try_from(end)
        .unwrap_or(usize::MAX)
        .min(len)
        .max(start);
    Ok(start..end)
}

#[cfg(feature = "memory")]
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
