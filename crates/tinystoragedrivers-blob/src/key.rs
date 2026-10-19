//! Shared validation and ordering rules for blob engines.

use tinystoragedrivers_types::{Result, StorageError};

pub(crate) const MAX_KEY_LEN: usize = 1024;

pub(crate) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > MAX_KEY_LEN
        || key.bytes().any(|b| b == 0 || b.is_ascii_control())
    {
        return Err(StorageError::invalid_input("invalid blob key"));
    }
    Ok(())
}

pub(crate) fn validate_prefix(prefix: &str, after: Option<&str>) -> Result<()> {
    if prefix.len() > MAX_KEY_LEN || prefix.bytes().any(|b| b == 0 || b.is_ascii_control()) {
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

pub(crate) fn clamp_range(len: usize, start: u64, end: u64) -> std::ops::Range<usize> {
    let start = usize::try_from(start).unwrap_or(usize::MAX).min(len);
    let end = usize::try_from(end)
        .unwrap_or(usize::MAX)
        .min(len)
        .max(start);
    start..end
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
