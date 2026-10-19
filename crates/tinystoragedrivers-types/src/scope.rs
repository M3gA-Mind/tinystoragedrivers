//! Validated opaque scope identifiers.

use crate::{Result, StorageError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum serialized scope length in UTF-8 bytes.
pub const MAX_SCOPE_LEN: usize = 256;

/// An opaque namespace boundary. Accepted legacy strings are preserved exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Scope(String);
impl Scope {
    /// Validates a pre-existing scope string without changing its bytes.
    ///
    /// # Errors
    /// Returns `InvalidInput` when the value is empty, oversized, or contains
    /// whitespace or control characters.
    pub fn new(root: &str) -> Result<Self> {
        if root.is_empty()
            || root.len() > MAX_SCOPE_LEN
            || root.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(StorageError::invalid_input(
                "scope must be 1 to 256 bytes without whitespace or control characters",
            ));
        }
        Ok(Self(root.to_owned()))
    }
    /// Returns the exact serialized scope string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Appends a generic child segment using uppercase percent escapes.
    ///
    /// # Errors
    /// Returns `InvalidInput` for an empty segment or a result over 256 bytes.
    pub fn child(&self, segment: &str) -> Result<Self> {
        if segment.is_empty() {
            return Err(StorageError::invalid_input(
                "scope child segment must not be empty",
            ));
        }
        let mut encoded = String::with_capacity(segment.len());
        for byte in segment.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                encoded.push(char::from(byte));
            } else {
                encoded.push('%');
                encoded.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
                encoded.push(char::from(b"0123456789ABCDEF"[(byte & 0x0f) as usize]));
            }
        }
        let joined = format!("{}/{encoded}", self.0);
        if joined.len() > MAX_SCOPE_LEN {
            return Err(StorageError::invalid_input(
                "scope child exceeds the 256 byte limit",
            ));
        }
        Self::new(&joined)
    }
}
impl Serialize for Scope {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::new(&value).map_err(serde::de::Error::custom)
    }
}
