//! Opaque validated storage keys.

use crate::{Result, StorageError};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum accepted key length in bytes.
pub const MAX_KEY_LEN: usize = 512;

/// An opaque key with data-only parsing helpers.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(String);
impl Key {
    /// Validates an already-encoded key.
    ///
    /// # Errors
    /// Returns `InvalidInput` for an empty key, a key longer than 512 bytes,
    /// or a key containing NUL.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_KEY_LEN || value.contains('\0') {
            return Err(StorageError::invalid_input(
                "key must be 1 to 512 bytes without NUL",
            ));
        }
        Ok(Self(value))
    }
    /// Returns the key string exactly as stored.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    /// Splits a composite key and decodes its supported percent escapes.
    #[must_use]
    pub fn split(&self) -> Option<Vec<String>> {
        if self.is_hashed() {
            return None;
        }
        self.0.split('/').map(unescape).collect()
    }
    /// Indicates whether the value uses the reserved digest marker.
    #[must_use]
    pub fn is_hashed(&self) -> bool {
        self.0.starts_with('~')
    }
}
fn unescape(input: &str) -> Option<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(at) = rest.find('%') {
        out.push_str(&rest[..at]);
        let tail = &rest[at..];
        let (ch, n) = [("%25", '%'), ("%2F", '/'), ("%00", '\0'), ("%7E", '~')]
            .into_iter()
            .find_map(|(seq, ch)| tail.starts_with(seq).then_some((ch, seq.len())))?;
        out.push(ch);
        rest = &tail[n..];
    }
    out.push_str(rest);
    Some(out)
}
impl Serialize for Key {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}
impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(d)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[path = "key_tests.rs"]
mod tests;
