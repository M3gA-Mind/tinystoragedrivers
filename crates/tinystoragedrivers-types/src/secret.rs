//! Secret name rules shared by every secret-store implementation.

use crate::{Result, StorageError};
use std::fmt;

/// The longest secret name or prefix, in bytes.
pub const MAX_SECRET_NAME_LEN: usize = 256;

/// An opaque revision token for one stored secret. Debug output never exposes
/// its bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct SecretVersion([u8; 32]);

impl SecretVersion {
    /// Rebuild a token from its opaque wire representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the opaque wire representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Consume the token into its opaque wire representation.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for SecretVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretVersion(..)")
    }
}

/// Condition required by a conditional secret write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretPrecondition {
    /// The secret must not exist.
    Absent,
    /// The secret must still have the revision returned by a versioned read.
    Version(SecretVersion),
}

/// Validate a secret name: 1 to [`MAX_SECRET_NAME_LEN`] bytes, with no control
/// characters. The message never repeats the name, keeping misplaced secret
/// material out of diagnostics.
///
/// # Errors
/// Returns [`StorageError::invalid_input`] when the name is invalid.
pub fn validate_secret_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_SECRET_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "secret name must be 1 to {MAX_SECRET_NAME_LEN} bytes"
        )));
    }
    validate_secret_text(name)
}

/// Validate a secret-list prefix: empty, or a prefix of some valid name.
///
/// # Errors
/// Returns [`StorageError::invalid_input`] when the prefix is invalid.
pub fn validate_secret_prefix(prefix: &str) -> Result<()> {
    if prefix.len() > MAX_SECRET_NAME_LEN {
        return Err(StorageError::invalid_input(format!(
            "secret name prefix must be at most {MAX_SECRET_NAME_LEN} bytes"
        )));
    }
    validate_secret_text(prefix)
}

fn validate_secret_text(text: &str) -> Result<()> {
    if text.chars().any(char::is_control) {
        return Err(StorageError::invalid_input(
            "secret names must not contain control characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "secret_tests.rs"]
mod tests;
