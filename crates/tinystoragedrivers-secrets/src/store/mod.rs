//! The [`SecretStore`] port and the name rules every driver enforces.

#[cfg(any(feature = "file", feature = "keyring"))]
use tinystoragedrivers_types::{Result, StorageError};

pub use tinystoragedrivers_api::SecretStore;

/// Require `value` to be UTF-8, for the drivers whose formats hold text.
#[cfg(any(feature = "file", feature = "keyring"))]
pub(crate) fn require_utf8<'a>(value: &'a [u8], driver: &str) -> Result<&'a str> {
    std::str::from_utf8(value).map_err(|_| {
        StorageError::invalid_input(format!("the {driver} secret store holds utf-8 text only"))
    })
}

#[cfg(all(test, any(feature = "file", feature = "keyring")))]
#[path = "mod_tests.rs"]
mod tests;
