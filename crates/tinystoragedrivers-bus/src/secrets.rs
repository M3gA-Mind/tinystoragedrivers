//! Wire shapes for the Secrets object.
//!
//! Confidential: servers admit only attested in-process callers, so every call
//! uses the confidential route. Values and names never appear in `Debug`
//! output, logs or errors; hosts and clients zeroize buffers after use.
use serde::{Deserialize, Serialize};

/// Secrets object name under [`crate::STORAGE_INTERFACE`].
pub const SECRETS_OBJECT: &str = "Secrets";
/// Finite selector set; servers must reject every other selector.
pub const SECRETS_SELECTORS: [&str; 4] = ["Get", "Set", "Delete", "List"];

/// Return whether `selector` belongs to the fixed Secrets selector set.
#[must_use]
pub fn is_secrets_selector(selector: &str) -> bool {
    SECRETS_SELECTORS.contains(&selector)
}

/// Request addressing one secret (`Get`, `Delete`).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretNameRequest {
    /// Name inside the captured grant.
    pub name: String,
}
/// Store a secret value.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetSecretRequest {
    /// Name inside the captured grant.
    pub name: String,
    /// Secret bytes.
    pub value: Vec<u8>,
}
/// Secret value reply; `None` when absent.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretValue {
    /// Secret bytes.
    pub value: Vec<u8>,
}
/// List secret names by prefix.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSecretsRequest {
    /// Literal name prefix.
    #[serde(default)]
    pub prefix: String,
}

macro_rules! redacted_debug {
    ($($ty:ident),*) => {$(
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($ty)).finish_non_exhaustive()
            }
        }
    )*};
}
redacted_debug!(
    SecretNameRequest,
    SetSecretRequest,
    SecretValue,
    ListSecretsRequest
);

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
