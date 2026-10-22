//! Wire shapes for the Secrets object.
//!
//! Confidential: servers admit only attested in-process callers, so every call
//! uses the confidential route. Values and names never appear in `Debug`
//! output, logs or errors; hosts and clients zeroize buffers after use.
use serde::{Deserialize, Serialize};
pub use tinystoragedrivers_types::SecretPrecondition;

/// Secrets object name under [`crate::STORAGE_INTERFACE`].
pub const SECRETS_OBJECT: &str = "Secrets";
/// Finite selector set; servers must reject every other selector.
pub const SECRETS_SELECTORS: [&str; 7] = [
    "Capabilities",
    "Get",
    "GetVersioned",
    "Set",
    "SetIf",
    "Delete",
    "List",
];

/// Return whether `selector` belongs to the fixed Secrets selector set.
#[must_use]
pub fn is_secrets_selector(selector: &str) -> bool {
    SECRETS_SELECTORS.contains(&selector)
}

/// Reply to confidential `Capabilities`; non-confidential callers are denied
/// before the host inspects the bound secret store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretsCapabilitiesReply {
    /// Whether the bound store can enumerate secret names.
    pub enumerable: bool,
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
/// Versioned read reply; the revision token is opaque to callers.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionedSecretValue {
    /// Secret bytes.
    pub value: Vec<u8>,
    /// Opaque revision token.
    pub version: tinystoragedrivers_types::SecretVersion,
}
/// Conditional write request.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetSecretIfRequest {
    /// Secret name.
    pub name: String,
    /// Secret bytes.
    pub value: Vec<u8>,
    /// Required prior state.
    pub condition: SecretPrecondition,
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
    VersionedSecretValue,
    SetSecretIfRequest,
    ListSecretsRequest
);

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
