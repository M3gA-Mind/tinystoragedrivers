//! Wire data and native compare-and-swap conditions.
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use tinystoragedrivers_types::{Meta, Precondition, Version, Versioned};
/// Read in the captured collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    /// Document identifier, never an authority selector.
    #[serde(rename = "document_id")]
    pub id: String,
}
/// Conditional native write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutRequest {
    /// Document identifier.
    #[serde(rename = "document_id")]
    pub id: String,
    /// Inline JSON object, encoded as JSON text on the wire.
    #[serde(rename = "document_json", with = "crate::inline_json")]
    pub doc: Value,
    /// Atomic write condition.
    pub precondition: Precondition,
}
/// Conditional native deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteRequest {
    /// Document identifier.
    #[serde(rename = "document_id")]
    pub id: String,
    /// Atomic deletion condition.
    pub precondition: Precondition,
}
/// Sanitized native categories; backend prose and source chains stay local.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    /// Required resource absent.
    NotFound,
    /// Atomic condition failed.
    Conflict,
    /// Unique resource exists.
    AlreadyExists,
    /// Native operation unsupported.
    Unsupported,
    /// Native input rejected.
    InvalidInput,
    /// Serialization failed.
    Serialization,
    /// Cryptography failed.
    Crypto,
    /// Service unavailable.
    Unavailable,
    /// Driver failed.
    Backend,
    /// Native lease guard no longer holds.
    Fenced,
}
/// Content-free failure; no native message or source crosses the ABI.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageFailure {
    /// Actionable error category.
    pub kind: StorageKind,
}
impl std::fmt::Display for StorageFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("document storage operation failed")
    }
}
impl std::error::Error for StorageFailure {}
#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
