//! Wire data and native compare-and-swap conditions.
use serde::{Deserialize, Serialize};
use serde_json::Value;
/// Native document version, represented losslessly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Version(pub u64);
/// Condition checked atomically by the native store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "version",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Precondition {
    /// Unconditional mutation.
    #[default]
    None,
    /// Document must not exist.
    Absent,
    /// Document must have this exact version.
    Version(Version),
}
/// What the driver recorded about a document, outside its body.
///
/// Mirrors the native `Meta`: the same four fields, the same JSON. A host
/// sends it with every document it returns; times are driver-clock Unix epoch
/// milliseconds and are never accepted from a module.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Meta {
    /// Body version the writer declared; `0` means undeclared.
    #[serde(default)]
    pub schema: u32,
    /// First write, Unix epoch milliseconds.
    #[serde(default)]
    pub created_ms: i64,
    /// Latest write, Unix epoch milliseconds.
    #[serde(default)]
    pub updated_ms: i64,
    /// Instant from which the document reads as absent, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
}
/// Document data and version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Versioned {
    /// Document identifier.
    #[serde(rename = "document_id")]
    pub id: String,
    /// Native version.
    pub version: Version,
    /// Inline JSON document, encoded as JSON text on the wire.
    #[serde(rename = "document_json", with = "crate::inline_json")]
    pub doc: Value,
    /// Record metadata. Absent from a host that predates contract version 4
    /// and from the legacy `DocumentCollection1` callback, which never sends
    /// it; readers treat absence as unknown metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}
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
    /// Scope generation retired or unissued, or scope reset pending.
    ScopeGenerationRejected,
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
