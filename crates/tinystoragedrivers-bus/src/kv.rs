//! Synchronous wire envelopes for native key/value operations.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The native key/value object name.
pub const KV_OBJECT: &str = "Kv";
/// Finite selector set for the key/value object.
pub const KV_SELECTORS: [&str; 7] = [
    "Get",
    "Put",
    "Cas",
    "Delete",
    "List",
    "PutWithTtl",
    "Capabilities",
];

/// Reply to `Capabilities` for the KV port.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvCapabilitiesReply {
    /// Optional KV capability names.
    pub capabilities: Vec<String>,
}

/// Whether `selector` belongs to the fixed KV selector set.
#[must_use]
pub fn is_kv_selector(selector: &str) -> bool {
    KV_SELECTORS.contains(&selector)
}

/// One key address.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvKeyRequest {
    /// Native key, scoped by the host binding.
    pub key: String,
}

/// Unconditional key write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvPutRequest {
    /// Native key, scoped by the host binding.
    pub key: String,
    /// JSON value.
    pub value: Value,
}

/// Conditional key write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvCasRequest {
    /// Native key, scoped by the host binding.
    pub key: String,
    /// JSON value.
    pub value: Value,
    /// Atomic write condition.
    pub expected: tinystoragedrivers_types::Precondition,
}

/// Conditional key deletion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvDeleteRequest {
    /// Native key, scoped by the host binding.
    pub key: String,
    /// Atomic delete condition.
    pub expected: tinystoragedrivers_types::Precondition,
}

/// Paginated prefix read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvListRequest {
    /// Literal key prefix.
    pub prefix: String,
    /// Opaque continuation cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<tinystoragedrivers_types::Cursor>,
    /// Maximum page size.
    pub limit: usize,
}

/// TTL-backed key write.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvPutWithTtlRequest {
    /// Native key, scoped by the host binding.
    pub key: String,
    /// JSON value.
    pub value: Value,
    /// Lifetime in milliseconds.
    pub ttl_ms: u64,
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;
