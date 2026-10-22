//! Synchronous serde wire shapes for the Documents object.
//!
//! These types describe data only. A server binds scope, collection
//! authority, schema grants and caller rights out of band.
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Precondition, Versioned};
pub use tinystoragedrivers_types::{
    CollectionSpec, Direction, ExpectedDocument, Filter, IndexSpec, Query, ReadCheck, SearchHit,
    SearchSpec, Sort, WriteOp, WriteResult,
};

/// Root storage interface named by the storage bus contract.
pub const STORAGE_INTERFACE: &str = "ai.tinyhumans.tinystorage.Storage";
/// Documents object name under [`STORAGE_INTERFACE`].
pub const DOCUMENTS_OBJECT: &str = "Documents";
/// Finite selector set; servers must reject every other selector.
pub const DOCUMENTS_SELECTORS: [&str; 17] = [
    "Get",
    "Put",
    "Delete",
    "Query",
    "Count",
    "DeleteWhere",
    "Claim",
    "AtomicBatch",
    "AtomicBatchChecked",
    "Search",
    "EnsureCollection",
    "ValidateCollectionSpec",
    "DropCollection",
    "Collections",
    "WatchOpen",
    "WatchClose",
    "Capabilities",
];

/// Return whether `selector` belongs to the fixed Documents selector set.
#[must_use]
pub fn is_documents_selector(selector: &str) -> bool {
    DOCUMENTS_SELECTORS.contains(&selector)
}

/// Query request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Query definition.
    pub query: Query,
}
/// Query result page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryResponse {
    /// Page records.
    pub items: Vec<Versioned<Value>>,
    /// Next opaque cursor.
    #[serde(default)]
    pub next: Option<String>,
}
/// Read one document from a host-granted collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GetDocumentRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Document id.
    #[serde(rename = "document_id")]
    pub id: String,
}
/// Write one document in a host-granted collection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutDocumentRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Document id.
    #[serde(rename = "document_id")]
    pub id: String,
    /// JSON object encoded as JSON text on this interface.
    #[serde(rename = "document_json", with = "crate::inline_json")]
    pub doc: Value,
    /// Atomic native condition.
    pub precondition: Precondition,
    /// Body version the writer declares; `0` (omitted) means undeclared.
    /// Needs `record_metadata`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub schema: u32,
    /// Unix epoch milliseconds from which the document reads as absent.
    /// Needs `record_metadata`. There is deliberately no `created_ms`: that
    /// is a host-only migration option and a request naming it fails to decode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_ms: Option<i64>,
}
/// Serde `skip_serializing_if` predicate: omit a zero `schema`.
#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde passes a reference"
)]
fn is_zero(value: &u32) -> bool {
    *value == 0
}
/// Delete one document from a host-granted collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteDocumentRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Document id.
    #[serde(rename = "document_id")]
    pub id: String,
    /// Atomic native condition.
    pub precondition: Precondition,
}
/// Count request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CountRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Predicate.
    #[serde(default)]
    pub filter: Filter,
}
/// Delete matching documents request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteWhereRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Predicate.
    pub filter: Filter,
}
/// Atomic claim request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Predicate.
    pub filter: Filter,
    /// Ordered claim preference.
    #[serde(default)]
    pub sort: Vec<Sort>,
    /// RFC 7396 merge patch.
    #[serde(with = "crate::inline_json")]
    pub patch: Value,
}
/// Batch request. The host enforces count and encoded-size limits.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRequest {
    /// Read dependencies; empty for `AtomicBatch`.
    #[serde(default)]
    pub checks: Vec<ReadCheck>,
    /// Ordered writes.
    pub ops: Vec<WriteOp>,
}
/// Batch response in input order.
pub type BatchResponse = Vec<WriteResult>;
/// Full-text search request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Search text.
    pub text: String,
    /// Maximum hits.
    pub limit: usize,
}
/// Host-owned schema declaration request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnsureCollectionRequest {
    /// Collection spec, still checked against grant rights.
    pub spec: CollectionSpec,
}
/// Validate an existing declaration without modifying it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidateCollectionSpecRequest {
    /// Required declaration.
    pub spec: CollectionSpec,
}
/// Collection operation request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionRequest {
    /// Collection name inside the captured grant.
    pub collection: String,
}
/// List collection names from the captured scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionsRequest {
    /// Literal name prefix.
    #[serde(default)]
    pub prefix: String,
}

/// Open a filtered document watch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchOpenRequest {
    /// Collection selected within the captured host grant.
    pub collection: String,
    /// Subscription filter.
    pub filter: Filter,
    /// Resume cursor bound to the same query and captured authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<u8>>,
    /// Maximum quiet wait in milliseconds, between 1 and 60,000 inclusive.
    pub interval_ms: u64,
}
/// Reply to `WatchOpen`: the handle that later `WatchChanged` signals carry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchOpenResponse {
    /// Host-issued watch handle, scoped to this caller binding.
    pub watch_id: String,
}
/// Reply to `Capabilities`: the optional port capabilities the admitted
/// backend provides to this caller, by their stable lowercase names
/// (`full_text`, `transactions`, `watch`, ...). Host-only capabilities such as
/// fencing and scope purge are never listed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesReply {
    /// Capability names.
    pub capabilities: Vec<String>,
}

/// Close a host-owned watch subscription.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchCloseRequest {
    /// Host-issued watch handle, scoped to this caller binding.
    pub watch_id: String,
}
/// Content-free watch notice; callers reread state through Documents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatchNotice {
    /// Relevant state may have changed.
    Invalidated {
        /// Opaque resume cursor bytes, if available.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<Vec<u8>>,
    },
    /// The subscription can no longer preserve its invalidation boundary.
    ResetRequired {
        /// Sanitized reset category only.
        reason: WatchReset,
    },
}
/// Sanitized watch reset category; backend details remain in the host.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchReset {
    /// The source cannot resume from the provided cursor.
    CursorExpired,
    /// The native watch encountered an opaque failure.
    Backend,
}
/// Content-free `TinyBus` signal emitted when a watch is invalidated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchChanged {
    /// Host-issued subscription handle.
    pub watch_id: String,
    /// Invalidation or terminal reset, with no record payload.
    pub notice: WatchNotice,
}

#[cfg(test)]
#[path = "documents_tests.rs"]
mod tests;
