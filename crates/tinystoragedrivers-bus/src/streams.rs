//! Wire shapes for the Streams object (append-only JSON logs).
//!
//! Records are inline JSON text so arbitrary business JSON is never mistaken
//! for a native stream reference. Scope and stream grants are host-captured.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Streams object name under [`crate::STORAGE_INTERFACE`].
pub const STREAMS_OBJECT: &str = "Streams";
/// Finite selector set; servers must reject every other selector.
pub const STREAMS_SELECTORS: [&str; 10] = [
    "Append",
    "AppendBatch",
    "AppendBatchIfOffset",
    "ReadWindow",
    "Len",
    "TruncateBefore",
    "DeleteStream",
    "Streams",
    "Seal",
    "Info",
];

/// Return whether `selector` belongs to the fixed Streams selector set.
#[must_use]
pub fn is_streams_selector(selector: &str) -> bool {
    STREAMS_SELECTORS.contains(&selector)
}

/// One inline JSON record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamRecord(#[serde(with = "crate::inline_json")] pub Value);

/// Append one record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendRequest {
    /// Stream name inside the captured grant.
    pub stream: String,
    /// Record to append.
    #[serde(rename = "record_json", with = "crate::inline_json")]
    pub record: Value,
}
/// Append many records atomically; at most `MAX_BATCH_OPS`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendBatchRequest {
    /// Stream name inside the captured grant.
    pub stream: String,
    /// Records in append order.
    pub records: Vec<StreamRecord>,
}
/// Append only when the persistent next offset matches.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendBatchIfOffsetRequest {
    /// Stream name inside the captured grant.
    pub stream: String,
    /// Required persistent next offset.
    pub expected_next_offset: u64,
    /// Records in append order.
    pub records: Vec<StreamRecord>,
}
/// Offset returned by appends: the offset of the first appended record, or the
/// current next offset for an empty batch.
pub type AppendResponse = u64;
/// Read a window of records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadWindowRequest {
    /// Stream name inside the captured grant.
    pub stream: String,
    /// First offset to return.
    pub from: u64,
    /// Maximum records; servers clamp to `MAX_PAGE_ITEMS` and the byte cap.
    pub limit: usize,
}
/// One record and its offset.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamItem {
    /// Dense offset; never reused after truncation.
    pub offset: u64,
    /// Record.
    #[serde(rename = "record_json", with = "crate::inline_json")]
    pub record: Value,
}
/// Window response in offset order. A short window is not end-of-stream
/// evidence when the server clamped it; callers continue from the last offset+1.
pub type ReadWindowResponse = Vec<StreamItem>;
/// Request addressing one stream (`Len`, `DeleteStream`, `Seal`, `Info`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamRequest {
    /// Stream name inside the captured grant.
    pub stream: String,
}
/// Reply to `Seal`: where the sealed stream's entries lie. Mirrors the native
/// `SealOutcome`; sealing twice returns equal replies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealResponse {
    /// First retained offset; everything below it was truncated before the seal.
    pub base: u64,
    /// The stream's length: the offset an append would have received.
    pub len: u64,
    /// Size of the compressed blob holding the entries, in bytes.
    pub blob_bytes: u64,
}
/// Reply to `Info`: the stream's extent and recorded times, or JSON `null` for
/// a stream that does not exist. Mirrors the native `StreamInfo`. Times are
/// epoch milliseconds from the host's clock; `null` means unknown or, for
/// `sealed_ms`, not sealed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamInfoResponse {
    /// First retained offset.
    pub base: u64,
    /// The stream's length: the offset an append would receive.
    pub len: u64,
    /// When the stream was sealed.
    pub sealed_ms: Option<i64>,
    /// When a record was last appended.
    pub last_append_ms: Option<i64>,
}
/// Drop records below an offset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TruncateBeforeRequest {
    /// Stream name inside the captured grant.
    pub stream: String,
    /// First offset to keep.
    pub offset: u64,
}
/// List stream names.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListStreamsRequest {
    /// Literal name prefix.
    #[serde(default)]
    pub prefix: String,
}

#[cfg(test)]
#[path = "streams_tests.rs"]
mod tests;
