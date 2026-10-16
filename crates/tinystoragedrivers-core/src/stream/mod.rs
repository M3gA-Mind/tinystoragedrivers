//! The stream port: append-only logs of JSON records.
//!
//! Transcripts, event journals and audit logs are streams. Every record gets
//! the next offset of its stream, starting at 0, with no gaps, so a reader that
//! remembers "I have seen up to offset 41" resumes exactly where it stopped.
//! Offsets are never reused, even after [`StreamStore::truncate_before`].

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, StorageError};

mod sealed;
mod types;

pub use sealed::{SEALED_BLOB_PREFIX, SealedLog, is_reserved_blob_key, sealed_blob_key};
pub use types::{SealOutcome, SealPolicy, SealWhen, StreamInfo};

/// One record and its offset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEntry {
    /// Position in the stream, starting at 0.
    pub offset: u64,
    /// The record.
    pub value: Value,
}

/// Append-only JSON logs, bound to one scope.
#[async_trait]
pub trait StreamStore: Send + Sync + std::fmt::Debug {
    /// Append one record and return its offset.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn append(&self, stream: &str, value: Value) -> Result<u64>;

    /// Append several records contiguously and return the first one's offset.
    /// An empty batch appends nothing and returns the current length.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error, with nothing appended.
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64>;

    /// Atomically append only when the persistent next offset equals
    /// `expected_next_offset`. Returns that offset, including for an empty
    /// batch. Truncation does not reset the comparison offset.
    ///
    /// # Errors
    ///
    /// `Conflict` when the offset differs (nothing appended), `Fenced` for a
    /// stale fence, or `Unsupported(ConditionalStreamAppend)` when absent.
    /// Validation and aborted writes append nothing.
    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        let _ = (stream, expected_next_offset, values);
        Err(StorageError::unsupported(
            crate::Capability::ConditionalStreamAppend,
            "this driver cannot conditionally append a stream",
        ))
    }

    /// Up to `limit` records starting at offset `from`, in offset order.
    /// Offsets below the truncation point are skipped.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>>;

    /// The offset the next append will receive: the number of records ever
    /// appended. A stream that does not exist has length 0.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn len(&self, stream: &str) -> Result<u64>;

    /// Discard every record below `offset` and report how many went. The
    /// stream's length is unchanged.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64>;

    /// Remove the stream entirely, so its length returns to 0. Reports
    /// whether it existed.
    ///
    /// # Errors
    ///
    /// An invalid stream name, or a backend error.
    async fn delete_stream(&self, stream: &str) -> Result<bool>;

    /// Close the stream to appends and keep its entries as one compressed
    /// blob (see [`SealedLog`]), so a finished journal costs one row instead
    /// of one per entry.
    ///
    /// Sealing changes nothing a reader can see. `read_window`, `len` and
    /// `streams` return exactly the offsets, values and lengths they returned
    /// before, whatever the driver does underneath.
    ///
    /// - The blob lives in the same scope's blob store under
    ///   [`sealed_blob_key`]. A driver writes the blob first, then marks the
    ///   stream sealed, then deletes the per-entry storage, so a crash at any
    ///   step leaves a readable stream and a repeated `seal` finishes the job.
    /// - Sealing is idempotent: sealing a sealed stream changes nothing and
    ///   returns an equal [`SealOutcome`].
    /// - A stream that exists but retains nothing (everything truncated)
    ///   seals with an empty blob and `len == base`.
    /// - Every append, including an empty batch and a conditional append, is
    ///   `Conflict` on a sealed stream, and so is `truncate_before`.
    ///   [`delete_stream`](Self::delete_stream) removes the stream and its
    ///   blob, after which the name is free to start a new, unsealed stream.
    ///
    /// Which streams to seal, and when, is the owner's decision; see
    /// [`SealPolicy`], which maintenance applies.
    ///
    /// # Errors
    ///
    /// `NotFound` when the stream does not exist (a name nothing was ever
    /// appended to), `Conflict` when the stream changed while it was being
    /// sealed (retry), `Fenced` for a stale fence, an invalid stream name,
    /// `Unsupported(StreamSeal)` when the driver cannot seal (the default), or
    /// a backend error.
    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        let _ = stream;
        Err(StorageError::unsupported(
            crate::Capability::StreamSeal,
            "this driver cannot seal a stream",
        ))
    }

    /// Remove `stream` only if it is sealed and was sealed at exactly
    /// `sealed_ms` (the value [`info`](Self::info) reported), atomically with
    /// that check, and report whether it did. Retention uses this so that a
    /// stream deleted and recreated after it was inspected is never removed
    /// in the old one's place.
    ///
    /// Returns `false`, and changes nothing, when the stream is missing, is
    /// not sealed, or was sealed at another time. A recreated stream that is
    /// sealed again within the same millisecond is indistinguishable; the
    /// instant is the incarnation's identity.
    ///
    /// # Errors
    ///
    /// An invalid stream name, `Fenced` for a stale fence,
    /// `Unsupported(StreamSeal)` when the driver cannot seal (the default), or
    /// a backend error.
    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        let _ = (stream, sealed_ms);
        Err(StorageError::unsupported(
            crate::Capability::StreamSeal,
            "this driver cannot conditionally delete a sealed stream",
        ))
    }

    /// The extent and recorded times of `stream`, or `None` when it does not
    /// exist. A sealed stream still reports (its head outlives the seal) with
    /// `sealed_ms` set; `len` and `base` equal what [`len`](Self::len) and
    /// the seal outcome report.
    ///
    /// Reading time is part of the [`Capability::StreamSeal`](crate::Capability::StreamSeal)
    /// capability, not a capability of its own: a driver that can seal records
    /// when it sealed and when a stream last grew, which is everything a
    /// [`SealPolicy`] needs.
    ///
    /// # Errors
    ///
    /// An invalid stream name, `Unsupported(StreamSeal)` when the driver
    /// records no times (the default), or a backend error.
    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        let _ = stream;
        Err(StorageError::unsupported(
            crate::Capability::StreamSeal,
            "this driver does not record stream times",
        ))
    }

    /// Names of the streams in this scope that start with `prefix`, sorted.
    ///
    /// # Errors
    ///
    /// A backend error.
    async fn streams(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Validate a stream name: 1 to 512 bytes without NUL.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_stream(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > crate::document::MAX_ID_LEN || name.contains('\0') {
        return Err(StorageError::invalid_input(
            "stream name must be 1 to 512 bytes without NUL",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
