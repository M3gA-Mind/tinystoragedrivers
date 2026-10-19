//! Append-only streams with dense offsets.

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_types::{
    Capabilities, Capability, Result, SealOutcome, StorageError, StreamEntry, StreamInfo,
};

/// Append-only JSON values bound to one scope.
#[async_trait]
pub trait StreamStore: Send + Sync + std::fmt::Debug {
    /// Optional stream behaviors provided by this driver.
    fn capabilities(&self) -> Capabilities;

    /// Append one value and return the next offset.
    ///
    /// # Errors
    /// Returns an error for an invalid stream or backend failure.
    async fn append(&self, stream: &str, value: Value) -> Result<u64>;

    /// Append a batch and return the next offset.
    ///
    /// # Errors
    /// Returns an error for invalid values or a backend failure.
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64>;

    /// Append if the stream still has the expected next offset.
    ///
    /// # Errors
    /// Returns `Unsupported(ConditionalStreamAppend)` by default, a conflict
    /// when the offset changed, or a backend failure.
    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        let _ = (stream, expected_next_offset, values);
        Err(StorageError::unsupported(
            Capability::ConditionalStreamAppend,
            "conditional stream append is unsupported",
        ))
    }

    /// Read a window beginning at an absolute offset.
    ///
    /// # Errors
    /// Returns an error for an invalid stream or backend failure.
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>>;

    /// Return the next append offset.
    ///
    /// # Errors
    /// Returns an error for an invalid stream or backend failure.
    async fn len(&self, stream: &str) -> Result<u64>;

    /// Remove entries before an offset without reusing their offsets.
    ///
    /// # Errors
    /// Returns an error for an invalid stream or backend failure.
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64>;

    /// Delete a stream, resetting its offsets if it is later recreated.
    ///
    /// # Errors
    /// Returns an error for an invalid stream or backend failure.
    async fn delete_stream(&self, stream: &str) -> Result<bool>;

    /// List stream names beginning with a prefix.
    ///
    /// # Errors
    /// Returns an error for an invalid prefix or backend failure.
    async fn streams(&self, prefix: &str) -> Result<Vec<String>>;

    /// Seal a stream when `StreamSeal` is supported.
    ///
    /// # Errors
    /// Returns `Unsupported(StreamSeal)` by default or a backend failure.
    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        let _ = stream;
        Err(StorageError::unsupported(
            Capability::StreamSeal,
            "stream sealing is unsupported",
        ))
    }

    /// Delete a sealed stream only if its seal time matches.
    ///
    /// # Errors
    /// Returns `Unsupported(StreamSeal)` by default or a backend failure.
    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        let _ = (stream, sealed_ms);
        Err(StorageError::unsupported(
            Capability::StreamSeal,
            "stream sealing is unsupported",
        ))
    }

    /// Read retained offsets and seal/growth timestamps.
    ///
    /// # Errors
    /// Returns `Unsupported(StreamSeal)` by default or a backend failure.
    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        let _ = stream;
        Err(StorageError::unsupported(
            Capability::StreamSeal,
            "stream metadata is unsupported",
        ))
    }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
