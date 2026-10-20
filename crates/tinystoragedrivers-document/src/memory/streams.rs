//! [`StreamStore`] for the in-memory driver.

use std::sync::Arc;

use serde_json::Value;

use super::MemoryDb;
use super::state::DbState;
use crate::stream::SealedLog;
use crate::{sealed_blob_key, validate_stream};
use tinystoragedrivers_api::{StreamStore, async_trait};
use tinystoragedrivers_types::{
    BlobMeta, Capabilities, Capability, Result, Scope, SealOutcome, StorageError, StreamEntry,
    StreamInfo,
};

/// In-memory streams bound to one scope.
#[derive(Clone)]
pub struct MemoryStreams {
    db: Arc<MemoryDb>,
    scope: Scope,
    clock: crate::Clock,
}

impl std::fmt::Debug for MemoryStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStreams")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl MemoryStreams {
    pub(super) fn new(db: Arc<MemoryDb>, scope: Scope, clock: crate::Clock) -> Self {
        Self { db, scope, clock }
    }

    fn now_ms(&self) -> i64 {
        i64::try_from((self.clock)()).unwrap_or(i64::MAX)
    }

    fn key(&self, stream: &str) -> (String, String) {
        (self.scope.as_str().to_owned(), stream.to_owned())
    }
}

/// The steps of sealing, kept separate so the in-memory path follows the
/// same write-then-mark protocol as persistent engines.
impl MemoryStreams {
    fn blob_key(&self, stream: &str) -> Result<(String, String)> {
        Ok(self.key(&sealed_blob_key(stream)?))
    }

    fn write_blob_in(&self, state: &mut DbState, stream: &str) -> Result<SealOutcome> {
        let blob_key = self.blob_key(stream)?;
        let stored = state
            .streams
            .get(&self.key(stream))
            .ok_or_else(|| StorageError::not_found("stream does not exist"))?;
        if stored.sealed_len.is_some() {
            return Err(StorageError::conflict("stream is already sealed"));
        }
        let log = SealedLog::new(stored.base, stored.entries.iter().cloned().collect())?;
        let bytes = log.encode()?;
        let outcome = SealOutcome::new(log.base(), log.len(), bytes.len() as u64);
        let meta = BlobMeta {
            key: blob_key.1.clone(),
            len: bytes.len() as u64,
            content_type: Some("application/zstd".to_owned()),
        };
        state.blobs.insert(blob_key, (meta, bytes, None));
        Ok(outcome)
    }

    fn mark_in(&self, state: &mut DbState, stream: &str, written: SealOutcome) -> Result<()> {
        let blob_key = self.blob_key(stream)?;
        let now_ms = self.now_ms();
        let blob_len = state
            .blobs
            .get(&blob_key)
            .map(|(_, bytes, _)| bytes.len() as u64);
        let stored = state
            .streams
            .get_mut(&self.key(stream))
            .ok_or_else(|| StorageError::conflict("stream was deleted while sealing"))?;
        if stored.base != written.base || stored.len() != written.len {
            return Err(StorageError::conflict("stream changed while sealing"));
        }
        if blob_len != Some(written.blob_bytes) {
            return Err(StorageError::conflict("sealed blob changed while sealing"));
        }
        stored.sealed_len = Some(written.len);
        stored.sealed_ms = Some(now_ms);
        Ok(())
    }

    fn drop_entries_in(&self, state: &mut DbState, stream: &str) {
        if let Some(stored) = state.streams.get_mut(&self.key(stream))
            && stored.sealed_len.is_some()
        {
            stored.entries.clear();
        }
    }

    /// The outcome of an already sealed stream, from its head and blob.
    fn sealed_outcome(&self, state: &DbState, stream: &str) -> Result<Option<SealOutcome>> {
        let Some(stored) = state.streams.get(&self.key(stream)) else {
            return Ok(None);
        };
        let Some(len) = stored.sealed_len else {
            return Ok(None);
        };
        let (_, bytes, _) = state
            .blobs
            .get(&self.blob_key(stream)?)
            .ok_or_else(missing_blob)?;
        Ok(Some(SealOutcome::new(stored.base, len, bytes.len() as u64)))
    }

    /// The entries of a sealed stream, decoded from its blob.
    fn sealed_log(&self, state: &DbState, stream: &str) -> Result<SealedLog> {
        let (_, bytes, _) = state
            .blobs
            .get(&self.blob_key(stream)?)
            .ok_or_else(missing_blob)?;
        SealedLog::decode(bytes)
    }
}

fn missing_blob() -> StorageError {
    StorageError::backend("sealed stream has no blob")
}

impl MemoryStreams {
    fn append_inner(&self, stream: &str, expected: Option<u64>, values: Vec<Value>) -> Result<u64> {
        validate_stream(stream)?;
        let mut state = self.db.lock()?;
        if state
            .streams
            .get(&self.key(stream))
            .is_some_and(|stored| stored.sealed_len.is_some())
        {
            return Err(StorageError::conflict("stream is sealed"));
        }
        let first = state
            .streams
            .get(&self.key(stream))
            .map_or(0, super::state::StoredStream::len);
        if expected.is_some_and(|offset| offset != first) {
            return Err(StorageError::conflict("stream next offset changed"));
        }
        first
            .checked_add(values.len() as u64)
            .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
        if values.is_empty() {
            return Ok(state
                .streams
                .get(&self.key(stream))
                .map_or(0, super::state::StoredStream::len));
        }
        let now_ms = self.now_ms();
        let entry = state.streams.entry(self.key(stream)).or_default();
        let first = entry.len();
        entry.entries.extend(values);
        entry.last_append_ms = Some(now_ms);
        Ok(first)
    }
}

#[async_trait]
impl StreamStore for MemoryStreams {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Streams)
            .with(Capability::ConditionalStreamAppend)
            .with(Capability::StreamSeal)
    }

    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.append_inner(stream, None, values)
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.append_inner(stream, Some(expected_next_offset), values)
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let state = self.db.lock()?;
        let Some(entry) = state.streams.get(&self.key(stream)) else {
            return Ok(Vec::new());
        };
        if entry.sealed_len.is_some() {
            return Ok(self.sealed_log(&state, stream)?.window(from, limit));
        }
        let start = from.max(entry.base);
        Ok((start..entry.len())
            .take(limit)
            .filter_map(|offset| {
                let index = usize::try_from(offset - entry.base).ok()?;
                entry.entries.get(index).map(|value| StreamEntry {
                    offset,
                    value: value.clone(),
                })
            })
            .collect())
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        let state = self.db.lock()?;
        Ok(state
            .streams
            .get(&self.key(stream))
            .map_or(0, super::state::StoredStream::len))
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let mut state = self.db.lock()?;
        let Some(entry) = state.streams.get_mut(&self.key(stream)) else {
            return Ok(0);
        };
        if entry.sealed_len.is_some() {
            return Err(StorageError::conflict("stream is sealed"));
        }
        let cut = offset.clamp(entry.base, entry.len());
        let removed = cut - entry.base;
        for _ in 0..removed {
            entry.entries.pop_front();
        }
        entry.base = cut;
        Ok(removed)
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let mut state = self.db.lock()?;
        // The blob goes after the stream, so a crash never leaves a sealed
        // stream without its entries. It goes even when the stream is not
        // there, to clear one a crashed seal left.
        let existed = state.streams.remove(&self.key(stream)).is_some();
        state.blobs.remove(&self.blob_key(stream)?);
        Ok(existed)
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        validate_stream(stream)?;
        // The check and the delete share one hold of the lock.
        let mut state = self.db.lock()?;
        let key = self.key(stream);
        if state.streams.get(&key).and_then(|stored| stored.sealed_ms) != Some(sealed_ms) {
            return Ok(false);
        }
        state.streams.remove(&key);
        state.blobs.remove(&self.blob_key(stream)?);
        Ok(true)
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        validate_stream(stream)?;
        // The three steps run under one hold of the lock, so no append can land
        // between the snapshot and the mark: it is either in the blob or refused.
        let mut state = self.db.lock()?;
        if let Some(outcome) = self.sealed_outcome(&state, stream)? {
            self.drop_entries_in(&mut state, stream);
            return Ok(outcome);
        }
        let written = self.write_blob_in(&mut state, stream)?;
        self.mark_in(&mut state, stream, written)?;
        self.drop_entries_in(&mut state, stream);
        Ok(written)
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        validate_stream(stream)?;
        let state = self.db.lock()?;
        Ok(state.streams.get(&self.key(stream)).map(|stored| {
            StreamInfo::new(
                stored.base,
                stored.len(),
                stored.sealed_ms,
                stored.last_append_ms,
            )
        }))
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let state = self.db.lock()?;
        Ok(state
            .streams
            .keys()
            .filter(|(scope, name)| scope == self.scope.as_str() && name.starts_with(prefix))
            .map(|(_, name)| name.clone())
            .collect())
    }
}
