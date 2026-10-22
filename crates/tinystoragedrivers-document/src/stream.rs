//! Deterministic sealed-stream encoding shared by primitive engines.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_types::{Result, StorageError, StreamEntry};

const VERSION: u32 = 1;
const LEVEL: i32 = 3;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SealedLog {
    version: u32,
    base: u64,
    values: Vec<Value>,
}

impl SealedLog {
    pub(crate) fn new(base: u64, values: Vec<Value>) -> Result<Self> {
        base.checked_add(values.len() as u64)
            .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
        Ok(Self {
            version: VERSION,
            base,
            values,
        })
    }
    pub(crate) fn base(&self) -> u64 {
        self.base
    }
    pub(crate) fn len(&self) -> u64 {
        self.base + self.values.len() as u64
    }
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(self)
            .map_err(|error| StorageError::backend("serialize sealed stream").with_source(error))?;
        zstd::bulk::compress(&json, LEVEL)
            .map_err(|error| StorageError::backend("compress sealed stream").with_source(error))
    }
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let json = zstd::bulk::decompress(bytes, 64 * 1024 * 1024).map_err(|error| {
            StorageError::backend("decompress sealed stream").with_source(error)
        })?;
        let log: Self = serde_json::from_slice(&json)
            .map_err(|error| StorageError::backend("decode sealed stream").with_source(error))?;
        if log.version != VERSION || log.base.checked_add(log.values.len() as u64).is_none() {
            return Err(StorageError::backend("sealed stream has an invalid format"));
        }
        Ok(log)
    }
    pub(crate) fn window(&self, from: u64, limit: usize) -> Vec<StreamEntry> {
        let start = from.max(self.base);
        if start >= self.len() || limit == 0 {
            return Vec::new();
        }
        let offset = usize::try_from(start - self.base).unwrap_or(usize::MAX);
        self.values
            .iter()
            .skip(offset)
            .take(limit)
            .enumerate()
            .map(|(i, value)| StreamEntry {
                offset: start + i as u64,
                value: value.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
