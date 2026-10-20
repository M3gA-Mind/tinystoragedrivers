//! In-memory implementation of the scoped blob port.

use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_types::{
    Blob, BlobListEntry, BlobListPage, BlobMeta, Capabilities, Capability,
    MAX_BLOB_LIST_PAGE_ITEMS, Result, Scope, StorageError, is_reserved_blob_key,
};

use crate::key::{
    MAX_KEY_LEN, clamp_range, now_ms, validate_key, validate_prefix, validate_sealed_key,
};

#[derive(Debug, Clone)]
struct Record {
    bytes: Vec<u8>,
    content_type: Option<String>,
    expires_at_ms: Option<u64>,
    written_at_ms: u64,
}

/// A scope-bound, thread-safe in-memory blob engine.
#[derive(Debug, Clone)]
pub struct MemoryBlobStore {
    scope: Scope,
    records: Arc<Mutex<BTreeMap<(String, String), Record>>>,
    clock: fn() -> u64,
}

impl MemoryBlobStore {
    /// Creates an empty blob engine bound to `scope`.
    #[must_use]
    pub fn new(scope: Scope) -> Self {
        Self::with_clock(scope, now_ms)
    }

    /// Creates a blob engine using an injected Unix-millisecond clock.
    #[must_use]
    pub fn with_clock(scope: Scope, clock: fn() -> u64) -> Self {
        Self {
            scope,
            records: Arc::new(Mutex::new(BTreeMap::new())),
            clock,
        }
    }

    fn live(record: &Record, now: u64) -> bool {
        record.expires_at_ms.is_none_or(|expires| expires > now)
    }

    fn meta(key: &str, record: &Record) -> BlobMeta {
        BlobMeta {
            key: key.to_owned(),
            len: record.bytes.len() as u64,
            content_type: record.content_type.clone(),
        }
    }
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;

#[async_trait]
impl BlobStore for MemoryBlobStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Blobs)
            .with(Capability::BlobTtl)
            .with(Capability::BlobListing)
    }

    fn max_key_len(&self) -> usize {
        MAX_KEY_LEN
    }

    fn supports_sealed_blobs(&self) -> bool {
        true
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, false)
    }

    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl), false)
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        validate_key(key)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .get(&(self.scope.as_str().to_owned(), key.to_owned()))
            .filter(|record| Self::live(record, now))
            .map(|record| Blob {
                meta: Self::meta(key, record),
                bytes: record.bytes.clone(),
            }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;
        clamp_range(0, range.start, range.end)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        let Some(record) = records
            .get(&(self.scope.as_str().to_owned(), key.to_owned()))
            .filter(|record| Self::live(record, now))
        else {
            return Ok(None);
        };
        Ok(Some(
            record.bytes[clamp_range(record.bytes.len(), range.start, range.end)?].to_vec(),
        ))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_key(key)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .get(&(self.scope.as_str().to_owned(), key.to_owned()))
            .filter(|record| Self::live(record, now))
            .map(|record| Self::meta(key, record)))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        validate_key(key)?;
        let now = (self.clock)();
        let mut records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .remove(&(self.scope.as_str().to_owned(), key.to_owned()))
            .is_some_and(|record| Self::live(&record, now)))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_prefix(prefix, None)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .iter()
            .filter(|((scope, key), record)| {
                scope == self.scope.as_str()
                    && key.starts_with(prefix)
                    && !is_reserved_blob_key(key)
                    && Self::live(record, now)
            })
            .map(|((_, key), record)| Self::meta(key, record))
            .collect())
    }

    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, true)
    }

    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        validate_sealed_key(key)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .get(&(self.scope.as_str().to_owned(), key.to_owned()))
            .filter(|record| Self::live(record, now))
            .map(|record| Blob {
                meta: Self::meta(key, record),
                bytes: record.bytes.clone(),
            }))
    }

    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        validate_sealed_key(key)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .get(&(self.scope.as_str().to_owned(), key.to_owned()))
            .filter(|record| Self::live(record, now))
            .map(|record| Self::meta(key, record)))
    }

    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        validate_sealed_key(key)?;
        let now = (self.clock)();
        let mut records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .remove(&(self.scope.as_str().to_owned(), key.to_owned()))
            .is_some_and(|record| Self::live(&record, now)))
    }

    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_sealed_key(prefix)?;
        let now = (self.clock)();
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        Ok(records
            .iter()
            .filter(|((scope, key), record)| {
                scope == self.scope.as_str() && key.starts_with(prefix) && Self::live(record, now)
            })
            .map(|((_, key), record)| Self::meta(key, record))
            .collect())
    }

    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS {
            return Err(StorageError::invalid_input(
                "invalid blob listing page size",
            ));
        }
        validate_prefix(prefix, after)?;
        let records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        let mut rows = records.iter().filter(|((scope, key), _)| {
            scope == self.scope.as_str()
                && key.starts_with(prefix)
                && !is_reserved_blob_key(key)
                && after.is_none_or(|cursor| key.as_bytes() > cursor.as_bytes())
        });
        let mut items = Vec::with_capacity(limit);
        for ((_, key), record) in rows.by_ref().take(limit) {
            items.push(BlobListEntry {
                meta: Self::meta(key, record),
                written_at_ms: Some(record.written_at_ms),
            });
        }
        let next_after = if rows.next().is_some() {
            items.last().map(|entry| entry.meta.key.clone())
        } else {
            None
        };
        Ok(BlobListPage { items, next_after })
    }

    async fn sweep_expired(&self) -> Result<u64> {
        let now = (self.clock)();
        let mut records = self
            .records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?;
        let before = records.len();
        records.retain(|_, record| Self::live(record, now));
        Ok((before - records.len()) as u64)
    }
}

impl MemoryBlobStore {
    fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
        allow_sealed: bool,
    ) -> Result<BlobMeta> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let now = (self.clock)();
        let expires_at_ms = ttl.map(|duration| {
            now.saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        });
        let record = Record {
            bytes,
            content_type: content_type.map(str::to_owned),
            expires_at_ms,
            written_at_ms: now,
        };
        let meta = Self::meta(key, &record);
        self.records
            .lock()
            .map_err(|_| StorageError::unavailable("blob map lock poisoned"))?
            .insert((self.scope.as_str().to_owned(), key.to_owned()), record);
        Ok(meta)
    }
}
