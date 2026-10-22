//! A KV decorator that hides native expiry support.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_types::{
    Capability, Cursor, Page, Precondition, Result, StorageError, Version, Versioned,
};

/// A [`KvStore`] wrapper that advertises no TTL and rejects TTL writes.
#[derive(Debug)]
pub struct NoNativeTtl {
    inner: Arc<dyn KvStore>,
}

impl NoNativeTtl {
    /// Wraps a key/value store while preserving its non-TTL behavior.
    #[must_use]
    pub fn new(inner: Arc<dyn KvStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl KvStore for NoNativeTtl {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities().without(Capability::Ttl)
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        self.inner.get(key).await
    }
    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        self.inner.put(key, value).await
    }
    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        self.inner.cas(key, value, expected).await
    }
    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        self.inner.delete(key, expected).await
    }
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        self.inner.list(prefix, cursor, limit).await
    }
    async fn put_with_ttl(&self, _key: &str, _value: Value, _ttl: Duration) -> Result<Version> {
        Err(StorageError::unsupported(
            Capability::Ttl,
            "native KV expiry disabled by test decorator",
        ))
    }
}

#[cfg(test)]
#[path = "no_native_ttl_tests.rs"]
mod tests;
