//! Compare-and-swap JSON key/value port.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Page, Precondition, Result, StorageError, Version, Versioned,
};

/// JSON key/value operations bound to one scope.
#[async_trait]
pub trait KvStore: Send + Sync + std::fmt::Debug {
    /// Optional KV behaviors provided by this driver.
    fn capabilities(&self) -> Capabilities;

    /// Read a value and its version.
    ///
    /// # Errors
    /// Returns an error for an invalid key or backend failure.
    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>>;

    /// Replace or insert a value unconditionally.
    ///
    /// # Errors
    /// Returns an error for an invalid key or backend failure.
    async fn put(&self, key: &str, value: Value) -> Result<Version>;

    /// Compare and swap a value using an optimistic precondition.
    ///
    /// # Errors
    /// Returns an error for an invalid key, failed precondition, or backend
    /// failure.
    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version>;

    /// Delete a value using an optimistic precondition.
    ///
    /// # Errors
    /// Returns an error for an invalid key, failed precondition, or backend
    /// failure.
    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool>;

    /// List matching values in stable key order.
    ///
    /// # Errors
    /// Returns an error for an invalid prefix/cursor or backend failure.
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>>;

    /// Write a value that expires after the supplied duration.
    ///
    /// # Errors
    /// Returns `Unsupported(Ttl)` by default, or an invalid key or backend
    /// error.
    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        let _ = (key, value, ttl);
        Err(StorageError::unsupported(
            Capability::Ttl,
            "KV expiry is unsupported",
        ))
    }
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;
