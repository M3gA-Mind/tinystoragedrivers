//! Per-scope envelope keys persisted on the configured document backend.
//!
//! Only wrapped key material reaches storage. KMS binds wrapping to the scope
//! and format version. Pods racing to create an envelope converge using an
//! absent precondition; the loser unwraps the winner before encrypting secrets.
//! Rotation rewraps the existing data key, so existing ciphertext stays valid.
//! Cached plaintext is zeroized and expires. Every async call checks that the
//! backend envelope still matches before using it, including after a reset.
//! Reset must be fenced or serialized against writes: checking a key cannot
//! atomically prevent a concurrent reset between key acquisition and a write.
//! Cache misses never fall back to
//! a local file or another key source.

#[cfg(any(feature = "kms-aws", feature = "kms-gcp"))]
mod cloud;
#[cfg(feature = "kms-aws")]
pub use cloud::{AwsCredentials, AwsCredentialsProvider, AwsKms};
#[cfg(feature = "kms-gcp")]
pub use cloud::{GcpAccessTokenProvider, GcpKms};

use crate::{KeyProvider, crypto};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tinystoragedrivers_core::{
    ErrorKind, Precondition, Result, Scope, StorageBackend, StorageError,
};
use zeroize::Zeroizing;

/// The per-scope collection holding wrapped keys.
pub const ENVELOPE_COLLECTION: &str = "secret_keys";
/// The single envelope id in each scope.
pub const ENVELOPE_ID: &str = "data-key-v1";

/// Cloud transport port. Implementations must authenticate `scope` and the
/// `tinystoragedrivers-secrets/v1` label as encryption context or AAD.
#[async_trait]
pub trait KmsClient: Send + Sync + fmt::Debug {
    /// Wrap one data key under `key_id` with authenticated scope context.
    ///
    /// # Errors
    /// A KMS/authentication failure; never return plaintext as wrapped output.
    async fn wrap(&self, key_id: &str, scope: &Scope, key: &[u8; 32]) -> Result<Vec<u8>>;
    /// Unwrap an envelope, verifying its key identity and scope context.
    ///
    /// # Errors
    /// A KMS/authentication failure or an invalid 32-byte data key.
    async fn unwrap(
        &self,
        key_id: &str,
        scope: &Scope,
        wrapped: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>>;
}

struct CachedKey {
    key: Zeroizing<[u8; 32]>,
    loaded: Instant,
    envelope: Value,
}

/// KMS-backed `KeyProvider`, shared across tenants and backed by scoped stores.
///
/// Async callers load envelopes via `data_key_async`. The synchronous API
/// fails closed because it cannot validate a cached envelope against storage.
pub struct KmsKeyProvider {
    storage: Arc<dyn StorageBackend>,
    kms: Arc<dyn KmsClient>,
    initial_key_id: String,
    ttl: Duration,
    cache: Mutex<HashMap<Scope, CachedKey>>,
    loading: Mutex<HashMap<Scope, Weak<tokio::sync::Mutex<()>>>>,
}
impl fmt::Debug for KmsKeyProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KmsKeyProvider(..)")
    }
}
impl KmsKeyProvider {
    /// Build a provider. `key_id` is used only for new envelopes; existing
    /// envelopes carry their own KMS key identity. Zero TTL disables caching.
    ///
    /// # Errors
    /// Invalid input when `key_id` is empty.
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        kms: Arc<dyn KmsClient>,
        key_id: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self> {
        let initial_key_id = key_id.into();
        if initial_key_id.is_empty() {
            return Err(StorageError::invalid_input("kms key id is empty"));
        }
        Ok(Self {
            storage,
            kms,
            initial_key_id,
            ttl,
            cache: Mutex::new(HashMap::new()),
            loading: Mutex::new(HashMap::new()),
        })
    }
    /// Drop this pod's cached key for `scope`, zeroizing its material.
    ///
    /// # Errors
    /// A poisoned internal cache lock.
    pub fn invalidate(&self, scope: &Scope) -> Result<()> {
        self.cache
            .lock()
            .map_err(|_| StorageError::backend("key cache lock failed"))?
            .remove(scope);
        Ok(())
    }
    // Only callers working on one scope share a lock. Weak entries are pruned
    // when a new operation starts, so idle tenant locks are not retained.
    fn scope_lock(&self, scope: &Scope) -> Result<Arc<tokio::sync::Mutex<()>>> {
        let mut loading = self
            .loading
            .lock()
            .map_err(|_| StorageError::backend("key loading lock map failed"))?;
        loading.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = loading.get(scope).and_then(Weak::upgrade) {
            return Ok(lock);
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        loading.insert(scope.clone(), Arc::downgrade(&lock));
        Ok(lock)
    }
    fn cached(&self, scope: &Scope, envelope: &Value) -> Result<Option<Zeroizing<[u8; 32]>>> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| StorageError::backend("key cache lock failed"))?;
        if cache
            .get(scope)
            .is_some_and(|entry| entry.loaded.elapsed() >= self.ttl || &entry.envelope != envelope)
        {
            cache.remove(scope);
        }
        Ok(cache.get(scope).map(|entry| entry.key.clone()))
    }
    fn remember(&self, scope: &Scope, key: &Zeroizing<[u8; 32]>, envelope: Value) -> Result<()> {
        if !self.ttl.is_zero() {
            self.cache
                .lock()
                .map_err(|_| StorageError::backend("key cache lock failed"))?
                .insert(
                    scope.clone(),
                    CachedKey {
                        key: key.clone(),
                        loaded: Instant::now(),
                        envelope,
                    },
                );
        }
        Ok(())
    }
    async fn open(&self, scope: &Scope, doc: &Value) -> Result<Zeroizing<[u8; 32]>> {
        if doc.get("format").and_then(Value::as_u64) != Some(1)
            || doc.get("scope").and_then(Value::as_str) != Some(scope.as_str())
        {
            return Err(StorageError::crypto("envelope scope or format mismatch"));
        }
        let id = doc
            .get("key_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| StorageError::crypto("invalid envelope key identity"))?;
        let wrapped: Vec<u8> = serde_json::from_value(
            doc.get("wrapped")
                .cloned()
                .ok_or_else(|| StorageError::crypto("missing wrapped key"))?,
        )
        .map_err(|_| StorageError::crypto("invalid wrapped key"))?;
        self.kms.unwrap(id, scope, &wrapped).await
    }
    async fn envelope(&self, scope: &Scope, id: &str, key: &[u8; 32]) -> Result<Value> {
        let wrapped = self.kms.wrap(id, scope, key).await?;
        if wrapped.is_empty() {
            return Err(StorageError::crypto("kms returned an empty envelope"));
        }
        Ok(json!({ "format": 1, "scope": scope.as_str(), "key_id": id, "wrapped": wrapped }))
    }
    /// Rewrap this scope's existing data key under `new_key_id` using CAS.
    /// Existing secrets and caches remain decryptable because the data key
    /// itself is unchanged. Concurrent rotations return a conflict; retry by
    /// reading the winning envelope. No envelope is created by rotation.
    ///
    /// # Errors
    /// Missing envelope, invalid key identity, KMS/storage failure or conflict.
    pub async fn rotate(&self, scope: &Scope, new_key_id: &str) -> Result<()> {
        if new_key_id.is_empty() {
            return Err(StorageError::invalid_input("kms key id is empty"));
        }
        let loading = self.scope_lock(scope)?;
        let _guard = loading.lock().await;
        let storage = self.storage.for_scope(scope)?;
        let docs = storage.documents();
        let found = docs
            .get(ENVELOPE_COLLECTION, ENVELOPE_ID)
            .await?
            .ok_or_else(|| StorageError::not_found("scope has no key envelope"))?;
        let key = self.open(scope, &found.doc).await?;
        let doc = self.envelope(scope, new_key_id, &key).await?;
        docs.put(
            ENVELOPE_COLLECTION,
            ENVELOPE_ID,
            doc,
            Precondition::Version(found.version),
        )
        .await?;
        self.invalidate(scope)?;
        Ok(())
    }
}
#[async_trait]
impl KeyProvider for KmsKeyProvider {
    fn data_key(&self, _scope: &Scope) -> Result<Zeroizing<[u8; 32]>> {
        Err(StorageError::unavailable(
            "kms key requires async envelope validation",
        ))
    }
    async fn data_key_async(&self, scope: &Scope) -> Result<Zeroizing<[u8; 32]>> {
        let loading = self.scope_lock(scope)?;
        let _guard = loading.lock().await;
        let storage = self.storage.for_scope(scope)?;
        let docs = storage.documents();
        let (key, envelope) = if let Some(found) =
            docs.get(ENVELOPE_COLLECTION, ENVELOPE_ID).await?
        {
            if let Some(key) = self.cached(scope, &found.doc)? {
                return Ok(key);
            }
            (self.open(scope, &found.doc).await?, found.doc)
        } else {
            let candidate = crypto::generate_key()?;
            let doc = self
                .envelope(scope, &self.initial_key_id, &candidate)
                .await?;
            match docs
                .put(
                    ENVELOPE_COLLECTION,
                    ENVELOPE_ID,
                    doc.clone(),
                    Precondition::Absent,
                )
                .await
            {
                Ok(_) => (candidate, doc),
                Err(error)
                    if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) =>
                {
                    let found = docs
                        .get(ENVELOPE_COLLECTION, ENVELOPE_ID)
                        .await?
                        .ok_or_else(|| {
                            StorageError::unavailable("winning key envelope disappeared")
                        })?;
                    (self.open(scope, &found.doc).await?, found.doc)
                }
                Err(error) => return Err(error),
            }
        };
        self.remember(scope, &key, envelope)?;
        Ok(key)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
