//! Envelope sharing, rotation, cache validation and failures of the KMS provider.
use super::*;
use crate::{DocumentSecrets, SecretStore};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tinystoragedrivers_api::{DocumentStore, Storage};
use tinystoragedrivers_types::{
    Capabilities, CollectionSpec, Filter, Meta, Page, Precondition, Query, Result, SearchHit, Sort,
    Version, Versioned, WriteOp, WriteOptions, WriteResult,
};
use tokio::sync::Mutex;

type ScopedDocuments = Arc<Mutex<HashMap<(String, String, String), Versioned<Value>>>>;

#[derive(Debug, Default)]
struct FakeBackend {
    documents: ScopedDocuments,
}

#[derive(Debug)]
struct FakeDocuments {
    scope: String,
    documents: ScopedDocuments,
}

#[async_trait]
impl DocumentStore for FakeDocuments {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        Ok(self
            .documents
            .lock()
            .await
            .get(&(self.scope.clone(), collection.to_owned(), id.to_owned()))
            .cloned())
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        let key = (self.scope.clone(), collection.to_owned(), id.to_owned());
        let mut documents = self.documents.lock().await;
        let current = documents.get(&key);
        let allowed = match (precondition, current) {
            (Precondition::None, _) | (Precondition::Absent, None) => true,
            (Precondition::Version(expected), Some(found)) => found.version == expected,
            _ => false,
        };
        if !allowed {
            return Err(StorageError::conflict("fake document precondition failed"));
        }
        let version = current.map_or(Version::FIRST, |found| found.version.next().unwrap());
        documents.insert(
            key,
            Versioned {
                id: id.to_owned(),
                version,
                doc,
                meta: Meta::default(),
            },
        );
        Ok(version)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        let key = (self.scope.clone(), collection.to_owned(), id.to_owned());
        let mut documents = self.documents.lock().await;
        let Some(found) = documents.get(&key) else {
            return if precondition == Precondition::None || precondition == Precondition::Absent {
                Ok(false)
            } else {
                Err(StorageError::conflict("fake document precondition failed"))
            };
        };
        if let Precondition::Version(expected) = precondition
            && found.version != expected
        {
            return Err(StorageError::conflict("fake document precondition failed"));
        }
        documents.remove(&key);
        Ok(true)
    }

    async fn query(&self, collection: &str, _: &Query) -> Result<Page<Versioned<Value>>> {
        let documents = self.documents.lock().await;
        let items = documents
            .iter()
            .filter(|((scope, found_collection, _), _)| {
                scope == &self.scope && found_collection == collection
            })
            .map(|(_, found)| found.clone())
            .collect();
        Ok(Page { items, next: None })
    }

    async fn count(&self, collection: &str, _: &Filter) -> Result<u64> {
        Ok(self.query(collection, &Query::all()).await?.items.len() as u64)
    }

    async fn delete_where(&self, collection: &str, _: &Filter) -> Result<u64> {
        let mut documents = self.documents.lock().await;
        let before = documents.len();
        documents.retain(|(scope, found_collection, _), _| {
            scope != &self.scope || found_collection != collection
        });
        Ok((before - documents.len()) as u64)
    }

    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        Ok(None)
    }

    async fn atomic_batch(&self, _: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        Err(StorageError::unsupported(
            tinystoragedrivers_types::Capability::Transactions,
            "fake document batch is unsupported",
        ))
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
        _: WriteOptions,
    ) -> Result<Version> {
        self.put(collection, id, value, precondition).await
    }

    async fn search(&self, _: &str, _: &str, _: usize) -> Result<Vec<SearchHit>> {
        Ok(Vec::new())
    }

    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.delete_where(collection, &Filter::default()).await?;
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for FakeBackend {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    fn for_scope(&self, scope: &Scope) -> Result<Storage> {
        let unavailable = Storage::unsupported();
        Ok(Storage::new(
            Arc::new(FakeDocuments {
                scope: scope.as_str().to_owned(),
                documents: Arc::clone(&self.documents),
            }),
            Arc::clone(unavailable.streams()),
            Arc::clone(unavailable.kv()),
            Arc::clone(unavailable.blobs()),
            Arc::clone(unavailable.queues()),
            Arc::clone(unavailable.secrets()),
        ))
    }
}

#[derive(Debug, Default)]
struct FakeKms {
    calls: AtomicUsize,
    fail: AtomicBool,
}
#[async_trait]
impl KmsClient for FakeKms {
    async fn wrap(&self, key_id: &str, scope: &Scope, key: &[u8; 32]) -> Result<Vec<u8>> {
        tokio::task::yield_now().await;
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(StorageError::unavailable("fake outage"));
        }
        Ok(format!(
            "{key_id}:{}:{}",
            scope.as_str(),
            crate::crypto::encrypt_enc2(&Zeroizing::new([7; 32]), key)?
        )
        .into_bytes())
    }
    async fn unwrap(
        &self,
        key_id: &str,
        scope: &Scope,
        wrapped: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        tokio::task::yield_now().await;
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(StorageError::unavailable("fake outage"));
        }
        let raw = std::str::from_utf8(wrapped).map_err(|_| StorageError::crypto("bad envelope"))?;
        let prefix = format!("{key_id}:{}:", scope.as_str());
        let body = raw
            .strip_prefix(&prefix)
            .ok_or_else(|| StorageError::crypto("context mismatch"))?;
        let plain = crate::crypto::decrypt_enc2(&Zeroizing::new([7; 32]), body)?;
        let bytes: [u8; 32] = plain
            .as_slice()
            .try_into()
            .map_err(|_| StorageError::crypto("bad key"))?;
        Ok(Zeroizing::new(bytes))
    }
}

#[tokio::test]
async fn separate_pods_share_persisted_keys_and_rotation_preserves_secrets() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let first = Arc::new(
        KmsKeyProvider::new(
            storage.clone(),
            kms.clone(),
            "key-v1",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let scope = Scope::new("tenant-a").unwrap();
    let scoped = storage.for_scope(&scope).unwrap();
    let secret = DocumentSecrets::new(&scoped, scope.clone(), first.clone());
    secret.set("token", b"private").await.unwrap();
    let second = Arc::new(
        KmsKeyProvider::new(
            storage.clone(),
            kms.clone(),
            "key-v2",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let other = DocumentSecrets::new(&scoped, scope.clone(), second.clone());
    assert_eq!(&**other.get("token").await.unwrap().unwrap(), b"private");
    second.rotate(&scope, "key-v2").await.unwrap();
    first.invalidate(&scope).unwrap();
    assert_eq!(&**secret.get("token").await.unwrap().unwrap(), b"private");
    let b = Scope::new("tenant-b").unwrap();
    assert_ne!(
        *second.data_key_async(&scope).await.unwrap(),
        *second.data_key_async(&b).await.unwrap()
    );
    assert!(second.data_key(&Scope::new("uncached").unwrap()).is_err());
}

#[tokio::test]
async fn kms_failure_never_creates_an_unencrypted_fallback() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    kms.fail.store(true, Ordering::SeqCst);
    let provider = KmsKeyProvider::new(storage.clone(), kms, "key", Duration::ZERO).unwrap();
    let scope = Scope::new("tenant").unwrap();
    assert!(provider.data_key_async(&scope).await.is_err());
    assert!(
        storage
            .for_scope(&scope)
            .unwrap()
            .documents()
            .get(ENVELOPE_COLLECTION, ENVELOPE_ID)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn deleting_the_envelope_invalidates_cached_plaintext_on_other_pods() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let old =
        KmsKeyProvider::new(storage.clone(), kms.clone(), "key", Duration::from_secs(60)).unwrap();
    let scope = Scope::new("tenant").unwrap();
    let old_key = old.data_key_async(&scope).await.unwrap();
    storage
        .for_scope(&scope)
        .unwrap()
        .documents()
        .drop_collection(ENVELOPE_COLLECTION)
        .await
        .unwrap();
    let new = KmsKeyProvider::new(storage, kms, "key", Duration::from_secs(60)).unwrap();
    let new_key = new.data_key_async(&scope).await.unwrap();
    assert_ne!(*old_key, *new_key);
    assert_eq!(*old.data_key_async(&scope).await.unwrap(), *new_key);
}

#[tokio::test]
async fn concurrent_pods_use_the_winning_envelope() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let first = KmsKeyProvider::new(storage.clone(), kms.clone(), "key", Duration::ZERO).unwrap();
    let second = KmsKeyProvider::new(storage, kms, "key", Duration::ZERO).unwrap();
    let scope = Scope::new("tenant").unwrap();
    let (one, two) = tokio::join!(first.data_key_async(&scope), second.data_key_async(&scope));
    assert_eq!(*one.unwrap(), *two.unwrap());
}

#[tokio::test]
async fn copying_an_envelope_to_another_scope_fails_closed() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let provider = KmsKeyProvider::new(storage.clone(), kms, "key", Duration::ZERO).unwrap();
    let a = Scope::new("a").unwrap();
    let b = Scope::new("b").unwrap();
    provider.data_key_async(&a).await.unwrap();
    let docs = storage.for_scope(&a).unwrap();
    let mut stolen = docs
        .documents()
        .get(ENVELOPE_COLLECTION, ENVELOPE_ID)
        .await
        .unwrap()
        .unwrap()
        .doc;
    stolen["scope"] = Value::String(b.as_str().into());
    storage
        .for_scope(&b)
        .unwrap()
        .documents()
        .put(ENVELOPE_COLLECTION, ENVELOPE_ID, stolen, Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        provider.data_key_async(&b).await.unwrap_err().kind(),
        ErrorKind::Crypto
    );
}

#[tokio::test]
async fn a_valid_cache_avoids_kms_calls_but_invalidation_forces_unwrap() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let provider =
        KmsKeyProvider::new(storage, kms.clone(), "key", Duration::from_secs(60)).unwrap();
    let scope = Scope::new("tenant").unwrap();
    let first = provider.data_key_async(&scope).await.unwrap();
    assert_eq!(*provider.data_key_async(&scope).await.unwrap(), *first);
    assert_eq!(kms.calls.load(Ordering::SeqCst), 1);
    provider.invalidate(&scope).unwrap();
    assert_eq!(*provider.data_key_async(&scope).await.unwrap(), *first);
    assert_eq!(kms.calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn failed_rotation_leaves_the_persisted_envelope_unchanged() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let provider =
        KmsKeyProvider::new(storage.clone(), kms.clone(), "key", Duration::ZERO).unwrap();
    let scope = Scope::new("tenant").unwrap();
    provider.data_key_async(&scope).await.unwrap();
    let scoped = storage.for_scope(&scope).unwrap();
    let before = scoped
        .documents()
        .get(ENVELOPE_COLLECTION, ENVELOPE_ID)
        .await
        .unwrap();
    kms.fail.store(true, Ordering::SeqCst);
    assert!(provider.rotate(&scope, "next-key").await.is_err());
    assert_eq!(
        scoped
            .documents()
            .get(ENVELOPE_COLLECTION, ENVELOPE_ID)
            .await
            .unwrap(),
        before
    );
    assert!(provider.data_key_async(&scope).await.is_err());
}

#[tokio::test]
async fn concurrent_rotations_detect_conflict_without_changing_the_data_key() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let first = KmsKeyProvider::new(storage.clone(), kms.clone(), "key", Duration::ZERO).unwrap();
    let second = KmsKeyProvider::new(storage, kms, "key", Duration::ZERO).unwrap();
    let scope = Scope::new("tenant").unwrap();
    let before = first.data_key_async(&scope).await.unwrap();
    let (one, two) = tokio::join!(
        first.rotate(&scope, "next-a"),
        second.rotate(&scope, "next-b")
    );
    let ((Ok(()), Err(loser)) | (Err(loser), Ok(()))) = (one, two) else {
        panic!("one CAS rotation must win and one must conflict");
    };
    assert_eq!(loser.kind(), ErrorKind::Conflict);
    assert_eq!(*first.data_key_async(&scope).await.unwrap(), *before);
    assert_eq!(*second.data_key_async(&scope).await.unwrap(), *before);
}

#[derive(Debug, Default)]
struct BlockingKms {
    blocked: AtomicBool,
    started: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl KmsClient for BlockingKms {
    async fn wrap(&self, _: &str, scope: &Scope, key: &[u8; 32]) -> Result<Vec<u8>> {
        if scope.as_str() == "slow" && self.blocked.load(Ordering::SeqCst) {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(key.to_vec())
    }
    async fn unwrap(&self, _: &str, _: &Scope, wrapped: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
        Ok(Zeroizing::new(wrapped.try_into().unwrap()))
    }
}

#[tokio::test]
async fn a_blocked_tenant_rotation_does_not_stall_another_tenant_key_load() {
    use std::{future::Future, task::Poll};
    let kms = Arc::new(BlockingKms::default());
    let provider = Arc::new(
        KmsKeyProvider::new(
            Arc::new(FakeBackend::default()),
            kms.clone(),
            "key",
            Duration::from_secs(60),
        )
        .unwrap(),
    );
    let slow = Scope::new("slow").unwrap();
    provider.data_key_async(&slow).await.unwrap();
    kms.blocked.store(true, Ordering::SeqCst);
    let rotate = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.rotate(&slow, "new-key").await })
    };
    kms.started.notified().await;
    let other = Scope::new("other").unwrap();
    let mut load = Box::pin(provider.data_key_async(&other));
    // The fake backend and other scope KMS work complete immediately. Poll once
    // to prove this work can finish while the slow scope is still blocked.
    let independent =
        std::future::poll_fn(|cx| Poll::Ready(load.as_mut().poll(cx).is_ready())).await;
    kms.release.notify_one();
    rotate.await.unwrap().unwrap();
    assert!(
        independent,
        "another tenant must not wait for the blocked rotation"
    );
}

#[tokio::test]
async fn malformed_envelopes_fail_before_kms_access() {
    let storage: Arc<dyn StorageBackend> = Arc::new(FakeBackend::default());
    let kms = Arc::new(FakeKms::default());
    let provider =
        KmsKeyProvider::new(storage.clone(), kms.clone(), "key", Duration::ZERO).unwrap();
    let scope = Scope::new("tenant").unwrap();
    let scoped = storage.for_scope(&scope).unwrap();
    for doc in [
        json!({"format": 2, "scope": "tenant", "key_id": "key", "wrapped": []}),
        json!({"format": 1, "scope": "other", "key_id": "key", "wrapped": []}),
        json!({"format": 1, "scope": "tenant", "key_id": "", "wrapped": []}),
        json!({"format": 1, "scope": "tenant", "key_id": "key"}),
        json!({"format": 1, "scope": "tenant", "key_id": "key", "wrapped": "bad"}),
    ] {
        scoped
            .documents()
            .put(ENVELOPE_COLLECTION, ENVELOPE_ID, doc, Precondition::None)
            .await
            .unwrap();
        assert_eq!(
            provider.data_key_async(&scope).await.unwrap_err().kind(),
            ErrorKind::Crypto
        );
    }
    assert_eq!(kms.calls.load(Ordering::SeqCst), 0);
    assert_eq!(format!("{provider:?}"), "KmsKeyProvider(..)");
    assert!(KmsKeyProvider::new(storage, kms, "", Duration::ZERO).is_err());
    assert_eq!(
        provider.rotate(&scope, "").await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        provider
            .rotate(&Scope::new("absent").unwrap(), "new-key")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::NotFound
    );
}

#[tokio::test]
async fn one_provider_serializes_same_scope_loads_without_duplicate_kms_calls() {
    let kms = Arc::new(FakeKms::default());
    let provider = KmsKeyProvider::new(
        Arc::new(FakeBackend::default()),
        kms.clone(),
        "key",
        Duration::from_secs(60),
    )
    .unwrap();
    let scope = Scope::new("tenant").unwrap();
    let (one, two) = tokio::join!(
        provider.data_key_async(&scope),
        provider.data_key_async(&scope)
    );
    assert_eq!(*one.unwrap(), *two.unwrap());
    assert_eq!(kms.calls.load(Ordering::SeqCst), 1);
}
