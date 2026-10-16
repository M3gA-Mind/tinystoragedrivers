//! The suite's own helpers, and that it catches a broken driver.

use super::*;
use crate::{MemoryStorage, StorageError};

#[test]
fn unique_names_never_repeat() {
    let a = unique("x");
    let b = unique("x");
    assert_ne!(a, b);
    assert!(crate::validate_collection(&a).is_ok());
}

#[test]
fn ok_and_fails_report_mismatches() {
    assert_eq!(ok(Ok::<_, StorageError>(3), "check"), 3);
    fails(
        Err::<(), _>(StorageError::conflict("x")),
        ErrorKind::Conflict,
        "check",
    );
    let caught =
        std::panic::catch_unwind(|| ok(Err::<(), _>(StorageError::backend("boom")), "named"));
    assert!(caught.is_err());
    let caught =
        std::panic::catch_unwind(|| fails(Ok::<_, StorageError>(1), ErrorKind::Conflict, "named"));
    assert!(caught.is_err());
}

#[tokio::test]
async fn single_scope_mode_requires_foreign_scopes_to_fail() {
    let outcome = tokio::spawn(async {
        run(&MemoryStorage::new(), true).await;
    })
    .await;
    assert!(
        outcome.is_err(),
        "a multi-scope backend fails the single-scope check"
    );
}

/// A driver with no optional capabilities, built by narrowing the memory
/// driver, so the suite's "without the capability" branches run.
mod baseline {
    use std::sync::Arc;

    use serde_json::Value;

    use crate::{
        Capabilities, Capability, CollectionSpec, DocumentStore, Filter, Page, Precondition, Query,
        Result, Scope, ScopedStorage, Sort, StorageBackend, StorageError, Version, Versioned,
    };

    #[derive(Debug)]
    pub(super) struct Narrow(pub(super) crate::MemoryStorage, pub(super) bool);

    #[derive(Debug)]
    struct NarrowDocs(Arc<dyn DocumentStore>, bool);

    #[async_trait::async_trait]
    impl DocumentStore for NarrowDocs {
        fn capabilities(&self) -> Capabilities {
            if self.1 {
                Capabilities::none().with(Capability::CollectionEnumeration)
            } else {
                Capabilities::none()
            }
        }
        async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
            if !self.1 {
                return Err(StorageError::unsupported(
                    Capability::CollectionEnumeration,
                    "no listing",
                ));
            }
            if prefix.starts_with("conf_iso_only_") {
                // Fault injection: pretend every scope can see the queried collection.
                return Ok(vec![prefix.to_owned()]);
            }
            self.0.collections(prefix).await
        }
        async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
            if spec.ttl_field.is_some() {
                return Err(StorageError::unsupported(Capability::Ttl, "no expiry"));
            }
            self.0.ensure_collection(spec).await
        }
        async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
            self.0.get(c, id).await
        }
        async fn put(&self, c: &str, id: &str, d: Value, p: Precondition) -> Result<Version> {
            self.0.put(c, id, d, p).await
        }
        async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
            self.0.delete(c, id, p).await
        }
        async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
            self.0.query(c, q).await
        }
        async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
            self.0.count(c, f).await
        }
        async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
            self.0.delete_where(c, f).await
        }
        async fn claim(
            &self,
            c: &str,
            f: &Filter,
            s: &[Sort],
            p: &Value,
        ) -> Result<Option<Versioned<Value>>> {
            self.0.claim(c, f, s, p).await
        }
        async fn drop_collection(&self, c: &str) -> Result<()> {
            self.0.drop_collection(c).await
        }
    }

    #[derive(Debug)]
    struct NarrowStreams(Arc<dyn crate::StreamStore>);
    #[async_trait::async_trait]
    impl crate::StreamStore for NarrowStreams {
        async fn append(&self, s: &str, v: Value) -> Result<u64> {
            self.0.append(s, v).await
        }
        async fn append_batch(&self, s: &str, v: Vec<Value>) -> Result<u64> {
            self.0.append_batch(s, v).await
        }
        async fn read_window(&self, s: &str, f: u64, l: usize) -> Result<Vec<crate::StreamEntry>> {
            self.0.read_window(s, f, l).await
        }
        async fn len(&self, s: &str) -> Result<u64> {
            self.0.len(s).await
        }
        async fn truncate_before(&self, s: &str, o: u64) -> Result<u64> {
            self.0.truncate_before(s, o).await
        }
        async fn delete_stream(&self, s: &str) -> Result<bool> {
            self.0.delete_stream(s).await
        }
        async fn streams(&self, p: &str) -> Result<Vec<String>> {
            self.0.streams(p).await
        }
    }

    impl StorageBackend for Narrow {
        fn driver(&self) -> &'static str {
            "narrow"
        }
        fn capabilities(&self) -> Capabilities {
            Capabilities::none()
        }
        fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
            let inner = self.0.for_scope(scope)?;
            Ok(ScopedStorage::new(
                scope.clone(),
                self.driver(),
                Arc::new(NarrowDocs(Arc::clone(inner.documents()), self.1)),
                Arc::new(NarrowStreams(Arc::clone(inner.streams()))),
                Arc::clone(inner.blobs()),
            ))
        }
    }
}

#[tokio::test]
async fn a_driver_without_optional_capabilities_passes() {
    run(&baseline::Narrow(MemoryStorage::new(), false), false).await;
}

#[tokio::test]
async fn conformance_rejects_cross_scope_collection_enumeration() {
    let outcome = tokio::spawn(async {
        run(&baseline::Narrow(MemoryStorage::new(), true), false).await;
    })
    .await;
    assert!(
        outcome.is_err(),
        "collection enumeration must reject a tenant leak"
    );
}

#[tokio::test]
async fn overlapping_blob_boundary_checks_cannot_delete_each_others_payloads() {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let blobs = storage.blobs();
    let limit = blobs.max_key_len();
    for utf8 in [false, true] {
        let first = super::blobs::boundary_key(limit, utf8);
        let second = super::blobs::boundary_key(limit, utf8);
        assert_eq!(first.len(), limit);
        assert_eq!(second.len(), limit);
        crate::validate_blob_key(&first).unwrap();
        crate::validate_blob_key(&second).unwrap();
        blobs.put(&first, vec![1], None).await.unwrap();
        blobs.put(&second, vec![2], None).await.unwrap();
        assert!(blobs.delete(&second).await.unwrap());
        let retained = blobs.get(&first).await.unwrap();
        assert!(
            retained.is_some(),
            "another boundary check deleted our blob"
        );
        assert_eq!(retained.unwrap().bytes, [1]);
        assert!(blobs.delete(&first).await.unwrap());
    }
}

/// A driver that claims `RecordMetadata` but keeps none: every write forwards
/// as a plain put and every read reports unknown metadata.
mod forgetful {
    use std::sync::Arc;

    use serde_json::Value;

    use crate::{
        Capabilities, Capability, CollectionSpec, DocumentStore, Filter, Page, Precondition, Query,
        Result, Scope, ScopedStorage, Sort, StorageBackend, Version, Versioned, WriteOptions,
    };

    #[derive(Debug)]
    struct ForgetfulDocs(Arc<dyn DocumentStore>);

    fn forget(found: Versioned<Value>) -> Versioned<Value> {
        Versioned {
            meta: crate::Meta::unknown(),
            ..found
        }
    }

    #[async_trait::async_trait]
    impl DocumentStore for ForgetfulDocs {
        fn capabilities(&self) -> Capabilities {
            self.0.capabilities().with(Capability::RecordMetadata)
        }
        async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
            self.0.ensure_collection(spec).await
        }
        async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
            Ok(self.0.get(c, id).await?.map(forget))
        }
        async fn put(&self, c: &str, id: &str, d: Value, p: Precondition) -> Result<Version> {
            self.0.put(c, id, d, p).await
        }
        async fn put_with(
            &self,
            c: &str,
            id: &str,
            d: Value,
            p: Precondition,
            _: WriteOptions,
        ) -> Result<Version> {
            self.0.put(c, id, d, p).await
        }
        async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
            self.0.delete(c, id, p).await
        }
        async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
            self.0.query(c, q).await
        }
        async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
            self.0.count(c, f).await
        }
        async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
            self.0.delete_where(c, f).await
        }
        async fn claim(
            &self,
            c: &str,
            f: &Filter,
            s: &[Sort],
            p: &Value,
        ) -> Result<Option<Versioned<Value>>> {
            self.0.claim(c, f, s, p).await
        }
        async fn drop_collection(&self, c: &str) -> Result<()> {
            self.0.drop_collection(c).await
        }
    }

    pub(super) fn storage() -> ScopedStorage {
        let inner = crate::MemoryStorage::new()
            .for_scope(&Scope::local())
            .unwrap();
        ScopedStorage::new(
            Scope::local(),
            "forgetful",
            Arc::new(ForgetfulDocs(Arc::clone(inner.documents()))),
            Arc::clone(inner.streams()),
            Arc::clone(inner.blobs()),
        )
    }
}

#[tokio::test]
async fn conformance_rejects_a_driver_that_claims_metadata_but_keeps_none() {
    let outcome = tokio::spawn(async {
        super::schema::run(&forgetful::storage()).await;
    })
    .await;
    assert!(
        outcome.is_err(),
        "a driver reporting RecordMetadata must persist schema and times"
    );
}
