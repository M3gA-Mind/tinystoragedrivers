//! The suite's own helpers, and that it catches a broken driver.

use super::*;
use std::sync::Arc;

use crate::{MemoryStorage, StorageBackend, StorageError};

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

#[derive(Debug)]
struct SqliteMissingCapabilities;

#[async_trait::async_trait]
impl StorageBackend for SqliteMissingCapabilities {
    fn driver(&self) -> &'static str {
        "sqlite"
    }
    fn capabilities(&self) -> crate::Capabilities {
        crate::Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        MemoryStorage::new().for_scope(scope)
    }
}

#[tokio::test]
async fn conformance_rejects_a_driver_missing_process_capability_declarations() {
    let failure = tokio::spawn(async {
        run(&SqliteMissingCapabilities, false).await;
    })
    .await;
    assert!(
        failure.is_err(),
        "undeclared SQLite flags must fail conformance"
    );
}

#[derive(Debug)]
struct UnregisteredDriver(MemoryStorage);

#[async_trait::async_trait]
impl StorageBackend for UnregisteredDriver {
    fn driver(&self) -> &'static str {
        "new_backend"
    }
    fn document_driver(&self) -> &'static str {
        "new_backend"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0.capabilities().without(crate::Capability::Fencing)
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        if scope != &Scope::local() {
            return Err(StorageError::invalid_input("operator-only test backend"));
        }
        self.0.for_scope(scope)
    }
}

#[tokio::test]
async fn external_driver_can_supply_explicit_process_capability_expectations() {
    run_with_process_capabilities(
        &UnregisteredDriver(MemoryStorage::new()),
        true,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn conformance_rejects_an_unregistered_driver_identity() {
    let failure = tokio::spawn(async {
        run(&UnregisteredDriver(MemoryStorage::new()), false).await;
    })
    .await;
    let panic = failure
        .expect_err("new driver identities must be rejected")
        .into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("unregistered backend driver"),
        "expected an unregistered identity error, got {message:?}"
    );
}

#[derive(Debug)]
struct SqliteIdentityWrapper(MemoryStorage);

#[async_trait::async_trait]
impl StorageBackend for SqliteIdentityWrapper {
    fn driver(&self) -> &'static str {
        "external_wrapper"
    }
    fn document_driver(&self) -> &'static str {
        "sqlite"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0.capabilities()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        self.0.for_scope(scope)
    }
}

#[tokio::test]
async fn wrappers_cannot_inherit_native_expectations_from_document_identity() {
    let failure = tokio::spawn(async {
        run(&SqliteIdentityWrapper(MemoryStorage::new()), true).await;
    })
    .await;
    let panic = failure
        .expect_err("a wrapper must declare its process capabilities explicitly")
        .into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("unregistered backend driver"),
        "expected an unregistered wrapper error, got {message:?}"
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
        fn document_driver(&self) -> &'static str {
            "memory"
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
    run_with_process_capabilities(
        &baseline::Narrow(MemoryStorage::new(), false),
        false,
        false,
        false,
    )
    .await;
}

#[tokio::test]
async fn conformance_rejects_cross_scope_collection_enumeration() {
    let outcome = tokio::spawn(async {
        run_with_process_capabilities(
            &baseline::Narrow(MemoryStorage::new(), true),
            false,
            false,
            false,
        )
        .await;
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

/// A clock that only moves every 100 ms, far coarser than any fixed pause the
/// suite could assume.
fn coarse_storage() -> MemoryStorage {
    MemoryStorage::with_clock(Arc::new(|| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        u64::try_from(now / 100 * 100).unwrap_or(0)
    }))
}

#[tokio::test]
async fn time_ordered_checks_do_not_assume_a_clock_resolution() {
    let backend = coarse_storage();
    let scoped = backend.for_scope(&crate::Scope::local()).unwrap();
    schema::run(&scoped).await;
    seal::run(&scoped).await;
}

/// The memory driver reporting [`Capability::SharedAcrossProcesses`], so the
/// shared-handle checks have a store whose handles do agree.
#[derive(Debug, Clone)]
struct Claims(MemoryStorage);

impl StorageBackend for Claims {
    fn driver(&self) -> &'static str {
        "claims"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0
            .capabilities()
            .with(crate::Capability::SharedAcrossProcesses)
            .with(crate::Capability::CrossProcessCas)
    }
    fn for_scope(&self, scope: &crate::Scope) -> crate::Result<crate::ScopedStorage> {
        self.0.for_scope(scope)
    }
    fn for_scope_fenced(
        &self,
        scope: &crate::Scope,
        fence: &crate::Fence,
    ) -> crate::Result<crate::ScopedStorage> {
        self.0.for_scope_fenced(scope, fence)
    }
}

#[derive(Debug, Clone)]
struct SharedWithoutCas(MemoryStorage);

impl StorageBackend for SharedWithoutCas {
    fn driver(&self) -> &'static str {
        "shared_without_cas"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0
            .capabilities()
            .without(crate::Capability::Fencing)
            .with(crate::Capability::SharedAcrossProcesses)
    }
    fn for_scope(&self, scope: &crate::Scope) -> crate::Result<crate::ScopedStorage> {
        if scope != &crate::Scope::local() {
            return Err(StorageError::invalid_input("operator-only test backend"));
        }
        self.0.for_scope(scope)
    }
}

#[tokio::test]
async fn conformance_rejects_shared_without_cross_process_cas() {
    let failure = tokio::spawn(async {
        run_with_process_capabilities(&SharedWithoutCas(MemoryStorage::new()), true, true, false)
            .await;
    })
    .await
    .expect_err("SharedAcrossProcesses must include CrossProcessCas");
    let panic = failure.into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("declares SharedAcrossProcesses without required CrossProcessCas"),
        "expected the capability implication diagnostic, got {message:?}"
    );
}

#[tokio::test]
async fn handles_that_agree_pass_the_shared_checks() {
    let store = MemoryStorage::new();
    run_shared(|| {
        let backend = Claims(store.clone());
        async move { backend }
    })
    .await;
}

/// A driver that supports cross-process conditional writes without promising
/// general visibility for every storage port.
#[derive(Debug, Clone)]
struct CasOnly(MemoryStorage);

impl StorageBackend for CasOnly {
    fn driver(&self) -> &'static str {
        "cas_only"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0
            .capabilities()
            .without(crate::Capability::Fencing)
            .with(crate::Capability::CrossProcessCas)
    }
    fn for_scope(&self, scope: &crate::Scope) -> crate::Result<crate::ScopedStorage> {
        self.0.for_scope(scope)
    }
}

/// A CAS-claiming backend whose document adapter ignores write preconditions.
#[derive(Debug, Clone)]
struct BrokenCasOnly(MemoryStorage);

#[derive(Debug)]
struct UnconditionalDocs(
    Arc<dyn crate::DocumentStore>,
    Option<Arc<tokio::sync::Barrier>>,
);

#[async_trait::async_trait]
impl crate::DocumentStore for UnconditionalDocs {
    fn capabilities(&self) -> crate::Capabilities {
        self.0.capabilities()
    }
    async fn ensure_collection(&self, spec: &crate::CollectionSpec) -> Result<()> {
        self.0.ensure_collection(spec).await
    }
    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<crate::Versioned<serde_json::Value>>> {
        self.0.get(collection, id).await
    }
    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
        _precondition: crate::Precondition,
    ) -> Result<crate::Version> {
        self.0
            .put(collection, id, doc, crate::Precondition::None)
            .await
    }
    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
        _precondition: crate::Precondition,
        _options: crate::WriteOptions,
    ) -> Result<crate::Version> {
        self.0
            .put(collection, id, doc, crate::Precondition::None)
            .await
    }
    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: crate::Precondition,
    ) -> Result<bool> {
        self.0.delete(collection, id, precondition).await
    }
    async fn query(
        &self,
        collection: &str,
        query: &crate::Query,
    ) -> Result<crate::Page<crate::Versioned<serde_json::Value>>> {
        self.0.query(collection, query).await
    }
    async fn count(&self, collection: &str, filter: &crate::Filter) -> Result<u64> {
        self.0.count(collection, filter).await
    }
    async fn delete_where(&self, collection: &str, filter: &crate::Filter) -> Result<u64> {
        self.0.delete_where(collection, filter).await
    }
    async fn claim(
        &self,
        collection: &str,
        filter: &crate::Filter,
        sort: &[crate::Sort],
        patch: &serde_json::Value,
    ) -> Result<Option<crate::Versioned<serde_json::Value>>> {
        if let Some(barrier) = &self.1 {
            let mut query = crate::Query::filter(filter.clone()).limit(1);
            for order in sort {
                query = query.sort(order.clone());
            }
            let Some(candidate) = self
                .0
                .query(collection, &query)
                .await?
                .items
                .into_iter()
                .next()
            else {
                return Ok(None);
            };
            barrier.wait().await;
            let mut claimed = candidate.doc;
            if let (Some(target), Some(changes)) = (claimed.as_object_mut(), patch.as_object()) {
                target.extend(
                    changes
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                );
            }
            let version = self
                .0
                .put(
                    collection,
                    &candidate.id,
                    claimed.clone(),
                    crate::Precondition::None,
                )
                .await?;
            return Ok(Some(crate::Versioned {
                id: candidate.id,
                version,
                doc: claimed,
                meta: candidate.meta,
            }));
        }
        self.0.claim(collection, filter, sort, patch).await
    }
    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.0.drop_collection(collection).await
    }
}

impl StorageBackend for BrokenCasOnly {
    fn driver(&self) -> &'static str {
        "broken_cas_only"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0
            .capabilities()
            .without(crate::Capability::Fencing)
            .with(crate::Capability::CrossProcessCas)
    }
    fn for_scope(&self, scope: &crate::Scope) -> Result<crate::ScopedStorage> {
        let storage = self.0.for_scope(scope)?;
        Ok(crate::ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(UnconditionalDocs(Arc::clone(storage.documents()), None)),
            Arc::clone(storage.streams()),
            Arc::clone(storage.blobs()),
        ))
    }
}

#[derive(Debug, Clone)]
struct BrokenClaimCas(MemoryStorage, Arc<tokio::sync::Barrier>);

impl StorageBackend for BrokenClaimCas {
    fn driver(&self) -> &'static str {
        "broken_claim_cas"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0
            .capabilities()
            .without(crate::Capability::Fencing)
            .with(crate::Capability::CrossProcessCas)
    }
    fn for_scope(&self, scope: &crate::Scope) -> Result<crate::ScopedStorage> {
        let storage = self.0.for_scope(scope)?;
        Ok(crate::ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(UnconditionalDocs(
                Arc::clone(storage.documents()),
                Some(Arc::clone(&self.1)),
            )),
            Arc::clone(storage.streams()),
            Arc::clone(storage.blobs()),
        ))
    }
}

#[derive(Debug, Clone)]
struct BrokenFencedCas(MemoryStorage);

impl StorageBackend for BrokenFencedCas {
    fn driver(&self) -> &'static str {
        "broken_fenced_cas"
    }
    fn capabilities(&self) -> crate::Capabilities {
        self.0
            .capabilities()
            .with(crate::Capability::CrossProcessCas)
            .with(crate::Capability::Fencing)
    }
    fn for_scope(&self, scope: &crate::Scope) -> Result<crate::ScopedStorage> {
        self.0.for_scope(scope)
    }
    fn for_scope_fenced(
        &self,
        scope: &crate::Scope,
        _fence: &crate::Fence,
    ) -> Result<crate::ScopedStorage> {
        // Broken implementation: returns an ordinary handle that never checks
        // the guard as part of its writes.
        self.0.for_scope(scope)
    }
}

#[tokio::test]
async fn cas_only_handles_pass_conditional_race_checks() {
    let store = MemoryStorage::new();
    run_shared(|| {
        let backend = CasOnly(store.clone());
        async move { backend }
    })
    .await;
}

#[tokio::test]
async fn cas_only_driver_that_ignores_preconditions_fails_conditional_race_checks() {
    let persistent = MemoryStorage::new();
    let outcome = tokio::spawn(async move {
        run_shared(|| {
            let backend = BrokenCasOnly(persistent.clone());
            async move { backend }
        })
        .await;
    })
    .await
    .expect_err("unconditional writes must fail CAS conformance");
    let panic = outcome.into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("exactly one racing version-guarded put must win"),
        "expected the conditional race check to fail, got {message:?}"
    );
}

#[tokio::test]
async fn cas_only_driver_with_non_atomic_claim_fails_conditional_race_checks() {
    let store = MemoryStorage::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(super::shared::RACERS));
    let outcome = tokio::spawn(async move {
        run_shared(|| {
            let backend = BrokenClaimCas(store.clone(), Arc::clone(&barrier));
            async move { backend }
        })
        .await;
    })
    .await
    .expect_err("non-atomic claims must fail CAS conformance");
    let panic = outcome.into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("exactly one racing document claim must win"),
        "expected the document claim race check to fail, got {message:?}"
    );
}

#[tokio::test]
async fn cross_process_cas_rejects_a_stale_fence_from_another_backend_handle() {
    let store = MemoryStorage::new();
    let outcome = tokio::spawn(async move {
        run_shared(|| {
            let backend = BrokenFencedCas(store.clone());
            async move { backend }
        })
        .await;
    })
    .await
    .expect_err("stale cross-handle fences must be rejected");
    let panic = outcome.into_panic();
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains("cross-open stale fence: expected fenced"),
        "expected the stale-fence check to fail, got {message:?}"
    );
}

#[tokio::test]
async fn a_driver_without_the_capability_gets_the_reopen_check_only() {
    let store = MemoryStorage::new();
    run_shared(|| {
        let backend = store.clone();
        async move { backend }
    })
    .await;

    // Independent stores behind a driver that does not claim sharing still get
    // the reopen check, and a fresh store is not a reopen.
    let outcome = tokio::spawn(async {
        run_shared(|| async { MemoryStorage::new() }).await;
    })
    .await;
    assert!(
        outcome.is_err(),
        "a reopen that finds an empty store fails the reopen check"
    );
}

#[tokio::test]
async fn handles_on_separate_stores_fail_the_shared_checks() {
    let outcome = tokio::spawn(async {
        run_shared(|| async { Claims(MemoryStorage::new()) }).await;
    })
    .await;
    assert!(
        outcome.is_err(),
        "a driver that claims sharing but keeps handles apart must fail"
    );
}
