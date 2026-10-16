//! Seal and retention policies applied by `Registry::maintain`, with an
//! injected clock so every instant is exact.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;

use super::*;
use crate::schema::{Owner, Schema, ScopeClass};
use crate::{
    ErrorKind, MaintenanceBudget, MemoryStorage, Scope, SealPolicy, SealWhen, StorageBackend,
};

struct Rig {
    clock: Arc<AtomicU64>,
    backend: MemoryStorage,
    storage: crate::ScopedStorage,
}

fn rig() -> Rig {
    let clock = Arc::new(AtomicU64::new(1_000));
    let reader = Arc::clone(&clock);
    let backend = MemoryStorage::with_clock(Arc::new(move || reader.load(Ordering::SeqCst)));
    let storage = backend.for_scope(&Scope::local()).unwrap();
    Rig {
        clock,
        backend,
        storage,
    }
}

impl Rig {
    fn at(&self, ms: u64) {
        self.clock.store(ms, Ordering::SeqCst);
    }

    async fn append(&self, stream: &str) {
        self.storage
            .streams()
            .append(stream, json!(1))
            .await
            .unwrap();
    }

    async fn sealed(&self, stream: &str) -> bool {
        self.storage
            .streams()
            .info(stream)
            .await
            .unwrap()
            .is_some_and(|info| info.is_sealed())
    }

    async fn exists(&self, stream: &str) -> bool {
        self.storage.streams().info(stream).await.unwrap().is_some()
    }
}

fn registry(policy: SealPolicy) -> Registry {
    let schema = Schema::new(Owner::new("shop").unwrap()).stream_with(
        "shop.log",
        ScopeClass::Tenant,
        policy,
    );
    Registry::compose([schema]).unwrap()
}

fn idle(ms: u64) -> SealPolicy {
    SealPolicy::new(SealWhen::Idle(ms))
}

#[tokio::test]
async fn seals_only_idle_streams_older_than_the_threshold() {
    let rig = rig();
    let registry = registry(idle(500));
    rig.append("shop.log/old").await; // t = 1000
    rig.at(1_400);
    rig.append("shop.log/new").await; // t = 1400
    rig.append("shop.logger").await; // a different family sharing the prefix
    rig.at(1_600);

    let report = registry
        .maintain(&rig.storage, 1_600, MaintenanceBudget::unlimited())
        .await
        .unwrap();

    assert!(rig.sealed("shop.log/old").await, "idle for 600 > 500");
    assert!(!rig.sealed("shop.log/new").await, "idle for 200");
    assert!(!rig.sealed("shop.logger").await, "not in the family");
    assert_eq!((report.sealed, report.deleted), (1, 0));
}

#[tokio::test]
async fn the_base_stream_belongs_to_its_family_and_idle_is_a_strict_threshold() {
    let rig = rig();
    let registry = registry(idle(500));
    rig.append("shop.log").await; // t = 1000

    registry
        .maintain(&rig.storage, 1_500, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert!(!rig.sealed("shop.log").await, "exactly 500 is not older");

    registry
        .maintain(&rig.storage, 1_501, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert!(rig.sealed("shop.log").await);
}

#[tokio::test]
async fn an_already_sealed_stream_is_not_sealed_again() {
    let rig = rig();
    let registry = registry(idle(100));
    rig.append("shop.log/a").await;
    rig.at(2_000);

    let first = registry
        .maintain(&rig.storage, 2_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    let second = registry
        .maintain(&rig.storage, 9_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();

    assert_eq!(first.sealed, 1);
    assert_eq!(second.sealed, 0, "nothing left to seal");
    let info = rig
        .storage
        .streams()
        .info("shop.log/a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(info.sealed_ms, Some(2_000), "the first seal time stands");
}

#[tokio::test]
async fn retention_deletes_streams_sealed_longer_ago_than_retain_ms() {
    let rig = rig();
    let registry = registry(SealPolicy::new(SealWhen::OnFinish).retaining_ms(1_000));
    rig.append("shop.log/a").await;
    rig.append("shop.log/b").await;
    rig.at(2_000);
    rig.storage.streams().seal("shop.log/a").await.unwrap(); // sealed_ms 2000

    let early = registry
        .maintain(&rig.storage, 3_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!(early.deleted, 0, "2000 + 1000 < 3000 is false");
    assert!(rig.exists("shop.log/a").await);

    let late = registry
        .maintain(&rig.storage, 3_001, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!(late.deleted, 1);
    assert!(!rig.exists("shop.log/a").await);
    assert!(rig.exists("shop.log/b").await, "unsealed streams are kept");
}

#[tokio::test]
async fn on_finish_streams_are_never_sealed_by_maintenance() {
    let rig = rig();
    let registry = registry(SealPolicy::new(SealWhen::OnFinish).retaining_ms(10));
    rig.append("shop.log/a").await;

    let report = registry
        .maintain(&rig.storage, 1_000_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();

    assert!(!rig.sealed("shop.log/a").await);
    assert_eq!((report.sealed, report.deleted), (0, 0));
}

#[tokio::test]
async fn idle_streams_are_sealed_then_deleted_after_retention() {
    let rig = rig();
    let registry = registry(idle(100).retaining_ms(500));
    rig.append("shop.log/a").await;
    rig.at(1_200);

    let sealing = registry
        .maintain(&rig.storage, 1_200, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!((sealing.sealed, sealing.deleted), (1, 0));

    rig.at(1_800);
    let deleting = registry
        .maintain(&rig.storage, 1_800, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!((deleting.sealed, deleting.deleted), (0, 1));
    assert!(!rig.exists("shop.log/a").await);
}

#[tokio::test]
async fn a_never_policy_without_retention_touches_nothing() {
    let rig = rig();
    let registry = registry(SealPolicy::default());
    rig.append("shop.log/a").await;
    rig.at(500_000);
    rig.storage.streams().seal("shop.log/a").await.unwrap();

    let report = registry
        .maintain(&rig.storage, 9_000_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();

    assert!(rig.exists("shop.log/a").await);
    assert_eq!(report.sealed + report.deleted, 0);
    assert_eq!(
        report.inspected, 0,
        "a policy with no work is not even listed"
    );
}

#[tokio::test]
async fn the_budget_caps_streams_touched_and_the_next_call_continues() {
    let rig = rig();
    let registry = registry(idle(10));
    for name in ["a", "b", "c", "d", "e"] {
        rig.append(&format!("shop.log/{name}")).await;
    }
    let budget = MaintenanceBudget::streams_up_to(2);

    let first = registry
        .maintain(&rig.storage, 5_000, budget)
        .await
        .unwrap();
    assert_eq!(first.sealed, 2);
    assert!(first.exhausted, "work remains");

    let second = registry
        .maintain(&rig.storage, 5_000, budget)
        .await
        .unwrap();
    assert_eq!(second.sealed, 2);
    assert!(second.exhausted);

    let third = registry
        .maintain(&rig.storage, 5_000, budget)
        .await
        .unwrap();
    assert_eq!(third.sealed, 1);
    assert!(!third.exhausted, "everything was done within the budget");
}

#[tokio::test]
async fn a_zero_stream_budget_does_no_work() {
    let rig = rig();
    let registry = registry(idle(10));
    rig.append("shop.log/a").await;

    let report = registry
        .maintain(&rig.storage, 5_000, MaintenanceBudget::streams_up_to(0))
        .await
        .unwrap();

    assert!(!rig.sealed("shop.log/a").await);
    assert!(report.exhausted);
}

/// A stream port that lists one stream and answers `info` the way a driver
/// that predates (or lacks) recorded times does.
#[derive(Debug)]
struct Untimed {
    old_data: bool,
}

#[async_trait::async_trait]
impl crate::StreamStore for Untimed {
    async fn append(&self, _: &str, _: serde_json::Value) -> crate::Result<u64> {
        unreachable!("policy never appends")
    }
    async fn append_batch(&self, _: &str, _: Vec<serde_json::Value>) -> crate::Result<u64> {
        unreachable!("policy never appends")
    }
    async fn read_window(
        &self,
        _: &str,
        _: u64,
        _: usize,
    ) -> crate::Result<Vec<crate::StreamEntry>> {
        unreachable!("policy never reads")
    }
    async fn len(&self, _: &str) -> crate::Result<u64> {
        unreachable!("policy never reads")
    }
    async fn truncate_before(&self, _: &str, _: u64) -> crate::Result<u64> {
        unreachable!("policy never truncates")
    }
    async fn delete_stream(&self, _: &str) -> crate::Result<bool> {
        panic!("policy deleted a stream whose seal time is unknown")
    }
    async fn seal(&self, _: &str) -> crate::Result<crate::SealOutcome> {
        panic!("policy sealed a stream whose last append is unknown")
    }
    async fn info(&self, stream: &str) -> crate::Result<Option<crate::StreamInfo>> {
        if self.old_data {
            Ok(Some(crate::StreamInfo::new(0, 1, None, None)))
        } else {
            let _ = stream;
            Err(crate::StorageError::unsupported(
                crate::Capability::StreamSeal,
                "no times",
            ))
        }
    }
    async fn streams(&self, _: &str) -> crate::Result<Vec<String>> {
        Ok(vec!["shop.log/a".to_owned()])
    }
}

fn untimed(old_data: bool) -> crate::ScopedStorage {
    let memory = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    crate::ScopedStorage::new(
        Scope::local(),
        "untimed",
        Arc::clone(memory.documents()),
        Arc::new(Untimed { old_data }),
        Arc::clone(memory.blobs()),
    )
}

#[tokio::test]
async fn a_stream_without_recorded_times_is_left_alone() {
    // Idle needs a last-append time; retention a seal time. Without one the
    // policy has nothing to compare, so it must not guess (the stub panics if
    // it seals or deletes).
    let registry = registry(idle(10).retaining_ms(10));

    let report = registry
        .maintain(&untimed(true), 1_000_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();

    assert_eq!(report.inspected, 1);
    assert_eq!((report.sealed, report.deleted), (0, 0));
}

#[tokio::test]
async fn a_driver_that_records_no_times_is_unsupported() {
    let registry = registry(idle(10));
    let error = registry
        .maintain(&untimed(false), 5_000, MaintenanceBudget::unlimited())
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::StreamSeal)
    );
}

/// A backend that cannot list scopes.
#[derive(Debug)]
struct Plain;

impl StorageBackend for Plain {
    fn driver(&self) -> &'static str {
        "plain"
    }
    fn capabilities(&self) -> crate::Capabilities {
        crate::Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> crate::Result<crate::ScopedStorage> {
        MemoryStorage::new().for_scope(scope)
    }
}

#[tokio::test]
async fn maintain_all_visits_every_scope_and_shares_one_budget() {
    let rig = rig();
    let registry = registry(idle(10));
    let other = rig
        .backend
        .for_scope(&Scope::tenant("ann").unwrap())
        .unwrap();
    rig.append("shop.log/a").await;
    other
        .streams()
        .append("shop.log/a", json!(1))
        .await
        .unwrap();
    other
        .streams()
        .append("shop.log/b", json!(1))
        .await
        .unwrap();

    let capped = registry
        .maintain_all(&rig.backend, 5_000, MaintenanceBudget::streams_up_to(2))
        .await
        .unwrap();
    assert_eq!(capped.sealed, 2);
    assert!(capped.exhausted);

    let rest = registry
        .maintain_all(&rig.backend, 5_000, MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!(rest.sealed, 1);
    assert_eq!(rest.scopes, 2);
}

#[tokio::test]
async fn maintain_all_needs_scope_enumeration() {
    let registry = registry(idle(10));
    let error = registry
        .maintain_all(&Plain, 5_000, MaintenanceBudget::unlimited())
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(crate::Capability::ScopeEnumeration)
    );
}
