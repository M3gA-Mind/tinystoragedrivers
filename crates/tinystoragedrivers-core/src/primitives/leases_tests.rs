use std::sync::atomic::{AtomicU64, Ordering};

use super::*;
use crate::{ErrorKind, MemoryStorage, StorageBackend};

struct Rig {
    storage: crate::ScopedStorage,
    leases: Leases,
    now: Arc<AtomicU64>,
}

fn rig() -> Rig {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&now);
    let leases = Leases::new(storage.documents())
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)))
        .with_retry(RetryPolicy::immediate());
    Rig {
        storage,
        leases,
        now,
    }
}

#[tokio::test]
async fn the_first_acquire_starts_at_epoch_one() {
    let rig = rig();
    let lease = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (lease.holder.as_str(), lease.epoch, lease.expires_ms),
        ("a", 1, 1_100)
    );
    assert_eq!(rig.leases.current("host", "p1").await.unwrap(), Some(lease));
}

#[tokio::test]
async fn a_live_lease_blocks_other_holders_but_not_its_own() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.leases
            .acquire("host", "p1", "b", 100)
            .await
            .unwrap()
            .is_none()
    );
    rig.now.store(1_050, Ordering::SeqCst);
    let again = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((again.epoch, again.expires_ms), (1, 1_150));
}

#[tokio::test]
async fn takeover_after_expiry_bumps_the_epoch_and_renew_keeps_it() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    rig.now.store(1_100, Ordering::SeqCst);
    assert!(rig.leases.current("host", "p1").await.unwrap().is_none());
    let taken = rig
        .leases
        .acquire("host", "p1", "b", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((taken.holder.as_str(), taken.epoch), ("b", 2));
    rig.now.store(1_150, Ordering::SeqCst);
    let renewed = rig
        .leases
        .renew("host", "p1", "b", 2, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((renewed.epoch, renewed.expires_ms), (2, 1_250));
    // The old holder returns after another lapse and takeover: epoch 3.
    rig.now.store(1_300, Ordering::SeqCst);
    let back = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(back.epoch, 3);
}

#[tokio::test]
async fn renew_refuses_a_stranger_a_lapsed_lease_and_a_missing_one() {
    let rig = rig();
    assert!(
        rig.leases
            .renew("host", "p1", "a", 1, 100)
            .await
            .unwrap()
            .is_none()
    );
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.leases
            .renew("host", "p1", "b", 1, 100)
            .await
            .unwrap()
            .is_none()
    );
    rig.now.store(1_100, Ordering::SeqCst);
    assert!(
        rig.leases
            .renew("host", "p1", "a", 1, 100)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn only_the_holder_releases_and_the_epoch_continues() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(!rig.leases.release("host", "p1", "b", 1).await.unwrap());
    assert!(rig.leases.current("host", "p1").await.unwrap().is_some());
    assert!(rig.leases.release("host", "p1", "a", 1).await.unwrap());
    assert!(!rig.leases.release("host", "p1", "a", 1).await.unwrap());
    assert!(rig.leases.current("host", "p1").await.unwrap().is_none());
    let next = rig
        .leases
        .acquire("host", "p1", "b", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(next.epoch, 2);
}

#[tokio::test]
async fn leases_are_per_owner_and_key() {
    let rig = rig();
    rig.leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert!(
        rig.leases
            .acquire("host", "p2", "b", 100)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        rig.leases
            .acquire("cron", "p1", "b", 100)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn a_non_positive_ttl_is_refused() {
    let rig = rig();
    for ttl in [0, -5] {
        let error = rig
            .leases
            .acquire("host", "p1", "a", ttl)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn the_fence_follows_the_epoch() {
    let rig = rig();
    let scope = rig.storage.scope().clone();
    let fence = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap()
        .fence(&scope);
    assert_eq!(fence.collection(), LEASES);
    assert!(fence.validate().is_ok());

    let backend = MemoryStorage::new();
    // A fresh backend whose leases the fence guards.
    let storage = backend.for_scope(&scope).unwrap();
    let clock_now = Arc::new(AtomicU64::new(1_000));
    let clock = Arc::clone(&clock_now);
    let leases =
        Leases::new(storage.documents()).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let a = leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    let fenced = backend.for_scope_fenced(&scope, &a.fence(&scope)).unwrap();
    fenced
        .documents()
        .put(
            "work",
            "w",
            serde_json::json!({"n": 1}),
            crate::Precondition::None,
        )
        .await
        .unwrap();
    clock_now.store(2_000, Ordering::SeqCst);
    leases
        .acquire("host", "p1", "b", 100)
        .await
        .unwrap()
        .unwrap();
    let error = fenced
        .documents()
        .put(
            "work",
            "w",
            serde_json::json!({"n": 2}),
            crate::Precondition::None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
}

#[test]
fn debug_does_not_need_the_clock() {
    assert!(format!("{:?}", rig().leases).contains("Leases"));
}

#[tokio::test]
async fn reacquiring_a_lapsed_or_released_lease_advances_the_epoch_for_the_same_holder() {
    let rig = rig();
    let first = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    // Lapse: the earlier tenure's fence must not authorize the new one.
    rig.now.store(2_000, Ordering::SeqCst);
    let second = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((first.epoch, second.epoch), (1, 2));
    // Release, then the same holder takes it again.
    assert!(rig.leases.release("host", "p1", "a", 2).await.unwrap());
    let third = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.epoch, 3);
    // A live re-acquire and a renew keep the epoch.
    let live = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    let renewed = rig
        .leases
        .renew("host", "p1", "a", 3, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((live.epoch, renewed.epoch), (3, 3));
}

#[tokio::test]
async fn a_stale_tenure_cannot_renew_or_release_the_new_one() {
    let rig = rig();
    let old = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    // The lease lapses and the same holder string reacquires it.
    rig.now.store(2_000, Ordering::SeqCst);
    let new = rig
        .leases
        .acquire("host", "p1", "a", 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((old.epoch, new.epoch), (1, 2));
    // The stalled actor of the first tenure is refused.
    assert!(
        rig.leases
            .renew("host", "p1", "a", old.epoch, 100)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !rig.leases
            .release("host", "p1", "a", old.epoch)
            .await
            .unwrap()
    );
    // The new tenure is untouched: same expiry, still live, still renewable.
    assert_eq!(rig.leases.current("host", "p1").await.unwrap(), Some(new));
    let renewed = rig
        .leases
        .renew("host", "p1", "a", 2, 100)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(renewed.epoch, 2);
    assert!(rig.leases.release("host", "p1", "a", 2).await.unwrap());
}
