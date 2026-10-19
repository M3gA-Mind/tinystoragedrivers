//! Tests for the shared lease contract.

use super::*;

#[cfg(feature = "file")]
#[tokio::test]
async fn a_local_lock_excludes_an_independent_store_until_release() {
    let root = tempfile::tempdir().unwrap();
    let first = LocalLeases::new(root.path(), "node-a", 30_000).unwrap();
    let second = LocalLeases::new(root.path(), "node-b", 30_000).unwrap();

    let grant = first.acquire("alice", 1_000).await.unwrap();
    assert!(matches!(
        second.acquire("alice", 1_000).await,
        Err(LeaseError::Held(_))
    ));

    first.release(grant).await.unwrap();
    let next = second.acquire("alice", 1_000).await.unwrap();
    assert_eq!(next.epoch, 2);
    assert!(!next.previous_unclean);
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_lock_excludes_a_separate_process() {
    if std::env::var_os("TSD_LOCAL_LOCK_CHILD").is_some() {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let first = LocalLeases::new(root.path(), "parent", 30_000).unwrap();
    let grant = first.acquire("alice", 1_000).await.unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "tests::local_lock_child_observes_parent",
        ])
        .env("TSD_LOCAL_LOCK_CHILD", "1")
        .env("TSD_LOCAL_LOCK_ROOT", root.path())
        .status()
        .unwrap();
    assert!(result.success());
    first.release(grant).await.unwrap();
}

#[cfg(feature = "file")]
#[tokio::test]
#[ignore = "invoked as a child process by local_lock_excludes_a_separate_process"]
async fn local_lock_child_observes_parent() {
    let root = std::path::PathBuf::from(std::env::var_os("TSD_LOCAL_LOCK_ROOT").unwrap());
    let child = LocalLeases::new(root, "child", 30_000).unwrap();
    assert!(matches!(
        child.acquire("alice", 1_000).await,
        Err(LeaseError::Held(_))
    ));
}

#[cfg(feature = "file")]
#[tokio::test]
async fn local_reentry_preserves_epoch_and_expiry_is_reported() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalLeases::new(root.path(), "node-a", 100).unwrap();
    let grant = store.acquire("alice", 1_000).await.unwrap();
    let reentrant = store.acquire("alice", 1_025).await.unwrap();
    assert_eq!(reentrant.epoch, grant.epoch);
    assert_eq!(reentrant.expires_ms, 1_125);
    assert!(!reentrant.previous_unclean);
    assert!(matches!(
        store.renew(&grant, 1_040).await,
        Err(LeaseError::Lost)
    ));
    let renewed = store.renew(&reentrant, 1_050).await.unwrap();
    assert_eq!(renewed.version.0, reentrant.version.0 + 1);
    let other = LocalLeases::new(root.path(), "node-b", 100).unwrap();
    assert!(matches!(
        other.acquire("alice", 1_200).await,
        Err(LeaseError::Held(_))
    ));
    assert_eq!(store.driver_fence("alice", "node-a", grant.epoch), None);
}

#[test]
fn retry_hint_is_zero_for_released_and_lapsed_records() {
    let live = LeaseRecord {
        holder: "node".into(),
        endpoint: None,
        epoch: 3,
        expires_ms: 1_500,
        released: false,
    };
    assert!(is_live(&live, 1_499));
    assert_eq!(retry_after_ms(&live, 1_000), 500);
    assert!(!is_live(&live, 1_500));
    assert_eq!(retry_after_ms(&live, 1_500), 0);
    let released = LeaseRecord {
        released: true,
        ..live
    };
    assert_eq!(retry_after_ms(&released, 1_000), 0);
}

#[test]
fn canonical_lease_record_reads_the_previous_host_field_names() {
    let record: LeaseRecord = serde_json::from_value(serde_json::json!({
        "owner": "node-a", "epoch": 9, "expires_at_ms": 5000
    }))
    .unwrap();
    assert_eq!(record.holder, "node-a");
    assert_eq!(record.expires_ms, 5000);
    assert_eq!(record.endpoint, None);
    assert!(!record.released);
}
