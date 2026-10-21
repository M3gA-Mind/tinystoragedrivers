//! Local lease validation, durable-record, and filesystem failure tests.

use super::*;
use tinystoragedrivers_core::{ErrorKind, Version};

#[test]
fn constructor_rejects_empty_holder_and_non_directory_root() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        LocalLeases::new(dir.path(), "").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );

    let root = dir.path().join("regular-file");
    std::fs::write(&root, b"not a directory").unwrap();
    assert_eq!(
        LocalLeases::new(&root, "node-a").unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[tokio::test]
async fn empty_keys_are_rejected_before_filesystem_access() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalLeases::new(dir.path(), "node-a").unwrap();

    let error = store.inspect("").await.unwrap_err();

    assert!(matches!(
        error,
        LeaseError::Storage(storage) if storage.kind() == ErrorKind::InvalidInput
    ));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn malformed_persisted_records_fail_as_serialization_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalLeases::new(dir.path(), "node-a").unwrap();
    let record = store.directory("alice").unwrap().join(RECORD_FILE);
    std::fs::create_dir_all(record.parent().unwrap()).unwrap();
    std::fs::write(&record, b"{not-json").unwrap();

    let error = store.inspect("alice").await.unwrap_err();

    assert!(matches!(
        error,
        LeaseError::Storage(storage) if storage.kind() == ErrorKind::Serialization
    ));
    assert_eq!(std::fs::read(&record).unwrap(), b"{not-json");
}

#[tokio::test]
async fn filesystem_errors_reading_a_record_remain_backend_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalLeases::new(dir.path(), "node-a").unwrap();
    let record_dir = store.directory("alice").unwrap();
    std::fs::write(&record_dir, b"not a directory").unwrap();

    let error = store.inspect("alice").await.unwrap_err();

    assert!(matches!(
        error,
        LeaseError::Storage(storage) if storage.kind() == ErrorKind::Backend
    ));
}

#[tokio::test]
async fn renewal_reports_version_space_exhaustion() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalLeases::new(dir.path(), "node-a").unwrap();
    let grant = store.acquire("alice", 1_000).await.unwrap();
    {
        let mut held = store.held();
        held.get_mut("alice").unwrap().version = Version(u64::MAX);
    }
    let overflow = LeaseGrant {
        version: Version(u64::MAX),
        ..grant
    };

    let error = store.renew(&overflow, 1_001).await.unwrap_err();

    assert!(matches!(
        error,
        LeaseError::Storage(storage)
            if storage.to_string().contains("lease version space exhausted")
    ));
}
