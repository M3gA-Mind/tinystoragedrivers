//! Public API check for reusable atomic filesystem operations.

#![cfg(feature = "file")]

use tinystoragedrivers_document::fsio::{fsync_dir, write_atomic};

#[test]
fn exported_atomic_write_and_directory_sync_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");

    write_atomic(&path, b"first").unwrap();
    write_atomic(&path, b"second").unwrap();
    fsync_dir(dir.path()).unwrap();

    assert_eq!(std::fs::read(path).unwrap(), b"second");
}
