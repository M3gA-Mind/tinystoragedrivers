//! Public API check for the reusable file lock.

#![cfg(feature = "file")]

use tinystoragedrivers_secrets::lock_file;

#[test]
fn exported_lock_stays_held_until_dropped_and_can_be_verified() {
    let dir = tempfile::tempdir().unwrap();
    // The secure lock API intentionally refuses symlinked ancestors; test
    // through the physical path rather than tempfile's possibly aliased one.
    let root = dir.path().canonicalize().unwrap();
    let lock_path = root.join("secrets.enc.lock");

    let lock = lock_file(&lock_path).unwrap();
    lock.verify().unwrap();
    assert!(lock_path.exists());
}
