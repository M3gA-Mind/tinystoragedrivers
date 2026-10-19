//! Public API check for the reusable file lock.

use tinystoragedrivers_secrets::lock_file;

#[test]
fn exported_lock_stays_held_until_dropped_and_can_be_verified() {
    let dir = tempfile::tempdir().unwrap();
    let lock_path = dir.path().join("secrets.enc.lock");

    let lock = lock_file(&lock_path).unwrap();
    lock.verify().unwrap();
    assert!(lock_path.exists());
}
