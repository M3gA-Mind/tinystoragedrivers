//! The memory driver against the conformance suite, plus binary values.

use super::*;
use crate::testkit::{secrets_conformance, versioned_secrets_conformance};
use tinystoragedrivers_types::{ErrorKind, SecretPrecondition};

#[tokio::test]
async fn passes_the_secrets_conformance_suite() {
    let store = MemorySecrets::new();
    secrets_conformance(&store).await;
    assert_eq!(store.backend_name(), "memory");
}

#[tokio::test]
async fn passes_versioned_secret_conformance() {
    versioned_secrets_conformance(&MemorySecrets::new()).await;
}

#[tokio::test]
async fn holds_binary_values_and_redacts_debug() {
    let store = MemorySecrets::new();
    store.set("bin", &[0, 0xff, 0x80]).await.unwrap();
    assert_eq!(
        store.get("bin").await.unwrap().unwrap().as_slice(),
        [0, 0xff, 0x80]
    );
    assert_eq!(format!("{store:?}"), "MemorySecrets { len: 1 }");
}

#[test]
fn a_poisoned_lock_is_recovered() {
    let store = std::sync::Arc::new(MemorySecrets::new());
    let clone = std::sync::Arc::clone(&store);
    let _ = std::thread::spawn(move || {
        let _guard = clone.entries();
        panic!("poison the lock");
    })
    .join();
    assert_eq!(store.entries().len(), 0);
}

#[tokio::test]
async fn conditional_writes_reject_stale_versions_and_delete_recreate_aba() {
    let store = MemorySecrets::new();
    let first = store
        .set_if("credential", b"first", SecretPrecondition::Absent)
        .await
        .unwrap();
    assert_eq!(
        store
            .set_if("credential", b"duplicate", SecretPrecondition::Absent)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    store.delete("credential").await.unwrap();
    store
        .set_if("credential", b"recreated", SecretPrecondition::Absent)
        .await
        .unwrap();
    assert_eq!(
        store
            .set_if("credential", b"stale", SecretPrecondition::Version(first))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
}

#[tokio::test]
async fn versioned_reads_bind_revisions_to_a_store_and_snapshot() {
    let first = MemorySecrets::new();
    let second = MemorySecrets::new();
    first.set("credential", b"value").await.unwrap();
    second.set("credential", b"value").await.unwrap();
    let (value, revision) = first.get_versioned("credential").await.unwrap().unwrap();
    assert_eq!(value.as_slice(), b"value");
    assert_ne!(
        second.get_versioned("credential").await.unwrap().unwrap().1,
        revision
    );
    first.set("credential", b"value").await.unwrap();
    assert_ne!(
        first.get_versioned("credential").await.unwrap().unwrap().1,
        revision
    );
}
