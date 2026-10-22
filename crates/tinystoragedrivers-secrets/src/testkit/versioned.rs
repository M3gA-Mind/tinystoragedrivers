//! Conditional-write conformance for stores that implement revision tokens.

use std::panic::resume_unwind;

use tinystoragedrivers_types::{ErrorKind, Result, SecretPrecondition, SecretVersion};

use crate::store::SecretStore;

/// Verify the CAS contract on disposable names in `store`.
///
/// # Panics
///
/// Panics when a store fails a versioned-read or conditional-write guarantee.
pub async fn versioned_secrets_conformance(store: &dyn SecretStore) {
    let name = format!("tsd-cas:{}:{}:token", std::process::id(), unique_suffix());
    let other = format!("{name}:other");
    let outcome = super::CatchUnwind(Box::pin(async {
        let created = absent_write_and_read(store, &name).await;
        let updated = conditional_updates(store, &name, &other, created).await;
        delete_and_recreate(store, &name, updated).await;
    }))
    .await;
    let _ = store.delete(&name).await;
    let _ = store.delete(&other).await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

async fn absent_write_and_read(store: &dyn SecretStore, name: &str) -> SecretVersion {
    let missing = format!("{name}:missing");
    assert!(
        store
            .get_versioned(&missing)
            .await
            .expect("versioned reads accept missing names")
            .is_none()
    );
    let created = store
        .set_if(name, b"first", SecretPrecondition::Absent)
        .await
        .expect("an absent precondition creates the secret");
    let (value, read_token) = store
        .get_versioned(name)
        .await
        .expect("versioned read succeeds")
        .expect("the created secret exists");
    assert_eq!(value.as_slice(), b"first");
    assert_eq!(created, read_token, "write token matches its read snapshot");
    assert_eq!(
        store
            .set_if(name, b"duplicate", SecretPrecondition::Absent)
            .await
            .expect_err("an existing secret does not satisfy Absent")
            .kind(),
        ErrorKind::Conflict
    );
    created
}

async fn conditional_updates(
    store: &dyn SecretStore,
    name: &str,
    other: &str,
    created: SecretVersion,
) -> SecretVersion {
    let updated = store
        .set_if(name, b"second", SecretPrecondition::Version(created))
        .await
        .expect("the returned token authorizes the next write");
    assert_ne!(created, updated, "each successful write advances revision");
    expect_conflict(
        store
            .set_if(other, b"cross-key", SecretPrecondition::Version(created))
            .await,
        "a token for one key cannot authorize another",
    );
    expect_conflict(
        store
            .set_if(name, b"stale", SecretPrecondition::Version(created))
            .await,
        "a stale token cannot write",
    );
    store
        .set(other, b"unrelated")
        .await
        .expect("write another name");
    let current = if store.backend_name() == "encrypted_file" {
        expect_conflict(
            store
                .set_if(
                    name,
                    b"stale-file-snapshot",
                    SecretPrecondition::Version(updated),
                )
                .await,
            "a file rewrite conservatively invalidates snapshot tokens",
        );
        store
            .get_versioned(name)
            .await
            .expect("versioned read succeeds")
            .expect("secret remains present")
            .1
    } else {
        updated
    };
    let written = store
        .set_if(name, b"third", SecretPrecondition::Version(current))
        .await
        .expect("the current token authorizes the next write");
    assert_ne!(written, updated, "the successful write returns a new token");
    assert_eq!(
        store
            .get_versioned(name)
            .await
            .expect("versioned read succeeds")
            .expect("secret remains present")
            .1,
        written,
        "the new token matches the latest read snapshot"
    );
    written
}

async fn delete_and_recreate(store: &dyn SecretStore, name: &str, previous: SecretVersion) {
    assert!(store.delete(name).await.expect("delete succeeds"));
    store
        .set_if(name, b"recreated", SecretPrecondition::Absent)
        .await
        .expect("an absent precondition recreates the secret");
    expect_conflict(
        store
            .set_if(name, b"aba", SecretPrecondition::Version(previous))
            .await,
        "delete and recreate cannot revive a stale token",
    );
}

fn expect_conflict(result: Result<SecretVersion>, reason: &str) {
    assert_eq!(
        result.expect_err(reason).kind(),
        ErrorKind::Conflict,
        "{reason}"
    );
}

fn unique_suffix() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}
