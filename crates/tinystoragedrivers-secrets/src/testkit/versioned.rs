//! Conditional-write conformance for stores that implement revision tokens.

use std::panic::resume_unwind;
use tinystoragedrivers_types::{ErrorKind, SecretPrecondition};

use crate::store::SecretStore;

/// Verify the CAS contract on a disposable name in `store`.
///
/// # Panics
///
/// Panics when a store fails a versioned-read or conditional-write guarantee.
pub async fn versioned_secrets_conformance(store: &dyn SecretStore) {
    let name = format!("tsd-cas:{}:{}:token", std::process::id(), unique_suffix());
    let other = format!("{name}:other");

    let outcome = super::CatchUnwind(Box::pin(async {
        let created = store
            .set_if(&name, b"first", SecretPrecondition::Absent)
            .await
            .expect("an absent precondition creates the secret");
        let (first_value, first_read) = store
            .get_versioned(&name)
            .await
            .expect("versioned read succeeds")
            .expect("the created secret exists");
        assert_eq!(first_value.as_slice(), b"first");
        assert_eq!(created, first_read, "write token matches its read snapshot");

        let updated = store
            .set_if(&name, b"second", SecretPrecondition::Version(created))
            .await
            .expect("the returned token authorizes the next write");
        assert_ne!(created, updated, "each successful write advances revision");
        assert_eq!(
            store
                .set_if(&other, b"cross-key", SecretPrecondition::Version(created))
                .await
                .expect_err("a token for one key cannot authorize another")
                .kind(),
            ErrorKind::Conflict
        );
        assert_eq!(
            store
                .set_if(&name, b"stale", SecretPrecondition::Version(created))
                .await
                .expect_err("a stale token cannot write")
                .kind(),
            ErrorKind::Conflict
        );

        store
            .set(&other, b"unrelated")
            .await
            .expect("write another name");
        let current = if store.backend_name() == "encrypted_file" {
            assert_eq!(
                store
                    .set_if(&name, b"third", SecretPrecondition::Version(updated))
                    .await
                    .expect_err("a file rewrite conservatively invalidates snapshot tokens")
                    .kind(),
                ErrorKind::Conflict
            );
            store
                .get_versioned(&name)
                .await
                .expect("versioned read succeeds")
                .expect("secret remains present")
                .1
        } else {
            updated
        };
        let third = store
            .set_if(&name, b"third", SecretPrecondition::Version(current))
            .await
            .expect("the current token authorizes the next write");
        assert_ne!(third, updated, "the successful write returns a new token");
        assert_eq!(
            store
                .get_versioned(&name)
                .await
                .expect("versioned read succeeds")
                .expect("secret remains present")
                .1,
            third,
            "the new token matches the latest read snapshot"
        );

        let before_delete = store
            .get_versioned(&name)
            .await
            .expect("versioned read succeeds")
            .expect("the secret exists")
            .1;
        assert!(store.delete(&name).await.expect("delete succeeds"));
        store
            .set_if(&name, b"recreated", SecretPrecondition::Absent)
            .await
            .expect("an absent precondition recreates the secret");
        assert_eq!(
            store
                .set_if(&name, b"aba", SecretPrecondition::Version(before_delete))
                .await
                .expect_err("delete and recreate cannot revive a stale token")
                .kind(),
            ErrorKind::Conflict
        );
    }))
    .await;
    let _ = store.delete(&name).await;
    let _ = store.delete(&other).await;
    if let Err(panic) = outcome {
        resume_unwind(panic);
    }
}

fn unique_suffix() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos())
}
