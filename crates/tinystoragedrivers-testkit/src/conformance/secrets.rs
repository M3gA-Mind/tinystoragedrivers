//! Secret access, enumeration, and conditional update checks.

use tinystoragedrivers_api::SecretStore;
use tinystoragedrivers_types::{ErrorKind, SecretPrecondition};

use super::{empty, fails, ok, unique};

/// Runs secret behavior checks against one secret handle.
///
/// # Panics
/// Panics when the secret driver violates a required behavior.
pub async fn run(secrets: &dyn SecretStore) {
    let prefix = unique("secret");
    let a = format!("{prefix}_a");
    let b = format!("{prefix}_b");
    ok(secrets.set(&a, b"first").await, "set");
    assert_eq!(
        ok(secrets.get(&a).await, "get").unwrap().as_slice(),
        b"first"
    );
    if secrets.enumerable() {
        let listed = ok(secrets.list(&prefix).await, "list");
        assert_eq!(listed, [a.clone()]);
    }
    match secrets.get_versioned(&a).await {
        Ok(Some((value, version))) => {
            assert_eq!(value.as_slice(), b"first");
            let next = ok(
                secrets
                    .set_if(&a, b"second", SecretPrecondition::Version(version.clone()))
                    .await,
                "conditional set",
            );
            assert_ne!(next, version);
            fails(
                secrets
                    .set_if(&a, b"stale", SecretPrecondition::Version(version))
                    .await,
                ErrorKind::Conflict,
                "stale conditional set",
            );
            assert_eq!(
                ok(secrets.get(&a).await, "read conditional value")
                    .unwrap()
                    .as_slice(),
                b"second"
            );
        }
        Ok(None) => panic!("written secret disappeared"),
        Err(error)
            if error.kind()
                == ErrorKind::Unsupported(
                    tinystoragedrivers_types::Capability::ConditionalSecretWrites,
                ) => {}
        Err(error) => panic!("versioned secret read failed: {error}"),
    }
    fails(
        secrets.set("", b"invalid").await,
        ErrorKind::InvalidInput,
        "empty secret name",
    );
    assert!(ok(secrets.delete(&a).await, "delete"));
    assert!(!ok(secrets.delete(&a).await, "delete missing"));
    ok(secrets.set(&b, b"cleanup").await, "set cleanup");
    assert!(ok(secrets.delete(&b).await, "cleanup"));
    if secrets.enumerable() {
        empty(
            &ok(secrets.list(&prefix).await, "list after cleanup"),
            "deleted secrets",
        );
    }
}
