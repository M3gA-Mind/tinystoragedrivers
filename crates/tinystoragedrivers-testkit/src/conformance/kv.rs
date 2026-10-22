//! Version, conditional write, and listing checks for key/value ports.

use std::time::Duration;

use serde_json::json;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_types::{ErrorKind, Precondition};

use super::{empty, fails, ok, unique};

/// Runs key/value behavior checks against one KV handle.
///
/// # Panics
/// Panics when the KV driver violates a required behavior.
pub async fn run(kv: &dyn KvStore) {
    let prefix = unique("kv");
    let a = format!("{prefix}/a");
    let b = format!("{prefix}/b");
    let first = ok(kv.put(&a, json!({"n": 1})).await, "put");
    fails(
        kv.cas(&a, json!({"n": 8}), Precondition::Absent).await,
        ErrorKind::Conflict,
        "insert requires absence",
    );
    assert_eq!(
        ok(kv.get(&a).await, "get").expect("put key is present").doc,
        json!({"n": 1})
    );
    let second = ok(
        kv.cas(&a, json!({"n": 2}), Precondition::Version(first))
            .await,
        "cas",
    );
    assert_ne!(second, first);
    fails(
        kv.cas(&a, json!({"n": 3}), Precondition::Version(first))
            .await,
        ErrorKind::Conflict,
        "stale CAS",
    );
    assert!(ok(
        kv.delete(&a, Precondition::Version(second)).await,
        "delete before recreation"
    ));
    let recreated = ok(kv.put(&a, json!({"n": 5})).await, "recreate key");
    assert_ne!(recreated, second, "recreated keys must not reuse revisions");
    fails(
        kv.cas(&a, json!({"n": 6}), Precondition::Version(second))
            .await,
        ErrorKind::Conflict,
        "stale pre-delete revision after recreation",
    );
    if kv
        .capabilities()
        .contains(tinystoragedrivers_types::Capability::Ttl)
    {
        ok(
            kv.put_with_ttl(
                &format!("{prefix}/ttl"),
                json!("live"),
                Duration::from_secs(60),
            )
            .await,
            "put native TTL",
        );
        assert!(ok(kv.get(&format!("{prefix}/ttl")).await, "read live TTL").is_some());
    }
    ok(kv.put(&b, json!({"n": 4})).await, "put sibling");
    let page = ok(kv.list(&prefix, None, 1).await, "list first page");
    assert_eq!(page.items.len(), 1);
    let cursor = page.next.expect("second entry has a cursor");
    let next = ok(kv.list(&prefix, Some(cursor), 10).await, "list next page");
    assert_ne!(next.items, Vec::new(), "continuation page has items");
    assert_ne!(page.items[0].id, next.items[0].id);
    assert!(ok(
        kv.delete(&a, Precondition::Version(recreated)).await,
        "conditional delete"
    ));
    assert!(!ok(
        kv.delete(&a, Precondition::None).await,
        "delete missing"
    ));
    fails(
        kv.put("", json!(null)).await,
        ErrorKind::InvalidInput,
        "empty key",
    );
    ok(kv.delete(&b, Precondition::None).await, "cleanup");
    if kv
        .capabilities()
        .contains(tinystoragedrivers_types::Capability::Ttl)
    {
        ok(
            kv.delete(&format!("{prefix}/ttl"), Precondition::None)
                .await,
            "cleanup TTL",
        );
    }
    empty(
        &ok(kv.list(&prefix, None, 10).await, "list after cleanup").items,
        "deleted keys",
    );
}
