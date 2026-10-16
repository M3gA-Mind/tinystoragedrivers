//! Scope enumeration and maintenance checks, gated on their capabilities.

use serde_json::json;

use super::{fails, ok, unique};
use crate::backend::{Checkpoint, MaintenanceBudget, StorageBackend};
use crate::capabilities::Capability;
use crate::document::Precondition;
use crate::error::ErrorKind;
use crate::scope::Scope;

pub(super) async fn run(backend: &dyn StorageBackend) {
    scopes(backend).await;
    maintenance(backend).await;
}

fn scope(text: &str) -> Scope {
    Scope::new(text).expect("valid scope")
}

/// The scopes `scopes` is checked against, one per way a scope holds a record.
struct Seeded {
    root: String,
    /// The scopes that hold a record under `root`, in byte order.
    expected: Vec<Scope>,
    /// The one that holds only a tombstone.
    tombstone: Scope,
}

/// Write one record into each of several scopes sharing a unique prefix, plus
/// an opened-but-empty scope and a lookalike a `_` wildcard would match.
async fn seed(backend: &dyn StorageBackend) -> Seeded {
    let root = unique("scopes");
    let coll = unique("scopes_coll");
    let by_doc = scope(&root);
    let by_stream = scope(&format!("{root}/agent:a"));
    let by_blob = scope(&format!("{root}/agent:b"));
    let tombstone = scope(&format!("{root}%"));
    // Differs from `root` only where a `_` wildcard would match.
    let lookalike = scope(&root.replacen('_', "-", 1));
    let docs = |scope: &Scope| {
        ok(backend.for_scope(scope), "scopes: for_scope")
            .documents()
            .clone()
    };
    for scope in [&by_doc, &lookalike] {
        ok(
            docs(scope)
                .put(&coll, "d", json!({"n": 1}), Precondition::None)
                .await,
            "scopes: document",
        );
    }
    ok(
        ok(backend.for_scope(&by_stream), "scopes: stream scope")
            .streams()
            .append(&unique("scopes_stream"), json!(1))
            .await,
        "scopes: stream",
    );
    ok(
        ok(backend.for_scope(&by_blob), "scopes: blob scope")
            .blobs()
            .put("k", vec![1], None)
            .await,
        "scopes: blob",
    );
    let gone = docs(&tombstone);
    ok(
        gone.put(&coll, "gone", json!({"n": 1}), Precondition::None)
            .await,
        "scopes: tombstone seed",
    );
    ok(
        gone.delete(&coll, "gone", Precondition::None).await,
        "scopes: delete",
    );
    drop(ok(
        backend.for_scope(&scope(&format!("{root}-empty"))),
        "scopes: untouched",
    ));
    let mut expected = vec![by_doc, tombstone.clone(), by_stream, by_blob];
    expected.sort_by(|a, b| a.as_str().as_bytes().cmp(b.as_str().as_bytes()));
    Seeded {
        root,
        expected,
        tombstone,
    }
}

/// `scopes` lists exactly the scopes with a stored record, by literal prefix,
/// sorted.
async fn scopes(backend: &dyn StorageBackend) {
    if !backend
        .capabilities()
        .contains(Capability::ScopeEnumeration)
    {
        fails(
            backend.scopes("").await,
            ErrorKind::Unsupported(Capability::ScopeEnumeration),
            "scopes without the capability",
        );
        return;
    }
    let Seeded {
        root,
        expected,
        tombstone,
    } = seed(backend).await;
    assert_eq!(
        ok(backend.scopes(&root).await, "scopes: by prefix"),
        expected,
        "scopes lists exactly the written scopes, sorted, by literal prefix"
    );
    assert_eq!(
        ok(
            backend.scopes(&format!("{root}/")).await,
            "scopes: principals"
        ),
        vec![expected[2].clone(), expected[3].clone()],
        "a prefix ending in a separator lists only what sits under it"
    );
    assert_eq!(
        ok(
            backend.scopes(&format!("{root}%")).await,
            "scopes: metacharacter"
        ),
        vec![tombstone],
        "a `%` in the prefix is literal, and a deleted document still counts"
    );
    assert!(
        ok(
            backend.scopes(&format!("{root}~")).await,
            "scopes: no match"
        )
        .is_empty(),
        "a prefix nothing starts with lists nothing"
    );
    let everything = ok(backend.scopes("").await, "scopes: empty prefix");
    assert!(
        expected.iter().all(|scope| everything.contains(scope)),
        "the empty prefix lists every scope"
    );
    assert!(
        everything
            .windows(2)
            .all(|pair| pair[0].as_str().as_bytes() < pair[1].as_str().as_bytes()),
        "the listing is sorted and free of duplicates"
    );
}

/// `maintain` and `checkpoint` succeed and leave data readable, or refuse.
async fn maintenance(backend: &dyn StorageBackend) {
    if !backend.capabilities().contains(Capability::Maintenance) {
        fails(
            backend.maintain(MaintenanceBudget::default()).await,
            ErrorKind::Unsupported(Capability::Maintenance),
            "maintain without the capability",
        );
        fails(
            backend.checkpoint(Checkpoint::Passive).await,
            ErrorKind::Unsupported(Capability::Maintenance),
            "checkpoint without the capability",
        );
        return;
    }
    let coll = unique("maint");
    let docs = ok(backend.for_scope(&Scope::local()), "maintenance: for_scope")
        .documents()
        .clone();
    ok(
        docs.put(&coll, "kept", json!({"n": 1}), Precondition::None)
            .await,
        "maintenance: put",
    );
    ok(
        backend.maintain(MaintenanceBudget::unlimited()).await,
        "maintain unlimited",
    );
    ok(
        backend.maintain(MaintenanceBudget::reclaim_up_to(0)).await,
        "maintain with an empty budget",
    );
    ok(
        backend.checkpoint(Checkpoint::Passive).await,
        "passive checkpoint",
    );
    ok(
        backend.checkpoint(Checkpoint::Truncate).await,
        "truncate checkpoint",
    );
    let kept = ok(docs.get(&coll, "kept").await, "maintenance: get");
    assert!(kept.is_some(), "maintenance must not lose records");
}
