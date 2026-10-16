//! Scope and database isolation checks.

use serde_json::json;

use super::{empty, fails, has, ok, unique};
use crate::backend::{ScopedStorage, StorageBackend};
use crate::capabilities::Capability;
use crate::document::Precondition;
use crate::error::ErrorKind;
use crate::filter::Filter;
use crate::scope::Scope;

/// Two scopes on one backend never see each other's data.
pub(super) async fn run(backend: &dyn StorageBackend) {
    let a = ok(backend.for_scope(&Scope::local()), "scope a");
    let b = ok(
        backend.for_scope(&Scope::new("conformance-b").expect("valid scope")),
        "scope b",
    );
    let coll = unique("iso");
    let stream = unique("iso_stream");
    let key = format!("{}/k", unique("iso_blob"));

    ok(
        a.documents()
            .put(&coll, "same", json!({"owner": "a"}), Precondition::None)
            .await,
        "a put",
    );
    assert!(
        ok(b.documents().get(&coll, "same").await, "b get").is_none(),
        "documents leak"
    );
    ok(
        b.documents()
            .put(&coll, "same", json!({"owner": "b"}), Precondition::Absent)
            .await,
        "b may create the same id",
    );
    collection_listing(&a, &b).await;
    assert_eq!(
        ok(a.documents().count(&coll, &Filter::All).await, "a count"),
        1
    );
    assert_eq!(
        ok(
            b.documents().delete_where(&coll, &Filter::All).await,
            "b delete_where"
        ),
        1,
        "delete_where stays in its scope"
    );
    let still =
        ok(a.documents().get(&coll, "same").await, "a re-read").expect("a's document survives");
    assert_eq!(still.doc["owner"], "a");
    assert!(
        ok(
            b.documents()
                .claim(&coll, &Filter::All, &[], &json!({"x": 1}))
                .await,
            "b claim"
        )
        .is_none(),
        "claims stay in their scope"
    );
    ok(b.documents().drop_collection(&coll).await, "b drop");
    assert!(ok(a.documents().get(&coll, "same").await, "a after b drop").is_some());

    ok(a.streams().append(&stream, json!(1)).await, "a append");
    assert_eq!(
        ok(b.streams().len(&stream).await, "b len"),
        0,
        "streams leak"
    );
    if has(&a, Capability::ConditionalStreamAppend) && has(&b, Capability::ConditionalStreamAppend)
    {
        ok(
            a.streams()
                .append_batch_if_offset(&stream, 1, vec![json!(2)])
                .await,
            "scope a conditional",
        );
        ok(
            b.streams()
                .append_batch_if_offset(&stream, 0, vec![json!(3)])
                .await,
            "scope b conditional",
        );
        assert_eq!(ok(a.streams().len(&stream).await, "scope a offset"), 2);
        ok(
            b.streams().delete_stream(&stream).await,
            "cleanup scope b conditional",
        );
    }
    empty(
        &ok(b.streams().streams(&stream).await, "b list"),
        "expected nothing",
    );

    ok(a.blobs().put(&key, vec![1], None).await, "a blob");
    assert!(
        ok(b.blobs().get(&key).await, "b blob").is_none(),
        "blobs leak"
    );
    empty(
        &ok(b.blobs().list(&key).await, "b list blobs"),
        "expected nothing",
    );

    assert_eq!(b.scope().as_str(), "conformance-b");
}

async fn collection_listing(a: &ScopedStorage, b: &ScopedStorage) {
    if !has(a, Capability::CollectionEnumeration) || !has(b, Capability::CollectionEnumeration) {
        for storage in [a, b] {
            if !has(storage, Capability::CollectionEnumeration) {
                fails(
                    storage.documents().collections("").await,
                    ErrorKind::Unsupported(Capability::CollectionEnumeration),
                    "scope collections without the capability",
                );
            }
        }
        return;
    }
    let only_a = unique("iso_only_a");
    let only_b = unique("iso_only_b");
    ok(
        a.documents()
            .put(&only_a, "only", json!({}), Precondition::None)
            .await,
        "a put in an A-only collection",
    );
    ok(
        b.documents()
            .put(&only_b, "only", json!({}), Precondition::None)
            .await,
        "b put in a B-only collection",
    );
    assert_eq!(
        ok(a.documents().collections(&only_b).await, "a list B-only"),
        Vec::<String>::new(),
        "A cannot enumerate B-only collections"
    );
    assert_eq!(
        ok(b.documents().collections(&only_a).await, "b list A-only"),
        Vec::<String>::new(),
        "B cannot enumerate A-only collections"
    );
}
