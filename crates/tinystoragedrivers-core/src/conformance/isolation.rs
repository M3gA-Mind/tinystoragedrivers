//! Scope and database isolation checks.

use serde_json::json;

use super::{empty, fails, ok, unique};
use crate::backend::{ScopedStorage, StorageBackend};
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
        ok(
            a.documents().collections("iso_only_b").await,
            "a list B-only"
        ),
        Vec::<String>::new(),
        "A cannot enumerate B-only collections"
    );
    assert_eq!(
        ok(
            b.documents().collections("iso_only_a").await,
            "b list A-only"
        ),
        Vec::<String>::new(),
        "B cannot enumerate A-only collections"
    );
}

/// Named databases on one backend are independent of each other and of the
/// root.
pub(super) async fn databases(backend: &dyn StorageBackend) {
    let first = ok(backend.database("conformance_one"), "database one");
    let again = ok(backend.database("conformance_one"), "database one again");
    let second = ok(backend.database("conformance_two"), "database two");
    let coll = unique("db");

    let one = ok(first.for_scope(&Scope::local()), "scope one");
    let one_again = ok(again.for_scope(&Scope::local()), "scope one again");
    let two = ok(second.for_scope(&Scope::local()), "scope two");
    let root = ok(backend.for_scope(&Scope::local()), "root scope");

    ok(
        one.documents()
            .put(&coll, "x", json!({}), Precondition::None)
            .await,
        "write one",
    );
    assert!(
        ok(
            one_again.documents().get(&coll, "x").await,
            "same name, same data"
        )
        .is_some()
    );
    assert!(ok(two.documents().get(&coll, "x").await, "other database").is_none());
    assert!(ok(root.documents().get(&coll, "x").await, "root").is_none());

    let stream = unique("db_stream");
    ok(one.streams().append(&stream, json!(1)).await, "append one");
    assert_eq!(ok(one_again.streams().len(&stream).await, "same stream"), 1);
    assert_eq!(ok(two.streams().len(&stream).await, "other stream"), 0);
    assert_eq!(ok(root.streams().len(&stream).await, "root stream"), 0);

    let key = format!("{}/k", unique("db_blob"));
    ok(one.blobs().put(&key, vec![1], None).await, "blob one");
    assert!(ok(one_again.blobs().head(&key).await, "same blob").is_some());
    assert!(ok(two.blobs().head(&key).await, "other blob").is_none());
    assert!(ok(root.blobs().head(&key).await, "root blob").is_none());

    fails(
        backend.database("Bad Name").map(|_| ()),
        ErrorKind::InvalidInput,
        "invalid database",
    );
}
