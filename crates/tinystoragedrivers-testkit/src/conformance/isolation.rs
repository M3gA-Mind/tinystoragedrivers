//! Scope isolation across every storage port.

#![allow(
    clippy::panic,
    clippy::expect_used,
    clippy::too_many_lines,
    reason = "conformance failures are reported by assertions"
)]

use serde_json::json;
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_types::{
    Capability, ExpectedDocument, Filter, NewMessage, Precondition, QueueSpec, ReadCheck, Scope,
    WriteOp,
};

fn ok<T>(result: tinystoragedrivers_types::Result<T>, check: &str) -> T {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{check}: unexpected error {error}"),
    }
}

/// Verify two scopes on one backend cannot read, enumerate, or delete one
/// another's records, streams, keys, blobs, queue messages, or secrets.
pub async fn run(backend: &dyn StorageBackend) {
    let a_scope = Scope::new("conformance-isolation-a").expect("scope a");
    let b_scope = Scope::new("conformance-isolation-b").expect("scope b");
    let a = ok(backend.for_scope(&a_scope), "open scope a");
    let b = ok(backend.for_scope(&b_scope), "open scope b");
    let collection = "isolation_items";
    let stream = "isolation_stream";
    let key = "isolation_key";
    let queue = "isolation_queue";
    let secret = "isolation_secret";

    ok(
        a.documents()
            .put(
                collection,
                "same",
                json!({"owner":"a"}),
                Precondition::Absent,
            )
            .await,
        "a document write",
    );
    assert!(
        ok(
            b.documents().get(collection, "same").await,
            "b document read"
        )
        .is_none()
    );
    ok(
        b.documents()
            .put(
                collection,
                "same",
                json!({"owner":"b"}),
                Precondition::Absent,
            )
            .await,
        "b may create the same document id",
    );
    assert_eq!(
        ok(
            a.documents().count(collection, &Filter::All).await,
            "a count"
        ),
        1
    );
    assert_eq!(
        ok(
            b.documents().delete_where(collection, &Filter::All).await,
            "b delete"
        ),
        1
    );
    assert_eq!(
        ok(
            a.documents().get(collection, "same").await,
            "a document survives"
        )
        .expect("a document remains")
        .doc["owner"],
        "a"
    );
    if a.documents()
        .capabilities()
        .contains(Capability::ValidatedBatch)
    {
        let dependency = ReadCheck {
            collection: collection.into(),
            id: "same".into(),
            expected: ExpectedDocument::Version(
                ok(
                    a.documents().get(collection, "same").await,
                    "validated scope dependency",
                )
                .expect("scope a retained its record")
                .version,
            ),
        };
        let claim = WriteOp::Put {
            collection: "isolation_claims".into(),
            id: "claim".into(),
            doc: json!({"owner":"b"}),
            precondition: Precondition::Absent,
        };
        let hidden = ReadCheck {
            expected: ExpectedDocument::Absent,
            ..dependency
        };
        ok(
            b.documents()
                .atomic_batch_checked(vec![hidden], vec![claim])
                .await,
            "scope b cannot validate scope a's dependency",
        );
        assert!(
            ok(
                a.documents().get("isolation_claims", "claim").await,
                "scope a cannot see scope b's claim",
            )
            .is_none()
        );
    }

    ok(
        a.streams().append(stream, json!("a")).await,
        "a stream append",
    );
    assert_eq!(ok(b.streams().len(stream).await, "b stream len"), 0);
    ok(
        b.streams().append(stream, json!("b")).await,
        "b stream append",
    );
    assert_eq!(ok(a.streams().len(stream).await, "a stream len"), 1);

    ok(a.kv().put(key, json!({"owner":"a"})).await, "a key write");
    assert!(ok(b.kv().get(key).await, "b key read").is_none());
    ok(b.kv().put(key, json!({"owner":"b"})).await, "b key write");
    assert_eq!(
        ok(a.kv().get(key).await, "a key survives")
            .expect("scope a key remains")
            .doc["owner"],
        "a"
    );

    ok(
        a.blobs().put(key, b"a".to_vec(), None).await,
        "a blob write",
    );
    assert!(ok(b.blobs().get(key).await, "b blob read").is_none());
    ok(
        b.blobs().put(key, b"b".to_vec(), None).await,
        "b blob write",
    );
    assert_eq!(
        ok(a.blobs().get(key).await, "a blob survives")
            .expect("scope a blob remains")
            .bytes,
        b"a"
    );

    ok(
        a.queues().ensure_queue(queue, &QueueSpec::default()).await,
        "a queue spec",
    );
    ok(
        a.queues()
            .enqueue(queue, &[NewMessage::new(json!("a"))])
            .await,
        "a enqueue",
    );
    ok(
        b.queues().ensure_queue(queue, &QueueSpec::default()).await,
        "b queue spec",
    );
    assert_eq!(
        ok(b.queues().receive(queue, 1, 60_000, "b").await, "b receive"),
        Vec::new(),
        "scope b cannot receive scope a's message"
    );
    assert_eq!(
        ok(a.queues().receive(queue, 1, 60_000, "a").await, "a receive").len(),
        1
    );

    ok(a.secrets().set(secret, b"a").await, "a secret write");
    assert!(ok(b.secrets().get(secret).await, "b secret read").is_none());
    ok(b.secrets().set(secret, b"b").await, "b secret write");
    assert_eq!(
        ok(a.secrets().get(secret).await, "a secret survives")
            .expect("scope a secret remains")
            .as_slice(),
        b"a"
    );

    ok(
        a.documents().drop_collection(collection).await,
        "a collection drop",
    );
    assert!(
        ok(
            b.documents().get(collection, "same").await,
            "b after a drop"
        )
        .is_none()
    );
}

/// Check nested tenant-like scopes sharing the same textual prefix.
pub async fn nested(backend: &dyn StorageBackend) {
    let parent = Scope::new("conformance-nested").expect("parent scope");
    let child_a = parent.child("agent:a").expect("agent scope");
    let child_b = parent.child("module:b").expect("module scope");
    let scopes = [
        ok(backend.for_scope(&parent), "parent handle"),
        ok(backend.for_scope(&child_a), "agent handle"),
        ok(backend.for_scope(&child_b), "module handle"),
    ];
    let names = ["parent", "agent", "module"];
    let collection = "nested_items";
    let stream = "nested_stream";
    let key = "nested_key";
    for (storage, name) in scopes.iter().zip(names) {
        ok(
            storage
                .documents()
                .put(
                    collection,
                    "same",
                    json!({"owner":name}),
                    Precondition::Absent,
                )
                .await,
            "nested document write",
        );
        ok(
            storage.streams().append(stream, json!(name)).await,
            "nested stream append",
        );
        ok(storage.kv().put(key, json!(name)).await, "nested key write");
        ok(
            storage
                .blobs()
                .put(key, name.as_bytes().to_vec(), None)
                .await,
            "nested blob write",
        );
    }
    for (storage, name) in scopes.iter().zip(names) {
        let doc = ok(
            storage.documents().get(collection, "same").await,
            "nested read",
        )
        .expect("scope has its document");
        assert_eq!(doc.doc["owner"], name);
        assert_eq!(
            ok(
                storage.streams().read_window(stream, 0, 10).await,
                "nested stream read"
            )
            .len(),
            1
        );
        assert_eq!(
            ok(storage.kv().get(key).await, "nested key read")
                .expect("nested scope has its key")
                .doc,
            name
        );
        assert_eq!(
            ok(storage.blobs().get(key).await, "nested blob read")
                .expect("nested scope has its blob")
                .bytes,
            name.as_bytes()
        );
    }
}
