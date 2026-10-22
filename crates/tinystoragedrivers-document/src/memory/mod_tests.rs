use super::*;
use tinystoragedrivers_types::{Capability, CollectionSpec, Precondition};

#[tokio::test]
async fn scoped_document_and_stream_handles_share_only_their_scope() {
    let connection = MemoryConnection::new();
    let alice = connection.for_scope(Scope::new("alice").unwrap());
    let bob = connection.for_scope(Scope::new("bob").unwrap());
    let alice_documents = alice.documents();
    let bob_documents = bob.documents();
    let alice_streams = alice.streams();
    let bob_streams = bob.streams();
    assert!(
        alice_documents
            .capabilities()
            .contains(Capability::Documents)
    );
    assert!(alice_streams.capabilities().contains(Capability::Streams));

    let spec = CollectionSpec::new("notes");
    alice_documents.ensure_collection(&spec).await.unwrap();
    alice_documents
        .put(
            "notes",
            "one",
            serde_json::json!({"text": "private"}),
            Precondition::None,
        )
        .await
        .unwrap();
    alice_streams
        .append("events", serde_json::json!({"kind": "private"}))
        .await
        .unwrap();

    assert!(bob_documents.get("notes", "one").await.unwrap().is_none());
    assert_eq!(bob_streams.len("events").await.unwrap(), 0);
    assert_eq!(alice_streams.len("events").await.unwrap(), 1);
}

#[tokio::test]
async fn cloned_connections_share_state_and_injected_clock() {
    let connection = MemoryConnection::with_clock(Arc::new(|| 42));
    let cloned_connection = connection.clone();
    let storage = cloned_connection.for_scope(Scope::new("shared").unwrap());
    let cloned_storage = storage.clone();
    assert!(format!("{connection:?}").contains("MemoryConnection"));
    assert!(format!("{storage:?}").contains("MemoryStorage"));

    let documents = storage.documents();
    documents
        .put(
            "notes",
            "one",
            serde_json::json!({"value": 1}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        cloned_storage
            .documents()
            .get("notes", "one")
            .await
            .unwrap()
            .unwrap()
            .meta
            .created_ms,
        42
    );
    assert_eq!(
        cloned_storage
            .streams()
            .append("events", serde_json::json!("first"))
            .await
            .unwrap(),
        0
    );
    assert_eq!(cloned_storage.streams().len("events").await.unwrap(), 1);

    let default_connection = MemoryConnection::default();
    assert!(format!("{default_connection:?}").contains("MemoryConnection"));
}

#[tokio::test]
async fn memory_streams_preserve_offsets_and_seal_lifecycle() {
    let storage =
        MemoryConnection::with_clock(Arc::new(|| 100)).for_scope(Scope::new("local").unwrap());
    let streams = storage.streams();
    assert!(streams.capabilities().contains(Capability::StreamSeal));
    assert_eq!(streams.info("events").await.unwrap(), None);
    assert_eq!(streams.append_batch("events", vec![]).await.unwrap(), 0);
    assert_eq!(
        streams
            .append_batch(
                "events",
                vec![serde_json::json!("a"), serde_json::json!("b")]
            )
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        streams
            .append_batch_if_offset("events", 2, vec![serde_json::json!("c")])
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        streams
            .append_batch_if_offset("events", 2, vec![])
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(streams.truncate_before("events", 1).await.unwrap(), 1);
    let outcome = streams.seal("events").await.unwrap();
    assert_eq!((outcome.base, outcome.len), (1, 3));
    assert_eq!(streams.seal("events").await.unwrap(), outcome);
    assert_eq!(
        streams
            .info("events")
            .await
            .unwrap()
            .unwrap()
            .last_append_ms,
        Some(100)
    );
    assert_eq!(
        streams
            .read_window("events", 0, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.offset, entry.value))
            .collect::<Vec<_>>(),
        vec![(1, serde_json::json!("b")), (2, serde_json::json!("c"))]
    );
    assert_eq!(
        streams
            .append("events", serde_json::json!("late"))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    let sealed_ms = streams
        .info("events")
        .await
        .unwrap()
        .unwrap()
        .sealed_ms
        .unwrap();
    assert!(
        !streams
            .delete_sealed_stream("events", sealed_ms + 1)
            .await
            .unwrap()
    );
    assert!(
        streams
            .delete_sealed_stream("events", sealed_ms)
            .await
            .unwrap()
    );
    assert_eq!(
        streams
            .append("events", serde_json::json!("new"))
            .await
            .unwrap(),
        0
    );
}
