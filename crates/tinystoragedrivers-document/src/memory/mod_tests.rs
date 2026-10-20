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
