use crate::file::encode::{dir_components, file_stem};
use crate::{FileDocuments, FileStreams};
use tempfile::tempdir;
use tinystoragedrivers_api::{DocumentStore, StreamStore};
use tinystoragedrivers_types::{Capability, CollectionSpec, Precondition, Scope};

#[tokio::test]
async fn file_documents_and_streams_share_root_and_keep_scope_isolated() {
    let directory = tempdir().unwrap();
    let connection = tinystoragedrivers_connect::FileConnection::open(directory.path()).unwrap();
    let alice = Scope::new("alice").unwrap();
    let bob = Scope::new("bob").unwrap();
    let documents = FileDocuments::new(connection.clone(), alice.clone());
    let streams = FileStreams::new(connection.clone(), alice);
    assert!(documents.capabilities().contains(Capability::Documents));
    assert!(streams.capabilities().contains(Capability::Streams));

    documents
        .ensure_collection(&CollectionSpec::new("notes"))
        .await
        .unwrap();
    documents
        .put(
            "notes",
            "one",
            serde_json::json!({"text":"saved"}),
            Precondition::None,
        )
        .await
        .unwrap();
    streams
        .append_batch("events", vec![serde_json::json!({"n":1})])
        .await
        .unwrap();
    let seal = streams.seal("events").await.unwrap();
    assert_eq!(seal.len, 1);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap()[0].value,
        serde_json::json!({"n":1})
    );

    let other_docs = FileDocuments::new(connection.clone(), bob.clone());
    let other_streams = FileStreams::new(connection, bob);
    assert!(other_docs.get("notes", "one").await.unwrap().is_none());
    assert_eq!(other_streams.len("events").await.unwrap(), 0);
}

#[tokio::test]
async fn reads_legacy_file_documents_and_streams_in_place() {
    let directory = tempdir().unwrap();
    let scope = Scope::new("legacy:scope").unwrap();
    let scope_path = dir_components(scope.as_str());
    let docs = directory
        .path()
        .join("scopes")
        .join(&scope_path)
        .join("docs")
        .join(dir_components("notes"));
    std::fs::create_dir_all(&docs).unwrap();
    std::fs::write(
        docs.join(format!("{}.json", file_stem("old"))),
        r#"{"id":"old","version":3,"doc":{"text":"from 1.0"}}"#,
    )
    .unwrap();
    let stream_dir = directory
        .path()
        .join("scopes")
        .join(scope_path)
        .join("streams");
    std::fs::create_dir_all(&stream_dir).unwrap();
    std::fs::write(
        stream_dir.join(format!("{}.meta.json", file_stem("events"))),
        r#"{"name":"events","base":0}"#,
    )
    .unwrap();
    std::fs::write(
        stream_dir.join(format!("{}.jsonl", file_stem("events"))),
        "{\"offset\":0,\"value\":{\"kind\":\"old\"}}\n",
    )
    .unwrap();

    let connection = tinystoragedrivers_connect::FileConnection::open(directory.path()).unwrap();
    let documents = FileDocuments::new(connection.clone(), scope.clone());
    let streams = FileStreams::new(connection, scope);
    assert_eq!(
        documents
            .get("notes", "old")
            .await
            .unwrap()
            .unwrap()
            .version
            .0,
        3
    );
    assert_eq!(
        streams.read_window("events", 0, 1).await.unwrap()[0].value,
        serde_json::json!({"kind":"old"})
    );
}
