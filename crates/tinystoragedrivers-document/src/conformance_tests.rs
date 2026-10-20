//! Portable document and stream checks run against each standalone v2 engine.

use serde_json::json;
use tinystoragedrivers_api::{DocumentStore, StreamStore};
use tinystoragedrivers_types::{
    Capability, CollectionSpec, ErrorKind, ExpectedDocument, Filter, Precondition, Query,
    ReadCheck, Scope, WriteOp,
};

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{}_{}", std::process::id(), nanos)
}

#[allow(
    clippy::too_many_lines,
    reason = "one shared contract suite keeps every primitive's comparable cases together"
)]
async fn run_portable_cases(documents: &dyn DocumentStore, streams: &dyn StreamStore) {
    let suffix = unique_suffix();
    let collection = format!("portable_{suffix}");
    let stream = format!("portable/{suffix}");
    let spec = CollectionSpec::new(&collection);
    documents.ensure_collection(&spec).await.unwrap();

    let first = documents
        .put(
            &collection,
            "one",
            json!({"state":"ready","n":1}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert_eq!(first.0, 1);
    assert_eq!(
        documents
            .put(&collection, "one", json!({}), Precondition::Absent)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    let page = documents
        .query(&collection, &Query::filter(Filter::eq("state", "ready")))
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].doc["n"], json!(1));

    if documents
        .capabilities()
        .contains(Capability::ValidatedBatch)
    {
        let dependency_version = documents
            .put(
                &collection,
                "dependency",
                json!({"state":"ready"}),
                Precondition::Absent,
            )
            .await
            .unwrap();
        let check = ReadCheck {
            collection: collection.clone(),
            id: "dependency".into(),
            expected: ExpectedDocument::Version(dependency_version),
        };
        documents
            .atomic_batch_checked(
                vec![check.clone()],
                vec![WriteOp::Put {
                    collection: collection.clone(),
                    id: "checked".into(),
                    doc: json!({"claimed":true}),
                    precondition: Precondition::Absent,
                }],
            )
            .await
            .unwrap();
        let stale = documents
            .atomic_batch_checked(
                vec![ReadCheck {
                    expected: ExpectedDocument::Absent,
                    ..check
                }],
                vec![WriteOp::Put {
                    collection: collection.clone(),
                    id: "must-roll-back".into(),
                    doc: json!({"claimed":true}),
                    precondition: Precondition::Absent,
                }],
            )
            .await;
        assert_eq!(stale.unwrap_err().kind(), ErrorKind::Conflict);
        assert!(
            documents
                .get(&collection, "must-roll-back")
                .await
                .unwrap()
                .is_none()
        );

        let result = documents
            .atomic_batch(vec![WriteOp::Put {
                collection: collection.clone(),
                id: "two".into(),
                doc: json!({"n":2}),
                precondition: Precondition::Absent,
            }])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        let failed = documents
            .atomic_batch(vec![
                WriteOp::Put {
                    collection: collection.clone(),
                    id: "three".into(),
                    doc: json!({"n":3}),
                    precondition: Precondition::Absent,
                },
                WriteOp::Put {
                    collection: collection.clone(),
                    id: "one".into(),
                    doc: json!({"n":9}),
                    precondition: Precondition::Absent,
                },
            ])
            .await;
        assert_eq!(failed.unwrap_err().kind(), ErrorKind::Conflict);
        assert!(documents.get(&collection, "three").await.unwrap().is_none());
    }

    assert_eq!(streams.append(&stream, json!("a")).await.unwrap(), 0);
    assert_eq!(
        streams
            .append_batch(&stream, vec![json!("b"), json!("c")])
            .await
            .unwrap(),
        1
    );
    if streams
        .capabilities()
        .contains(Capability::ConditionalStreamAppend)
    {
        assert_eq!(
            streams
                .append_batch_if_offset(&stream, 3, vec![json!("d")])
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            streams
                .append_batch_if_offset(&stream, 3, vec![json!("stale")])
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Conflict
        );
    }
    assert_eq!(streams.truncate_before(&stream, 1).await.unwrap(), 1);
    assert_eq!(
        streams.read_window(&stream, 0, 8).await.unwrap(),
        vec![
            tinystoragedrivers_types::StreamEntry {
                offset: 1,
                value: json!("b"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 2,
                value: json!("c"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 3,
                value: json!("d"),
            }
        ]
    );
    if streams.capabilities().contains(Capability::StreamSeal) {
        let sealed = streams.seal(&stream).await.unwrap();
        assert_eq!((sealed.base, sealed.len), (1, 4));
        assert_eq!(streams.seal(&stream).await.unwrap(), sealed);
        assert_eq!(streams.read_window(&stream, 0, 8).await.unwrap().len(), 3);
        let append_error = streams
            .append(&stream, json!("after seal"))
            .await
            .unwrap_err();
        assert_eq!(append_error.kind(), ErrorKind::Conflict, "{append_error}");
    }

    tinystoragedrivers_testkit::documents::run(documents).await;
    tinystoragedrivers_testkit::validated::run(documents).await;
    tinystoragedrivers_testkit::watch::run(documents).await;
    tinystoragedrivers_testkit::streams::run(streams).await;
}

#[cfg(feature = "memory")]
#[tokio::test]
async fn memory_passes_portable_document_and_stream_cases() {
    let storage = crate::memory::MemoryConnection::new().for_scope(Scope::new("portable").unwrap());
    run_portable_cases(storage.documents().as_ref(), storage.streams().as_ref()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_passes_portable_document_and_stream_cases() {
    let directory = tempfile::tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let scope = Scope::new("portable").unwrap();
    let documents = crate::sqlite::SqliteDocuments::new(connection.clone(), scope.clone());
    let streams = crate::sqlite::SqliteStreams::new(connection, scope);
    run_portable_cases(&documents, &streams).await;
}

#[cfg(feature = "file")]
#[tokio::test]
async fn file_passes_portable_document_and_stream_cases() {
    let directory = tempfile::tempdir().unwrap();
    let connection = tinystoragedrivers_connect::FileConnection::open(directory.path()).unwrap();
    let scope = Scope::new("portable").unwrap();
    let documents = crate::FileDocuments::new(connection.clone(), scope.clone());
    let streams = crate::FileStreams::new(connection, scope);
    run_portable_cases(&documents, &streams).await;
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn mongodb_passes_portable_document_and_stream_cases() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let database = format!("tsd26_portable_{}", unique_suffix());
    let connection = tinystoragedrivers_connect::MongoConnection::connect(&uri, &database)
        .await
        .expect("test MongoDB is available");
    let scope = Scope::new("portable").unwrap();
    let documents = crate::mongodb::MongoDocuments::new(connection.clone(), scope.clone());
    let streams = crate::mongodb::MongoStreams::new(connection.clone(), scope)
        .with_blob_store(std::sync::Arc::new(crate::mongodb::TestBlobs::default()));
    run_portable_cases(&documents, &streams).await;
    connection.database().drop().await.unwrap();
}
