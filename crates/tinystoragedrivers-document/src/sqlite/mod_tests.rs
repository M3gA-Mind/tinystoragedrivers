use super::*;
use tempfile::tempdir;
use tinystoragedrivers_api::{DocumentStore, StreamStore};
use tinystoragedrivers_types::{
    Capability, CollectionSpec, ExpectedDocument, Filter, IndexSpec, Precondition, Query,
    ReadCheck, Scope, Sort, WriteCondition, WriteOp,
};

#[tokio::test]
async fn conditional_write_rejects_guard_expired_by_collection_ttl() {
    let directory = tempdir().unwrap();
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open_with_clock(
        directory.path(),
        std::sync::Arc::new(|| 100),
    )
    .unwrap();
    let guard_scope = Scope::new("ttl-guard").unwrap();
    let target_scope = Scope::new("ttl-target").unwrap();
    let ordinary = SqliteDocuments::new(connection.clone(), guard_scope.clone());
    ordinary
        .ensure_collection(&CollectionSpec::new("leases").ttl("expires_at"))
        .await
        .unwrap();
    ordinary
        .put(
            "leases",
            "owner",
            serde_json::json!({"holder": "worker", "expires_at": 99}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert!(ordinary.get("leases", "owner").await.unwrap().is_none());

    let condition = WriteCondition::new(
        guard_scope,
        "leases",
        "owner",
        Filter::eq("holder", "worker"),
    )
    .unwrap();
    let conditioned = SqliteDocuments::conditioned(connection, target_scope, condition);
    let error = conditioned
        .put(
            "items",
            "must-not-write",
            serde_json::json!({"value": true}),
            Precondition::None,
        )
        .await
        .expect_err("a TTL-expired guard must not authorize a mutation");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    assert!(
        conditioned
            .get("items", "must-not-write")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn document_and_stream_engines_share_the_connection_and_scope() {
    let directory = tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let scope = Scope::new("alice").unwrap();
    let documents = SqliteDocuments::new(connection.clone(), scope.clone());
    let streams = SqliteStreams::new(connection.clone(), scope.clone());
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
            serde_json::json!({"text":"shared"}),
            Precondition::None,
        )
        .await
        .unwrap();
    streams
        .append_batch("events", vec![serde_json::json!({"kind":"shared"})])
        .await
        .unwrap();
    let seal = streams.seal("events").await.unwrap();
    assert_eq!(seal.len, 1);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap()[0].value,
        serde_json::json!({"kind":"shared"})
    );
    assert!(
        streams
            .append("events", serde_json::json!({}))
            .await
            .is_err()
    );

    let failed = documents
        .atomic_batch_checked(
            vec![ReadCheck {
                collection: "notes".into(),
                id: "missing".into(),
                expected: ExpectedDocument::Version(tinystoragedrivers_types::Version(1)),
            }],
            vec![WriteOp::Put {
                collection: "notes".into(),
                id: "must-not-write".into(),
                doc: serde_json::json!({}),
                precondition: Precondition::None,
            }],
        )
        .await;
    assert!(failed.is_err());
    assert!(
        documents
            .get("notes", "must-not-write")
            .await
            .unwrap()
            .is_none()
    );

    let reopened =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let other = Scope::new("bob").unwrap();
    assert!(
        SqliteDocuments::new(reopened.clone(), other.clone())
            .get("notes", "one")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        SqliteStreams::new(reopened, other)
            .len("events")
            .await
            .unwrap(),
        0
    );
    assert_eq!(streams.len("events").await.unwrap(), 1);
}

#[tokio::test]
async fn reads_the_be94_legacy_fixture_without_changing_primitive_rows() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("storage.db");
    std::fs::write(
        &path,
        include_bytes!("../../../../tests/fixtures/storage-be94f75.db"),
    )
    .unwrap();

    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open(&path).unwrap();
    let scope = Scope::new("local").unwrap();
    let documents = SqliteDocuments::new(connection.clone(), scope.clone());
    let streams = SqliteStreams::new(connection.clone(), scope);
    assert_eq!(
        documents
            .get("fixture", "document")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"source":"be94f75","value":42})
    );
    let events = streams.read_window("fixture.events", 0, 10).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].value,
        serde_json::json!({"source":"be94f75","sequence":1})
    );
    let (columns, preserved) = connection
        .run_now(|conn| {
            let mut statement = conn.prepare("PRAGMA table_info(blobs)").map_err(
                tinystoragedrivers_connect::sqlite::error::during("inspect fixture blob schema"),
            )?;
            let columns = statement
                .query_map([], |row| row.get::<_, String>(1))
                .map_err(tinystoragedrivers_connect::sqlite::error::during(
                    "inspect fixture blob schema",
                ))?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(tinystoragedrivers_connect::sqlite::error::during(
                    "inspect fixture blob schema",
                ))?;
            let preserved = conn
                .query_row(
                    "SELECT bytes FROM blobs WHERE scope='local' AND key='fixture/blob'",
                    [],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .map_err(tinystoragedrivers_connect::sqlite::error::during(
                    "read fixture blob",
                ))?;
            Ok((columns, preserved))
        })
        .unwrap();
    assert_eq!(
        columns,
        [
            "scope",
            "key",
            "content_type",
            "size",
            "sha256",
            "expires_ms",
            "bytes"
        ]
    );
    assert_eq!(preserved, b"be94f75-preserved-blob");
}

#[tokio::test]
async fn sqlite_ordered_queries_page_scalars_and_fall_back_for_structural_values() {
    let directory = tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let documents = SqliteDocuments::new(connection, Scope::new("sorts").unwrap());
    documents
        .ensure_collection(&CollectionSpec::new("items").index(IndexSpec::new("by_rank", ["rank"])))
        .await
        .unwrap();
    for (id, rank) in [("three", 3), ("one", 1), ("two", 2)] {
        documents
            .put(
                "items",
                id,
                serde_json::json!({"rank": rank}),
                Precondition::None,
            )
            .await
            .unwrap();
    }

    let first = documents
        .query("items", &Query::all().sort(Sort::asc("rank")).limit(2))
        .await
        .unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    let cursor = first.next.expect("limited query returns a continuation");
    let second = documents
        .query(
            "items",
            &Query::all().sort(Sort::asc("rank")).limit(2).after(cursor),
        )
        .await
        .unwrap();
    assert_eq!(second.items[0].id, "three");
    assert!(second.next.is_none());

    documents
        .put(
            "items",
            "array",
            serde_json::json!({"rank":[0,1]}),
            Precondition::None,
        )
        .await
        .unwrap();
    let structural = documents
        .query("items", &Query::all().sort(Sort::desc("rank")))
        .await
        .unwrap();
    assert_eq!(structural.items[0].id, "array");
}
