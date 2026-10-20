use tinystoragedrivers_document::{
    memory::MemoryConnection,
    sqlite::{SqliteDocuments, SqliteStreams},
};
use tinystoragedrivers_types::Precondition;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn six_port_memory_storage_uses_real_engines() {
    let scope = Scope::new("testkit_bundle").unwrap();
    let storage = crate::memory_storage(&scope);
    super::conformance::documents::run(storage.documents().as_ref()).await;
    super::conformance::validated::run(storage.documents().as_ref()).await;
    super::conformance::watch::run(storage.documents().as_ref()).await;
    super::conformance::semantics::run(storage.documents().as_ref()).await;
    super::conformance::streams::run(storage.streams().as_ref()).await;
    super::conformance::seal::run(storage.streams()).await;
    super::conformance::queue::run(storage.queues().as_ref()).await;

    storage
        .kv()
        .put("test", serde_json::json!({"v": 1}))
        .await
        .unwrap();
    assert_eq!(storage.kv().get("test").await.unwrap().unwrap().doc["v"], 1);
    storage.blobs().put("test", vec![1, 2], None).await.unwrap();
    assert_eq!(
        storage.blobs().get("test").await.unwrap().unwrap().bytes,
        [1, 2]
    );
    storage.secrets().set("test", b"secret").await.unwrap();
    assert_eq!(
        storage
            .secrets()
            .get("test")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"secret"
    );
    assert!(storage.secrets().delete("test").await.unwrap());
}

#[tokio::test]
async fn memory_backend_caches_and_separates_scope_bundles() {
    use tinystoragedrivers_api::StorageBackend;

    let backend = crate::memory_backend();
    super::conformance::isolation::run(&backend).await;
    super::conformance::isolation::nested(&backend).await;
    let a = backend
        .for_scope(&Scope::new("testkit_a").unwrap())
        .unwrap();
    a.documents()
        .put(
            "items",
            "one",
            serde_json::json!({"scope":"a"}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let a_again = backend
        .for_scope(&Scope::new("testkit_a").unwrap())
        .unwrap();
    assert!(
        a_again
            .documents()
            .get("items", "one")
            .await
            .unwrap()
            .is_some()
    );
    let b = backend
        .for_scope(&Scope::new("testkit_b").unwrap())
        .unwrap();
    assert!(b.documents().get("items", "one").await.unwrap().is_none());
    a.secrets().set("token", b"a").await.unwrap();
    assert!(b.secrets().get("token").await.unwrap().is_none());
}

#[tokio::test]
async fn memory_document_and_stream_ports_pass_shared_conformance() {
    let scope = Scope::new("testkit_memory").unwrap();
    let storage = MemoryConnection::new().for_scope(scope);
    super::documents::run(storage.documents().as_ref()).await;
    super::validated::run(storage.documents().as_ref()).await;
    super::conformance::watch::run(storage.documents().as_ref()).await;
    super::conformance::semantics::run(storage.documents().as_ref()).await;
    super::streams::run(storage.streams().as_ref()).await;
    let streams = storage.streams();
    super::conformance::seal::run(&streams).await;
}

#[tokio::test]
async fn sqlite_document_and_stream_ports_pass_shared_conformance() {
    let directory = tempfile::tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let scope = Scope::new("testkit_sqlite").unwrap();
    let documents = SqliteDocuments::new(connection.clone(), scope.clone());
    let streams = std::sync::Arc::new(SqliteStreams::new(connection, scope))
        as std::sync::Arc<dyn tinystoragedrivers_api::StreamStore>;
    super::documents::run(&documents).await;
    super::validated::run(&documents).await;
    super::conformance::watch::run(&documents).await;
    super::conformance::semantics::run(&documents).await;
    super::streams::run(streams.as_ref()).await;
    super::conformance::seal::run(&streams).await;
}
