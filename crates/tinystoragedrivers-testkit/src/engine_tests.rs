use super::*;
use tinystoragedrivers_api::{BlobStore, DocumentStore, SecretStore};
use tinystoragedrivers_document::{
    memory::MemoryConnection,
    sqlite::{SqliteDocuments, SqliteStreams},
};
use tinystoragedrivers_types::Precondition;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn six_port_memory_storage_uses_real_engines() {
    let scope = Scope::new("testkit_bundle").unwrap();
    let storage = memory_storage(&scope);
    conformance::documents::run(storage.documents().as_ref()).await;
    conformance::validated::run(storage.documents().as_ref()).await;
    conformance::watch::run(storage.documents().as_ref()).await;
    conformance::semantics::run(storage.documents().as_ref()).await;
    conformance::streams::run(storage.streams().as_ref()).await;
    conformance::seal::run(storage.streams()).await;
    conformance::queue::run(storage.queues().as_ref()).await;
    conformance::blobs::run(storage.blobs().as_ref()).await;
    conformance::kv::run(storage.kv().as_ref()).await;
    conformance::secrets::run(storage.secrets().as_ref()).await;

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
async fn failing_decorator_forwards_all_six_ports_and_optional_operations() {
    let scope = Scope::new("failing_six_ports").unwrap();
    let storage = memory_storage(&scope);
    let documents = Failing::new(std::sync::Arc::clone(storage.documents()));
    conformance::documents::run(&documents).await;
    conformance::validated::run(&documents).await;
    conformance::watch::run(&documents).await;
    conformance::semantics::run(&documents).await;
    let optional_collection = tinystoragedrivers_types::CollectionSpec::new("optional_probe");
    let _ = documents
        .validate_collection_spec(&optional_collection)
        .await;
    let _ = documents
        .put_with(
            "optional_probe",
            "item",
            serde_json::json!({"value": 1}),
            Precondition::None,
            tinystoragedrivers_types::WriteOptions::default(),
        )
        .await;

    let streams: std::sync::Arc<dyn tinystoragedrivers_api::StreamStore> =
        std::sync::Arc::new(Failing::new(std::sync::Arc::clone(storage.streams())));
    conformance::streams::run(streams.as_ref()).await;
    conformance::seal::run(&streams).await;

    let queue = Failing::new(std::sync::Arc::clone(storage.queues()));
    conformance::queue::run(&queue).await;
    let blobs = Failing::new(std::sync::Arc::clone(storage.blobs()));
    conformance::blobs::run(&blobs).await;
    assert_eq!(blobs.max_key_len(), storage.blobs().max_key_len());
    let _ = blobs.get_stream("optional_probe").await;
    let _ = blobs
        .put_expiring(
            "optional_probe",
            Vec::new(),
            None,
            std::time::Duration::from_secs(1),
        )
        .await;
    let _ = blobs
        .put_stream_expiring(
            "optional_stream_probe",
            Box::pin(futures_util::stream::empty::<
                tinystoragedrivers_types::Result<bytes::Bytes>,
            >()),
            0,
            None,
            std::time::Duration::from_secs(1),
        )
        .await;
    let _ = blobs.sweep_expired().await;
    let _ = blobs
        .presign_get("optional_probe", std::time::Duration::from_secs(1))
        .await;
    let _ = blobs
        .presign_put("optional_probe", std::time::Duration::from_secs(1))
        .await;
    let _ = blobs
        .put_tree(
            "optional_tree",
            vec![tinystoragedrivers_types::TreeFile::new("one", vec![1])],
        )
        .await;
    let kv = Failing::new(std::sync::Arc::clone(storage.kv()));
    conformance::kv::run(&kv).await;
    let secrets = Failing::new(std::sync::Arc::clone(storage.secrets()));
    conformance::secrets::run(&secrets).await;
    assert_eq!(secrets.backend_name(), storage.secrets().backend_name());
}

#[tokio::test]
async fn memory_backend_caches_and_separates_scope_bundles() {
    use tinystoragedrivers_api::StorageBackend;

    let backend = memory_backend();
    conformance::isolation::run(&backend).await;
    conformance::isolation::nested(&backend).await;
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
    documents::run(storage.documents().as_ref()).await;
    validated::run(storage.documents().as_ref()).await;
    conformance::watch::run(storage.documents().as_ref()).await;
    conformance::semantics::run(storage.documents().as_ref()).await;
    streams::run(storage.streams().as_ref()).await;
    let streams = storage.streams();
    conformance::seal::run(&streams).await;
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
    documents::run(&documents).await;
    validated::run(&documents).await;
    conformance::watch::run(&documents).await;
    conformance::semantics::run(&documents).await;
    streams::run(streams.as_ref()).await;
    conformance::seal::run(&streams).await;
}
