use tinystoragedrivers_document::{
    memory::MemoryConnection,
    sqlite::{SqliteDocuments, SqliteStreams},
};
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn memory_document_and_stream_ports_pass_shared_conformance() {
    let scope = Scope::new("testkit_memory").unwrap();
    let storage = MemoryConnection::new().for_scope(scope);
    super::documents::run(storage.documents().as_ref()).await;
    super::validated::run(storage.documents().as_ref()).await;
    super::conformance::watch::run(storage.documents().as_ref()).await;
    super::conformance::semantics::run(storage.documents().as_ref()).await;
    super::streams::run(storage.streams().as_ref()).await;
}

#[tokio::test]
async fn sqlite_document_and_stream_ports_pass_shared_conformance() {
    let directory = tempfile::tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let scope = Scope::new("testkit_sqlite").unwrap();
    let documents = SqliteDocuments::new(connection.clone(), scope.clone());
    let streams = SqliteStreams::new(connection, scope);
    super::documents::run(&documents).await;
    super::validated::run(&documents).await;
    super::conformance::watch::run(&documents).await;
    super::conformance::semantics::run(&documents).await;
    super::streams::run(&streams).await;
}
