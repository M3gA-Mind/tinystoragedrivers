//! Document engine conformance using the public memory and SQLite adapters.

use super::*;
use crate::testkit::document_secrets_conformance;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn memory_documents_pass_secret_and_cas_conformance() {
    let scope = Scope::new("secrets-memory-conformance").unwrap();
    let connection = tinystoragedrivers_document::memory::MemoryConnection::new();
    let storage = connection.for_scope(scope.clone());
    document_secrets_conformance(storage.documents(), scope).await;
}

#[tokio::test]
async fn sqlite_documents_pass_secret_and_cas_conformance() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let scope = Scope::new("secrets-sqlite-conformance").unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(dir.path()).unwrap();
    let documents = Arc::new(tinystoragedrivers_document::sqlite::SqliteDocuments::new(
        connection,
        scope.clone(),
    ));
    document_secrets_conformance(documents, scope).await;
}
