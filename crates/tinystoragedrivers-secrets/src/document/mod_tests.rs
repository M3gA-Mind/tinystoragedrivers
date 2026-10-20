//! Document engine conformance using the public primitive adapters.

use super::*;
use crate::testkit::document_secrets_conformance;
use tinystoragedrivers_types::{ErrorKind, Precondition, Scope, SecretVersion};

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

#[tokio::test]
async fn file_documents_pass_secret_and_cas_conformance() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let scope = Scope::new("secrets-file-conformance").unwrap();
    let connection = tinystoragedrivers_connect::FileConnection::open(dir.path()).unwrap();
    let documents = Arc::new(tinystoragedrivers_document::FileDocuments::new(
        connection,
        scope.clone(),
    ));
    document_secrets_conformance(documents, scope).await;
}

#[tokio::test]
async fn mongodb_documents_pass_secret_and_cas_conformance() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database = format!("tsd30_secrets_{}_{}", std::process::id(), nonce);
    let connection = tinystoragedrivers_connect::MongoConnection::connect(&uri, &database)
        .await
        .expect("TSD_MONGO_URL is available");
    let scope = Scope::new("secrets-mongodb-conformance").unwrap();
    let documents = Arc::new(tinystoragedrivers_document::mongodb::MongoDocuments::new(
        connection.clone(),
        scope.clone(),
    ));
    document_secrets_conformance(documents, scope).await;
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn collection_scope_debug_and_invalid_collection_are_supported() {
    let scope = Scope::new("secrets-collection-test").unwrap();
    let connection = tinystoragedrivers_document::memory::MemoryConnection::new();
    let storage = connection.for_scope(scope.clone());
    let keys: Arc<dyn KeyProvider> =
        Arc::new(crate::DerivedKeys::new(crypto::generate_key().unwrap()));
    let store = DocumentSecrets::from_parts(storage.documents(), scope.clone(), keys)
        .with_collection("custom-secrets")
        .unwrap();
    assert_eq!(store.collection(), "custom-secrets");
    assert_eq!(store.scope(), &scope);
    assert!(format!("{store:?}").contains("custom-secrets"));
    assert_eq!(
        store.with_collection("bad\0collection").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn malformed_document_ciphertext_is_rejected_by_all_secret_reads_and_cas() {
    let scope = Scope::new("secrets-malformed-document").unwrap();
    let connection = tinystoragedrivers_document::memory::MemoryConnection::new();
    let storage = connection.for_scope(scope.clone());
    let documents = storage.documents();
    let keys: Arc<dyn KeyProvider> =
        Arc::new(crate::DerivedKeys::new(crypto::generate_key().unwrap()));
    let store = DocumentSecrets::from_parts(Arc::clone(&documents), scope, keys);
    documents
        .put(
            DEFAULT_COLLECTION,
            "malformed",
            serde_json::json!({ "other": "field" }),
            Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        store.get("malformed").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        store.get_versioned("malformed").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        store
            .set_if(
                "malformed",
                b"value",
                SecretPrecondition::Version(SecretVersion::from_bytes([0; 32])),
            )
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
}
