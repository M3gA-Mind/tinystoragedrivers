//! The facade's public surface: open from a URL and pass the conformance suite.

use tinystoragedrivers::{StorageConfig, conformance};

#[test]
fn redacts_urls_through_the_public_api() {
    assert_eq!(
        tinystoragedrivers::redact_url("mongodb://app:secret@host/db"),
        "mongodb://app:***@host/db"
    );
}

#[tokio::test]
async fn a_memory_url_opens_a_conforming_backend() {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory").unwrap())
        .await
        .unwrap();
    conformance::run(backend.as_ref(), false).await;
}

#[cfg(all(feature = "secrets", feature = "document"))]
#[tokio::test]
async fn the_secrets_feature_re_exports_the_secret_store_drivers() {
    use std::sync::Arc;
    use tinystoragedrivers::secrets::{DerivedKeys, DocumentSecrets, SecretStore, crypto};
    use tinystoragedrivers::v2::{api, types};

    let scope = types::Scope::new("facade-secrets-test").unwrap();
    let connection = tinystoragedrivers_document::memory::MemoryConnection::new();
    let scoped = connection.for_scope(scope.clone());
    let unsupported = api::Storage::unsupported();
    let storage = api::Storage::new(
        scoped.documents(),
        scoped.streams(),
        Arc::clone(unsupported.kv()),
        Arc::clone(unsupported.blobs()),
        Arc::clone(unsupported.queues()),
        Arc::clone(unsupported.secrets()),
    );
    let secrets = DocumentSecrets::new(
        &storage,
        scope,
        Arc::new(DerivedKeys::new(crypto::generate_key().unwrap())),
    );
    secrets.set("api_key", b"sk-live").await.unwrap();
    assert_eq!(
        secrets.get("api_key").await.unwrap().unwrap().as_slice(),
        b"sk-live"
    );
}

#[cfg(feature = "file")]
#[tokio::test]
async fn a_file_url_opens_a_conforming_backend() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("file:{}", dir.path().display());
    let backend = tinystoragedrivers::open(&StorageConfig::parse(&url).unwrap())
        .await
        .unwrap();
    assert_eq!(backend.driver(), "file");
    conformance::run(backend.as_ref(), false).await;
}

/// With the `mongodb` feature and `TSD_MONGO_URL` set, a MongoDB URL opens a
/// conforming backend too.
#[cfg(feature = "mongodb")]
#[tokio::test]
async fn live_a_mongodb_url_opens_a_conforming_backend() {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!(
            "live_a_mongodb_url_opens_a_conforming_backend: TSD_MONGO_URL is not set; skipping"
        );
        return;
    };
    let backend = tinystoragedrivers::open(&StorageConfig::parse(&url).unwrap())
        .await
        .unwrap();
    assert_eq!(backend.driver(), "mongodb");
    conformance::run(backend.as_ref(), false).await;
}
