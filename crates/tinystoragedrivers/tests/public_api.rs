//! The facade's public surface composes API handles from a URL.

use tinystoragedrivers::{StorageConfig, types::Scope};
#[cfg(feature = "mongodb")]
use tinystoragedrivers_types::Precondition;
use tinystoragedrivers_types::StorageError;

async fn check_core_conformance(
    backend: &dyn tinystoragedrivers::StorageBackend,
) -> Result<(), StorageError> {
    let scope = Scope::new("facade-conformance")?;
    let storage = backend.for_scope(&scope)?;
    tinystoragedrivers_testkit::conformance::documents::run(storage.documents().as_ref()).await;
    tinystoragedrivers_testkit::conformance::streams::run(storage.streams().as_ref()).await;
    tinystoragedrivers_testkit::conformance::kv::run(storage.kv().as_ref()).await;
    tinystoragedrivers_testkit::conformance::blobs::run(storage.blobs().as_ref()).await;
    tinystoragedrivers_testkit::conformance::queue::run(storage.queues().as_ref()).await;
    Ok(())
}

#[test]
fn redacts_urls_through_the_public_api() {
    assert_eq!(
        tinystoragedrivers::redact_url("mongodb://app:secret@host/db"),
        "mongodb://app:***@host/db"
    );
}

#[tokio::test]
async fn a_memory_url_opens_a_conforming_backend() -> Result<(), StorageError> {
    let backend = tinystoragedrivers::open(&StorageConfig::parse("memory")?).await?;
    check_core_conformance(backend.as_ref()).await?;
    let storage = backend.for_scope(&Scope::new("facade-secrets")?)?;
    tinystoragedrivers_testkit::conformance::secrets::run(storage.secrets().as_ref()).await;
    Ok(())
}

#[cfg(feature = "file")]
#[tokio::test]
async fn a_file_url_opens_a_conforming_backend() -> Result<(), StorageError> {
    let dir = tempfile::tempdir().map_err(|error| StorageError::backend(error.to_string()))?;
    let url = format!("file:{}", dir.path().display());
    let config = StorageConfig::parse(&url)?;
    let backend = tinystoragedrivers::open(&config).await?;
    assert_eq!(config.driver(), "file");
    check_core_conformance(backend.as_ref()).await?;
    Ok(())
}

/// With the `mongodb` feature and `TSD_MONGO_URL` set, a MongoDB URL opens a
/// conforming backend too.
#[cfg(feature = "mongodb")]
#[tokio::test]
async fn live_a_mongodb_url_opens_a_conforming_backend() -> Result<(), StorageError> {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!(
            "live_a_mongodb_url_opens_a_conforming_backend: TSD_MONGO_URL is not set; skipping"
        );
        return Ok(());
    };
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| StorageError::backend(error.to_string()))?
        .as_nanos();
    let StorageConfig::MongoDb { uri, .. } = StorageConfig::parse(&url)? else {
        return Err(StorageError::backend(
            "Mongo URL parsed as a non-Mongo configuration",
        ));
    };
    let database = format!("tsd_facade_{}_{}", std::process::id(), suffix);
    let config = StorageConfig::MongoDb {
        uri: uri.clone(),
        database: database.clone(),
    };
    let backend = tinystoragedrivers::open(&config).await?;
    check_core_conformance(backend.as_ref()).await?;

    let scope_a = Scope::new(&format!("facade-scope-a-{suffix}"))?;
    let scope_b = Scope::new(&format!("facade-scope-b-{suffix}"))?;
    let first = backend.for_scope(&scope_a)?;
    let reopened = backend.for_scope(&scope_a)?;
    let second = backend.for_scope(&scope_b)?;
    let key = format!("facade-scope-check-{suffix}");

    first
        .kv()
        .put(&key, serde_json::json!("first scope"))
        .await?;
    let reopened_value = reopened
        .kv()
        .get(&key)
        .await?
        .ok_or_else(|| StorageError::backend("reopened Mongo scope lost its KV value"))?;
    assert_eq!(reopened_value.doc, serde_json::json!("first scope"));
    assert!(second.kv().get(&key).await?.is_none());
    second
        .kv()
        .put(&key, serde_json::json!("second scope"))
        .await?;
    let first_value = first.kv().get(&key).await?.ok_or_else(|| {
        StorageError::backend("Mongo scope value changed after another-scope write")
    })?;
    assert_eq!(first_value.doc, serde_json::json!("first scope"));
    assert!(first.kv().delete(&key, Precondition::None).await?);
    assert!(second.kv().delete(&key, Precondition::None).await?);

    let connection = tinystoragedrivers_connect::MongoConnection::connect(&uri, &database)
        .await
        .map_err(|error| StorageError::backend(error.to_string()))?;
    connection
        .database()
        .drop()
        .await
        .map_err(|error| StorageError::backend(error.to_string()))?;
    Ok(())
}
