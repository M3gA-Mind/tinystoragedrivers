//! Live regressions for concurrent collection declaration and index publication.

use super::*;
use crate::MongoStorage;
use tinystoragedrivers_core::{Capability, Precondition, Scope, StorageBackend};

#[tokio::test]
async fn live_a_newer_search_declaration_retries_the_index_build_before_publication() -> Result<()>
{
    let Ok(url) = std::env::var("TSD_MONGO_URL") else {
        return Ok(());
    };
    let configured = mongodb::options::ClientOptions::parse(&url)
        .await
        .unwrap()
        .default_database
        .unwrap_or_else(|| "tsd_test".into());
    let fixture = format!("spec_merge_{}", mongodb::bson::oid::ObjectId::new());
    let backend = MongoStorage::connect(&url, &configured)
        .await
        .unwrap()
        .fixture_named(&fixture)
        .unwrap();
    if !backend.capabilities().contains(Capability::Transactions) {
        eprintln!(
            "skipped transaction-dependent search declaration regression: standalone MongoDB"
        );
        return Ok(());
    }
    assert_ne!(backend.shared.db.name(), configured);
    let name = "search_union";
    let first = CollectionSpec::new(name).searchable(["first"]);
    let second = CollectionSpec::new(name).searchable(["second"]);
    // The first caller built its indexes before the second caller published.
    backend.shared.build_indexes(&first, None).await.unwrap();
    backend.shared.ensure(&second).await.unwrap();
    let encoded = serde_json::to_string(&first).unwrap();
    backend.shared.prepare_validation(None).await.unwrap();
    let published = crate::fence::in_write!(&backend.shared, None, session => {
        backend.shared.hold_validation(None, session).await?;
        backend.shared.write_spec(name, &encoded, None, Some(session)).await
    })
    .unwrap();
    // This is the real outer ensure retry, using the declaration just read.
    if published.is_none() {
        backend.shared.ensure(&first).await.unwrap();
    }
    let docs = backend.for_scope(&Scope::local()).unwrap();
    docs.documents()
        .put(
            name,
            "a",
            serde_json::json!({"first":"alphaonly", "second":"betaonly"}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        docs.documents()
            .search(name, "alphaonly", 10)
            .await
            .unwrap()
            .len(),
        1,
        "persisted union must not omit the first caller's field from the text index"
    );
    assert_eq!(
        docs.documents()
            .search(name, "betaonly", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    let stored = backend
        .shared
        .raw(META)
        .find_one(doc! {"_id":name})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        decode_meta(&stored).unwrap().0,
        second.merge(&first).unwrap()
    );
    Ok(())
}
