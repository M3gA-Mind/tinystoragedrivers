//! The driver against a real MongoDB, named `live_*`.
//!
//! Set `TSD_MONGO_URL` (for example
//! `mongodb://localhost:27017/tsd_test?directConnection=true`, a single-node
//! replica set) to run them; without it each test prints a note and passes.

mod support;

use support::{connect, skip};
use tinystoragedrivers_core::conformance;

#[tokio::test]
async fn live_conformance_on_a_replica_set() {
    let Some(backend) = connect().await else {
        return skip("live_conformance_on_a_replica_set");
    };
    conformance::run(&backend, false).await;
}

#[tokio::test]
async fn live_conformance_without_transactions() {
    let Some(backend) = connect().await else {
        return skip("live_conformance_without_transactions");
    };
    conformance::run(&backend.without_transactions(), false).await;
}

#[tokio::test]
async fn live_true_standalone_keeps_single_document_writes_and_refuses_checked_batches() {
    use tinystoragedrivers_core::{Capability, ErrorKind, Precondition, Scope, StorageBackend};
    let Ok(url) = std::env::var("TSD_MONGO_STANDALONE_URL") else {
        return;
    };
    let backend = tinystoragedrivers_mongodb::MongoStorage::connect(&url, &support::database(&url))
        .await
        .unwrap();
    for capability in [
        Capability::Transactions,
        Capability::ValidatedBatch,
        Capability::Fencing,
        Capability::CrossDatabaseFencing,
        Capability::ScopePurge,
        Capability::AtomicScopePurge,
    ] {
        assert!(!backend.capabilities().contains(capability));
    }
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    scoped
        .documents()
        .put(
            "plain",
            "d",
            serde_json::json!({"works":true}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        scoped
            .documents()
            .get("plain", "d")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        scoped
            .documents()
            .atomic_batch_checked(vec![], vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ValidatedBatch)
    );
    let request = tinystoragedrivers_core::PurgeRequest {
        operation_id: "standalone_reset".into(),
        mode: tinystoragedrivers_core::PurgeMode::Quiescent,
    };
    assert_eq!(
        scoped.purge(&request).await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    assert_eq!(
        backend
            .for_scope_generation(&Scope::local(), tinystoragedrivers_core::ScopeGeneration(1))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ScopePurge)
    );
    conformance::run(&backend, false).await;
}
