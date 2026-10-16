//! Fenced transactions: the refusal for unfenceable writes, and (against a
//! live replica set) the guard hold and concurrent fenced writers.

use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{Capability, ErrorKind, Fence, Precondition, Scope, StorageBackend};

use super::*;
use crate::MongoStorage;

#[test]
fn unfenceable_writes_name_the_capability() {
    let error = unfenceable("a blob put");
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::Fencing));
    assert!(error.message().contains("a blob put"), "{error}");
}

fn labelled(labels: &[&str]) -> mongodb::error::Error {
    use mongodb::error::{ErrorKind as MongoKind, WriteConcernError, WriteFailure};
    let concern: WriteConcernError = mongodb::bson::from_document(doc! {
        "code": 64, "codeName": "WriteConcernFailed", "errmsg": "x", "errorLabels": labels,
    })
    .unwrap();
    MongoKind::Write(WriteFailure::WriteConcernError(concern)).into()
}

#[test]
fn a_commit_settles_as_done_retry_or_failure() {
    let mut last = kept_aborting();
    assert_eq!(settle(&mut last, 7, Ok(())).unwrap(), Some(7));
    assert_eq!(
        settle(&mut last, 7, Err(labelled(&["TransientTransactionError"]))).unwrap(),
        None
    );
    assert!(last.message().contains("commit a fenced write"), "{last}");
    let unknown = settle(
        &mut last,
        7,
        Err(labelled(&["UnknownTransactionCommitResult"])),
    )
    .unwrap_err();
    assert_eq!(unknown.kind(), ErrorKind::Backend);
    assert!(!unknown.is_retryable());
    let failed = settle(&mut last, 7, Err(labelled(&[]))).unwrap_err();
    assert_eq!(failed.kind(), ErrorKind::Backend);
}

fn configured_database(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    let path = rest.split_once('/').map_or("", |(_, path)| path);
    path.split(['?', '/'])
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or("tsd_test")
}

async fn live_root(test: &str) -> Option<MongoStorage> {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("{test}: TSD_MONGO_URL is not set; skipping");
        return None;
    };
    Some(
        MongoStorage::connect(&url, configured_database(&url))
            .await
            .unwrap(),
    )
}

async fn live(test: &str) -> Option<MongoStorage> {
    // Isolate schema-wide validation metadata using the driver's actual logical
    // collection prefix; never require a grant beyond the configured database.
    Some(
        live_root(test)
            .await?
            .fixture_named(&unique("fixture"))
            .unwrap(),
    )
}

async fn live_peer(backend: &MongoStorage) -> MongoStorage {
    let url = std::env::var("TSD_MONGO_URL").unwrap();
    // Reconnect independently, keeping the physical database.
    MongoStorage::connect(&url, backend.shared.db.name())
        .await
        .unwrap()
}

fn unique(label: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "fence_{label}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

#[tokio::test]
async fn live_a_fenced_write_holds_the_guard_document() {
    let Some(backend) = live("live_a_fenced_write_holds_the_guard_document").await else {
        return;
    };
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let leases = unique("leases");
    let cluster = Scope::new("cluster").unwrap();
    let guards = backend.for_scope(&cluster).unwrap();
    guards
        .documents()
        .put(&leases, "l", json!({"epoch": 1}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster.clone(), &leases, "l", "epoch", 1);
    let fenced = backend.for_scope_fenced(&Scope::local(), &fence).unwrap();
    let coll = unique("data");
    fenced
        .documents()
        .put(&coll, "a", json!({}), Precondition::None)
        .await
        .unwrap();
    let raw = backend
        .shared
        .raw(&leases)
        .find_one(doc! {"_id": document_id(&cluster, "l")})
        .await
        .unwrap()
        .unwrap();
    assert_eq!(raw.get_i64(FENCE).unwrap(), 1, "{raw}");
    // The hold is invisible to readers and leaves the version alone.
    let guard = guards.documents().get(&leases, "l").await.unwrap().unwrap();
    assert_eq!(guard.doc, json!({"epoch": 1}));
    assert_eq!(guard.version.0, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_concurrent_fenced_writers_all_land() {
    let Some(backend) = live("live_concurrent_fenced_writers_all_land").await else {
        return;
    };
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let leases = unique("leases");
    let cluster = Scope::new("cluster").unwrap();
    backend
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 3}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster, &leases, "l", "epoch", 3);
    let fenced = Arc::new(backend.for_scope_fenced(&Scope::local(), &fence).unwrap());
    let coll = unique("data");
    let writers: Vec<_> = (0..6)
        .map(|n| {
            let (fenced, coll) = (Arc::clone(&fenced), coll.clone());
            tokio::spawn(async move {
                fenced
                    .documents()
                    .put(&coll, &format!("d{n}"), json!({"n": n}), Precondition::None)
                    .await
            })
        })
        .collect();
    let mut landed = 0;
    for writer in writers {
        match writer.await.unwrap() {
            Ok(_) => landed += 1,
            // Contention past every attempt is reported as retryable, never
            // as a silent unfenced write.
            Err(error) => assert!(error.is_retryable(), "{error}"),
        }
    }
    assert!(landed >= 1);
    let count = backend
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .count(&coll, &tinystoragedrivers_core::Filter::All)
        .await
        .unwrap();
    assert_eq!(count, landed);
}

#[tokio::test]
async fn live_the_fenced_transaction_renders_its_fence() {
    let Some(backend) = live("live_the_fenced_transaction_renders_its_fence").await else {
        return;
    };
    let fence = Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    let binding = Fencing {
        fence,
        shared: std::sync::Arc::clone(&backend.shared),
    };
    let txn = FencedTxn::start(&backend.shared, &binding).await.unwrap();
    let rendered = format!("{txn:?}");
    assert!(
        rendered.contains("leases") && rendered.contains("attempts"),
        "{rendered}"
    );
}

#[tokio::test]
async fn live_transient_failures_retry_until_the_attempts_run_out() {
    let Some(backend) = live("live_transient_failures_retry_until_the_attempts_run_out").await
    else {
        return;
    };
    if !backend.capabilities().contains(Capability::Fencing) {
        return;
    }
    let leases = unique("leases");
    let cluster = Scope::new("cluster").unwrap();
    backend
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 1}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster, &leases, "l", "epoch", 1);
    let binding = Fencing {
        fence: fence.clone(),
        shared: std::sync::Arc::clone(&backend.shared),
    };
    let mut txn = FencedTxn::start(&backend.shared, &binding).await.unwrap();
    for _ in 0..FENCE_ATTEMPTS {
        txn.begin().await.unwrap();
        let outcome: tinystoragedrivers_core::Result<()> =
            Err(StorageError::unavailable("simulated transient failure"));
        assert!(txn.finish(outcome).await.unwrap().is_none());
    }
    let exhausted = txn.begin().await.unwrap_err();
    assert!(exhausted.is_retryable());
    assert!(exhausted.message().contains("simulated"), "{exhausted}");
    // A non-transient write error ends the write at once.
    let binding = Fencing {
        fence: fence.clone(),
        shared: std::sync::Arc::clone(&backend.shared),
    };
    let mut txn = FencedTxn::start(&backend.shared, &binding).await.unwrap();
    txn.begin().await.unwrap();
    let outcome: tinystoragedrivers_core::Result<()> = Err(StorageError::conflict("cas lost"));
    assert_eq!(
        txn.finish(outcome).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
}
#[tokio::test]
async fn live_changed_dependency_prevents_checked_batch_claim() {
    let Some(backend) = live("live_changed_dependency_prevents_checked_batch_claim").await else {
        return;
    };
    if !backend.capabilities().contains(Capability::Transactions) {
        return;
    }
    let scope = backend.for_scope(&Scope::local()).unwrap();
    let docs = scope.documents();
    let collection = unique("checked_dependency");
    let version = docs
        .put(
            &collection,
            "dep",
            json!({"done":true}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    docs.put(
        &collection,
        "dep",
        json!({"done":false}),
        Precondition::None,
    )
    .await
    .unwrap();
    let error = docs
        .atomic_batch_checked(
            vec![tinystoragedrivers_core::ReadCheck {
                collection: collection.clone(),
                id: "dep".into(),
                expected: tinystoragedrivers_core::ExpectedDocument::Version(version),
            }],
            vec![tinystoragedrivers_core::WriteOp::Put {
                collection: collection.clone(),
                id: "claim".into(),
                doc: json!({}),
                precondition: Precondition::Absent,
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert!(docs.get(&collection, "claim").await.unwrap().is_none());
}

#[tokio::test]
async fn live_forced_off_writer_conflicts_with_a_checked_dependency_snapshot() {
    let Some(backend) =
        live("live_forced_off_writer_conflicts_with_a_checked_dependency_snapshot").await
    else {
        return;
    };
    if !backend.capabilities().contains(Capability::ValidatedBatch) {
        return;
    }
    let collection = unique("checked_snapshot");
    let scope = Scope::local();
    let docs = backend.for_scope(&scope).unwrap();
    let version = docs
        .documents()
        .put(
            &collection,
            "dep",
            json!({"done":true}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let mut session = backend.shared.client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    // Pin the dependency snapshot, then pause before its metadata lock.
    let stored = backend
        .shared
        .raw(&collection)
        .find_one(doc! {"_id": document_id(&scope, "dep")})
        .session(&mut session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(Stored::decode(&stored).unwrap().version, version);
    let forced_off = live_peer(&backend).await.without_transactions();
    assert!(
        !forced_off
            .capabilities()
            .contains(Capability::ValidatedBatch)
    );
    forced_off
        .for_scope(&scope)
        .unwrap()
        .documents()
        .put(
            &collection,
            "dep",
            json!({"done":false}),
            Precondition::None,
        )
        .await
        .unwrap();
    // Even the forced-off handle must have moved the shared metadata lock:
    // this stale snapshot cannot acquire it and commit a dependent claim.
    let outcome =
        crate::documents::MongoDocuments::new(Arc::clone(&backend.shared), scope.clone(), None)
            .hold_validation(&mut session)
            .await;
    session.abort_transaction().await.unwrap();
    let error = outcome.unwrap_err();
    assert!(error.is_retryable(), "{error}");
    let result = docs
        .documents()
        .atomic_batch_checked(
            vec![tinystoragedrivers_core::ReadCheck {
                collection: collection.clone(),
                id: "dep".into(),
                expected: tinystoragedrivers_core::ExpectedDocument::Version(version),
            }],
            vec![tinystoragedrivers_core::WriteOp::Put {
                collection: collection.clone(),
                id: "claim".into(),
                doc: json!({}),
                precondition: Precondition::Absent,
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(result.kind(), ErrorKind::Conflict);
    assert!(
        docs.documents()
            .get(&collection, "claim")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn live_unrelated_scope_does_not_invalidate_dependency_snapshot() {
    let Some(backend) = live("live_unrelated_scope_does_not_invalidate_dependency_snapshot").await
    else {
        return;
    };
    if !backend.capabilities().contains(Capability::ValidatedBatch) {
        return;
    }
    let collection = unique("scope_validation");
    let a = Scope::new(unique("scope_a")).unwrap();
    let b = Scope::new(unique("scope_b")).unwrap();
    let first = backend.for_scope(&a).unwrap();
    let second = backend.for_scope(&b).unwrap();
    first
        .documents()
        .put(&collection, "dep", json!({}), Precondition::None)
        .await
        .unwrap();
    second
        .documents()
        .put(&collection, "dep", json!({}), Precondition::None)
        .await
        .unwrap();
    let mut session = backend.shared.client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    backend
        .shared
        .raw(&collection)
        .find_one(doc! {"_id": document_id(&a, "dep")})
        .session(&mut session)
        .await
        .unwrap()
        .unwrap();
    second
        .documents()
        .put(
            &collection,
            "dep",
            json!({"changed":true}),
            Precondition::None,
        )
        .await
        .unwrap();
    backend
        .shared
        .hold_validation(Some(&a), &mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();
}

#[tokio::test]
async fn live_every_document_mutator_invalidates_a_dependency_snapshot() {
    use tinystoragedrivers_core::{CollectionSpec, Filter, IndexSpec};
    let Some(backend) = live("live_every_document_mutator_invalidates_a_dependency_snapshot").await
    else {
        return;
    };
    if !backend.capabilities().contains(Capability::ValidatedBatch) {
        return;
    }
    let scope = Scope::new(unique("mutation_scope")).unwrap();
    let ports = backend.for_scope(&scope).unwrap();
    let docs = ports.documents();
    for action in [
        "delete",
        "delete_where",
        "drop",
        "claim",
        "sweep",
        "schema",
        "retire",
    ] {
        let collection = unique(action);
        docs.ensure_collection(&CollectionSpec::new(&collection).ttl("exp"))
            .await
            .unwrap();
        docs.put(
            &collection,
            "dep",
            json!({"exp":1,"done":false}),
            Precondition::None,
        )
        .await
        .unwrap();
        let mut session = backend.shared.client.start_session().await.unwrap();
        session.start_transaction().await.unwrap();
        backend
            .shared
            .raw("_tsd_validated_batch")
            .find_one(doc! {"_id": format!("scope:{}", scope.as_str())})
            .session(&mut session)
            .await
            .unwrap()
            .unwrap();
        match action {
            "delete" => {
                docs.delete(&collection, "other", Precondition::None)
                    .await
                    .unwrap();
            }
            "delete_where" => {
                docs.delete_where(&collection, &Filter::All).await.unwrap();
            }
            "drop" => {
                docs.drop_collection(&collection).await.unwrap();
            }
            "claim" => {
                docs.claim(&collection, &Filter::All, &[], &json!({"claimed":true}))
                    .await
                    .unwrap();
            }
            "sweep" => {
                backend.sweep_expired(&scope, &collection).await.unwrap();
            }
            "schema" => {
                docs.ensure_collection(
                    &CollectionSpec::new(&collection).index(IndexSpec::new("done", ["done"])),
                )
                .await
                .unwrap();
            }
            "retire" => {
                docs.ensure_collection(
                    &CollectionSpec::new(&collection)
                        .index(IndexSpec::new("done", ["done"]).unique()),
                )
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        let outcome = backend
            .shared
            .hold_validation(Some(&scope), &mut session)
            .await;
        session.abort_transaction().await.unwrap();
        let error = outcome.unwrap_err();
        assert!(error.is_retryable(), "{action}: {error}");
    }
}

#[tokio::test]
async fn live_checked_dependencies_share_one_expiry_instant() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tinystoragedrivers_core::{CollectionSpec, ExpectedDocument, ReadCheck, WriteOp};
    let Some(backend) = live("live_checked_dependencies_share_one_expiry_instant").await else {
        return;
    };
    if !backend.capabilities().contains(Capability::Transactions) {
        eprintln!("skipped transaction-dependent checked expiry regression: standalone MongoDB");
        return;
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let clock = Arc::clone(&calls);
    let backend = backend.with_clock(Arc::new(move || {
        match clock.fetch_add(1, Ordering::SeqCst) {
            0 => 999,
            _ => 1001,
        }
    }));
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    let docs = scoped.documents();
    let collection = unique("checked_expiry_instant");
    docs.ensure_collection(&CollectionSpec::new(&collection).ttl("exp"))
        .await
        .unwrap();
    let version = docs
        .put(
            &collection,
            "dep",
            json!({"exp":1000}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    calls.store(0, Ordering::SeqCst);
    let error = docs
        .atomic_batch_checked(
            vec![
                ReadCheck {
                    collection: collection.clone(),
                    id: "dep".into(),
                    expected: ExpectedDocument::Version(version),
                },
                ReadCheck {
                    collection: collection.clone(),
                    id: "dep".into(),
                    expected: ExpectedDocument::Absent,
                },
            ],
            vec![WriteOp::Put {
                collection: collection.clone(),
                id: "claim".into(),
                doc: json!({}),
                precondition: Precondition::Absent,
            }],
        )
        .await
        .expect_err("no instant can satisfy version and absence of the same dependency");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert!(docs.get(&collection, "claim").await.unwrap().is_none());
}

#[tokio::test]
async fn live_distinct_test_fixtures_keep_schema_mutation_out_of_dependency_snapshot() {
    use tinystoragedrivers_core::{CollectionSpec, IndexSpec};
    let Some(first) =
        live("live_distinct_test_fixtures_keep_schema_mutation_out_of_dependency_snapshot").await
    else {
        return;
    };
    let Some(other) = live("independent schema fixture").await else {
        return;
    };
    if !first.capabilities().contains(Capability::ValidatedBatch) {
        return;
    }
    let scope = Scope::new(unique("fixture_scope")).unwrap();
    let collection = unique("fixture_dependency");
    first
        .for_scope(&scope)
        .unwrap()
        .documents()
        .put(&collection, "dep", json!({}), Precondition::None)
        .await
        .unwrap();
    let mut session = first.shared.client.start_session().await.unwrap();
    session.start_transaction().await.unwrap();
    first
        .shared
        .raw(&collection)
        .find_one(doc! {"_id": document_id(&scope, "dep")})
        .session(&mut session)
        .await
        .unwrap()
        .unwrap();
    // A second test changes a schema while the first test's snapshot is pinned.
    // Its metadata update must stay in its own logical fixture namespace.
    other
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .ensure_collection(
            &CollectionSpec::new(unique("independent_schema"))
                .index(IndexSpec::new("done", ["done"])),
        )
        .await
        .unwrap();
    first
        .shared
        .hold_validation(Some(&scope), &mut session)
        .await
        .unwrap();
    session.commit_transaction().await.unwrap();
    assert_ne!(
        first.shared.db.name(),
        other.shared.db.name(),
        "fixtures must have distinct databases"
    );
    let peer = live_peer(&first).await;
    assert_eq!(
        peer.shared.db.name(),
        first.shared.db.name(),
        "independent peers must retain the fixture database"
    );
    let metadata_is_distinct = first.shared.raw("_tsd_validated_batch").namespace()
        != other.shared.raw("_tsd_validated_batch").namespace();
    assert!(
        metadata_is_distinct,
        "test fixtures must have separate metadata namespaces"
    );
}
