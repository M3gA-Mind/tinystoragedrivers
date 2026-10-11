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

async fn live(test: &str) -> Option<MongoStorage> {
    let Some(url) = std::env::var("TSD_MONGO_URL")
        .ok()
        .filter(|url| !url.is_empty())
    else {
        eprintln!("{test}: TSD_MONGO_URL is not set; skipping");
        return None;
    };
    // Requires the disposable-server cross-database test role documented in
    // the MongoDB README; the URL database alone is insufficient.
    // Each paused transaction must see only its intentional concurrent mutator,
    // not schema changes from another parallel regression in the same database.
    let database = unique("native_validation");
    Some(MongoStorage::connect(&url, &database).await.unwrap())
}

/// A different physical client to the same isolated fixture database.
async fn independent(backend: &MongoStorage) -> MongoStorage {
    let url = std::env::var("TSD_MONGO_URL").unwrap();
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
async fn live_named_target_holds_default_guard_across_independent_clients() {
    let Some(backend) =
        live("live_named_target_holds_default_guard_across_independent_clients").await
    else {
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
    let target = backend.database("cross_target").unwrap();
    let fenced = target
        .for_scope_fenced_in_database(
            &Scope::local(),
            &fence,
            &tinystoragedrivers_core::GuardDatabase::Default,
        )
        .unwrap();
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
    let independent = independent(&backend).await;
    independent
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(
        fenced
            .documents()
            .put(&coll, "a", json!({"stale": true}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        target
            .for_scope(&Scope::local())
            .unwrap()
            .documents()
            .get(&coll, "a")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({})
    );
    assert_eq!(
        fenced
            .streams()
            .append_batch_if_offset("empty", 0, vec![])
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
}

#[tokio::test]
async fn live_cross_database_guard_snapshot_cannot_survive_a_takeover() {
    let Some(backend) = live("live_cross_database_guard_snapshot_cannot_survive_a_takeover").await
    else {
        return;
    };
    if !backend
        .capabilities()
        .contains(Capability::CrossDatabaseFencing)
    {
        return;
    }
    let leases = unique("snapshot_leases");
    let cluster = Scope::new("cluster").unwrap();
    backend
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 1}), Precondition::Absent)
        .await
        .unwrap();
    let fence = Fence::epoch(cluster.clone(), &leases, "l", "epoch", 1);
    let binding = Fencing {
        fence: fence.clone(),
        shared: Arc::clone(&backend.shared),
    };
    let mut txn = FencedTxn::start(&backend.shared, &binding).await.unwrap();
    txn.session.start_transaction().await.unwrap();
    // Establish a snapshot of the default guard without holding it yet.
    backend
        .shared
        .raw(&leases)
        .find_one(doc! {"_id": document_id(&cluster, "l")})
        .session(&mut txn.session)
        .await
        .unwrap()
        .unwrap();
    let independent = independent(&backend).await;
    independent
        .for_scope(&cluster)
        .unwrap()
        .documents()
        .put(&leases, "l", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
    // A snapshot-only check would still pass epoch 1. The native guard write
    // must instead abort this transaction before any target write can commit.
    let error = txn.guard(&CollectionSpec::new(&leases)).await.unwrap_err();
    assert!(error.is_retryable(), "{error}");
    txn.session.abort_transaction().await.unwrap();
    let target = backend.database("snapshot_target").unwrap();
    let stale = target
        .for_scope_fenced_in_database(
            &Scope::local(),
            &fence,
            &tinystoragedrivers_core::GuardDatabase::Default,
        )
        .unwrap();
    assert_eq!(
        stale
            .documents()
            .put("data", "late", json!({}), Precondition::None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert!(
        target
            .for_scope(&Scope::local())
            .unwrap()
            .documents()
            .get("data", "late")
            .await
            .unwrap()
            .is_none()
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
    let forced_off = independent(&backend).await.without_transactions();
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
