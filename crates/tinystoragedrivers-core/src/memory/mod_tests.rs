//! The memory driver passes the conformance suite, and its own edge cases.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;

use super::*;
use crate::{CollectionSpec, ErrorKind, Filter, Precondition, Query};

#[tokio::test]
async fn passes_the_conformance_suite() {
    crate::conformance::run(&MemoryStorage::new(), false).await;
}

#[tokio::test]
async fn the_backend_sweep_hook_reclaims_only_lapsed_blobs_of_the_scope() {
    let now = Arc::new(AtomicU64::new(1_000));
    let clock_now = Arc::clone(&now);
    let storage = MemoryStorage::with_clock(Arc::new(move || clock_now.load(Ordering::SeqCst)));
    let other = Scope::new("other").unwrap();
    let blobs = storage.for_scope(&Scope::local()).unwrap().blobs().clone();
    let foreign = storage.for_scope(&other).unwrap().blobs().clone();
    for store in [&blobs, &foreign] {
        store
            .put_expiring("old", vec![1], None, std::time::Duration::from_millis(5))
            .await
            .unwrap();
    }
    blobs
        .put_expiring(
            "young",
            vec![2],
            None,
            std::time::Duration::from_millis(500),
        )
        .await
        .unwrap();
    blobs.put("forever", vec![3], None).await.unwrap();
    now.store(1_100, Ordering::SeqCst);

    let dynamic: Arc<dyn StorageBackend> = Arc::new(storage.clone());
    assert_eq!(
        dynamic.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        1
    );
    assert_eq!(
        dynamic.sweep_expired_blobs(&Scope::local()).await.unwrap(),
        0
    );
    let kept: Vec<String> = blobs
        .list("")
        .await
        .unwrap()
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    assert_eq!(kept, ["forever", "young"]);
    // The other scope's lapsed blob was not touched by this scope's sweep.
    assert_eq!(dynamic.sweep_expired_blobs(&other).await.unwrap(), 1);
}

#[tokio::test]
async fn expiry_follows_the_injected_clock() {
    let now = Arc::new(AtomicU64::new(1_000));
    let clock_now = Arc::clone(&now);
    let storage = MemoryStorage::with_clock(Arc::new(move || clock_now.load(Ordering::SeqCst)));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until"))
        .await
        .unwrap();
    let v1 = docs
        .put("leases", "l", json!({"until": 2_000}), Precondition::None)
        .await
        .unwrap();
    assert!(docs.get("leases", "l").await.unwrap().is_some());

    now.store(2_000, Ordering::SeqCst);
    assert!(
        docs.get("leases", "l").await.unwrap().is_none(),
        "expires at its time"
    );
    assert_eq!(docs.count("leases", &Filter::All).await.unwrap(), 0);

    let v2 = docs
        .put("leases", "l", json!({"until": 9_000}), Precondition::Absent)
        .await
        .unwrap();
    assert!(v2 > v1, "versions keep rising across expiry");

    now.store(9_000, Ordering::SeqCst);
    assert!(
        !docs
            .delete("leases", "l", Precondition::None)
            .await
            .unwrap(),
        "deleting an expired document reports nothing removed"
    );
}

#[tokio::test]
async fn query_pages_past_the_end_are_empty() {
    let storage = MemoryStorage::default();
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.put("c", "a", json!({}), Precondition::None)
        .await
        .unwrap();
    let query = Query::all();
    let fingerprint = documents::fingerprint("c", &Scope::local(), &query);
    let page = docs
        .query(
            "c",
            &query.after(
                crate::Cursor::keyset(
                    "mem",
                    fingerprint,
                    &crate::CursorPosition {
                        id: "z".into(),
                        values: vec![],
                    },
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(page.items.len(), 0);
    assert!(page.next.is_none());
    let error = docs
        .query(
            "c",
            &Query::all().after(crate::Cursor("mem:keyset1:x".into())),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn search_ignores_text_without_tokens() {
    let docs = MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("notes").searchable(["t"]))
        .await
        .unwrap();
    docs.put("notes", "a", json!({"t": "hello"}), Precondition::None)
        .await
        .unwrap();
    assert_eq!(docs.search("notes", "  ,, ", 5).await.unwrap().len(), 0);
}

#[test]
fn reports_its_driver_and_capabilities() {
    let storage = MemoryStorage::new();
    assert_eq!(storage.driver(), "memory");
    assert_eq!(
        storage.capabilities(),
        Capabilities::all()
            .without(crate::Capability::Watch)
            .without(crate::Capability::PresignedUrls)
            .without(crate::Capability::SharedAcrossProcesses)
            .without(crate::Capability::StreamingBlobs)
            .without(crate::Capability::SharedAcrossProcesses)
            .without(crate::Capability::CrossProcessCas)
    );
    assert_eq!(format!("{storage:?}"), "MemoryStorage { .. }");
    let docs = storage.for_scope(&Scope::local()).unwrap();
    assert!(format!("{:?}", docs.documents()).contains("local"));
}

#[test]
fn a_poisoned_lock_is_a_backend_error() {
    let db = Arc::new(MemoryDb::default());
    let poisoner = Arc::clone(&db);
    let _ = std::thread::spawn(move || {
        let _guard = poisoner.state.lock().unwrap();
        panic!("poison the lock");
    })
    .join();
    assert_eq!(db.lock().unwrap_err().kind(), ErrorKind::Backend);
}

#[tokio::test]
async fn cursors_only_resume_their_own_query() {
    let docs = MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    for id in ["a", "b", "c"] {
        docs.put("c", id, json!({"n": 1}), Precondition::None)
            .await
            .unwrap();
    }
    let page = docs.query("c", &Query::all().limit(1)).await.unwrap();
    let cursor = page.next.unwrap();
    let other = Query::filter(Filter::eq("n", 1))
        .limit(1)
        .after(cursor.clone());
    assert_eq!(
        docs.query("c", &other).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        docs.query("d", &Query::all().limit(1).after(cursor.clone()))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    let same = Query::all().limit(1).after(cursor);
    assert_eq!(docs.query("c", &same).await.unwrap().items[0].id, "b");
    assert_eq!(
        docs.query("c", &Query::all().limit(0))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn redeclaring_a_collection_keeps_its_unique_index() {
    let docs = MemoryStorage::new()
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(
        &CollectionSpec::new("users").index(crate::IndexSpec::new("by_email", ["email"]).unique()),
    )
    .await
    .unwrap();
    docs.ensure_collection(&CollectionSpec::new("users").ttl("expires_at"))
        .await
        .unwrap();
    docs.put("users", "a", json!({"email": "x"}), Precondition::None)
        .await
        .unwrap();
    let error = docs
        .put("users", "b", json!({"email": "x"}), Precondition::None)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    let clash = CollectionSpec::new("users").index(crate::IndexSpec::new("by_email", ["mail"]));
    assert_eq!(
        docs.ensure_collection(&clash).await.unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn an_exhausted_version_fails_the_write() {
    let mut state = state::DbState::default();
    state
        .docs
        .entry(("local".into(), "c".into()))
        .or_default()
        .insert(
            "max".into(),
            state::StoredDoc {
                version: crate::Version(u64::MAX),
                doc: json!({}),
                meta: crate::Meta::unknown(),
            },
        );
    let write = state::Write {
        doc: json!({}),
        precondition: Precondition::None,
        options: crate::WriteOptions::default(),
    };
    let error = state.put("local", "c", "max", write, 0).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn fenced_handles_say_so_in_their_debug_output() {
    let storage = MemoryStorage::new();
    let fence = crate::Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    let fenced = storage.for_scope_fenced(&Scope::local(), &fence).unwrap();
    for rendered in [
        format!("{:?}", fenced.documents()),
        format!("{:?}", fenced.streams()),
        format!("{:?}", fenced.blobs()),
    ] {
        assert!(rendered.contains("fenced: true"), "{rendered}");
    }
    let plain = storage.for_scope(&Scope::local()).unwrap();
    assert!(format!("{:?}", plain.streams()).contains("fenced: false"));
}

#[tokio::test]
async fn unfenced_stream_delete_blob_and_drop_writes_never_read_the_clock() {
    let reads = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&reads);
    let storage = MemoryStorage::with_clock(Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        0
    }));
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    // An append reads the clock for the stream's last-append time; a guard
    // check on an unfenced handle does not.
    scoped.streams().append("s", json!({"n": 1})).await.unwrap();
    let after_append = reads.load(Ordering::SeqCst);
    assert_eq!(after_append, 1, "one read stamps the append");
    scoped.streams().delete_stream("s").await.unwrap();
    scoped.blobs().put("b", vec![1], None).await.unwrap();
    scoped.blobs().delete("b").await.unwrap();
    scoped.documents().drop_collection("c").await.unwrap();
    assert_eq!(reads.load(Ordering::SeqCst), after_append);
}

#[tokio::test]
async fn fenced_document_writes_read_the_clock_under_the_database_lock() {
    use std::sync::{OnceLock, TryLockError};

    // The clock checks whether the database lock is held when it is read:
    // the guard check and the write are serialized only under that lock.
    let db: Arc<OnceLock<Arc<MemoryDb>>> = Arc::new(OnceLock::new());
    let unlocked = Arc::new(AtomicU64::new(0));
    let (seen_db, seen_unlocked) = (Arc::clone(&db), Arc::clone(&unlocked));
    let storage = MemoryStorage::with_clock(Arc::new(move || {
        if let Some(db) = seen_db.get()
            && !matches!(db.state.try_lock(), Err(TryLockError::WouldBlock))
        {
            seen_unlocked.fetch_add(1, Ordering::SeqCst);
        }
        0
    }));
    db.set(Arc::clone(&storage.db)).ok();

    storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .put("leases", "l", json!({"epoch": 1}), Precondition::None)
        .await
        .unwrap();
    let fence = crate::Fence::epoch(Scope::local(), "leases", "l", "epoch", 1);
    let docs = storage
        .for_scope_fenced(&Scope::local(), &fence)
        .unwrap()
        .documents()
        .clone();
    unlocked.store(0, Ordering::SeqCst);

    docs.put("c", "a", json!({"n": 1}), Precondition::None)
        .await
        .unwrap();
    docs.claim("c", &Filter::All, &[], &json!({"n": 2}))
        .await
        .unwrap();
    docs.atomic_batch(vec![crate::WriteOp::Delete {
        collection: "c".into(),
        id: "a".into(),
        precondition: Precondition::None,
    }])
    .await
    .unwrap();
    docs.delete("c", "a", Precondition::None).await.unwrap();
    docs.delete_where("c", &Filter::All).await.unwrap();
    assert_eq!(unlocked.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn changed_read_only_dependency_aborts_checked_batch_without_mutation() {
    use crate::{ExpectedDocument, ReadCheck, WriteOp};
    let store = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let docs = store.documents();
    let version = docs
        .put(
            "dependencies",
            "d",
            json!({"state": "complete"}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    docs.put(
        "dependencies",
        "d",
        json!({"state": "changed"}),
        Precondition::None,
    )
    .await
    .unwrap();
    let error = docs
        .atomic_batch_checked(
            vec![ReadCheck {
                collection: "dependencies".into(),
                id: "d".into(),
                expected: ExpectedDocument::Version(version),
            }],
            vec![WriteOp::Put {
                collection: "claims".into(),
                id: "c".into(),
                doc: json!({}),
                precondition: Precondition::Absent,
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::Conflict);
    assert!(docs.get("claims", "c").await.unwrap().is_none());
}

#[tokio::test]
async fn scopes_include_tombstone_only_scopes() {
    let storage = MemoryStorage::new();
    let kept = Scope::new("u:kept").unwrap();
    let deleted = Scope::new("u:deleted").unwrap();
    for scope in [&kept] {
        let handles = storage.for_scope(scope).unwrap();
        handles
            .documents()
            .put("c", "d", json!({"n": 1}), Precondition::None)
            .await
            .unwrap();
    }
    let docs = storage.for_scope(&deleted).unwrap();
    let docs = docs.documents();
    docs.put("c", "d", json!({"n": 1}), Precondition::None)
        .await
        .unwrap();
    docs.delete("c", "d", Precondition::None).await.unwrap();

    let listed = storage.scopes("u:").await.unwrap();
    assert_eq!(listed, [deleted, kept]);
}

#[tokio::test]
async fn maintenance_on_memory_reclaims_nothing() {
    let storage = MemoryStorage::new();
    let report = storage
        .maintain(crate::MaintenanceBudget::unlimited())
        .await
        .unwrap();
    assert_eq!(report.reclaimed_bytes, 0);
    storage
        .checkpoint(crate::Checkpoint::Truncate)
        .await
        .unwrap();
}
