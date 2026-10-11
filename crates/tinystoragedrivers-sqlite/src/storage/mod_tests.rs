//! Layouts, persistence, the clock, and handle construction.

use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, GuardDatabase, Precondition};

use super::*;

#[test]
fn picks_the_layout_from_the_extension() {
    assert!(is_file_path(Path::new("/x/state.db")));
    assert!(is_file_path(Path::new("/x/state.SQLITE")));
    assert!(!is_file_path(Path::new("/x/state")));
    assert!(!is_file_path(Path::new("/x/state.json")));
}

#[tokio::test]
async fn directory_mode_gives_each_database_its_own_file() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    assert!(storage.path().ends_with(ROOT_FILE));
    storage.database("flows").unwrap();
    assert!(dir.path().join("flows.db").exists());
    assert_eq!(storage.driver(), "sqlite");
    assert_eq!(
        storage.capabilities(),
        Capabilities::all()
            .without(tinystoragedrivers_core::Capability::PresignedUrls)
            .without(tinystoragedrivers_core::Capability::StreamingBlobs)
    );
    assert!(format!("{storage:?}").contains("storage.db"));
}

#[tokio::test]
async fn file_mode_prefixes_database_tables() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("one.sqlite");
    let storage = SqliteStorage::open(&file).unwrap();
    let flows = storage.database("flows").unwrap();
    flows
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .put("runs", "r1", json!({}), Precondition::None)
        .await
        .unwrap();
    let tables: i64 = storage
        .native()
        .with_connection(|conn| {
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name = 'flows___tsd_docs'",
                [],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(tables, 1);
    assert_eq!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "db"))
            .count(),
        0
    );
}

#[tokio::test]
async fn data_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    {
        let storage = SqliteStorage::open(dir.path()).unwrap();
        let scoped = storage.for_scope(&Scope::local()).unwrap();
        scoped
            .documents()
            .put("c", "a", json!({"n": 1}), Precondition::None)
            .await
            .unwrap();
        scoped
            .streams()
            .append("log", json!("first"))
            .await
            .unwrap();
        scoped.blobs().put("k", vec![1, 2, 3], None).await.unwrap();
    }
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scoped = storage.for_scope(&Scope::local()).unwrap();
    let doc = scoped.documents().get("c", "a").await.unwrap().unwrap();
    assert_eq!(doc.doc, json!({"n": 1}));
    assert_eq!(scoped.streams().len("log").await.unwrap(), 1);
    assert_eq!(
        scoped.blobs().get("k").await.unwrap().unwrap().bytes,
        [1, 2, 3]
    );
}

#[tokio::test]
async fn expiry_follows_the_injected_clock() {
    let dir = tempfile::tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(1_000));
    let reading = Arc::clone(&now);
    let storage = SqliteStorage::open(dir.path())
        .unwrap()
        .with_clock(Arc::new(move || reading.load(Ordering::SeqCst)));
    let docs = storage
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .clone();
    docs.ensure_collection(&CollectionSpec::new("leases").ttl("until").searchable(["t"]))
        .await
        .unwrap();
    docs.put(
        "leases",
        "l",
        json!({"until": 2_000, "t": "lease"}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert_eq!(docs.search("leases", "lease", 5).await.unwrap().len(), 1);
    now.store(2_000, Ordering::SeqCst);
    assert!(docs.get("leases", "l").await.unwrap().is_none());
    assert_eq!(docs.search("leases", "lease", 5).await.unwrap().len(), 0);
    assert!(system_clock() > 0);
}

#[test]
fn rejects_invalid_database_names() {
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    assert!(storage.database("Bad Name").is_err());
}

#[tokio::test]
async fn reopened_named_target_observes_default_guard_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let root = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("alice").unwrap();
    let guard_scope = Scope::new("cluster").unwrap();
    let guard = root.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put(
            "leases",
            "l",
            serde_json::json!({"epoch": 1}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    let target = SqliteStorage::open(dir.path())
        .unwrap()
        .database("target")
        .unwrap();
    let fence = Fence::epoch(guard_scope.clone(), "leases", "l", "epoch", 1);
    let held = target
        .for_scope_fenced_in_database(&scope, &fence, &GuardDatabase::Default)
        .unwrap();
    held.documents()
        .put(
            "docs",
            "d",
            serde_json::json!({"value": 1}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    drop(guard);
    let takeover = SqliteStorage::open(dir.path())
        .unwrap()
        .for_scope(&guard_scope)
        .unwrap();
    takeover
        .documents()
        .put(
            "leases",
            "l",
            serde_json::json!({"epoch": 2}),
            tinystoragedrivers_core::Precondition::None,
        )
        .await
        .unwrap();
    assert_eq!(
        held.documents()
            .put(
                "docs",
                "d",
                serde_json::json!({"value": 2}),
                tinystoragedrivers_core::Precondition::None
            )
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_core::ErrorKind::Fenced
    );
    let plain = target.for_scope(&scope).unwrap();
    assert_eq!(
        plain
            .documents()
            .get("docs", "d")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!({"value": 1})
    );
    assert!(
        target
            .for_scope(&Scope::new("bob").unwrap())
            .unwrap()
            .documents()
            .get("docs", "d")
            .await
            .unwrap()
            .is_none()
    );
}

#[test]
fn relative_layout_guards_keep_the_opened_root_after_working_directory_changes() {
    const CHILD: &str = "TSD_RELATIVE_LAYOUT_GUARD_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "storage::tests::relative_layout_guards_keep_the_opened_root_after_working_directory_changes", "--nocapture"])
            .env(CHILD, "1")
            .current_dir(dir.path())
            .output().unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let root = std::env::current_dir().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for path in ["", "relative_directory", "relative_file.db"] {
            std::env::set_current_dir(&root).unwrap();
            let backend = SqliteStorage::open(path).unwrap();
            let target = backend.database("target").unwrap();
            let guards = backend.database("guards").unwrap();
            let scope = Scope::local();
            let fence = Fence::epoch(scope.clone(), "leases", "holder", "epoch", 7);
            for source in [
                backend.for_scope(&scope).unwrap(),
                guards.for_scope(&scope).unwrap(),
            ] {
                source
                    .documents()
                    .put("leases", "holder", json!({"epoch": 7}), Precondition::None)
                    .await
                    .unwrap();
            }
            let moved = root.join(format!("moved-{path}"));
            std::fs::create_dir(&moved).unwrap();
            std::env::set_current_dir(&moved).unwrap();
            for locator in [
                GuardDatabase::Default,
                GuardDatabase::Named("guards".into()),
            ] {
                let bound = target
                    .for_scope_fenced_in_database(&scope, &fence, &locator)
                    .unwrap();
                bound
                    .documents()
                    .put("records", "one", json!({"value": 1}), Precondition::None)
                    .await
                    .unwrap();
            }
            assert!(
                !moved
                    .join(if path.is_empty() { ROOT_FILE } else { path })
                    .exists(),
                "binding must not recreate the relative layout under a new cwd"
            );
        }
    });
}

#[tokio::test]
async fn scope_purge_persists_revocation_across_reopen_and_named_databases() {
    use tinystoragedrivers_core::{
        ErrorKind, PurgeMode, PurgeRequest, PurgeStatus, ScopeGeneration,
    };
    for filename in ["directory", "single.db"] {
        let dir = tempfile::tempdir().unwrap();
        let location = dir.path().join(filename);
        let storage = SqliteStorage::open(&location).unwrap();
        let scope = Scope::new("reset").unwrap();
        let other = Scope::new("keep").unwrap();
        for backend in [
            Arc::new(storage.clone()) as Arc<dyn StorageBackend>,
            storage.database("sessions").unwrap(),
        ] {
            let target = backend.for_scope(&scope).unwrap();
            let keep = backend.for_scope(&other).unwrap();
            for ports in [&target, &keep] {
                ports
                    .documents()
                    .put("records", "one", json!({}), Precondition::None)
                    .await
                    .unwrap();
                ports.streams().append("events", json!({})).await.unwrap();
                ports.blobs().put("blob", vec![1], None).await.unwrap();
            }
            let request = PurgeRequest {
                operation_id: "reset_one".into(),
                mode: PurgeMode::Atomic,
            };
            let receipt = target.purge(&request).await.unwrap();
            assert_eq!(receipt.status, PurgeStatus::Complete);
            assert_eq!(receipt.generation, ScopeGeneration(1));
            assert_eq!(target.purge(&request).await.unwrap(), receipt);
            let verified = backend
                .for_scope_generation(&scope, receipt.generation)
                .unwrap();
            assert!(
                verified
                    .documents()
                    .get("records", "one")
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(verified.streams().len("events").await.unwrap(), 0);
            assert!(verified.blobs().get("blob").await.unwrap().is_none());
            assert!(
                keep.documents()
                    .get("records", "one")
                    .await
                    .unwrap()
                    .is_some()
            );
            assert_eq!(keep.streams().len("events").await.unwrap(), 1);
            assert!(keep.blobs().get("blob").await.unwrap().is_some());
            assert_eq!(
                target
                    .documents()
                    .put("records", "late", json!({}), Precondition::None)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Fenced
            );
            assert_eq!(
                target
                    .streams()
                    .append("events", json!({}))
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Fenced
            );
            assert_eq!(
                target
                    .blobs()
                    .put("late", vec![], None)
                    .await
                    .unwrap_err()
                    .kind(),
                ErrorKind::Fenced
            );
        }
        drop(storage);
        verify_reopened_generation(&location, &scope).await;
    }
}

async fn verify_reopened_generation(location: &Path, scope: &Scope) {
    use tinystoragedrivers_core::{ErrorKind, ScopeGeneration};
    let reopened = SqliteStorage::open(location).unwrap();
    let old = reopened
        .database("sessions")
        .unwrap()
        .for_scope(scope)
        .unwrap();
    assert_eq!(
        old.streams()
            .append("events", json!({}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    let fresh = reopened
        .database("sessions")
        .unwrap()
        .for_scope_generation(scope, ScopeGeneration(1))
        .unwrap();
    assert_eq!(
        fresh.streams().append("events", json!({})).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn failed_scope_purge_rolls_back_data_and_generation() {
    use tinystoragedrivers_core::{PurgeMode, PurgeRequest, ScopeGeneration};
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("reset").unwrap();
    let target = storage.for_scope(&scope).unwrap();
    target
        .documents()
        .put("records", "one", json!({}), Precondition::None)
        .await
        .unwrap();
    target.streams().append("events", json!({})).await.unwrap();
    target.blobs().put("blob", vec![1], None).await.unwrap();
    let trigger = format!(
        "CREATE TRIGGER fail_purge BEFORE DELETE ON {} BEGIN SELECT RAISE(ABORT, 'blocked'); END",
        crate::sql::ident(&storage.tables.blobs)
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute_batch(&trigger).unwrap();
            Ok(())
        })
        .unwrap();
    let request = PurgeRequest {
        operation_id: "failed_reset".into(),
        mode: PurgeMode::Atomic,
    };
    assert!(target.purge(&request).await.is_err());
    assert!(
        target
            .documents()
            .get("records", "one")
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(target.streams().len("events").await.unwrap(), 1);
    assert!(target.blobs().get("blob").await.unwrap().is_some());
    assert_eq!(
        target.streams().append("events", json!({})).await.unwrap(),
        1
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute_batch("DROP TRIGGER fail_purge").unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        target.purge(&request).await.unwrap().generation,
        ScopeGeneration(1)
    );
}

#[tokio::test]
async fn corrupt_or_exhausted_scope_generation_never_erases_data() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest, ScopeGeneration};
    let dir = tempfile::tempdir().unwrap();
    let storage = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("exhausted").unwrap();
    let target = storage.for_scope(&scope).unwrap();
    target
        .documents()
        .put("records", "one", json!({}), Precondition::None)
        .await
        .unwrap();
    let table = crate::sql::ident(&storage.tables.admissions);
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                &format!("INSERT INTO {table} VALUES (?1,-1,'old')"),
                [scope.as_str()],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
    assert_eq!(
        target
            .streams()
            .append("events", json!({}))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Serialization
    );
    storage
        .db
        .run_now(|conn| {
            conn.execute(
                &format!("UPDATE {table} SET generation=?1 WHERE scope=?2"),
                rusqlite::params![i64::MAX, scope.as_str()],
            )
            .unwrap();
            Ok(())
        })
        .unwrap();
    let admitted = storage
        .for_scope_generation(&scope, ScopeGeneration(u64::try_from(i64::MAX).unwrap()))
        .unwrap();
    assert_eq!(
        admitted
            .purge(&PurgeRequest {
                operation_id: "overflow_reset".into(),
                mode: PurgeMode::Atomic
            })
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert!(
        admitted
            .documents()
            .get("records", "one")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn retired_scope_handles_cannot_read_replacement_data() {
    use tinystoragedrivers_core::{ErrorKind, PurgeMode, PurgeRequest};
    let dir = tempfile::tempdir().unwrap();
    let backend = SqliteStorage::open(dir.path()).unwrap();
    let scope = Scope::new("retired_reader").unwrap();
    let old = backend.for_scope(&scope).unwrap();
    let receipt = old
        .purge(&PurgeRequest {
            operation_id: "retire_reader".into(),
            mode: PurgeMode::Atomic,
        })
        .await
        .unwrap();
    let fresh = backend
        .for_scope_generation(&scope, receipt.generation)
        .unwrap();
    fresh
        .documents()
        .put("records", "one", json!({"new":true}), Precondition::None)
        .await
        .unwrap();
    fresh
        .streams()
        .append("events", json!({"new":true}))
        .await
        .unwrap();
    fresh.blobs().put("blob", vec![2], None).await.unwrap();
    assert_eq!(
        old.documents()
            .get("records", "one")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        old.streams()
            .read_window("events", 0, 10)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(
        old.blobs().get("blob").await.unwrap_err().kind(),
        ErrorKind::Fenced
    );
}
