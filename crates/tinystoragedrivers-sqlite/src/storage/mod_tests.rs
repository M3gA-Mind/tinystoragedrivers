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
