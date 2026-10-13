//! Opening, databases, and the per-database lock.

use super::*;
use tinystoragedrivers_core::ErrorKind;

#[test]
fn opening_a_file_as_the_root_fails() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    assert_eq!(
        FileStorage::open(&file).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn rejects_invalid_database_names() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    assert_eq!(
        storage.database("Bad Name").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    let named = storage.database("one").unwrap();
    assert_eq!(named.driver(), "file");
    assert_eq!(named.capabilities(), CAPABILITIES);
    let nested = named.database("two").unwrap();
    assert!(format!("{nested:?}").contains("databases"));
}

#[test]
fn handles_on_one_directory_share_a_lock() {
    let dir = tempfile::tempdir().unwrap();
    let a = FileStorage::open(dir.path()).unwrap();
    let b = FileStorage::open(dir.path().join(".")).unwrap();
    assert!(Arc::ptr_eq(&a.db.lock, &b.db.lock));
    assert!(format!("{:?}", a.db).starts_with("Db"));
    assert!(system_clock() > 0);
}

#[tokio::test]
async fn a_poisoned_lock_is_taken_over() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let db = Arc::clone(&storage.db);
    let _ = std::thread::spawn(move || {
        let _guard = db.lock.lock().unwrap();
        panic!("poison the lock");
    })
    .join();
    assert_eq!(storage.db.run(|_| Ok(5)).await.unwrap(), 5);
}

#[tokio::test]
async fn a_panicking_task_is_a_backend_error() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let error = storage
        .db
        .run(|_| -> Result<()> { panic!("task panics") })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn windows_device_names_become_encoded_database_directories() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    storage.database("con").unwrap();
    assert!(storage.dir().join("databases/%63on").is_dir());
    assert!(!storage.dir().join("databases/con").exists());
}

#[cfg(unix)]
#[test]
fn a_symlinked_database_directory_shares_the_targets_lock() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FileStorage::open(dir.path()).unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir(&real).unwrap();
    std::fs::create_dir(dir.path().join("databases")).unwrap();
    std::os::unix::fs::symlink(&real, dir.path().join("databases/linked")).unwrap();
    let linked = storage.database("linked").unwrap();
    let direct = FileStorage::open(&real).unwrap();
    assert!(format!("{linked:?}").contains(real.canonicalize().unwrap().to_str().unwrap()));
    assert!(Arc::ptr_eq(
        &lock_for(&real.canonicalize().unwrap()),
        &direct.db.lock
    ));
}

#[tokio::test]
async fn reopened_named_target_observes_default_guard_takeover() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
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
    let target = FileStorage::open(dir.path())
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
    let takeover = FileStorage::open(dir.path())
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
fn unrelated_database_roots_have_independent_native_locks() {
    let left_dir = tempfile::tempdir().unwrap();
    let right_dir = tempfile::tempdir().unwrap();
    let left = FileStorage::open(left_dir.path()).unwrap();
    let right = FileStorage::open(right_dir.path()).unwrap();
    let held = left.db.lock();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let _other = right.db.lock();
        done_tx.send(()).unwrap();
    });
    started_rx.recv().unwrap();
    let independent = done_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .is_ok();
    drop(held);
    worker.join().unwrap();
    assert!(
        independent,
        "an unrelated directory must not wait for this root"
    );
}

#[tokio::test]
async fn a_fenced_handle_that_lost_its_lease_cannot_sweep_blobs() {
    let dir = tempfile::tempdir().unwrap();
    let root = FileStorage::open(dir.path()).unwrap();
    let scope = Scope::new("alice").unwrap();
    let guard_scope = Scope::new("cluster").unwrap();
    let lease = |epoch: u64| {
        let root = &root;
        let guard_scope = guard_scope.clone();
        async move {
            root.for_scope(&guard_scope)
                .unwrap()
                .documents()
                .put(
                    "leases",
                    "l",
                    serde_json::json!({"epoch": epoch}),
                    tinystoragedrivers_core::Precondition::None,
                )
                .await
                .unwrap();
        }
    };
    lease(1).await;
    let fence = Fence::epoch(guard_scope.clone(), "leases", "l", "epoch", 1);
    let held = root.for_scope_fenced(&scope, &fence).unwrap();
    held.blobs()
        .put_expiring("old", vec![1], None, std::time::Duration::from_millis(1))
        .await
        .unwrap();
    lease(2).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(
        held.blobs().sweep_expired().await.unwrap_err().kind(),
        ErrorKind::Fenced
    );
    // The refused sweep removed nothing: an unfenced sweep still finds it.
    assert_eq!(root.sweep_expired_blobs(&scope).await.unwrap(), 1);
}
