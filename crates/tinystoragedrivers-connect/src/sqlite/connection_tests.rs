use super::*;
use crate::sqlite::SqliteConnection;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

#[test]
fn canonical_paths_share_one_connection_and_run_inline_without_a_runtime() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("nested").join("storage.db");
    let first = Db::open(&path).unwrap();
    let second = Db::open(&path).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(first.path(), path.canonicalize().unwrap());
    assert!(format!("{first:?}").contains("Db"));

    let answer = block_on(first.run(|conn| {
        conn.execute_batch("CREATE TABLE IF NOT EXISTS probe (value INTEGER)")
            .map_err(during("create probe"))?;
        conn.execute("INSERT INTO probe VALUES (42)", [])
            .map_err(during("insert probe"))?;
        Ok(42)
    }))
    .unwrap();
    assert_eq!(answer, 42);
}

#[cfg(unix)]
#[test]
fn replacing_a_database_file_does_not_reuse_its_open_connection() {
    let directory = tempfile::tempdir().unwrap();
    let database_dir = directory.path().join("workspace");
    let path = database_dir.join("storage.db");
    let first = Db::open(&path).unwrap();
    first
        .run_now(|conn| {
            conn.execute_batch("CREATE TABLE probe (value INTEGER)")
                .map_err(during("create probe"))?;
            conn.execute("INSERT INTO probe VALUES (42)", [])
                .map_err(during("insert probe"))?;
            Ok(())
        })
        .unwrap();

    std::fs::remove_dir_all(&database_dir).unwrap();
    std::fs::create_dir_all(&database_dir).unwrap();

    let reopened = Db::open(&path).unwrap();
    assert!(
        !Arc::ptr_eq(&first, &reopened),
        "a replacement file at the same path needs its own connection"
    );
    let tables = reopened
        .run_now(|conn| {
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'probe'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(during("check replacement schema"))
        })
        .unwrap();
    assert_eq!(tables, 0, "the replacement file must start empty");
}

#[test]
fn database_parent_creation_errors_are_backend_errors() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    std::fs::write(&file, b"not a directory").unwrap();
    let error = Db::open(&file.join("storage.db")).unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

#[test]
fn runtime_free_task_panics_are_mapped_and_default_open_uses_a_directory() {
    let directory = tempfile::tempdir().unwrap();
    let connection = SqliteConnection::open(directory.path()).unwrap();
    assert_eq!(connection.path(), directory.path().join("storage.db"));
    assert!((connection.clock())() > 0);

    let db = Db::open(&directory.path().join("panic.db")).unwrap();
    let error = block_on(db.run(|_| -> Result<()> {
        std::panic::resume_unwind(Box::new("test panic"));
    }))
    .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

#[tokio::test]
async fn run_uses_the_blocking_pool_and_returns_operation_errors() {
    let directory = tempfile::tempdir().unwrap();
    let db = Db::open(&directory.path().join("storage.db")).unwrap();
    let value = db
        .run(|conn| {
            conn.query_row("SELECT 7", [], |row| row.get::<_, i64>(0))
                .map_err(during("select"))
        })
        .await
        .unwrap();
    assert_eq!(value, 7);

    let error = db
        .run(|_| Err::<(), _>(StorageError::invalid_input("rejected operation")))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}

#[tokio::test]
async fn blocking_task_panics_become_backend_errors() {
    let directory = tempfile::tempdir().unwrap();
    let db = Db::open(&directory.path().join("storage.db")).unwrap();
    let error = db
        .run(|_| -> Result<()> { panic!("test blocking task panic") })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

#[tokio::test]
async fn aborted_blocking_task_join_errors_are_mapped() {
    let task = tokio::spawn(std::future::pending::<()>());
    task.abort();
    let error = join_error(task.await.unwrap_err());
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

#[test]
fn database_path_without_a_filename_is_rejected() {
    let error = Db::open(Path::new("/")).unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}

#[test]
fn poisoned_connection_lock_returns_a_backend_error() {
    let directory = tempfile::tempdir().unwrap();
    let db = Db::open(&directory.path().join("storage.db")).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<()> = db.run_now(|_| panic!("test connection poison"));
    }));
    assert!(panic.is_err());
    let error = db.run_now(|_| Ok(())).unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

#[tokio::test]
async fn connection_handles_share_schema_clock_and_accessors() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("storage.db");
    let connection = SqliteConnection::open_with_clock(&path, Arc::new(|| 123)).unwrap();
    let clone = connection.clone();
    assert_eq!(connection.path(), path.canonicalize().unwrap());
    assert_eq!(connection.tables().docs(), "docs");
    assert_eq!((clone.clock())(), 123);
    let output = connection
        .run(|conn| {
            conn.execute("INSERT INTO docs (scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, body) VALUES ('s', 'c', 'i', 1, 0, 1, 1, NULL, '{}')", [])
                .map_err(during("insert document"))?;
            conn.query_row("SELECT count(*) FROM docs", [], |row| row.get::<_, i64>(0))
                .map_err(during("count documents"))
        })
        .await
        .unwrap();
    assert_eq!(output, 1);
    assert_eq!(
        connection
            .run_now(|conn| conn
                .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .map_err(during("journal mode")))
            .unwrap(),
        "wal"
    );
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::yield_now();
    }
}
