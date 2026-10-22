use super::*;

#[test]
fn cloned_connections_use_the_same_canonical_root_and_clock() {
    let dir = tempfile::tempdir().unwrap();
    let expected = dir.path().canonicalize().unwrap();
    let connection = FileConnection::open_with_clock(dir.path(), Arc::new(|| 37)).unwrap();
    let clone = connection.clone();
    assert_eq!(connection.root(), expected);
    assert_eq!(clone.root(), expected);
    assert_eq!((connection.clock())(), 37);
    assert_eq!((clone.clock())(), 37);
}

#[test]
fn default_open_supplies_a_clock_and_debug_shows_the_canonical_root() {
    let dir = tempfile::tempdir().unwrap();
    let connection = FileConnection::open(dir.path()).unwrap();
    assert!((connection.clock())() > 0);
    let rendered = format!("{connection:?}");
    assert!(rendered.contains("FileConnection"));
    assert!(rendered.contains(&dir.path().canonicalize().unwrap().display().to_string()));
}

#[test]
fn run_uses_the_inline_path_without_a_tokio_runtime() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let dir = tempfile::tempdir().unwrap();
    let connection = FileConnection::open(dir.path()).unwrap();
    let future = connection.run(|root| Ok(root.to_path_buf()));
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    let path = match future.as_mut().poll(&mut context) {
        Poll::Ready(Ok(path)) => path,
        Poll::Ready(Err(error)) => panic!("inline file operation failed: {error}"),
        Poll::Pending => panic!("inline file operation unexpectedly pended"),
    };
    assert_eq!(path, dir.path().canonicalize().unwrap());
}

#[tokio::test]
async fn async_and_sync_operations_share_the_root_lock() {
    let dir = tempfile::tempdir().unwrap();
    let connection = FileConnection::open(dir.path()).unwrap();
    let alias = FileConnection::open(dir.path().join(".")).unwrap();
    assert!(Arc::ptr_eq(&connection.lock, &alias.lock));
    let path = connection
        .run(|root| {
            let file = root.join("probe");
            std::fs::write(&file, b"ok")
                .map_err(|error| StorageError::backend("write test file").with_source(error))?;
            Ok(file)
        })
        .await
        .unwrap();
    assert_eq!(
        connection.run_now(|root| Ok(root.join("probe"))).unwrap(),
        path
    );
    assert_eq!(std::fs::read(path).unwrap(), b"ok");

    let error = connection
        .run(|_| Err::<(), _>(StorageError::invalid_input("rejected file operation")))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );

    let task_error = connection
        .run(|_| -> Result<()> {
            std::panic::resume_unwind(Box::new("test blocking task panic"));
        })
        .await
        .unwrap_err();
    assert_eq!(
        task_error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
}

#[test]
fn open_rejects_a_file_as_root_and_run_recovers_a_poisoned_lock() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("not-a-directory");
    std::fs::write(&file, b"x").unwrap();
    assert!(FileConnection::open(file.join("child")).is_err());

    let connection = FileConnection::open(dir.path()).unwrap();
    let lock = Arc::clone(&connection.lock);
    let panic = std::panic::catch_unwind(move || {
        let _guard = lock.lock().unwrap();
        panic!("poison shared lock");
    });
    assert!(panic.is_err());
    assert!(connection.run_now(|_| Ok(())).is_ok());
}
