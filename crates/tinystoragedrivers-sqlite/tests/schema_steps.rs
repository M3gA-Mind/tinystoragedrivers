//! Stream and blob migration steps over a real SQLite database: a crash in the
//! middle of a long stream, a reopen, and a resume that leaves exactly the
//! uninterrupted result.

#![allow(
    clippy::unwrap_used,
    clippy::assertions_on_result_states,
    reason = "test helpers report a failed call by panicking"
)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::schema::{MigrateOptions, Owner, Registry, Schema, ScopeClass, Step};
use tinystoragedrivers_core::{
    Result, Scope, ScopedStorage, SealOutcome, StorageBackend, StorageError, StreamEntry,
    StreamStore,
};
use tinystoragedrivers_sqlite::SqliteStorage;

const LEGACY: &str = "16:session_journal/";
const LONG: u64 = 1000;

/// A stream store that fails an append once its allowance runs out.
#[derive(Debug)]
struct Flaky {
    inner: Arc<dyn StreamStore>,
    appends_left: AtomicUsize,
}

impl Flaky {
    fn spend(&self) -> Result<()> {
        let left = self.appends_left.load(Ordering::SeqCst);
        if left == 0 {
            return Err(StorageError::backend("injected append failure"));
        }
        self.appends_left.store(left - 1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl StreamStore for Flaky {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.spend()?;
        self.inner.append(stream, value).await
    }
    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.spend()?;
        self.inner.append_batch(stream, values).await
    }
    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        self.inner.read_window(stream, from, limit).await
    }
    async fn len(&self, stream: &str) -> Result<u64> {
        self.inner.len(stream).await
    }
    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        self.inner.truncate_before(stream, offset).await
    }
    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        self.inner.delete_stream(stream).await
    }
    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.inner.seal(stream).await
    }
    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.streams(prefix).await
    }
    async fn info(&self, stream: &str) -> Result<Option<tinystoragedrivers_core::StreamInfo>> {
        self.inner.info(stream).await
    }
}

fn open(dir: &Path, appends: usize) -> (ScopedStorage, Arc<Flaky>) {
    let real = SqliteStorage::open(dir)
        .unwrap()
        .for_scope(&Scope::local())
        .unwrap();
    let flaky = Arc::new(Flaky {
        inner: Arc::clone(real.streams()),
        appends_left: AtomicUsize::new(appends),
    });
    let storage = ScopedStorage::new(
        Scope::local(),
        "sqlite",
        Arc::clone(real.documents()),
        flaky.clone(),
        Arc::clone(real.blobs()),
    );
    (storage, flaky)
}

fn registry() -> Registry {
    let journal = Step::stream("journal_v2", "harness.journal", 1, 2, |name, entries| {
        let id = name.strip_prefix(LEGACY).unwrap();
        Ok(Some((
            format!("harness.journal/{id}"),
            entries
                .into_iter()
                .map(|mut entry| {
                    entry.as_object_mut().unwrap().remove("run_id");
                    entry
                })
                .collect(),
        )))
    })
    .source_prefix(LEGACY);
    let attachments = Step::blobs(
        "attachments_v2",
        "harness/attachments/",
        1,
        2,
        |key, content_type, bytes| {
            let id = key.strip_prefix("attachments/").unwrap();
            Ok(Some((
                format!("harness/attachments/{id}"),
                content_type,
                bytes,
            )))
        },
    )
    .source_prefix("attachments/");
    Registry::compose([Schema::new(Owner::new("harness").unwrap())
        .stream("harness.journal", ScopeClass::Tenant)
        .blobs("harness/attachments/", ScopeClass::Tenant)
        .step(journal)
        .step(attachments)])
    .unwrap()
}

async fn seed(storage: &ScopedStorage) {
    for (run, count) in [("a", 5), ("b", LONG), ("c", 3)] {
        let values = (0..count).map(|n| json!({"run_id": run, "n": n})).collect();
        storage
            .streams()
            .append_batch(&format!("{LEGACY}{run}"), values)
            .await
            .unwrap();
    }
    storage.streams().seal(&format!("{LEGACY}a")).await.unwrap();
    for id in ["x", "dir/y"] {
        storage
            .blobs()
            .put(
                &format!("attachments/{id}"),
                id.as_bytes().to_vec(),
                Some("text/plain"),
            )
            .await
            .unwrap();
    }
}

async fn read_all(storage: &ScopedStorage, stream: &str) -> Vec<Value> {
    let entries = storage
        .streams()
        .read_window(stream, 0, 10_000)
        .await
        .unwrap();
    for (expected, entry) in (0..).zip(&entries) {
        assert_eq!(entry.offset, expected, "{stream}");
    }
    entries.into_iter().map(|entry| entry.value).collect()
}

async fn is_sealed(storage: &ScopedStorage, stream: &str) -> bool {
    storage
        .streams()
        .info(stream)
        .await
        .unwrap()
        .is_some_and(|info| info.sealed_ms.is_some())
}

#[tokio::test]
async fn a_crash_mid_stream_resumes_across_a_reopen_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry();
    let options = MigrateOptions::new().batch_size(100);
    {
        let (storage, _) = open(dir.path(), 4);
        registry
            .prepare(&SqliteStorage::open(dir.path()).unwrap())
            .await
            .unwrap();
        seed(&storage).await;
        registry.migrate(&storage, &options).await.unwrap_err();
        assert!(registry.check_ready("harness.journal").is_err());
    }

    let (storage, _) = open(dir.path(), usize::MAX);
    let report = registry.migrate(&storage, &options).await.unwrap();
    assert_eq!(report.ran.len(), 2);
    registry.check_ready("harness.journal").unwrap();
    registry.check_ready("harness/attachments/").unwrap();

    assert_eq!(storage.streams().streams(LEGACY).await.unwrap().len(), 0);
    assert_eq!(
        read_all(&storage, "harness.journal/b").await,
        (0..LONG).map(|n| json!({"n": n})).collect::<Vec<_>>()
    );
    assert_eq!(read_all(&storage, "harness.journal/a").await.len(), 5);
    assert_eq!(read_all(&storage, "harness.journal/c").await.len(), 3);
    assert!(is_sealed(&storage, "harness.journal/a").await);
    assert!(!is_sealed(&storage, "harness.journal/b").await);
    assert!(!is_sealed(&storage, &format!("{LEGACY}a")).await);

    assert_eq!(storage.blobs().list("attachments/").await.unwrap().len(), 0);
    let moved = storage
        .blobs()
        .get("harness/attachments/dir/y")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(moved.bytes, b"dir/y");
    assert_eq!(moved.meta.content_type.as_deref(), Some("text/plain"));

    let again = registry.migrate(&storage, &options).await.unwrap();
    assert_eq!(again.ran.len(), 0);
    assert_eq!(again.already_done, 2);
    assert_eq!(read_all(&storage, "harness.journal/b").await.len(), 1000);
}
