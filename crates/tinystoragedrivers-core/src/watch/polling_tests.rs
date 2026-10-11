//! Polling must preserve the explicit handle's native admission boundary.
use super::*;
use crate::{ErrorKind, Fence, MemoryStorage, Precondition, Scope, StorageBackend};
use serde_json::json;

async fn handles() -> (ScopedStorage, ScopedStorage) {
    let backend = MemoryStorage::new();
    let root = backend.for_scope(&Scope::local()).unwrap();
    root.documents()
        .put("leases", "owner", json!({"epoch": 1}), Precondition::Absent)
        .await
        .unwrap();
    let admitted = backend
        .for_scope_fenced(
            &Scope::new("profile").unwrap(),
            &Fence::new(Scope::local(), "leases", "owner", Filter::eq("epoch", 1)),
        )
        .unwrap();
    (root, admitted)
}

async fn takeover(root: &ScopedStorage) {
    root.documents()
        .put("leases", "owner", json!({"epoch": 2}), Precondition::None)
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn stale_handle_cannot_open_polling_subscription() {
    let (root, admitted) = handles().await;
    takeover(&root).await;
    let error = PollingWatch::open(admitted, "notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Fenced);
}

#[tokio::test(start_paused = true)]
async fn takeover_refuses_delivery_without_borrowing_new_owner() {
    let (root, admitted) = handles().await;
    let mut watch = PollingWatch::open(admitted, "notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(WatchNotice::Invalidated { cursor: None })
    ));
    takeover(&root).await;
    assert_eq!(watch.next().await.unwrap_err().kind(), ErrorKind::Fenced);
}

#[tokio::test(start_paused = true)]
async fn invalid_inputs_and_resume_cursors_are_refused() {
    let (_, admitted) = handles().await;
    assert!(
        PollingWatch::open(admitted.clone(), "", &Filter::All, &WatchOptions::default())
            .await
            .is_err()
    );
    assert!(
        PollingWatch::open(
            admitted.clone(),
            "notes",
            &Filter::eq("", 1),
            &WatchOptions::default()
        )
        .await
        .is_err()
    );
    assert!(
        PollingWatch::open(
            admitted.clone(),
            "notes",
            &Filter::All,
            &WatchOptions {
                interval: std::time::Duration::ZERO,
                ..WatchOptions::default()
            }
        )
        .await
        .is_err()
    );
    assert!(
        PollingWatch::open(
            admitted,
            "notes",
            &Filter::All,
            &WatchOptions {
                after: Some(super::super::WatchCursor::new(vec![1]).unwrap()),
                ..WatchOptions::default()
            }
        )
        .await
        .is_err()
    );
}

#[derive(Debug)]
struct NativeFailure;
impl std::fmt::Display for NativeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("native refusal")
    }
}
impl std::error::Error for NativeFailure {}

#[derive(Debug)]
struct RefusingDocuments {
    successes: std::sync::Mutex<usize>,
    source: std::sync::Arc<NativeFailure>,
    kind: ErrorKind,
}

#[crate::async_trait]
impl crate::DocumentStore for RefusingDocuments {
    fn capabilities(&self) -> crate::Capabilities {
        crate::Capabilities::none()
    }
    async fn atomic_batch_checked(
        &self,
        checks: Vec<crate::ReadCheck>,
        ops: Vec<crate::WriteOp>,
    ) -> Result<Vec<crate::WriteResult>> {
        assert!(checks.is_empty() && ops.is_empty());
        let remaining = {
            let mut successes = self.successes.lock().unwrap();
            let remaining = *successes;
            *successes = successes.saturating_sub(1);
            remaining
        };
        if remaining > 0 {
            return Ok(vec![]);
        }
        Err(StorageError::new(self.kind, "native admission refused")
            .with_source(self.source.clone()))
    }
    async fn ensure_collection(&self, _: &crate::CollectionSpec) -> Result<()> {
        unreachable!("polling does not declare collections")
    }
    async fn get(&self, _: &str, _: &str) -> Result<Option<crate::Versioned<serde_json::Value>>> {
        unreachable!()
    }
    async fn put(
        &self,
        _: &str,
        _: &str,
        _: serde_json::Value,
        _: Precondition,
    ) -> Result<crate::Version> {
        unreachable!("polling never writes documents")
    }
    async fn delete(&self, _: &str, _: &str, _: Precondition) -> Result<bool> {
        unreachable!()
    }
    async fn query(
        &self,
        _: &str,
        _: &crate::Query,
    ) -> Result<crate::Page<crate::Versioned<serde_json::Value>>> {
        unreachable!("polling has no persistent snapshot")
    }
    async fn count(&self, _: &str, _: &Filter) -> Result<u64> {
        unreachable!()
    }
    async fn delete_where(&self, _: &str, _: &Filter) -> Result<u64> {
        unreachable!()
    }
    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[crate::Sort],
        _: &serde_json::Value,
    ) -> Result<Option<crate::Versioned<serde_json::Value>>> {
        unreachable!()
    }
    async fn drop_collection(&self, _: &str) -> Result<()> {
        unreachable!()
    }
}

fn refusing(successes: usize, kind: ErrorKind) -> (ScopedStorage, std::sync::Arc<NativeFailure>) {
    let memory = MemoryStorage::new()
        .for_scope(&Scope::new("explicit").unwrap())
        .unwrap();
    let source = std::sync::Arc::new(NativeFailure);
    (
        ScopedStorage::new(
            memory.scope().clone(),
            "refusing",
            std::sync::Arc::new(RefusingDocuments {
                successes: std::sync::Mutex::new(successes),
                source: source.clone(),
                kind,
            }),
            memory.streams().clone(),
            memory.blobs().clone(),
        ),
        source,
    )
}

fn assert_source(error: &StorageError, kind: ErrorKind, source: &std::sync::Arc<NativeFailure>) {
    use std::error::Error;
    assert_eq!(error.kind(), kind);
    assert!(std::sync::Arc::ptr_eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::sync::Arc<NativeFailure>>()
            .unwrap(),
        source
    ));
}

#[tokio::test(start_paused = true)]
async fn explicit_native_refusal_keeps_source_at_open_and_delivery() {
    for kind in [
        ErrorKind::Unavailable,
        ErrorKind::Fenced,
        ErrorKind::Unsupported(crate::Capability::ValidatedBatch),
    ] {
        let (handle, source) = refusing(0, kind);
        assert_source(
            &PollingWatch::open(handle, "notes", &Filter::All, &WatchOptions::default())
                .await
                .unwrap_err(),
            kind,
            &source,
        );
        let (handle, source) = refusing(1, kind);
        let mut watch = PollingWatch::open(handle, "notes", &Filter::All, &WatchOptions::default())
            .await
            .unwrap();
        assert_source(&watch.next().await.unwrap_err(), kind, &source);
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_polling_wait_can_be_retried_and_drop_spawns_no_worker() {
    use std::future::Future;
    use std::task::Poll;
    let (_, handle) = handles().await;
    let mut watch = PollingWatch::open(handle, "notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();
    assert_eq!(format!("{watch:?}"), "PollingWatch { .. }");
    let mut waiting = Box::pin(watch.next());
    assert!(matches!(
        std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await,
        Poll::Pending
    ));
    drop(waiting);
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(WatchNotice::Invalidated { cursor: None })
    ));
    drop(watch);
}
