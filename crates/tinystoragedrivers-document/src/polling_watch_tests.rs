use super::*;
use crate::memory::MemoryConnection;
use tinystoragedrivers_types::{Scope, WatchCursor};

#[tokio::test]
async fn explicit_memory_handle_emits_conservative_invalidations() {
    let storage = MemoryConnection::new().for_scope(Scope::new("local").unwrap());
    let mut watch = PollingWatch::open(
        storage.documents(),
        "notes",
        &Filter::All,
        &WatchOptions {
            interval: std::time::Duration::from_millis(1),
            ..WatchOptions::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(format!("{watch:?}"), "PollingWatch { .. }");

    assert!(matches!(
        watch.next().await.unwrap(),
        Some(WatchNotice::Invalidated { cursor: None })
    ));
}

#[tokio::test]
async fn polling_rejects_invalid_queries_and_native_resume_cursors() {
    let storage = MemoryConnection::new().for_scope(Scope::new("local").unwrap());
    let documents = storage.documents();

    assert!(
        PollingWatch::open(
            documents.clone(),
            "",
            &Filter::All,
            &WatchOptions::default()
        )
        .await
        .is_err()
    );
    assert!(
        PollingWatch::open(
            documents.clone(),
            "notes",
            &Filter::eq("", 1),
            &WatchOptions::default()
        )
        .await
        .is_err()
    );
    assert!(
        PollingWatch::open(
            documents.clone(),
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
            documents,
            "notes",
            &Filter::All,
            &WatchOptions {
                after: Some(WatchCursor::new(vec![1]).unwrap()),
                ..WatchOptions::default()
            }
        )
        .await
        .is_err()
    );
}
