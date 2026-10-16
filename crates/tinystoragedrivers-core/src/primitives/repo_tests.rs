use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use serde::{Deserialize, Serialize};

use super::*;
use crate::document::Query;
use crate::error::StorageError;
use crate::{MemoryStorage, Scope, StorageBackend};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Job {
    name: String,
    runs: u32,
}

fn job(name: &str, runs: u32) -> Job {
    Job {
        name: name.into(),
        runs,
    }
}

fn docs() -> Arc<dyn DocumentStore> {
    Arc::clone(
        MemoryStorage::new()
            .for_scope(&Scope::local())
            .unwrap()
            .documents(),
    )
}

fn repo() -> Repo<Job> {
    Repo::new(&docs(), "cron.jobs")
        .unwrap()
        .with_retry(RetryPolicy::immediate())
}

#[test]
fn repo_rejects_an_invalid_collection() {
    let error = Repo::<Job>::new(&docs(), "_tsd.jobs").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn put_get_insert_delete_and_query() {
    let repo = repo();
    assert!(repo.get("a").await.unwrap().is_none());
    repo.insert("a", &job("a", 1)).await.unwrap();
    let error = repo.insert("a", &job("a", 2)).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    repo.put("a", &job("a", 3)).await.unwrap();
    repo.put("b", &job("b", 4)).await.unwrap();
    assert_eq!(repo.get("a").await.unwrap().unwrap().doc, job("a", 3));
    let page = repo.query(&Query::all()).await.unwrap();
    let names: Vec<_> = page.items.iter().map(|i| i.doc.name.as_str()).collect();
    assert_eq!(names, ["a", "b"]);
    assert!(repo.delete("a").await.unwrap());
    assert!(!repo.delete("a").await.unwrap());
    assert_eq!(repo.collection(), "cron.jobs");
}

#[tokio::test]
async fn get_reports_a_body_that_is_not_a_t() {
    let docs = docs();
    docs.put(
        "cron.jobs",
        "x",
        serde_json::json!({"nope": 1}),
        Precondition::None,
    )
    .await
    .unwrap();
    let repo = Repo::<Job>::new(&docs, "cron.jobs").unwrap();
    assert_eq!(
        repo.get("x").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[tokio::test]
async fn update_creates_then_modifies_and_returns_the_stored_value() {
    let repo = repo();
    let (first, v1) = repo
        .update("a", |cur| {
            assert!(cur.is_none());
            Ok(Some(job("a", 1)))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, job("a", 1));
    let (second, v2) = repo
        .update("a", |cur| Ok(cur.map(|j| job("a", j.runs + 1))))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.runs, 2);
    assert_ne!(v1, v2);
    assert_eq!(repo.get("a").await.unwrap().unwrap().version, v2);
}

#[tokio::test]
async fn update_that_declines_writes_nothing() {
    let repo = repo();
    assert!(repo.update("a", |_| Ok(None)).await.unwrap().is_none());
    assert!(repo.get("a").await.unwrap().is_none());
}

#[tokio::test]
async fn update_propagates_the_closure_error_without_writing() {
    let repo = repo();
    let error = repo
        .update("a", |_| Err(StorageError::invalid_input("no")))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(repo.get("a").await.unwrap().is_none());
}

#[tokio::test]
async fn update_retries_after_a_competing_write() {
    let docs = docs();
    let repo = Repo::<Job>::new(&docs, "cron.jobs")
        .unwrap()
        .with_retry(RetryPolicy::immediate());
    repo.put("a", &job("a", 10)).await.unwrap();
    let calls = AtomicU32::new(0);
    let (stored, _) = repo
        .update("a", |cur| {
            // A rival lands between this read and this write, once.
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                let rival = serde_json::to_value(job("a", 100)).unwrap();
                futures_block(docs.put("cron.jobs", "a", rival, Precondition::None)).unwrap();
            }
            Ok(cur.map(|j| job("a", j.runs + 1)))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(stored.runs, 101, "the retry saw the rival's value");
}

/// Run a memory-driver future (which never yields) from sync code.
fn futures_block<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(out) => out,
        std::task::Poll::Pending => panic!("memory driver yielded"),
    }
}

#[tokio::test]
async fn update_gives_up_with_conflict_after_the_budget() {
    let docs = docs();
    let repo = Repo::<Job>::new(&docs, "cron.jobs")
        .unwrap()
        .with_retry(RetryPolicy::new(3, Duration::ZERO, Duration::ZERO));
    repo.put("a", &job("a", 0)).await.unwrap();
    let calls = AtomicU32::new(0);
    let error = repo
        .update("a", |cur| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            // Always lose the race.
            let rival = serde_json::to_value(job("a", 1000 + n)).unwrap();
            futures_block(docs.put("cron.jobs", "a", rival, Precondition::None)).unwrap();
            Ok(cur)
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn update_sleeps_between_attempts_with_the_default_policy() {
    let docs = docs();
    let repo = Repo::<Job>::new(&docs, "cron.jobs").unwrap();
    repo.put("a", &job("a", 0)).await.unwrap();
    let calls = AtomicU32::new(0);
    let started = tokio::time::Instant::now();
    repo.update("a", |cur| {
        if calls.fetch_add(1, Ordering::SeqCst) == 0 {
            let rival = serde_json::to_value(job("a", 5)).unwrap();
            futures_block(docs.put("cron.jobs", "a", rival, Precondition::None)).unwrap();
        }
        Ok(cur)
    })
    .await
    .unwrap();
    let waited = started.elapsed();
    assert!(waited >= Duration::from_micros(500), "{waited:?}");
    assert!(waited <= Duration::from_millis(1), "{waited:?}");
}

#[test]
fn backoff_doubles_is_capped_and_jittered_into_the_upper_half() {
    let policy = RetryPolicy::default();
    for _ in 0..50 {
        let first = policy.pause(0);
        assert!(first >= Duration::from_micros(500) && first <= Duration::from_millis(1));
        let capped = policy.pause(20);
        assert!(capped >= Duration::from_millis(4) && capped <= Duration::from_millis(8));
    }
    assert_eq!(RetryPolicy::immediate().pause(3), Duration::ZERO);
    assert_eq!(RetryPolicy::immediate().attempts, 32);
}

#[tokio::test]
async fn writes_carry_the_repo_options() {
    let docs = docs();
    let repo = Repo::<Job>::new(&docs, "cron.jobs")
        .unwrap()
        .with_options(WriteOptions::new().schema(3));
    repo.put("a", &job("a", 1)).await.unwrap();
    assert_eq!(repo.get("a").await.unwrap().unwrap().meta.schema, 3);
    repo.update("a", Ok).await.unwrap();
    assert_eq!(repo.get("a").await.unwrap().unwrap().meta.schema, 3);
}

#[test]
fn debug_names_the_collection() {
    assert!(format!("{:?}", repo()).contains("cron.jobs"));
}
