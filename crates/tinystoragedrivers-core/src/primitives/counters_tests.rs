use super::*;
use crate::{MemoryStorage, Scope, StorageBackend};

fn counters() -> Counters {
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    Counters::new(storage.documents()).with_retry(RetryPolicy::immediate())
}

#[tokio::test]
async fn next_counts_from_one_per_owner_and_name() {
    let c = counters();
    assert_eq!(c.peek("cron", "runs").await.unwrap(), 0);
    assert_eq!(c.next("cron", "runs").await.unwrap(), 1);
    assert_eq!(c.next("cron", "runs").await.unwrap(), 2);
    assert_eq!(c.next("cron", "other").await.unwrap(), 1);
    assert_eq!(c.next("flows", "runs").await.unwrap(), 1);
    assert_eq!(c.peek("cron", "runs").await.unwrap(), 2);
}

#[tokio::test]
async fn set_at_least_raises_but_never_lowers() {
    let c = counters();
    assert_eq!(c.set_at_least("cron", "n", 10).await.unwrap(), 10);
    assert_eq!(c.set_at_least("cron", "n", 4).await.unwrap(), 10);
    assert_eq!(c.next("cron", "n").await.unwrap(), 11);
    assert_eq!(c.set_at_least("cron", "n", 11).await.unwrap(), 11);
}

#[tokio::test]
async fn next_refuses_to_overflow() {
    let c = counters();
    c.set_at_least("cron", "max", u64::MAX).await.unwrap();
    let error = c.next("cron", "max").await.unwrap_err();
    assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
}
