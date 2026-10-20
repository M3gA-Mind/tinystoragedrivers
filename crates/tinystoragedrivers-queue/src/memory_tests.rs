#![allow(
    clippy::expect_used,
    reason = "tests report failed queue operations by panicking"
)]

use super::*;
use crate::{MemoryQueue, QueueStore};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};

fn clock() -> (MemoryQueue, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(1_000));
    let time = Arc::clone(&now);
    (
        MemoryQueue::with_clock(Arc::new(move || time.load(Ordering::SeqCst))),
        now,
    )
}

#[tokio::test]
async fn priority_delay_and_ack_follow_the_queue_contract() {
    let (queue, now) = clock();
    queue
        .enqueue(
            "jobs",
            &[
                NewMessage::new(json!("first")),
                NewMessage::new(json!("later")).delay_ms(10),
                NewMessage::new(json!("urgent")).priority(9),
            ],
        )
        .await
        .expect("enqueue");
    let first = queue
        .receive("jobs", 10, 50, "worker")
        .await
        .expect("receive");
    assert_eq!(
        first
            .iter()
            .map(|d| d.payload.as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        ["urgent", "first"]
    );
    assert!(queue.ack("jobs", &first[0].receipt).await.expect("ack"));
    now.store(1_010, Ordering::SeqCst);
    assert_eq!(
        queue
            .receive("jobs", 10, 50, "worker")
            .await
            .expect("receive")[0]
            .payload,
        json!("later")
    );
}

#[tokio::test]
async fn dedupe_tombstones_expire_and_stale_receipts_do_not_settle() {
    let (queue, now) = clock();
    queue
        .ensure_queue(
            "dedupe",
            &QueueSpec {
                max_attempts: 2,
                dedupe_window_ms: 5,
            },
        )
        .await
        .expect("declare");
    let first = queue
        .enqueue("dedupe", &[NewMessage::new(json!(1)).dedupe_key("same")])
        .await
        .expect("enqueue");
    let lease = queue
        .receive("dedupe", 1, 20, "worker")
        .await
        .expect("receive")
        .remove(0);
    assert!(queue.ack("dedupe", &lease.receipt).await.expect("ack"));
    let duplicate = queue
        .enqueue("dedupe", &[NewMessage::new(json!(2)).dedupe_key("same")])
        .await
        .expect("dedupe");
    assert!(duplicate[0].duplicate);
    assert!(
        !queue
            .ack("dedupe", &lease.receipt)
            .await
            .expect("stale ack")
    );
    now.store(1_006, Ordering::SeqCst);
    let after_expiry = queue
        .enqueue("dedupe", &[NewMessage::new(json!(2)).dedupe_key("same")])
        .await
        .expect("after expiry");
    assert!(!after_expiry[0].duplicate);
    assert_eq!(first[0].id, duplicate[0].id);
    assert_ne!(first[0].id, after_expiry[0].id);
}

#[tokio::test]
async fn failed_dedupe_ack_keeps_the_leased_message_intact() {
    let (queue, now) = clock();
    queue
        .ensure_queue(
            "overflow",
            &QueueSpec {
                max_attempts: 1,
                dedupe_window_ms: 20,
            },
        )
        .await
        .expect("declare");
    let enqueued = queue
        .enqueue(
            "overflow",
            &[NewMessage::new(json!({"preserve": true})).dedupe_key("key")],
        )
        .await
        .expect("enqueue");
    let delivery = queue
        .receive("overflow", 1, 10, "worker")
        .await
        .expect("receive")
        .remove(0);
    now.store(i64::MAX as u64 - 10, Ordering::SeqCst);

    assert!(queue.ack("overflow", &delivery.receipt).await.is_err());
    let data = queue.lock().expect("queue lock");
    let message = data.messages.get(&enqueued[0].id).expect("leased message");
    assert_eq!(message.payload, json!({"preserve": true}));
    assert!(matches!(message.state, State::Leased { .. }));
}

#[tokio::test]
async fn groups_stay_sticky_until_a_delayed_retry_is_due() {
    let (queue, now) = clock();
    queue
        .enqueue(
            "groups",
            &[
                NewMessage::new(json!(1)).group("chat"),
                NewMessage::new(json!(2)).group("chat"),
            ],
        )
        .await
        .expect("enqueue");
    let first = queue
        .receive("groups", 10, 20, "worker")
        .await
        .expect("first receive");
    assert_eq!(first.len(), 1);
    assert!(
        queue
            .nack("groups", &first[0].receipt, Retry::After(10))
            .await
            .expect("retry")
    );
    assert_eq!(
        queue
            .receive("groups", 10, 5, "worker")
            .await
            .expect("blocked group"),
        Vec::<Delivery>::new()
    );
    now.store(1_010, Ordering::SeqCst);
    let retry = queue
        .receive("groups", 10, 5, "worker")
        .await
        .expect("retry due");
    assert_eq!(retry.len(), 1);
    assert_eq!(retry[0].payload, json!(1));
}

#[tokio::test]
async fn max_attempts_dead_letters_and_redrive_resets_attempts() {
    let (queue, _) = clock();
    queue
        .ensure_queue(
            "retries",
            &QueueSpec {
                max_attempts: 1,
                dedupe_window_ms: 0,
            },
        )
        .await
        .expect("declare");
    queue
        .enqueue("retries", &[NewMessage::new(json!("body"))])
        .await
        .expect("enqueue");
    let first = queue
        .receive("retries", 1, 1, "worker")
        .await
        .expect("first")
        .remove(0);
    assert!(
        queue
            .nack("retries", &first.receipt, Retry::After(0))
            .await
            .expect("nack")
    );
    let second = queue
        .receive("retries", 1, 1, "worker")
        .await
        .expect("dead-letter on attempt");
    assert_eq!(second, Vec::<Delivery>::new());
    let dead = queue
        .dead_letters("retries", 10)
        .await
        .expect("dead letters");
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "max attempts exceeded");
    assert!(
        queue
            .redrive("retries", &dead[0].id)
            .await
            .expect("redrive")
    );
    assert_eq!(
        queue
            .receive("retries", 1, 1, "worker")
            .await
            .expect("redeliver")[0]
            .attempt,
        1
    );
}

#[tokio::test]
async fn extend_stats_purge_and_input_checks_are_consistent() {
    let (queue, now) = clock();
    assert!(
        queue
            .ensure_queue(
                "invalid_spec",
                &QueueSpec {
                    max_attempts: 0,
                    dedupe_window_ms: 0,
                },
            )
            .await
            .is_err()
    );
    queue
        .enqueue(
            "stats",
            &[
                NewMessage::new(json!("leased")),
                NewMessage::new(json!("delayed")).delay_ms(100),
            ],
        )
        .await
        .expect("enqueue");
    let lease = queue
        .receive("stats", 1, 10, "worker")
        .await
        .expect("receive")
        .remove(0);
    assert!(
        queue
            .extend("stats", &lease.receipt, 50)
            .await
            .expect("extend")
    );
    let stats = queue.stats("stats").await.expect("stats");
    assert_eq!(stats.leased, 1);
    assert_eq!(stats.delayed, 1);
    now.store(1_060, Ordering::SeqCst);
    assert_eq!(queue.stats("stats").await.expect("expired lease").ready, 1);
    assert!(queue.receive("stats", 0, 10, "worker").await.is_err());
    assert!(queue.receive("stats", 1, 0, "worker").await.is_err());
    assert!(queue.enqueue("Bad", &[]).await.is_err());
    assert!(
        queue
            .enqueue("stats", &[NewMessage::new(json!(null)).group("")])
            .await
            .is_err()
    );
    assert!(
        queue
            .enqueue("stats", &[NewMessage::new(json!(null)).dedupe_key("")])
            .await
            .is_err()
    );
    assert!(
        queue
            .enqueue(
                "stats",
                &[NewMessage::new(json!(null))
                    .priority(tinystoragedrivers_types::MAX_PRIORITY + 1)],
            )
            .await
            .is_err()
    );
    assert_eq!(queue.purge("stats").await.expect("purge"), 2);
    assert_eq!(
        queue.stats("stats").await.expect("empty"),
        QueueStats::default()
    );
}
