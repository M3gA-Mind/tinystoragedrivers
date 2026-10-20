//! Document queue behavior over a test-only implementation of the public port.

#![cfg(feature = "document")]
#![allow(
    clippy::expect_used,
    reason = "integration assertions report fixture setup failures directly"
)]

#[path = "support/document_queue.rs"]
mod document_queue;
#[path = "support/memory_documents.rs"]
mod memory_documents;
#[path = "support/queue_port.rs"]
mod queue_port;

use std::sync::Arc;

use memory_documents::MemoryDocuments;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_queue::{DocumentQueue, NewMessage, QueueStore, ensure};
use tinystoragedrivers_types::{QueueSpec, QueueStats, Retry};

#[tokio::test]
async fn document_queue_passes_portable_queue_conformance_on_document_port() {
    let docs: Arc<dyn DocumentStore> = Arc::new(MemoryDocuments::default());
    ensure(&docs).await.expect("queue collections");
    queue_port::run(&DocumentQueue::new(&docs)).await;
    document_queue::run(&docs).await;
}

#[tokio::test]
async fn extending_a_group_lease_cannot_race_a_later_group_claim() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let base = 1_700_000_000_000;
    let now = Arc::new(AtomicU64::new(base));
    let clock = Arc::clone(&now);
    let queue =
        DocumentQueue::new(&docs).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let name = "extend_group_race";
    queue
        .enqueue(
            name,
            &[
                NewMessage::new(json!({"t":"first"})).group("same"),
                NewMessage::new(json!({"t":"later"})).group("same"),
            ],
        )
        .await
        .expect("enqueue grouped messages");
    let first = queue
        .receive(name, 1, 100, "first-worker")
        .await
        .expect("lease first message")
        .remove(0);

    let gate = backend.pause_next_message_put();
    let extend_queue = queue.clone();
    let receipt = first.receipt.clone();
    let extension = tokio::spawn(async move { extend_queue.extend(name, &receipt, 1_000).await });
    gate.wait_until_paused().await;

    now.store(base + 100, Ordering::SeqCst);
    let competing = queue
        .receive(name, 1, 1_000, "later-worker")
        .await
        .expect("receive at old lease boundary");
    gate.release();
    let extended = extension
        .await
        .expect("extension task")
        .expect("extend result");

    assert!(
        competing.is_empty(),
        "the extended message retains the group reservation"
    );
    assert!(extended, "the active receipt can be extended");
    assert_eq!(
        queue.stats(name).await.expect("stats"),
        QueueStats {
            ready: 1,
            delayed: 0,
            leased: 1,
            dead: 0,
        },
        "a group cannot have two active leases"
    );
}

#[tokio::test]
async fn delayed_nack_cannot_race_a_later_group_claim() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let base = 1_700_000_000_000;
    let now = Arc::new(AtomicU64::new(base));
    let clock = Arc::clone(&now);
    let queue =
        DocumentQueue::new(&docs).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let name = "nack_group_race";
    queue
        .enqueue(
            name,
            &[
                NewMessage::new(json!({"t":"first"})).group("same"),
                NewMessage::new(json!({"t":"later"})).group("same"),
            ],
        )
        .await
        .expect("enqueue grouped messages");
    let first = queue
        .receive(name, 1, 100, "first-worker")
        .await
        .expect("lease first message")
        .remove(0);

    let gate = backend.pause_next_group_get();
    now.store(base + 100, Ordering::SeqCst);
    let nack_queue = queue.clone();
    let receipt = first.receipt.clone();
    let nacking =
        tokio::spawn(async move { nack_queue.nack(name, &receipt, Retry::After(1_000)).await });
    gate.wait_until_paused().await;

    let competing = queue
        .receive(name, 1, 1_000, "later-worker")
        .await
        .expect("receive at old lease boundary");
    gate.release();
    let nacked = nacking.await.expect("nack task").expect("nack result");

    assert_eq!(competing.len(), 1, "one group message may be leased");
    assert!(!nacked, "the delayed nack lost the group-ownership race");
    assert_eq!(
        queue.stats(name).await.expect("stats"),
        QueueStats {
            ready: 1,
            delayed: 0,
            leased: 1,
            dead: 0,
        },
        "a group cannot have two active leases"
    );
}

#[tokio::test]
async fn delayed_nack_does_not_shorten_an_existing_group_reservation() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let base = 1_700_000_000_000;
    let base_i64 = i64::try_from(base).expect("fixed test timestamp fits i64");
    let now = Arc::new(AtomicU64::new(base));
    let clock = Arc::clone(&now);
    let queue =
        DocumentQueue::new(&docs).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let name = "short_nack_group_race";
    queue
        .enqueue(
            name,
            &[
                NewMessage::new(json!({"t":"first"})).group("same"),
                NewMessage::new(json!({"t":"later"})).group("same"),
            ],
        )
        .await
        .expect("enqueue grouped messages");
    let first = queue
        .receive(name, 1, 1_000, "first-worker")
        .await
        .expect("lease first message")
        .remove(0);
    backend.set_group_until(base_i64 + 5_000);

    let gate = backend.pause_next_message_put();
    now.store(base + 100, Ordering::SeqCst);
    let nack_queue = queue.clone();
    let receipt = first.receipt.clone();
    let nacking =
        tokio::spawn(async move { nack_queue.nack(name, &receipt, Retry::After(100)).await });
    gate.wait_until_paused().await;

    now.store(base + 1_500, Ordering::SeqCst);
    assert_eq!(
        backend.group_until(),
        base_i64 + 5_000,
        "pinning a retry must preserve a longer existing reservation"
    );
    gate.release();
    let nacked = nacking.await.expect("nack task").expect("nack result");

    assert!(nacked, "the current receipt can be delayed");
    assert_eq!(
        queue.stats(name).await.expect("stats"),
        QueueStats {
            ready: 2,
            delayed: 0,
            leased: 0,
            dead: 0,
        },
        "the later group message remains unleased while retry is pending"
    );
}

#[tokio::test]
async fn old_ack_cannot_release_a_recreated_deduped_messages_group() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let now = Arc::new(AtomicU64::new(1_700_000_000_000));
    let clock = Arc::clone(&now);
    let queue =
        DocumentQueue::new(&docs).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let name = "ack_dedupe_group_aba";
    queue
        .ensure_queue(name, &QueueSpec::default())
        .await
        .expect("zero-window dedupe queue");
    queue
        .enqueue(
            name,
            &[NewMessage::new(json!({"t":"first"}))
                .group("same")
                .dedupe_key("stable")],
        )
        .await
        .expect("enqueue first generation");
    let first = queue
        .receive(name, 1, 1_000, "first-worker")
        .await
        .expect("lease first generation")
        .remove(0);

    let gate = backend.pause_next_message_delete();
    let ack_queue = queue.clone();
    let old_receipt = first.receipt.clone();
    let old_ack = tokio::spawn(async move { ack_queue.ack(name, &old_receipt).await });
    gate.wait_until_paused().await;

    let recreated = queue
        .enqueue(
            name,
            &[NewMessage::new(json!({"t":"recreated"}))
                .group("same")
                .dedupe_key("stable")],
        )
        .await
        .expect("recreate stable dedupe id after deletion");
    assert_eq!(recreated[0].id, first.receipt.id);
    let current = queue
        .receive(name, 1, 1_000, "current-worker")
        .await
        .expect("lease recreated generation")
        .remove(0);

    gate.release();
    assert!(
        old_ack
            .await
            .expect("old ack task")
            .expect("old ack result")
    );

    queue
        .enqueue(
            name,
            &[NewMessage::new(json!({"t":"later"}))
                .group("same")
                .priority(9)],
        )
        .await
        .expect("enqueue later group message");
    let later = queue
        .receive(name, 1, 1_000, "later-worker")
        .await
        .expect("receive after old ack cleanup");

    assert!(
        later.is_empty(),
        "the recreated generation retains the group lease"
    );
    assert!(
        queue
            .ack(name, &current.receipt)
            .await
            .expect("ack recreated generation"),
        "the recreated generation receipt remains current"
    );
    assert_eq!(
        queue.stats(name).await.expect("stats"),
        QueueStats {
            ready: 1,
            delayed: 0,
            leased: 0,
            dead: 0,
        },
        "the later group message must not be leased concurrently"
    );
}

#[tokio::test]
async fn extend_does_not_take_a_group_reserved_by_another_live_owner() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let now_ms = 1_700_000_000_000_u64;
    let clock = Arc::new(AtomicU64::new(now_ms));
    let shared_clock = Arc::clone(&clock);
    let queue =
        DocumentQueue::new(&docs).with_clock(Arc::new(move || shared_clock.load(Ordering::SeqCst)));
    let name = "foreign_group_reservation";
    queue
        .enqueue(name, &[NewMessage::new(json!({"t":"first"})).group("same")])
        .await
        .expect("enqueue grouped message");
    let first = queue
        .receive(name, 1, 100, "first-worker")
        .await
        .expect("lease grouped message")
        .remove(0);
    backend.set_group_owner("another-message", i64::try_from(now_ms + 5_000).unwrap());

    assert!(
        !queue
            .extend(name, &first.receipt, 1_000)
            .await
            .expect("extend result"),
        "a live foreign reservation prevents extending this message"
    );
    assert!(
        queue
            .ack(name, &first.receipt)
            .await
            .expect("original receipt still settles"),
        "a failed extend leaves the message receipt untouched"
    );
}

#[tokio::test]
async fn failed_group_retry_rolls_back_only_its_own_reservation() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let queue = DocumentQueue::new(&docs);
    let name = "group_retry_rollback";
    queue
        .enqueue(name, &[NewMessage::new(json!({"t":"first"})).group("same")])
        .await
        .expect("enqueue grouped message");
    let first = queue
        .receive(name, 1, 1_000, "first-worker")
        .await
        .expect("lease grouped message")
        .remove(0);
    backend.remove_group_lock();

    let gate = backend.pause_next_group_put();
    let nack_queue = queue.clone();
    let receipt = first.receipt.clone();
    let nacking = tokio::spawn(async move {
        nack_queue
            .nack("group_retry_rollback", &receipt, Retry::After(1_000))
            .await
    });
    gate.wait_until_paused().await;

    assert!(
        queue
            .ack(name, &first.receipt)
            .await
            .expect("concurrent ack result"),
        "the current receipt can settle while retry is paused"
    );
    gate.release();
    assert!(
        !nacking.await.expect("nack task").expect("nack result"),
        "the retry loses after its receipt is acknowledged"
    );
    queue
        .enqueue(name, &[NewMessage::new(json!({"t":"later"})).group("same")])
        .await
        .expect("enqueue later group message");
    assert_eq!(
        queue
            .receive(name, 1, 1_000, "later-worker")
            .await
            .expect("later receive")
            .len(),
        1,
        "failed retry cleanup leaves no ghost group lock"
    );
}

#[tokio::test]
async fn dead_lettering_a_group_message_releases_the_group() {
    let docs: Arc<dyn DocumentStore> = Arc::new(MemoryDocuments::default());
    ensure(&docs).await.expect("queue collections");

    let queue = DocumentQueue::new(&docs);
    let name = "dead_letter_group_release";
    queue
        .enqueue(
            name,
            &[
                NewMessage::new(json!({"t":"first"})).group("same"),
                NewMessage::new(json!({"t":"later"})).group("same"),
            ],
        )
        .await
        .expect("enqueue grouped messages");
    let first = queue
        .receive(name, 1, 1_000, "first-worker")
        .await
        .expect("lease first message")
        .remove(0);
    assert!(
        queue
            .nack(name, &first.receipt, Retry::DeadLetter("poison".into()))
            .await
            .expect("dead-letter result")
    );

    let later = queue
        .receive(name, 1, 1_000, "later-worker")
        .await
        .expect("lease later group message");
    assert_eq!(later.len(), 1, "dead-lettering frees the group");
}

#[tokio::test]
async fn stale_same_message_claim_cannot_shorten_a_new_receivers_group_lease() {
    let backend = Arc::new(MemoryDocuments::default());
    let docs: Arc<dyn DocumentStore> = backend.clone();
    ensure(&docs).await.expect("queue collections");

    let base = 1_700_000_000_000_u64;
    let now = Arc::new(AtomicU64::new(base));
    let clock = Arc::clone(&now);
    let queue =
        DocumentQueue::new(&docs).with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    let name = "same_message_claim_reservation";
    queue
        .enqueue(
            name,
            &[
                NewMessage::new(json!({"t":"first"})).group("same"),
                NewMessage::new(json!({"t":"later"}))
                    .group("same")
                    .priority(9),
            ],
        )
        .await
        .expect("enqueue grouped messages");

    let gate = backend.pause_next_group_put_before_commit();
    let first_queue = queue.clone();
    let first_receive =
        tokio::spawn(async move { first_queue.receive(name, 1, 1, "stale-worker").await });
    gate.wait_until_paused().await;

    now.store(base + 2, Ordering::SeqCst);
    let current = queue
        .receive(name, 1, 100, "current-worker")
        .await
        .expect("new receiver claims expired message")
        .remove(0);
    gate.release();
    let stale = first_receive
        .await
        .expect("stale receive task")
        .expect("stale receive result");
    assert!(stale.is_empty(), "stale claim loses its message CAS");

    now.store(base + 3, Ordering::SeqCst);
    let later = queue
        .receive(name, 1, 100, "later-worker")
        .await
        .expect("later group receive");
    assert!(
        later.is_empty(),
        "the old receiver retry cannot shorten the new receipt's group lease"
    );
    assert!(
        queue
            .ack(name, &current.receipt)
            .await
            .expect("ack current receipt"),
        "the current receipt remains valid"
    );
}
