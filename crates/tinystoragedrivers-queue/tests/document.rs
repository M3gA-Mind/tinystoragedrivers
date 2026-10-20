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
use tinystoragedrivers_types::{QueueStats, Retry};

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

    let gate = backend.pause_next_group_get();
    now.store(base + 100, Ordering::SeqCst);
    let extend_queue = queue.clone();
    let receipt = first.receipt.clone();
    let extension = tokio::spawn(async move { extend_queue.extend(name, &receipt, 1_000).await });
    gate.wait_until_paused().await;

    let competing = queue
        .receive(name, 1, 1_000, "later-worker")
        .await
        .expect("receive at old lease boundary");
    gate.release();
    let extended = extension
        .await
        .expect("extension task")
        .expect("extend result");

    assert_eq!(competing.len(), 1, "one group message may be leased");
    assert!(!extended, "the extension lost the group-ownership race");
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
