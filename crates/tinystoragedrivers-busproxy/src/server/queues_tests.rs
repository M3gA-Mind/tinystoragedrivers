//! The Queues object enforces its limits before native I/O, sizes replies to
//! the frame and forwards queue port behavior from the supplied backend.

use serde_json::json;
use tinystoragedrivers_bus::StorageKind;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{NewMessage, Retry, Scope};

use super::*;
use crate::Admission;

fn server() -> StorageServer {
    StorageServer::new(memory_storage(&Scope::new("local").unwrap()))
}

async fn call(server: &StorageServer, selector: &str, request: Value) -> Reply {
    server
        .handle(Admission::Private, "Queues", selector, request)
        .await
}

async fn kind(server: &StorageServer, selector: &str, request: Value) -> StorageKind {
    call(server, selector, request).await.unwrap_err().kind
}

fn big(n: usize) -> Vec<NewMessage> {
    let filler = "x".repeat(200 * 1024);
    (0..n)
        .map(|i| NewMessage::new(json!({ "i": i, "fill": filler })))
        .collect()
}

#[tokio::test]
async fn unknown_selectors_and_oversized_enqueues_are_refused() {
    let server = server();
    assert_eq!(
        kind(&server, "Drop", json!({"queue": "q"})).await,
        StorageKind::InvalidInput
    );
    let huge = "x".repeat(MAX_QUEUE_MESSAGE_BYTES);
    assert_eq!(
        kind(
            &server,
            "Enqueue",
            json!({"queue": "q", "messages": [{"payload_json": serde_json::to_string(&huge).unwrap()}]})
        )
        .await,
        StorageKind::InvalidInput
    );
    let many: Vec<_> = (0..=MAX_BATCH_OPS)
        .map(|_| json!({"payload_json": "1"}))
        .collect();
    assert_eq!(
        kind(&server, "Enqueue", json!({"queue": "q", "messages": many})).await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Receive",
            json!({"queue": "q", "max": 0, "visibility_ms": 10, "consumer": "w"})
        )
        .await,
        StorageKind::InvalidInput
    );
    assert_eq!(
        kind(
            &server,
            "Receive",
            json!({"queue": "q", "max": 1, "visibility_ms": 0, "consumer": "w"})
        )
        .await,
        StorageKind::InvalidInput
    );
}

#[tokio::test]
async fn receive_leases_only_what_one_reply_can_carry() {
    let server = server();
    let queues = server.storage.queues();
    queues.enqueue("big", &big(45)).await.unwrap();
    let reply = call(
        &server,
        "Receive",
        json!({"queue": "big", "max": 45, "visibility_ms": 60_000, "consumer": "w"}),
    )
    .await
    .unwrap();
    let first: Vec<wire::QueueDelivery> = serde_json::from_value(reply.clone()).unwrap();
    assert!(!first.is_empty() && first.len() < 45, "{}", first.len());
    assert!(reply.to_string().len() <= MAX_INLINE_BYTES);
    let stats = queues.stats("big").await.unwrap();
    assert_eq!(
        stats.leased,
        first.len() as u64,
        "no lease beyond the reply"
    );
    let reply = call(
        &server,
        "Receive",
        json!({"queue": "big", "max": 45, "visibility_ms": 60_000, "consumer": "w"}),
    )
    .await
    .unwrap();
    let rest: Vec<wire::QueueDelivery> = serde_json::from_value(reply).unwrap();
    assert_eq!(first.len() + rest.len(), 45);
}

#[tokio::test]
async fn dead_letters_are_cut_at_the_inline_cap() {
    let server = server();
    let queues = server.storage.queues();
    queues.enqueue("dlq", &big(45)).await.unwrap();
    let leased = queues.receive("dlq", 45, 60_000, "w").await.unwrap();
    for delivery in &leased {
        assert!(
            queues
                .nack("dlq", &delivery.receipt, Retry::DeadLetter("x".into()))
                .await
                .unwrap()
        );
    }
    let reply = call(&server, "DeadLetters", json!({"queue": "dlq", "limit": 45}))
        .await
        .unwrap();
    let page: Vec<wire::QueueDeadLetter> = serde_json::from_value(reply).unwrap();
    assert!(!page.is_empty() && page.len() < 45, "{}", page.len());
}
