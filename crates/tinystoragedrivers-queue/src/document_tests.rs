use super::*;
use std::sync::Arc;

#[path = "../tests/support/memory_documents.rs"]
mod memory_documents;

#[tokio::test]
async fn document_engine_initializes_and_caches_queue_specs() {
    let docs: Arc<dyn DocumentStore> = Arc::new(memory_documents::MemoryDocuments::default());
    ensure(&docs).await.expect("collections");
    let queue = DocumentQueue::new(&docs);
    queue
        .ensure_queue(
            "cached",
            &QueueSpec {
                max_attempts: 3,
                dedupe_window_ms: 5,
            },
        )
        .await
        .expect("declare queue");
    assert_eq!(queue.spec("cached").await.expect("spec").max_attempts, 3);
}

#[test]
fn sequence_is_increasing_when_the_clock_steps_back() {
    let docs: Arc<dyn DocumentStore> = Arc::new(memory_documents::MemoryDocuments::default());
    let queue = DocumentQueue::new(&docs);
    let seqs: Vec<String> = [1_000, 1_000, 900, 2_000]
        .into_iter()
        .map(|now| queue.next_seq(now).expect("seq"))
        .collect();
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{seqs:?}");
}

#[test]
fn debug_does_not_print_document_contents() {
    let docs: Arc<dyn DocumentStore> = Arc::new(memory_documents::MemoryDocuments::default());
    assert!(format!("{:?}", DocumentQueue::new(&docs)).starts_with("DocumentQueue"));
}
