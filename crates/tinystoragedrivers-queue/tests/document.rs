//! Document queue behavior over a test-only implementation of the public port.

#![cfg(feature = "document")]

#[path = "support/document_queue.rs"]
mod document_queue;
#[path = "support/memory_documents.rs"]
mod memory_documents;
#[path = "support/queue_port.rs"]
mod queue_port;

use std::sync::Arc;

use memory_documents::MemoryDocuments;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_queue::{DocumentQueue, ensure};

#[tokio::test]
async fn document_queue_passes_portable_queue_conformance_on_document_port() {
    let docs: Arc<dyn DocumentStore> = Arc::new(MemoryDocuments::default());
    ensure(&docs).await.expect("queue collections");
    queue_port::run(&DocumentQueue::new(&docs)).await;
    document_queue::run(&docs).await;
}
