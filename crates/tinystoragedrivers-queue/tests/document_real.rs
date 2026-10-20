//! Run document queue behavior against the actual document engines.

#![cfg(feature = "document")]
#![allow(
    clippy::expect_used,
    reason = "integration tests report setup failures with explicit context"
)]

use std::sync::Arc;

use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_document::{memory::MemoryConnection, sqlite::SqliteDocuments};
use tinystoragedrivers_queue::{DocumentQueue, ensure};
use tinystoragedrivers_types::Scope;

#[path = "support/document_queue.rs"]
mod document_queue;
#[path = "support/queue_port.rs"]
mod queue_port;

async fn run_document_queue(docs: Arc<dyn DocumentStore>) {
    ensure(&docs).await.expect("queue collections");
    queue_port::run(&DocumentQueue::new(&docs)).await;
    document_queue::run(&docs).await;
}

#[tokio::test]
async fn document_queue_passes_conformance_on_real_memory_engine() {
    let scope = Scope::new("queue_test_memory").expect("scope");
    let docs = MemoryConnection::new().for_scope(scope).documents();
    run_document_queue(docs).await;
}

#[tokio::test]
async fn document_queue_passes_conformance_on_real_sqlite_engine() {
    let directory = tempfile::tempdir().expect("temporary database directory");
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path())
        .expect("SQLite connection");
    let scope = Scope::new("queue_test_sqlite").expect("scope");
    let docs = Arc::new(SqliteDocuments::new(connection, scope)) as Arc<dyn DocumentStore>;
    run_document_queue(docs).await;
}
