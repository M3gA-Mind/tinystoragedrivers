//! Native in-memory queue conformance tests.

#![cfg(feature = "memory")]

#[path = "support/queue_port.rs"]
mod queue_port;

use tinystoragedrivers_queue::MemoryQueue;

#[tokio::test]
async fn native_memory_queue_passes_the_queue_port_conformance_suite() {
    queue_port::run(&MemoryQueue::new()).await;
}
