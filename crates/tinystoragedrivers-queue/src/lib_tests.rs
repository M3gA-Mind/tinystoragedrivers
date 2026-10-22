use super::*;

#[tokio::test]
async fn memory_queue_passes_shared_testkit_conformance() {
    let store = MemoryQueue::new();
    tinystoragedrivers_testkit::queue::run(&store).await;
}
