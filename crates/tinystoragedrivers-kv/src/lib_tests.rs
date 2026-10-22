use super::*;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn memory_kv_passes_shared_testkit_conformance() {
    let store = MemoryKv::new(Scope::new("kv_testkit").unwrap());
    tinystoragedrivers_testkit::kv::run(&store).await;
}
