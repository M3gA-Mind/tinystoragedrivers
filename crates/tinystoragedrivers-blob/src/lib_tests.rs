use super::*;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn memory_blobs_pass_shared_testkit_conformance() {
    let store = MemoryBlobStore::new(Scope::new("blob_testkit").unwrap());
    tinystoragedrivers_testkit::blobs::run(&store).await;
}
