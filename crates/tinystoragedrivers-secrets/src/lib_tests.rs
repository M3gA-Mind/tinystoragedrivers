use super::*;

#[tokio::test]
async fn memory_secrets_pass_shared_testkit_conformance() {
    let store = MemorySecrets::new();
    tinystoragedrivers_testkit::secrets::run(&store).await;
}
