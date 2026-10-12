//! Typed clients never fall back to ordinary, unbound transport calls.
use super::*;
use crate::Precondition;
use serde_json::json;
use tinybus::{Connection, broker::Broker, transport::memory::MemoryBus};

#[tokio::test]
async fn client_refuses_other_interfaces_and_unbound_confidential_recipients() {
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let task = broker.spawn(bus.clone());
    let caller = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let other = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let target = other.unique_name().unwrap();
    assert!(
        DocumentClient::new(
            caller
                .proxy(
                    target.as_str(),
                    "/documents",
                    "ai.tinyhumans.storage.Other1"
                )
                .unwrap()
        )
        .is_err()
    );
    let client = DocumentClient::new(
        caller
            .proxy(target.as_str(), "/documents", crate::INTERFACE)
            .unwrap(),
    )
    .unwrap();
    assert!(client.get(GetRequest { id: "index".into() }).await.is_err());
    assert!(
        client
            .put(PutRequest {
                id: "index".into(),
                doc: json!({}),
                precondition: Precondition::Absent
            })
            .await
            .is_err()
    );
    assert!(
        client
            .delete(DeleteRequest {
                id: "index".into(),
                precondition: Precondition::None
            })
            .await
            .is_err()
    );
    task.abort();
}
