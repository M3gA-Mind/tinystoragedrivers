use super::*;

#[test]
fn parses_storage_and_hides_broker_credentials() {
    assert_eq!(QueueConfig::parse("").unwrap(), QueueConfig::Storage);
    assert_eq!(QueueConfig::parse("storage").unwrap(), QueueConfig::Storage);
    let parsed = QueueConfig::parse("amqps://user:secret@broker/vhost");
    if cfg!(feature = "rabbitmq") {
        let config = parsed.unwrap();
        assert_eq!(config.to_string(), "amqps://***@broker/vhost");
        assert!(!format!("{config:?}").contains("secret"));
    } else {
        assert!(parsed.unwrap_err().message().contains("rabbitmq feature"));
    }
}

#[tokio::test]
async fn storage_selection_preserves_the_supplied_primitive_bundle() {
    let backend = crate::open(&crate::StorageConfig::Memory).await.unwrap();
    let scope = Scope::new("local").unwrap();
    let storage = backend.for_scope(&scope).unwrap();
    let storage = open_queues(&QueueConfig::Storage, storage.clone(), &scope)
        .await
        .unwrap();
    storage.kv().put("key", serde_json::json!(1)).await.unwrap();
    assert_eq!(
        storage
            .kv()
            .get("key")
            .await
            .unwrap()
            .map(|value| value.doc),
        Some(serde_json::json!(1))
    );
}
