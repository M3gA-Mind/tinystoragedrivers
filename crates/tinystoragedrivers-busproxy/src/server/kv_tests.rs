//! KV wire limits apply to complete records, not only value bodies.
use super::*;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::Scope;

fn server() -> StorageServer {
    StorageServer::new(memory_storage(&Scope::new("kv-wire-bounds").unwrap()))
}

fn assert_invalid(result: &Result<Value, wire::StorageFailure>) {
    match result {
        Err(error) => assert_eq!(error.kind, wire::StorageKind::InvalidInput),
        Ok(_) => panic!("expected an inline-size refusal"),
    }
}

#[tokio::test]
async fn oversized_kv_writes_are_rejected_before_put_cas_or_ttl_mutates() {
    let server = server();
    let oversized = Value::String("x".repeat(wire::MAX_INLINE_BYTES));
    let put = server
        .handle(
            crate::transport::Admission::Private,
            "Kv",
            "Put",
            serde_json::json!({"key":"new","value":oversized}),
        )
        .await;
    assert_invalid(&put);
    assert_eq!(server.storage.kv().get("new").await.unwrap(), None);

    server
        .storage
        .kv()
        .put("stable", serde_json::json!("old"))
        .await
        .unwrap();
    let cas = server
        .handle(
            crate::transport::Admission::Private,
            "Kv",
            "Cas",
            serde_json::json!({"key":"stable","value":oversized,"expected":{"kind":"version","version":1}}),
        )
        .await;
    assert_invalid(&cas);
    assert_eq!(
        server
            .storage
            .kv()
            .get("stable")
            .await
            .unwrap()
            .unwrap()
            .doc,
        serde_json::json!("old")
    );

    let ttl = server
        .handle(
            crate::transport::Admission::Private,
            "Kv",
            "PutWithTtl",
            serde_json::json!({"key":"ttl","value":oversized,"ttl_ms":1000}),
        )
        .await;
    assert_invalid(&ttl);
    assert_eq!(server.storage.kv().get("ttl").await.unwrap(), None);
}

#[tokio::test]
async fn complete_kv_envelope_fits_inline_limit_and_existing_oversize_is_refused() {
    let server = server();
    let value = Value::String("x".repeat(wire::MAX_INLINE_BYTES - 512));
    let request = wire::KvPutRequest {
        key: "boundary".into(),
        value: value.clone(),
    };
    server
        .handle(
            crate::transport::Admission::Private,
            "Kv",
            "Put",
            serde_json::to_value(request).unwrap(),
        )
        .await
        .unwrap();
    let reply = server
        .handle(
            crate::transport::Admission::Private,
            "Kv",
            "Get",
            serde_json::json!({"key":"boundary"}),
        )
        .await
        .unwrap();
    assert!(reply.to_string().len() <= wire::MAX_INLINE_BYTES);

    let preexisting = Value::String("y".repeat(wire::MAX_INLINE_BYTES));
    server
        .storage
        .kv()
        .put("old-large", preexisting)
        .await
        .unwrap();
    let reply = server
        .handle(
            crate::transport::Admission::Private,
            "Kv",
            "Get",
            serde_json::json!({"key":"old-large"}),
        )
        .await;
    assert_invalid(&reply);
}
