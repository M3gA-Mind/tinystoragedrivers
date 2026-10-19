//! Gated live contract test for the native MongoDB KV engine.

use super::*;
use mongodb::bson::Document;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tinystoragedrivers_types::ErrorKind;

#[test]
fn serialized_record_preserves_json_null_values() {
    let record = Record {
        id: "scope:key".to_owned(),
        scope: "scope".to_owned(),
        key: "key".to_owned(),
        version: 1,
        expires_ms: None,
        deleted: false,
        value_json: "null".to_owned(),
    };
    let encoded = mongodb::bson::to_document(&record).unwrap();
    let decoded: Record = mongodb::bson::from_document(encoded).unwrap();
    assert_eq!(decoded.value().unwrap(), serde_json::Value::Null);
    assert!(decoded.live(0));
}

#[test]
fn serialized_record_preserves_json_numbers_and_object_keys() {
    let values = [
        json!(u64::MAX),
        json!({"nested": [1, u64::MAX]}),
        json!({"a\0b": 1, "dot.key": 2, "$operator": 3}),
    ];
    for value in values {
        let record = Record {
            id: "scope:key".to_owned(),
            scope: "scope".to_owned(),
            key: "key".to_owned(),
            version: 1,
            expires_ms: None,
            deleted: false,
            value_json: serde_json::to_string(&value).unwrap(),
        };
        let encoded = mongodb::bson::to_document(&record).unwrap();
        let decoded: Record = mongodb::bson::from_document(encoded).unwrap();
        assert_eq!(decoded.value().unwrap(), value);
    }
}

#[tokio::test]
async fn live_native_kv_contract() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let now = Arc::new(AtomicU64::new(10_000));
    let clock_value = Arc::clone(&now);
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database_name = format!("tsd_epic8131_kv_{}_{suffix}", std::process::id());
    let connection = MongoConnection::connect(&uri, &database_name)
        .await
        .unwrap()
        .with_clock(Arc::new(move || clock_value.load(Ordering::SeqCst)));
    let legacy = connection.database().collection::<Document>("kv");
    let legacy_id = format!("kv-legacy-{}-{suffix}", std::process::id());
    legacy
        .insert_one(doc! {"_id": &legacy_id, "value": "preserve"})
        .await
        .unwrap();
    let scope_name = format!("kv-test-{}-{suffix}", std::process::id());
    let scope = Scope::new(&scope_name).unwrap();
    let store = MongoKv::new(connection.clone(), scope.clone())
        .await
        .unwrap();

    let version = store.put("p/b", json!([1, null])).await.unwrap();
    assert_eq!(version, Version::FIRST);
    assert_eq!(
        store.get("p/b").await.unwrap().unwrap().doc,
        json!([1, null])
    );
    store.put("p/null", serde_json::Value::Null).await.unwrap();
    assert_eq!(
        store.get("p/null").await.unwrap().unwrap().doc,
        serde_json::Value::Null
    );
    assert_eq!(
        legacy.find_one(doc! {"_id": &legacy_id}).await.unwrap(),
        Some(doc! {"_id": &legacy_id, "value": "preserve"})
    );
    let conflict = store
        .cas("p/b", json!(false), Precondition::Absent)
        .await
        .unwrap_err();
    assert_eq!(conflict.kind(), ErrorKind::Conflict);
    store.put("p/a", json!(2)).await.unwrap();
    let page = store.list("p/", None, 1).await.unwrap();
    assert_eq!(page.items[0].id, "p/a");
    assert!(page.next.is_some());
    assert_eq!(
        store.list("p/", page.next, 1).await.unwrap().items[0].id,
        "p/b"
    );

    store
        .put_with_ttl("p/expiring", json!(3), Duration::from_millis(10))
        .await
        .unwrap();
    now.fetch_add(10, Ordering::SeqCst);
    assert!(store.get("p/expiring").await.unwrap().is_none());
    assert_eq!(store.put("p/expiring", json!(4)).await.unwrap(), Version(2));

    connection.database().drop().await.unwrap();
}
