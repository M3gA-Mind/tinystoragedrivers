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

#[test]
fn persisted_record_validation_and_mongo_error_mapping_are_typed() {
    let mut record = Record {
        id: "scope:key".to_owned(),
        scope: "scope".to_owned(),
        key: "key".to_owned(),
        version: -1,
        expires_ms: None,
        deleted: false,
        value_json: "not-json".to_owned(),
    };
    assert_eq!(record.version().unwrap_err().kind(), ErrorKind::Backend);
    assert!(record.value().is_err());
    assert!(record.live(0));
    record.version = 1;
    assert!(record.live(0));
    record.expires_ms = Some(1);
    assert!(record.live(0));
    assert!(!record.live(1));
    record.deleted = true;
    assert!(!record.live(0));

    assert_eq!(record_id("ab", "c"), "2:abc");
    assert_ne!(record_id("a", "bc"), record_id("ab", "c"));
    assert_eq!(regex_literal("a.b$[c]"), r"a\.b\$\[c\]");

    let custom = mongodb::error::Error::custom("driver failure");
    assert!(!duplicate_key(&custom));
    let mapped = mongo_error("read a value", custom);
    assert_eq!(mapped.kind(), ErrorKind::Unavailable);
    assert!(mapped.message().contains("MongoDB could not read a value"));
}

#[tokio::test]
async fn unreachable_mongodb_errors_are_returned_from_each_public_operation() {
    let client = mongodb::Client::with_uri_str(
        "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=25&connectTimeoutMS=25",
    )
    .await
    .unwrap();
    let store = MongoKv {
        collection: client
            .database("tsd_kv_unreachable_test")
            .collection(COLLECTION),
        scope: "unreachable".to_owned(),
        clock: Arc::new(|| 1_000),
    };
    assert!(format!("{store:?}").contains("unreachable"));
    for error in [
        store.get("key").await.unwrap_err(),
        store.put("key", json!(1)).await.unwrap_err(),
        store
            .cas("key", json!(1), Precondition::None)
            .await
            .unwrap_err(),
        store.delete("key", Precondition::None).await.unwrap_err(),
        store.list("prefix", None, 1).await.unwrap_err(),
        store
            .put_with_ttl("key", json!(1), Duration::from_secs(1))
            .await
            .unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::Unavailable);
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
    assert!(store.capabilities().contains(Capability::Kv));
    assert!(store.capabilities().contains(Capability::Ttl));
    assert!(
        store
            .capabilities()
            .contains(Capability::SharedAcrossProcesses)
    );
    assert!(store.capabilities().contains(Capability::CrossProcessCas));
    assert!(store.get("").await.is_err());
    assert!(store.put("bad\0key", json!(1)).await.is_err());
    assert!(store.list("bad\0prefix", None, 1).await.is_err());
    assert!(store.list("p/", None, 0).await.is_err());
    assert!(!store.delete("p/missing", Precondition::None).await.unwrap());
    assert!(
        store
            .delete("p/missing", Precondition::Version(Version::FIRST))
            .await
            .is_err()
    );

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
    let exact_json = json!({"large": u64::MAX, "nested": {"a\0b": [".", "$"]}});
    store.put("p/exact-json", exact_json.clone()).await.unwrap();
    assert_eq!(
        store.get("p/exact-json").await.unwrap().unwrap().doc,
        exact_json
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
    assert!(
        store
            .cas(
                "p/missing",
                json!(false),
                Precondition::Version(Version::FIRST)
            )
            .await
            .is_err()
    );
    store.put("p/a", json!(2)).await.unwrap();
    let page = store.list("p/", None, 1).await.unwrap();
    assert_eq!(page.items[0].id, "p/a");
    assert!(page.next.is_some());
    assert_eq!(
        store.list("p/", page.next, 1).await.unwrap().items[0].id,
        "p/b"
    );
    assert!(store.list("p/", None, 10).await.unwrap().next.is_none());

    let delete_version = store.put("p/delete", json!(1)).await.unwrap();
    assert!(
        store
            .delete("p/delete", Precondition::Version(delete_version))
            .await
            .unwrap()
    );
    assert!(store.get("p/delete").await.unwrap().is_none());
    assert_eq!(store.put("p/delete", json!(2)).await.unwrap(), Version(3));
    let stale_delete = store
        .delete("p/delete", Precondition::Version(Version(1)))
        .await
        .unwrap_err();
    assert_eq!(stale_delete.kind(), ErrorKind::Conflict);

    let race_version = store.put("p/race", json!(0)).await.unwrap();
    let left_store = store.clone();
    let right_store = store.clone();
    let (left, right) = tokio::join!(
        left_store.cas("p/race", json!(1), Precondition::Version(race_version)),
        right_store.cas("p/race", json!(2), Precondition::Version(race_version)),
    );
    let loser = match (left, right) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        outcome => panic!("expected one successful Mongo CAS, got {outcome:?}"),
    };
    assert_eq!(loser.kind(), ErrorKind::Conflict);

    store
        .put_with_ttl("p/expiring", json!(3), Duration::from_millis(10))
        .await
        .unwrap();
    now.fetch_add(10, Ordering::SeqCst);
    assert!(store.get("p/expiring").await.unwrap().is_none());
    assert!(
        !store
            .list("p/expiring", None, 10)
            .await
            .unwrap()
            .items
            .iter()
            .any(|item| item.id == "p/expiring")
    );
    assert_eq!(store.put("p/expiring", json!(4)).await.unwrap(), Version(2));

    connection.database().drop().await.unwrap();
}
