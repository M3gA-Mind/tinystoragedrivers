//! Native KV requests carry only scoped keys and values.
use super::*;
use serde_json::json;

#[test]
fn kv_selectors_are_fixed_and_request_rejects_scope() {
    assert_eq!(KV_OBJECT, "Kv");
    assert!(is_kv_selector("Get"));
    assert!(is_kv_selector("PutWithTtl"));
    assert!(!is_kv_selector("Arbitrary"));
    assert_eq!(KV_SELECTORS.len(), 7);
    assert!(serde_json::from_value::<KvKeyRequest>(json!({"key":"x","scope":"forged"})).is_err());
}

#[test]
fn kv_cas_and_cursor_use_shared_types() {
    let cas = KvCasRequest {
        key: "state".into(),
        value: json!({"ready":true}),
        expected: tinystoragedrivers_types::Precondition::Absent,
    };
    let encoded = serde_json::to_value(&cas).unwrap();
    assert_eq!(
        encoded,
        json!({"key":"state","value":{"ready":true},"expected":{"kind":"absent"}})
    );
    let list = KvListRequest {
        prefix: "state/".into(),
        cursor: Some(tinystoragedrivers_types::Cursor("next".into())),
        limit: 17,
    };
    assert_eq!(
        serde_json::from_value::<KvListRequest>(serde_json::to_value(list).unwrap())
            .unwrap()
            .limit,
        17
    );
}
