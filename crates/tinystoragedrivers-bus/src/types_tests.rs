//! Wire data excludes storage authority.
use super::*;
use serde_json::json;

#[test]
fn requests_refuse_arbitrary_authority_fields() {
    for field in ["scope", "collection", "url", "generation", "sender"] {
        let mut get = json!({"document_id":"entry"});
        get[field] = json!("forged");
        assert!(serde_json::from_value::<GetRequest>(get).is_err());
        let mut put =
            json!({"document_id":"entry","document_json":"{}","precondition":{"kind":"absent"}});
        put[field] = json!("forged");
        assert!(serde_json::from_value::<PutRequest>(put).is_err());
        let mut delete = json!({"document_id":"entry","precondition":{"kind":"none"}});
        delete[field] = json!("forged");
        assert!(serde_json::from_value::<DeleteRequest>(delete).is_err());
    }
}
#[test]
fn body_and_cas_condition_preserve_native_wire_values() {
    let request = PutRequest {
        id: "entry".into(),
        doc: json!({"collection":"data","scope":"data"}),
        precondition: Precondition::Version(Version(u64::MAX)),
    };
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(
        wire,
        json!({"document_id":"entry","document_json":"{\"collection\":\"data\",\"scope\":\"data\"}","precondition":{"kind":"version","version":u64::MAX}})
    );
    assert_eq!(serde_json::from_value::<PutRequest>(wire).unwrap(), request);
    let condition: Precondition =
        serde_json::from_value(json!({"kind":"absent","scope":"forged"})).unwrap();
    assert_eq!(
        serde_json::to_value(condition).unwrap(),
        json!({"kind":"absent"})
    );
}
#[test]
fn failures_contain_no_backend_message_or_source() {
    let failure = StorageFailure {
        kind: StorageKind::Fenced,
    };
    assert_eq!(
        serde_json::to_value(failure).unwrap(),
        json!({"kind":"fenced"})
    );
    assert_eq!(failure.to_string(), "document storage operation failed");
}

#[test]
fn meta_pins_its_wire_representation() {
    let meta = Meta {
        schema: 3,
        created_ms: 10,
        updated_ms: 20,
        expires_ms: Some(30),
    };
    let wire = serde_json::to_value(meta).unwrap();
    assert_eq!(
        wire,
        json!({"schema":3,"created_ms":10,"updated_ms":20,"expires_ms":30})
    );
    assert_eq!(serde_json::from_value::<Meta>(wire).unwrap(), meta);
    // No expiry is omitted, and every field defaults, so a sparse peer decodes.
    let open = serde_json::to_value(Meta::default()).unwrap();
    assert_eq!(open, json!({"schema":0,"created_ms":0,"updated_ms":0}));
    assert_eq!(
        serde_json::from_value::<Meta>(json!({})).unwrap(),
        Meta::default()
    );
    let metadata: Meta = serde_json::from_value(json!({"scope":"forged"})).unwrap();
    assert_eq!(serde_json::to_value(metadata).unwrap(), open);
}

#[test]
fn versioned_wire_values_use_shared_native_fields() {
    let bare = json!({"id":"a","version":1,"doc":{}});
    let old: Versioned<Value> = serde_json::from_value(bare.clone()).unwrap();
    assert_eq!(old.meta, Meta::unknown());
    let encoded = serde_json::to_value(&old).unwrap();
    assert_eq!(encoded["id"], "a");
    assert_eq!(encoded["doc"], json!({}));
    assert!(encoded.get("document_id").is_none());
    assert!(encoded.get("document_json").is_none());
    let meta = Meta {
        schema: 1,
        created_ms: 5,
        updated_ms: 6,
        expires_ms: None,
    };
    let full = Versioned { meta, ..old };
    let wire = serde_json::to_value(&full).unwrap();
    assert_eq!(
        wire["meta"],
        json!({"schema":1,"created_ms":5,"updated_ms":6})
    );
    assert_eq!(
        serde_json::from_value::<Versioned<Value>>(wire).unwrap(),
        full
    );
}
