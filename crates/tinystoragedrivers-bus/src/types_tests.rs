//! Wire data excludes storage authority.
use super::*;

#[test]
fn scope_generation_refusal_has_a_distinct_content_free_wire_kind() {
    let failure = StorageFailure {
        kind: StorageKind::ScopeGenerationRejected,
    };
    let wire = serde_json::to_value(failure).unwrap();
    assert_eq!(
        wire,
        serde_json::json!({"kind":"scope_generation_rejected"})
    );
    assert_eq!(
        serde_json::from_value::<StorageFailure>(wire).unwrap(),
        failure
    );
}
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
    assert!(
        serde_json::from_value::<Precondition>(json!({"kind":"absent","scope":"forged"})).is_err()
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
    assert!(serde_json::from_value::<Meta>(json!({"scope":"forged"})).is_err());
}

#[test]
fn versioned_meta_is_optional_and_omitted_when_absent() {
    let bare = json!({"document_id":"a","version":1,"document_json":"{}"});
    let old: Versioned = serde_json::from_value(bare.clone()).unwrap();
    assert_eq!(old.meta, None);
    // A reply without metadata serializes exactly as before contract version 4.
    assert_eq!(serde_json::to_value(&old).unwrap(), bare);
    let meta = Meta {
        schema: 1,
        created_ms: 5,
        updated_ms: 6,
        expires_ms: None,
    };
    let full = Versioned {
        meta: Some(meta),
        ..old
    };
    let wire = serde_json::to_value(&full).unwrap();
    assert_eq!(
        wire["meta"],
        json!({"schema":1,"created_ms":5,"updated_ms":6})
    );
    assert_eq!(serde_json::from_value::<Versioned>(wire).unwrap(), full);
}
