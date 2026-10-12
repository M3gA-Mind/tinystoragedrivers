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
    for field in [
        "scope",
        "collection",
        "url",
        "database",
        "generation",
        "sender",
    ] {
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
