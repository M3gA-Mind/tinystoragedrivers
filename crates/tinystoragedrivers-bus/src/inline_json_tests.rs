//! Inline JSON preserves business data without treating it as stream references.
use super::*;
use crate::{Precondition, PutRequest, Version, Versioned};
use serde_json::json;

#[test]
fn nested_stream_shaped_business_data_roundtrips_as_inline_text() {
    let document = json!({"nested":[{"id":"business-id"},{"id":"attachment","content_type":"text/plain","len":7}]});
    let request = PutRequest {
        id: "index".into(),
        doc: document.clone(),
        precondition: Precondition::Absent,
    };
    let wire = serde_json::to_value(&request).unwrap();
    assert_eq!(wire["document_id"], "index");
    assert!(wire["document_json"].is_string());
    assert_eq!(
        serde_json::from_value::<PutRequest>(wire).unwrap().doc,
        document
    );
    let result = Versioned {
        id: "index".into(),
        version: Version(1),
        doc: document.clone(),
        meta: None,
    };
    let wire = serde_json::to_value(result).unwrap();
    assert_eq!(
        serde_json::from_value::<Versioned>(wire).unwrap().doc,
        document
    );
}

#[test]
fn documents_require_inline_text_and_malformed_text_errors_are_fixed() {
    for input in [json!({"private":"secret"}), json!(7), Value::Null] {
        assert!(
            serde_json::from_value::<PutRequest>(
                json!({"document_id":"index","document_json":input,"precondition":{"kind":"none"}})
            )
            .is_err()
        );
    }
    let error = serde_json::from_value::<PutRequest>(json!({
        "document_id":"index", "document_json":"private malformed json",
        "precondition":{"kind":"none"}
    }))
    .unwrap_err()
    .to_string();
    assert!(error.contains("invalid inline document json"));
    assert!(!error.contains("private"));
}
