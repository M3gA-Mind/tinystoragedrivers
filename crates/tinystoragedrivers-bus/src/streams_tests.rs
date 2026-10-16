use super::*;
use serde_json::json;

#[test]
fn stream_messages_roundtrip_with_inline_json_records() {
    let req = AppendBatchIfOffsetRequest {
        stream: "log".into(),
        expected_next_offset: 7,
        records: vec![
            StreamRecord(json!({"a": {"id": 1}})),
            StreamRecord(json!(2)),
        ],
    };
    let wire = serde_json::to_value(&req).unwrap();
    assert!(wire["records"][0].is_string());
    assert_eq!(
        serde_json::from_value::<AppendBatchIfOffsetRequest>(wire).unwrap(),
        req
    );
    let item = StreamItem {
        offset: 3,
        record: json!({"x": 1}),
    };
    let wire = serde_json::to_value(&item).unwrap();
    assert_eq!(wire["record_json"], "{\"x\":1}");
    assert_eq!(serde_json::from_value::<StreamItem>(wire).unwrap(), item);
}

#[test]
fn stream_requests_reject_authority_selectors() {
    assert!(serde_json::from_value::<StreamRequest>(json!({"stream":"s","scope":"bob"})).is_err());
    assert!(
        serde_json::from_value::<ReadWindowRequest>(
            json!({"stream":"s","from":0,"limit":1,"db":"x"})
        )
        .is_err()
    );
}

#[test]
fn stream_selectors_are_fixed() {
    assert_eq!(STREAMS_OBJECT, "Streams");
    assert_eq!(STREAMS_SELECTORS.len(), 10);
    assert!(is_streams_selector("AppendBatchIfOffset"));
    assert!(is_streams_selector("Seal"));
    assert!(is_streams_selector("Info"));
    assert!(!is_streams_selector("Arbitrary"));
}

#[test]
fn seal_reply_pins_its_wire_representation() {
    let reply = SealResponse {
        base: 2,
        len: 9,
        blob_bytes: 120,
    };
    let wire = serde_json::to_value(reply).unwrap();
    assert_eq!(wire, json!({"base":2,"len":9,"blob_bytes":120}));
    assert_eq!(serde_json::from_value::<SealResponse>(wire).unwrap(), reply);
    assert!(
        serde_json::from_value::<SealResponse>(
            json!({"base":0,"len":0,"blob_bytes":0,"scope":"x"})
        )
        .is_err()
    );
}

#[test]
fn info_reply_pins_its_wire_representation() {
    let reply = StreamInfoResponse {
        base: 1,
        len: 8,
        sealed_ms: Some(1_700_000_000_500),
        last_append_ms: None,
    };
    let wire = serde_json::to_value(reply).unwrap();
    assert_eq!(
        wire,
        json!({"base":1,"len":8,"sealed_ms":1_700_000_000_500_i64,"last_append_ms":null})
    );
    assert_eq!(
        serde_json::from_value::<StreamInfoResponse>(wire).unwrap(),
        reply
    );
    assert!(
        serde_json::from_value::<StreamInfoResponse>(
            json!({"base":0,"len":0,"sealed_ms":null,"last_append_ms":null,"scope":"x"})
        )
        .is_err()
    );
}
