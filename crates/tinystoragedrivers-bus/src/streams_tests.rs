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
    assert_eq!(STREAMS_SELECTORS.len(), 8);
    assert!(is_streams_selector("AppendBatchIfOffset"));
    assert!(!is_streams_selector("Arbitrary"));
}
