use super::*;

#[test]
fn body_round_trip_handles_text_compressed_and_corrupt_values() {
    let small = serde_json::json!({"note": "short"});
    let encoded = encode(&small).unwrap();
    assert!(matches!(encoded, SqlValue::Text(_)));
    assert_eq!(decode(&encoded).unwrap(), small);

    let large = serde_json::json!({"note": "x".repeat(COMPRESS_OVER * 2)});
    let encoded = encode(&large).unwrap();
    assert!(matches!(encoded, SqlValue::Blob(_)));
    assert_eq!(decode(&encoded).unwrap(), large);

    assert!(decode(&SqlValue::Blob(vec![0, 1, 2])).is_err());
    assert!(decode(&SqlValue::Blob(vec![MARKER, 1, 2])).is_err());
    assert!(decode(&SqlValue::Integer(4)).is_err());
    assert!(decode(&SqlValue::Null).is_err());
    assert!(decode(&SqlValue::Text("not json".into())).is_err());
    assert!(encode(&serde_json::Value::Null).is_ok());
}

#[test]
fn unhelpful_compression_keeps_the_original_text() {
    let text = "x".repeat(COMPRESS_OVER + 1);
    let frame = vec![0; text.len()];
    let stored = compact(text.clone(), &frame);
    assert_eq!(stored, SqlValue::Text(text));
}
