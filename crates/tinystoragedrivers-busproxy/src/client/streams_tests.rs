use super::*;

#[test]
fn append_batch_conversion_is_bounded_and_preserves_record_order() {
    let converted = records(vec![serde_json::json!(1), serde_json::json!(2)]).unwrap();
    assert_eq!(converted[0].0, serde_json::json!(1));
    assert_eq!(converted[1].0, serde_json::json!(2));

    let oversized = vec![Value::Null; MAX_BATCH_OPS + 1];
    assert_eq!(
        records(oversized).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
}
