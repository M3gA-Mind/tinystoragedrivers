use super::*;
use serde_json::json;

#[test]
fn sealed_log_round_trip_and_window_clamp_to_retained_base() {
    let log = SealedLog::new(4, vec![json!("a"), json!("b")]).unwrap();
    assert_eq!(log.base(), 4);
    assert_eq!(log.len(), 6);
    assert_eq!(log.window(0, 8)[0].offset, 4);
    assert!(log.window(6, 8).is_empty());
    assert!(log.window(4, 0).is_empty());

    let decoded = SealedLog::decode(&log.encode().unwrap()).unwrap();
    assert_eq!(decoded.window(5, 1)[0].value, json!("b"));
    assert!(SealedLog::decode(b"not zstd").is_err());
    assert!(
        SealedLog::decode(
            &zstd::bulk::compress(br#"{"version":2,"base":0,"values":[]}"#, 3).unwrap()
        )
        .is_err()
    );
    assert!(SealedLog::decode(&zstd::bulk::compress(b"not json", 3).unwrap()).is_err());
    assert!(
        SealedLog::decode(
            &zstd::bulk::compress(
                br#"{"version":1,"base":18446744073709551615,"values":[1]}"#,
                3
            )
            .unwrap()
        )
        .is_err()
    );
}

#[test]
fn sealed_log_rejects_offsets_that_overflow() {
    assert!(SealedLog::new(u64::MAX, vec![json!(1)]).is_err());
}
