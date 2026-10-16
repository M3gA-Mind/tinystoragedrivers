use serde_json::json;

use super::*;

#[test]
fn small_bodies_stay_text() {
    let doc = json!({"a": 1});
    assert_eq!(encode(&doc).unwrap(), SqlValue::Text(r#"{"a":1}"#.into()));
    assert_eq!(decode(&encode(&doc).unwrap()).unwrap(), doc);
}

#[test]
fn large_bodies_become_a_marked_zstd_blob() {
    let doc = json!({"pad": "x".repeat(COMPRESS_OVER * 2)});
    let SqlValue::Blob(bytes) = encode(&doc).unwrap() else {
        panic!("expected a blob");
    };
    assert_eq!(bytes[0], MARKER);
    assert!(bytes.len() < 200);
    assert_eq!(decode(&SqlValue::Blob(bytes)).unwrap(), doc);
}

#[test]
fn dense_bodies_round_trip_in_whichever_form_is_smaller() {
    // Pseudo-random letters barely compress.
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let noise: String = (0..COMPRESS_OVER)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            char::from(b'a' + u8::try_from((state >> 59) & 0x0F).unwrap() + 1)
        })
        .collect();
    let doc = json!({"noise": noise, "more": noise.chars().rev().collect::<String>()});
    let encoded = encode(&doc).unwrap();
    assert_eq!(decode(&encoded).unwrap(), doc);
}

#[test]
fn corrupt_values_are_refused() {
    for bad in [
        SqlValue::Blob(Vec::new()),
        SqlValue::Blob(vec![9, 1, 2]),
        SqlValue::Blob(vec![MARKER, 1, 2, 3]),
        SqlValue::Integer(1),
        SqlValue::Null,
    ] {
        assert!(decode(&bad).is_err(), "{bad:?}");
    }
}
