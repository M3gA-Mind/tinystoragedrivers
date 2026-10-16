use super::*;

#[test]
fn rfc3339_converts_to_epoch_milliseconds() {
    assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        parse_rfc3339_ms("2024-05-01T12:30:00.250Z"),
        Some(1_714_566_600_250)
    );
    assert_eq!(
        parse_rfc3339_ms("2024-05-01T14:30:00+02:00"),
        Some(1_714_566_600_000)
    );
    assert_eq!(
        parse_rfc3339_ms("2024-05-01T10:00:00-02:30"),
        Some(1_714_566_600_000)
    );
    assert_eq!(
        parse_rfc3339_ms("2000-02-29T00:00:00.123456Z"),
        Some(951_782_400_123)
    );
}

#[test]
fn malformed_timestamps_are_not_recognised() {
    for text in [
        "",
        "yesterday",
        "2024-05-01",
        "2024-13-01T00:00:00Z",
        "2024-05-01T00:00:00",
        "2024-05-01T00:00:00+0200",
    ] {
        assert_eq!(parse_rfc3339_ms(text), None, "{text}");
    }
}

#[test]
fn the_first_recognised_field_wins() {
    let doc = serde_json::json!({"created_ms": "junk", "created_at": "1970-01-01T00:00:01Z", "createdAt": 5});
    assert_eq!(
        recognised_ms(&doc, &["created_ms", "created_at", "createdAt"]),
        Some(1000)
    );
    assert_eq!(recognised_ms(&doc, &["updated_ms"]), None);
}

#[test]
fn epoch_seconds_are_converted_to_milliseconds() {
    // `profiles.created_at` is epoch seconds in v0.x.
    let doc = serde_json::json!({"created_at": 1_700_000_000, "updatedAt": 1_700_000_000.5});
    assert_eq!(
        recognised_ms(&doc, &["created_ms", "created_at", "createdAt"]),
        Some(1_700_000_000_000)
    );
    assert_eq!(
        recognised_ms(&doc, &["updated_ms", "updated_at", "updatedAt"]),
        Some(1_700_000_000_500)
    );
}

#[test]
fn epoch_milliseconds_are_kept_as_they_are() {
    let doc =
        serde_json::json!({"created_ms": 1_700_000_000_123_i64, "created_at": 99_999_999_999_i64});
    assert_eq!(
        recognised_ms(&doc, &["created_ms"]),
        Some(1_700_000_000_123)
    );
    // The last value read as seconds, one below the cut-off.
    assert_eq!(
        recognised_ms(&doc, &["created_at"]),
        Some(99_999_999_999_000)
    );
    let ms = serde_json::json!({"created_at": 100_000_000_000_i64});
    assert_eq!(recognised_ms(&ms, &["created_at"]), Some(100_000_000_000));
}

#[test]
fn fractional_ms_fields_are_milliseconds_rounded_to_the_nearest() {
    // A modern value with a fraction is not dropped.
    let doc = serde_json::json!({"created_ms": 1_700_000_000_000.5_f64});
    assert_eq!(
        recognised_ms(&doc, &["created_ms"]),
        Some(1_700_000_000_001)
    );
    // A small `_ms` value is milliseconds, not seconds: never multiplied.
    let doc = serde_json::json!({"updated_ms": 1500.4_f64, "expires_ms": 1500.6_f64});
    assert_eq!(recognised_ms(&doc, &["updated_ms"]), Some(1500));
    assert_eq!(recognised_ms(&doc, &["expires_ms"]), Some(1501));
    let doc = serde_json::json!({"created_ms": -2.5_f64, "bad_ms": 1.0e30_f64});
    assert_eq!(recognised_ms(&doc, &["created_ms"]), Some(-3));
    assert_eq!(recognised_ms(&doc, &["bad_ms"]), None);
}

#[test]
fn fractional_values_of_an_unknown_unit_use_the_seconds_heuristic() {
    let doc = serde_json::json!({
        "seconds": 1_700_000_000.25_f64,
        "millis": 1_700_000_000_000.5_f64,
    });
    assert_eq!(recognised_ms(&doc, &["seconds"]), Some(1_700_000_000_250));
    assert_eq!(recognised_ms(&doc, &["millis"]), Some(1_700_000_000_001));
}
