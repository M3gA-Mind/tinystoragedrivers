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
