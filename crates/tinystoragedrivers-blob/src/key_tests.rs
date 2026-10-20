use super::*;

#[test]
fn keys_and_cursors_share_the_blob_namespace_rules() {
    validate_key("plain/path").unwrap();
    assert!(validate_key("").is_err());
    assert!(validate_key(&"k".repeat(MAX_KEY_LEN + 1)).is_err());
    assert!(validate_key("control\nkey").is_err());
    assert!(validate_key("_tsd/sealed/stream").is_err());

    validate_prefix("dir/", None).unwrap();
    assert!(validate_prefix(&"p".repeat(MAX_KEY_LEN + 1), None).is_err());
    assert!(validate_prefix("dir/\0", None).is_err());
    assert!(validate_prefix("_tsd/sealed/", None).is_err());
    assert!(validate_prefix("dir/", Some("other/key")).is_err());
    assert!(validate_prefix("dir/", Some("_tsd/sealed/key")).is_err());
    validate_prefix("dir/", Some("dir/after")).unwrap();
}

#[test]
fn byte_ranges_clamp_to_the_body_and_reject_reversed_bounds() {
    assert_eq!(clamp_range(5, 1, 4).unwrap(), 1..4);
    assert_eq!(clamp_range(5, 3, u64::MAX).unwrap(), 3..5);
    assert_eq!(clamp_range(5, u64::MAX, u64::MAX).unwrap(), 5..5);
    assert!(clamp_range(5, 4, 3).is_err());
}
