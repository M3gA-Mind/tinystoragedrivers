//! Secret-name validation contract.

use super::*;

#[test]
fn accepts_namespaced_printable_names() {
    validate_secret_name("alice:openai_api_key").unwrap();
    validate_secret_name("with space and ünïcode").unwrap();
    validate_secret_name(&"x".repeat(MAX_SECRET_NAME_LEN)).unwrap();
}

#[test]
fn rejects_empty_long_and_control_names_without_echoing_them() {
    for bad in [
        String::new(),
        "x".repeat(MAX_SECRET_NAME_LEN + 1),
        "nul\0byte".to_string(),
        "new\nline".to_string(),
        "sk-live\0secret".to_string(),
    ] {
        let error = validate_secret_name(&bad).unwrap_err();
        assert_eq!(error.kind(), crate::ErrorKind::InvalidInput);
        assert!(!error.to_string().contains("sk-live"));
    }
}

#[test]
fn prefixes_may_be_empty_but_not_long_or_control() {
    validate_secret_prefix("").unwrap();
    validate_secret_prefix("alice:").unwrap();
    assert_eq!(
        validate_secret_prefix(&"x".repeat(MAX_SECRET_NAME_LEN + 1))
            .unwrap_err()
            .kind(),
        crate::ErrorKind::InvalidInput
    );
    assert_eq!(
        validate_secret_prefix("a\0").unwrap_err().kind(),
        crate::ErrorKind::InvalidInput
    );
}

#[test]
fn secret_versions_round_trip_bytes_without_exposing_them_in_debug() {
    let version = SecretVersion::from_bytes([0xA5; 32]);
    assert_eq!(version.as_bytes(), &[0xA5; 32]);
    assert_eq!(version.into_bytes(), [0xA5; 32]);
    assert_eq!(format!("{version:?}"), "SecretVersion(..)");
    assert_eq!(
        SecretPrecondition::Version(version),
        SecretPrecondition::Version(SecretVersion::from_bytes([0xA5; 32]))
    );
}
