//! Tests for shared KV key, paging, and write-condition rules.

use super::*;

#[test]
fn cursor_is_bound_to_prefix_and_keyset_version() {
    let cursor = make_cursor("alpha/one", "alpha/").unwrap();
    assert_eq!(cursor_key(&cursor, "alpha/").unwrap(), "alpha/one");
    assert!(cursor_key(&cursor, "beta/").is_err());
}

#[test]
fn key_validation_rejects_empty_and_nul() {
    assert!(validate_key("").is_err());
    assert!(validate_key("a\0b").is_err());
    assert!(validate_key("a").is_ok());
    assert!(validate_prefix("").is_ok());
}

#[test]
fn preconditions_distinguish_absence_versions_and_unconditional_writes() {
    assert!(check_precondition(Precondition::Absent, None).is_ok());
    assert!(check_precondition(Precondition::Version(Version(4)), Some(Version(4))).is_ok());
    assert!(check_precondition(Precondition::None, Some(Version(4))).is_ok());
    assert!(is_conflict(
        &check_precondition(Precondition::Absent, Some(Version(1))).unwrap_err()
    ));
}

#[test]
fn version_exhaustion_is_reported_without_reuse() {
    assert_eq!(next_version(None).unwrap(), Version::FIRST);
    assert_eq!(next_version(Some(Version(8))).unwrap(), Version(9));
    assert!(next_version(Some(Version(u64::MAX))).is_err());
}
