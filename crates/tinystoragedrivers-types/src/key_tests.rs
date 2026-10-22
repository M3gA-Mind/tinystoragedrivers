//! Key validation, opaque serialization, and supported legacy decoding.
use super::*;

#[test]
fn accepts_valid_opaque_keys_and_rejects_invalid_lengths() {
    assert_eq!(
        Key::new("owner/opaque%2Fkey").unwrap().as_str(),
        "owner/opaque%2Fkey"
    );
    assert!(Key::new("").is_err());
    assert!(Key::new("bad\0key").is_err());
    assert!(Key::new("x".repeat(MAX_KEY_LEN + 1)).is_err());
}

#[test]
fn serde_validates_and_split_decodes_legacy_escapes() {
    let key = Key::new("part/a%2Fb/%25").unwrap();
    assert_eq!(key.split().unwrap(), ["part", "a/b", "%"]);
    assert_eq!(serde_json::to_string(&key).unwrap(), "\"part/a%2Fb/%25\"");
    assert_eq!(
        serde_json::from_str::<Key>("\"safe\"").unwrap(),
        Key::new("safe").unwrap()
    );
    assert!(serde_json::from_str::<Key>("\"\"").is_err());
    assert!(Key::new("%GG").unwrap().split().is_none());
    assert!(Key::new("~opaque-digest").unwrap().is_hashed());
}
