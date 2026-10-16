//! Record metadata: the wire form of `Meta` and `WriteOptions`.

use serde_json::json;

use super::*;
use crate::{Version, Versioned};

#[test]
fn meta_serializes_to_its_pinned_wire_form() {
    let meta = Meta {
        schema: 2,
        created_ms: 10,
        updated_ms: 20,
        expires_ms: Some(30),
    };
    assert_eq!(
        serde_json::to_value(meta).unwrap(),
        json!({"schema": 2, "created_ms": 10, "updated_ms": 20, "expires_ms": 30}),
    );
    let forever = Meta {
        expires_ms: None,
        ..meta
    };
    assert_eq!(
        serde_json::to_value(forever).unwrap(),
        json!({"schema": 2, "created_ms": 10, "updated_ms": 20}),
        "an absent expiry is omitted, not null"
    );
    assert_eq!(
        serde_json::from_value::<Meta>(json!({"schema": 2, "created_ms": 10, "updated_ms": 20}))
            .unwrap(),
        forever
    );
}

#[test]
fn unknown_meta_is_all_zero_with_no_expiry() {
    let unknown = Meta::unknown();
    assert_eq!(
        (
            unknown.schema,
            unknown.created_ms,
            unknown.updated_ms,
            unknown.expires_ms
        ),
        (0, 0, 0, None)
    );
    assert_eq!(Meta::default(), unknown);
    assert_eq!(serde_json::from_value::<Meta>(json!({})).unwrap(), unknown);
}

#[test]
fn meta_is_expired_at_or_after_its_expiry() {
    let meta = Meta {
        expires_ms: Some(100),
        ..Meta::unknown()
    };
    assert!(!meta.is_expired(99));
    assert!(meta.is_expired(100));
    assert!(meta.is_expired(101));
    assert!(!Meta::unknown().is_expired(i64::MAX));
}

#[test]
fn meta_exposes_its_reserved_paths() {
    let meta = Meta {
        schema: 3,
        created_ms: 10,
        updated_ms: 20,
        expires_ms: Some(30),
    };
    assert_eq!(meta.field("_schema"), Some(json!(3)));
    assert_eq!(meta.field("_created_ms"), Some(json!(10)));
    assert_eq!(meta.field("_updated_ms"), Some(json!(20)));
    assert_eq!(meta.field("_expires_ms"), Some(json!(30)));
    assert_eq!(meta.field("state"), None);
    assert_eq!(Meta::unknown().field("_expires_ms"), None);
}

#[test]
fn write_options_default_to_undeclared_and_never_expiring() {
    let options = WriteOptions::default();
    assert_eq!((options.schema, options.expires_ms), (0, None));
    assert!(options.is_default());
    let declared = WriteOptions::new().schema(4).expires_ms(99);
    assert_eq!((declared.schema, declared.expires_ms), (4, Some(99)));
    assert!(!declared.is_default());
    assert!(!WriteOptions::new().schema(1).is_default());
    assert!(!WriteOptions::new().expires_ms(1).is_default());
}

#[test]
fn write_options_serialize_to_their_pinned_wire_form() {
    assert_eq!(
        serde_json::to_value(WriteOptions::new().schema(4).expires_ms(99)).unwrap(),
        json!({"schema": 4, "expires_ms": 99}),
    );
    assert_eq!(
        serde_json::to_value(WriteOptions::new()).unwrap(),
        json!({"schema": 0}),
    );
    assert_eq!(
        serde_json::from_value::<WriteOptions>(json!({})).unwrap(),
        WriteOptions::new()
    );
}

#[test]
fn versioned_carries_meta_and_reads_older_payloads_without_it() {
    let found = Versioned {
        id: "a".to_owned(),
        version: Version(2),
        doc: json!({"n": 1}),
        meta: Meta {
            schema: 1,
            created_ms: 5,
            updated_ms: 6,
            expires_ms: None,
        },
    };
    let wire = serde_json::to_value(&found).unwrap();
    assert_eq!(
        wire["meta"],
        json!({"schema": 1, "created_ms": 5, "updated_ms": 6})
    );
    assert_eq!(
        serde_json::from_value::<Versioned<serde_json::Value>>(wire).unwrap(),
        found
    );
    let legacy: Versioned<serde_json::Value> =
        serde_json::from_value(json!({"id": "a", "version": 2, "doc": {}})).unwrap();
    assert_eq!(legacy.meta, Meta::unknown());
}
