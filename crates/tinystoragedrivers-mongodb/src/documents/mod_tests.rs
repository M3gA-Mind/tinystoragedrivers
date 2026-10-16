//! The pure rules behind every write: stored shape, preconditions, versions
//! and expiry.

use super::stored::*;
use mongodb::bson::{Bson, doc};
use serde_json::json;
use tinystoragedrivers_core::{
    CollectionSpec, ErrorKind, Meta, Precondition, Scope, Version, WriteOptions,
};

fn scope() -> Scope {
    Scope::new("alice").unwrap()
}

#[test]
fn encodes_and_decodes_the_stored_shape() {
    let body = json!({"n": 1});
    let meta = Meta {
        schema: 2,
        created_ms: 10,
        updated_ms: 20,
        expires_ms: Some(30),
    };
    let encoded = encode(&scope(), "k", Version(3), body.as_object().unwrap(), &meta).unwrap();
    assert_eq!(
        encoded,
        doc! {
            "_id": {"s": "alice", "k": "k"}, "_key": "k", "_v": 3_i64,
            "_schema": 2_i64, "_c": 10_i64, "_u": 20_i64, "_x": 30_i64,
            "d": {"n": 1_i64},
        }
    );
    let stored = Stored::decode(&encoded).unwrap();
    assert_eq!(
        stored,
        Stored {
            key: "k".into(),
            version: Version(3),
            body: body.clone(),
            meta,
            deleted: false,
        }
    );
    let versioned = stored.into_versioned();
    assert_eq!(
        (versioned.id.as_str(), versioned.version, versioned.doc),
        ("k", Version(3), body)
    );

    assert_eq!(
        encode(
            &scope(),
            "k",
            Version(u64::MAX),
            &serde_json::Map::new(),
            &Meta::unknown()
        )
            .unwrap_err()
            .kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        encode(
            &scope(),
            "k",
            Version(1),
            json!({"n": u64::MAX}).as_object().unwrap(),
            &Meta::unknown()
        )
        .unwrap_err()
        .kind(),
        ErrorKind::Serialization
    );
}

#[test]
fn rejects_stored_documents_missing_driver_fields() {
    for broken in [
        doc! {"_v": 1_i64, "d": {}},
        doc! {"_key": "k", "d": {}},
        doc! {"_key": "k", "_v": 0_i64, "d": {}},
        doc! {"_key": "k", "_v": -4_i64, "d": {}},
        doc! {"_key": "k", "_v": 1_i64},
        doc! {"_key": "k", "_v": 1_i64, "d": {"x": Bson::Double(f64::NAN)}},
    ] {
        assert_eq!(
            Stored::decode(&broken).unwrap_err().kind(),
            ErrorKind::Serialization,
            "{broken:?}"
        );
    }
}

#[test]
fn preconditions_match_the_memory_driver() {
    let v = Version(2);
    assert!(check(Precondition::None, Some(v)).is_ok());
    assert!(check(Precondition::Absent, None).is_ok());
    assert!(check(Precondition::Version(v), Some(v)).is_ok());
    for (precondition, live) in [
        (Precondition::Absent, Some(v)),
        (Precondition::Version(Version(1)), Some(v)),
        (Precondition::Version(v), None),
    ] {
        let error = check(precondition, live).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert!(!error.message().contains("alice"));
    }
}

#[test]
fn versions_rise_until_int64_runs_out() {
    assert_eq!(next_version(None).unwrap(), Version::FIRST);
    assert_eq!(next_version(Some(Version(4))).unwrap(), Version(5));
    let ceiling = Version(u64::try_from(i64::MAX).unwrap());
    assert_eq!(next_version(Some(Version(ceiling.0 - 1))).unwrap(), ceiling);
    assert_eq!(
        next_version(Some(ceiling)).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        next_version(Some(Version(u64::MAX))).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn selectors_pin_a_version() {
    assert_eq!(
        at_version(&scope(), "k", Version(2)),
        doc! {"_id": {"s": "alice", "k": "k"}, "_v": 2_i64}
    );
    assert_eq!(
        at_version(&scope(), "k", Version(u64::MAX)).get("_v"),
        Some(&Bson::Null)
    );
}

#[test]
fn expiry_uses_numbers_at_or_below_now() {
    let spec = CollectionSpec::new("c").ttl("exp");
    let stored = |body| Stored {
        key: "k".into(),
        version: Version(1),
        body,
        meta: Meta::unknown(),
        deleted: false,
    };
    assert!(!is_live(&spec, &stored(json!({"exp": 10})), 10));
    assert!(is_live(&spec, &stored(json!({"exp": 11})), 10));
    assert!(is_live(&spec, &stored(json!({"exp": "5"})), 10));
    assert!(is_live(&spec, &stored(json!({})), 10));
    let forever = CollectionSpec::new("c");
    assert!(is_live(&forever, &stored(json!({"exp": 1})), 10));
    // Only the record's own expiry applies without a TTL field.
    let with_expiry = |expires_ms| Stored {
        meta: Meta {
            expires_ms,
            ..Meta::unknown()
        },
        ..stored(json!({}))
    };
    assert!(!is_live(&forever, &with_expiry(Some(10)), 10));
    assert!(is_live(&forever, &with_expiry(Some(11)), 10));
    assert!(is_live(&forever, &with_expiry(None), 10));
    assert!(expired_filter(&forever, 10).matches_with(
        "k",
        &json!({}),
        &with_expiry(Some(10)).meta
    ));
    assert!(expired_filter(&spec, 10).matches("k", &json!({"exp": 10})));
}

#[test]
fn metadata_keeps_creation_across_updates_and_restarts_it_after_a_gap() {
    let before = Meta {
        schema: 1,
        created_ms: 100,
        updated_ms: 200,
        expires_ms: Some(900),
    };
    // An update keeps the creation time, replaces schema and expiry, and never
    // moves the update time backwards.
    let update = next_meta(Some(&before), WriteOptions::new().schema(2), 150);
    assert_eq!(
        update,
        Meta {
            schema: 2,
            created_ms: 100,
            updated_ms: 200,
            expires_ms: None,
        }
    );
    // Created afresh (never there, deleted or expired) starts both at now.
    let fresh = next_meta(None, WriteOptions::new().expires_ms(5), 150);
    assert_eq!((fresh.created_ms, fresh.updated_ms), (150, 150));
    assert_eq!(fresh.expires_ms, Some(5));
    // A migration may supply the creation time; the update time stays on the clock.
    let migrated = next_meta(None, WriteOptions::new().created_ms(7), 150);
    assert_eq!((migrated.created_ms, migrated.updated_ms), (7, 150));
}

#[test]
fn rows_written_before_metadata_decode_as_unknown() {
    let stored = Stored::decode(&doc! {"_key": "k", "_v": 1_i64, "d": {}}).unwrap();
    assert_eq!(stored.meta, Meta::unknown());
}

fn labelled(labels: &[&str]) -> mongodb::error::Error {
    use mongodb::error::{ErrorKind as MongoKind, WriteConcernError, WriteFailure};
    let concern: WriteConcernError = mongodb::bson::from_document(doc! {
        "code": 64, "codeName": "WriteConcernFailed", "errmsg": "x", "errorLabels": labels,
    })
    .unwrap();
    MongoKind::Write(WriteFailure::WriteConcernError(concern)).into()
}

#[test]
fn only_an_aborted_commit_is_replayed() {
    use super::{CommitFailure, commit_failure};
    assert_eq!(
        commit_failure(&labelled(&["TransientTransactionError"])),
        CommitFailure::Aborted
    );
    assert_eq!(
        commit_failure(&labelled(&["UnknownTransactionCommitResult"])),
        CommitFailure::Unknown
    );
    assert_eq!(
        commit_failure(&labelled(&[
            "TransientTransactionError",
            "UnknownTransactionCommitResult"
        ])),
        CommitFailure::Unknown,
        "an unknown outcome is never replayed"
    );
    assert_eq!(commit_failure(&labelled(&[])), CommitFailure::Failed);
}

#[test]
fn tombstones_are_never_live_and_queries_hide_them() {
    let spec = CollectionSpec::new("c");
    let buried = Stored {
        key: "k".into(),
        version: Version(4),
        body: json!({}),
        meta: Meta::unknown(),
        deleted: true,
    };
    assert!(!is_live(&spec, &buried, 0));
    let decoded = Stored::decode(&doc! {"_key": "k", "_v": 4_i64, "d": {}, "_del": true}).unwrap();
    assert_eq!(decoded, buried);
    assert_eq!(visible(doc! {}), doc! {"_del": {"$exists": false}});
    assert_eq!(
        visible(doc! {"_key": "k"}),
        doc! {"$and": [{"_del": {"$exists": false}}, {"_key": "k"}]}
    );
    assert_eq!(bury(), doc! {"$set": {"d": {}, "_del": true}});
}
