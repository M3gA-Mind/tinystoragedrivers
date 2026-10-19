//! Collection declarations merge and reject invalid storage constraints.
use super::*;

#[test]
fn declarations_validate_and_merge_without_losing_constraints() {
    let initial = CollectionSpec::new("jobs")
        .index(IndexSpec::new("owner", ["owner.id"]))
        .ttl("expires_at")
        .searchable(["summary"]);
    assert!(initial.validate().is_ok());
    let newer = CollectionSpec::new("jobs")
        .index(IndexSpec::new("state", ["state"]).unique())
        .searchable(["summary", "details"]);
    let merged = initial.merge(&newer).unwrap();
    assert_eq!(merged.indexes.len(), 2);
    assert_eq!(merged.search.unwrap().fields, ["summary", "details"]);
    assert_eq!(merged.ttl_field.as_deref(), Some("expires_at"));
}

#[test]
fn declarations_reject_invalid_names_paths_and_conflicts() {
    assert!(CollectionSpec::new("_tsd_owned").validate().is_err());
    assert!(CollectionSpec::new("bad/name").validate().is_err());
    assert!(
        CollectionSpec::new("jobs")
            .index(IndexSpec::new("bad", ["a..b"]))
            .validate()
            .is_err()
    );
    let left = CollectionSpec::new("jobs").index(IndexSpec::new("state", ["state"]));
    let right = CollectionSpec::new("jobs").index(IndexSpec::new("state", ["status"]));
    assert!(left.merge(&right).is_err());
    assert!(
        CollectionSpec::new("one")
            .merge(&CollectionSpec::new("two"))
            .is_err()
    );
}

#[test]
fn field_paths_require_nonempty_segments() {
    assert!(validate_field_path("owner.name").is_ok());
    assert!(validate_field_path("").is_err());
    assert!(validate_field_path("owner..name").is_err());
}

#[test]
fn merge_adds_expiry_and_search_declarations_and_accepts_identical_indexes() {
    let base = CollectionSpec::new("events").index(IndexSpec::new("by_state", ["state"]));
    let extension = CollectionSpec::new("events")
        .index(IndexSpec::new("by_state", ["state"]))
        .ttl("expires")
        .searchable(["summary"]);
    let merged = base.merge(&extension).unwrap();
    assert_eq!(merged.indexes.len(), 1);
    assert_eq!(merged.ttl_field.as_deref(), Some("expires"));
    assert_eq!(merged.search.unwrap().fields, ["summary"]);
}

#[test]
fn merge_rejects_changed_expiry_and_validation_rejects_empty_or_duplicate_indexes() {
    let first = CollectionSpec::new("events").ttl("expires");
    let second = CollectionSpec::new("events").ttl("delete_after");
    assert!(first.merge(&second).is_err());
    assert!(
        CollectionSpec::new("events")
            .index(IndexSpec::new("", ["state"]))
            .validate()
            .is_err()
    );
    assert!(
        CollectionSpec::new("events")
            .index(IndexSpec::new("state", std::iter::empty::<&str>()))
            .validate()
            .is_err()
    );
    assert!(
        CollectionSpec::new("events")
            .index(IndexSpec::new("same", ["a"]))
            .index(IndexSpec::new("same", ["b"]))
            .validate()
            .is_err()
    );
    assert!(
        CollectionSpec::new("events")
            .ttl("a..b")
            .validate()
            .is_err()
    );
    assert!(
        CollectionSpec::new("events")
            .searchable(["a..b"])
            .validate()
            .is_err()
    );
}

#[test]
fn collection_name_validation_covers_empty_and_length_limits() {
    assert!(validate_collection("").is_err());
    assert!(validate_collection(&"x".repeat(MAX_COLLECTION_LEN + 1)).is_err());
    assert!(validate_collection("valid-name.v1").is_ok());
}
