//! Filter evaluation, combinators, validation, serde form and sorting.

use serde_json::json;

use super::*;
use crate::{ErrorKind, Meta};

fn doc() -> Value {
    json!({"state": "queued", "n": 5, "owner": {"name": "ada"}, "maybe": null})
}

#[test]
fn equality_and_membership() {
    let d = doc();
    assert!(Filter::eq("state", "queued").matches("a", &d));
    assert!(Filter::eq("n", 5.0).matches("a", &d));
    assert!(!Filter::eq("missing", 1).matches("a", &d));
    assert!(Filter::ne("state", "done").matches("a", &d));
    assert!(Filter::ne("missing", 1).matches("a", &d));
    assert!(!Filter::ne("state", "queued").matches("a", &d));
    assert!(Filter::one_of("state", ["done", "queued"]).matches("a", &d));
    assert!(!Filter::one_of("state", ["done"]).matches("a", &d));
    assert!(!Filter::one_of("missing", ["x"]).matches("a", &d));
    assert!(Filter::eq("owner.name", "ada").matches("a", &d));
}

#[test]
fn id_field_addresses_the_document_id() {
    let d = doc();
    assert!(Filter::eq(ID_FIELD, "job-1").matches("job-1", &d));
    assert!(!Filter::eq(ID_FIELD, "job-1").matches("job-2", &d));
    assert!(Filter::gte(ID_FIELD, "job-1").matches("job-2", &d));
}

#[test]
fn ranges_respect_every_bound_and_type() {
    let d = doc();
    assert!(Filter::gt("n", 4).matches("a", &d));
    assert!(!Filter::gt("n", 5).matches("a", &d));
    assert!(Filter::gte("n", 5).matches("a", &d));
    assert!(Filter::lt("n", 6).matches("a", &d));
    assert!(!Filter::lt("n", 5).matches("a", &d));
    assert!(Filter::lte("n", 5).matches("a", &d));
    assert!(!Filter::gt("n", "1").matches("a", &d), "type mismatch");
    assert!(!Filter::gt("missing", 0).matches("a", &d));
    assert!(Filter::gt("n", 1).and(Filter::lt("n", 9)).matches("a", &d));
}

#[test]
fn existence_counts_null_as_present() {
    let d = doc();
    assert!(Filter::exists("maybe", true).matches("a", &d));
    assert!(Filter::exists("missing", false).matches("a", &d));
    assert!(!Filter::exists("state", false).matches("a", &d));
}

#[test]
fn combinators_compose_and_flatten() {
    let d = doc();
    assert!(Filter::All.matches("a", &d));
    assert_eq!(Filter::All.and(Filter::eq("x", 1)), Filter::eq("x", 1));
    assert_eq!(Filter::eq("x", 1).and(Filter::All), Filter::eq("x", 1));

    let three = Filter::eq("a", 1)
        .and(Filter::eq("b", 2))
        .and(Filter::eq("c", 3));
    let Filter::And { filters } = &three else {
        panic!("expected a conjunction")
    };
    assert_eq!(filters.len(), 3);

    let merged = Filter::eq("a", 1)
        .and(Filter::eq("b", 2))
        .and(Filter::eq("c", 3).and(Filter::eq("d", 4)));
    let Filter::And { filters } = &merged else {
        panic!("expected a conjunction")
    };
    assert_eq!(filters.len(), 4);

    let either = Filter::eq("state", "x")
        .or(Filter::eq("state", "y"))
        .or(Filter::eq("state", "queued"));
    assert!(either.matches("a", &d));
    assert!(
        !Filter::eq("state", "x")
            .or(Filter::eq("n", 1))
            .matches("a", &d)
    );
    assert!(Filter::eq("state", "done").negate().matches("a", &d));
}

#[test]
fn validation_rejects_untranslatable_clauses() {
    assert!(Filter::All.validate().is_ok());
    assert!(
        Filter::eq("a.b", 1)
            .and(Filter::gt("n", 1).negate())
            .or(Filter::exists("x", true))
            .validate()
            .is_ok()
    );
    for bad in [
        Filter::eq("", 1),
        Filter::ne("a..b", 1),
        Filter::one_of(".a", [1]),
        Filter::exists("a.", true),
        Filter::Range {
            field: "n".into(),
            gt: None,
            gte: None,
            lt: None,
            lte: None,
        },
        Filter::eq("ok", 1).and(Filter::eq("", 1)),
        Filter::eq("", 1).negate(),
    ] {
        assert_eq!(
            bad.validate().unwrap_err().kind(),
            ErrorKind::InvalidInput,
            "{bad:?}"
        );
    }
    assert!(
        Filter::Range {
            field: String::new(),
            gt: Some(json!(1)),
            gte: None,
            lt: None,
            lte: None
        }
        .validate()
        .is_err()
    );
}

#[test]
fn serde_form_is_tagged_by_op() {
    let filter = Filter::eq("state", "queued").and(Filter::lt("n", 3));
    let encoded = serde_json::to_value(&filter).unwrap();
    assert_eq!(
        encoded,
        json!({"op": "and", "filters": [
            {"op": "eq", "field": "state", "value": "queued"},
            {"op": "range", "field": "n", "lt": 3}
        ]})
    );
    let back: Filter = serde_json::from_value(encoded).unwrap();
    assert_eq!(back, filter);
}

#[test]
fn sorts_by_keys_then_id() {
    let mut items = vec![
        ("c".to_owned(), json!({"p": 1})),
        ("a".to_owned(), json!({"p": 2})),
        ("b".to_owned(), json!({"p": 1})),
        ("d".to_owned(), json!({})),
    ];
    sort_documents(&mut items, &[Sort::asc("p")], |(id, doc)| {
        (id.as_str(), doc)
    });
    let ids: Vec<_> = items.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["d", "b", "c", "a"]);

    sort_documents(&mut items, &[Sort::desc("p")], |(id, doc)| {
        (id.as_str(), doc)
    });
    let ids: Vec<_> = items.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c", "d"]);

    sort_documents(&mut items, &[], |(id, doc)| (id.as_str(), doc));
    let ids: Vec<_> = items.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["a", "b", "c", "d"]);

    assert_eq!(Direction::default(), Direction::Asc);
    assert_eq!(
        serde_json::to_value(Sort::desc("x")).unwrap(),
        json!({"field": "x", "direction": "desc"})
    );
}

fn meta() -> Meta {
    Meta {
        schema: 2,
        created_ms: 1_000,
        updated_ms: 2_000,
        expires_ms: Some(9_000),
    }
}

#[test]
fn reserved_metadata_paths_address_the_record_not_the_body() {
    let d = doc();
    let m = meta();
    assert!(Filter::eq(CREATED_FIELD, 1_000).matches_with("a", &d, &m));
    assert!(Filter::eq(UPDATED_FIELD, 2_000).matches_with("a", &d, &m));
    assert!(Filter::eq(SCHEMA_FIELD, 2).matches_with("a", &d, &m));
    assert!(Filter::eq(EXPIRES_FIELD, 9_000).matches_with("a", &d, &m));
    assert!(Filter::gte(CREATED_FIELD, 1_000).matches_with("a", &d, &m));
    assert!(!Filter::gt(CREATED_FIELD, 1_000).matches_with("a", &d, &m));
    assert!(Filter::lt(UPDATED_FIELD, 3_000).matches_with("a", &d, &m));
    assert!(Filter::one_of(SCHEMA_FIELD, [1, 2]).matches_with("a", &d, &m));
    assert!(Filter::exists(EXPIRES_FIELD, true).matches_with("a", &d, &m));
    let forever = Meta {
        expires_ms: None,
        ..m
    };
    assert!(Filter::exists(EXPIRES_FIELD, false).matches_with("a", &d, &forever));
    assert!(!Filter::eq(EXPIRES_FIELD, 9_000).matches_with("a", &d, &forever));
}

#[test]
fn a_body_field_with_a_reserved_name_is_shadowed_by_the_metadata() {
    let d = json!({"_created_ms": 5});
    assert!(!Filter::eq(CREATED_FIELD, 5).matches_with("a", &d, &meta()));
    assert!(Filter::eq(CREATED_FIELD, 1_000).matches_with("a", &d, &meta()));
}

#[test]
fn matches_without_metadata_sees_unknown_metadata() {
    let d = doc();
    assert!(Filter::eq(CREATED_FIELD, 0).matches("a", &d));
    assert!(Filter::eq(SCHEMA_FIELD, 0).matches("a", &d));
    assert!(Filter::exists(EXPIRES_FIELD, false).matches("a", &d));
}

fn meta_key(item: &(String, Value, Meta)) -> (&str, &Value, &Meta) {
    (item.0.as_str(), &item.1, &item.2)
}

#[test]
fn sorts_by_metadata_paths() {
    let mut items = vec![
        (
            "a".to_owned(),
            json!({}),
            Meta {
                created_ms: 30,
                ..Meta::unknown()
            },
        ),
        (
            "b".to_owned(),
            json!({}),
            Meta {
                created_ms: 10,
                ..Meta::unknown()
            },
        ),
        (
            "c".to_owned(),
            json!({}),
            Meta {
                created_ms: 20,
                ..Meta::unknown()
            },
        ),
        (
            "d".to_owned(),
            json!({}),
            Meta {
                created_ms: 20,
                ..Meta::unknown()
            },
        ),
    ];
    sort_documents_with_meta(&mut items, &[Sort::asc(CREATED_FIELD)], meta_key);
    let ids: Vec<_> = items.iter().map(|(id, ..)| id.as_str()).collect();
    assert_eq!(ids, ["b", "c", "d", "a"]);
    sort_documents_with_meta(&mut items, &[Sort::desc(CREATED_FIELD)], meta_key);
    let ids: Vec<_> = items.iter().map(|(id, ..)| id.as_str()).collect();
    assert_eq!(ids, ["a", "c", "d", "b"]);
}
