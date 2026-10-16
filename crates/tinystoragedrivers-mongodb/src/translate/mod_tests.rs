//! The shape and exactness of every translation.
//!
//! Whether a translation selects the right documents is checked against a
//! live server in `tests/live.rs`, which compares this driver with the memory
//! driver over a set of awkward documents. These tests pin the queries.

use super::*;
use serde_json::json;

fn guard(field: &str) -> Document {
    doc! {field: {"$not": {"$type": "array"}}}
}

#[test]
fn all_and_and_or_combine() {
    assert_eq!(filter(&Filter::All), Translated::everything());
    assert_eq!(
        filter(&Filter::And { filters: vec![] }),
        Translated::everything()
    );
    assert_eq!(
        filter(&Filter::Or { filters: vec![] }),
        Translated::nothing()
    );
    assert_eq!(
        filter(&Filter::All.and(Filter::exists("_id", true))),
        Translated::everything()
    );

    let both = filter(&Filter::eq("a", 1).and(Filter::eq("_id", "x")));
    assert!(both.exact);
    assert_eq!(
        both.query,
        doc! {"$and": [
            {"$and": [guard("d.a"), {"d.a": {"$eq": 1_i64}}]},
            {"_key": "x"},
        ]}
    );

    let single = filter(&Filter::And {
        filters: vec![Filter::All, Filter::eq("_id", "x")],
    });
    assert_eq!(single.query, doc! {"_key": "x"});

    let either = filter(&Filter::eq("_id", "a").or(Filter::eq("_id", "b")));
    assert_eq!(
        either,
        Translated::exact(doc! {"$or": [{"_key": "a"}, {"_key": "b"}]})
    );
    let open = filter(&Filter::eq("_id", "a").or(Filter::All));
    assert_eq!(open, Translated::everything());
}

#[test]
fn equality_guards_every_segment() {
    let nested = filter(&Filter::eq("owner.name", "ada"));
    assert!(nested.exact);
    assert_eq!(
        nested.query,
        doc! {"$and": [
            guard("d.owner"),
            guard("d.owner.name"),
            {"d.owner.name": {"$eq": "ada"}},
        ]}
    );
    let null = filter(&Filter::eq("x", Value::Null));
    assert!(null.exact);
    assert_eq!(
        null.query,
        doc! {"$and": [guard("d.x"), {"d.x": {"$type": "null"}}]}
    );
    for value in [json!(true), json!(1.5), json!("s")] {
        assert!(filter(&Filter::eq("x", value)).exact);
    }
}

#[test]
fn structured_or_unrepresentable_values_only_check_presence() {
    for value in [json!([1]), json!({"a": 1}), json!(u64::MAX)] {
        let translated = filter(&Filter::eq("x", value));
        assert!(!translated.exact);
        assert_eq!(translated.query, doc! {"d.x": {"$exists": true}});
    }
    let nested = filter(&Filter::eq("a.b", json!([1])));
    assert_eq!(
        nested.query,
        doc! {"$and": [guard("d.a"), {"d.a.b": {"$exists": true}}]}
    );
}

#[test]
fn ids_compare_as_strings() {
    assert_eq!(filter(&Filter::eq("_id", 1)), Translated::nothing());
    assert_eq!(
        filter(&Filter::ne("_id", "a")),
        Translated::exact(doc! {"$nor": [{"_key": "a"}]})
    );
    assert_eq!(
        filter(&Filter::gte("_id", "a").and(Filter::lt("_id", "m"))).query,
        doc! {"$and": [{"_key": {"$gte": "a"}}, {"_key": {"$lt": "m"}}]}
    );
    assert_eq!(filter(&Filter::gt("_id", 3)), Translated::nothing());
    assert_eq!(
        filter(&Filter::one_of("_id", ["a"])),
        Translated::exact(doc! {"$or": [{"_key": "a"}]})
    );
    assert_eq!(
        filter(&Filter::one_of("_id", Vec::<String>::new())),
        Translated::nothing()
    );
    assert_eq!(
        filter(&Filter::exists("_id", true)),
        Translated::everything()
    );
    assert_eq!(
        filter(&Filter::exists("_id", false)),
        Translated::exact(doc! {"$nor": [{}]})
    );
}

#[test]
fn ranges_keep_scalar_bounds_and_degrade_otherwise() {
    let rank = filter(&Filter::Range {
        field: "rank".into(),
        gt: Some(json!(1)),
        gte: None,
        lt: Some(json!("z")),
        lte: None,
    });
    assert!(rank.exact);
    assert_eq!(
        rank.query,
        doc! {"$and": [guard("d.rank"), {"d.rank": {"$gt": 1_i64, "$lt": "z"}}]}
    );
    for bound in [json!(null), json!([1]), json!({"a": 1}), json!(u64::MAX)] {
        let translated = filter(&Filter::gte("rank", bound));
        assert!(!translated.exact);
        assert_eq!(translated.query, doc! {"d.rank": {"$exists": true}});
    }
}

#[test]
fn exists_allows_array_values() {
    let present = filter(&Filter::exists("tags", true));
    assert_eq!(
        present,
        Translated::exact(doc! {"d.tags": {"$exists": true}})
    );
    let nested_absent = filter(&Filter::exists("a.b", false));
    assert!(nested_absent.exact);
    assert_eq!(
        nested_absent.query,
        doc! {"$nor": [{"$and": [guard("d.a"), {"d.a.b": {"$exists": true}}]}]}
    );
}

#[test]
fn positional_and_operator_paths_are_supersets() {
    let positional = filter(&Filter::eq("tags.1", "x"));
    assert!(!positional.exact);
    assert_eq!(positional.query, doc! {"d.tags.1": {"$eq": "x"}});
    let first_segment_numeric = filter(&Filter::eq("0.a", "x"));
    assert!(first_segment_numeric.exact);

    let operator = filter(&Filter::eq("a.$b", 1));
    assert_eq!(operator, Translated::superset(Document::new()));
    assert_eq!(
        filter(&Filter::exists("$a", true)),
        Translated::superset(Document::new())
    );
}

#[test]
fn negating_a_superset_defers_to_rust() {
    assert_eq!(
        filter(&Filter::ne("tags.0", "x")),
        Translated::superset(Document::new())
    );
    assert_eq!(
        filter(&Filter::eq("x", json!([1])).negate()),
        Translated::superset(Document::new())
    );
    let mixed = filter(&Filter::eq("x", json!([1])).and(Filter::eq("_id", "a")));
    assert!(!mixed.exact);
    let either = filter(&Filter::one_of("x", [json!([1]), json!(2)]));
    assert!(!either.exact);
    assert!(either.query.contains_key("$or"));
}

#[test]
fn sort_plans_rank_then_value_then_key() {
    let plan = sort_plan(&[Sort::desc("rank"), Sort::asc("_id")]).unwrap();
    assert_eq!(
        plan.sort,
        doc! {"_tsd_r0": -1, "_tsd_k0": -1, "_tsd_r1": 1, "_tsd_k1": 1, "_key": 1}
    );
    assert_eq!(
        plan.add_fields
            .get_document("_tsd_k0")
            .unwrap()
            .get_array("$ifNull")
            .unwrap(),
        &[Bson::String("$d.rank".into()), Bson::Null]
    );
    assert_eq!(
        plan.add_fields
            .get_document("_tsd_k1")
            .unwrap()
            .get_array("$ifNull")
            .unwrap(),
        &[Bson::String("$_key".into()), Bson::Null]
    );
    assert!(plan.add_fields.get_document("_tsd_r0").is_ok());
    assert_eq!(plan.unset, ["_tsd_k0", "_tsd_r0", "_tsd_k1", "_tsd_r1"]);
    assert_eq!(
        plan.complex
            .get_document("$expr")
            .unwrap()
            .get_array("$or")
            .unwrap()
            .len(),
        2
    );

    let by_key = sort_plan(&[]).unwrap();
    assert_eq!(by_key.sort, doc! {"_key": 1});
    assert_eq!(by_key.unset, Vec::<String>::new());

    assert_eq!(sort_plan(&[Sort::asc("a.$b")]), None);
    assert_eq!(sort_plan(&[Sort::asc("items.0")]), None);
    assert_eq!(sort_plan(&[Sort::asc("items.0.name")]), None);
    assert!(
        sort_plan(&[Sort::asc("0.name")]).is_some(),
        "the body is an object"
    );
}

#[test]
fn body_fields_live_under_d() {
    assert_eq!(body_field("a.b"), "d.a.b");
}

fn or_missing(field: &str, condition: Document) -> Document {
    doc! {"$or": [{field: condition}, {field: {"$exists": false}}]}
}

#[test]
fn metadata_predicates_that_zero_satisfies_also_accept_a_missing_field() {
    // A record written before the metadata fields existed decodes as 0.
    let lt = filter(&Filter::lt("_schema", 2));
    assert!(lt.exact);
    assert_eq!(lt.query, or_missing("_schema", doc! {"$lt": 2_i64}));

    let eq = filter(&Filter::eq("_schema", 0));
    assert!(eq.exact);
    assert_eq!(eq.query, or_missing("_schema", doc! {"$eq": 0_i64}));

    let within = filter(&Filter::lte("_created_ms", 50).and(Filter::gte("_created_ms", 0)));
    assert!(within.exact);
    assert_eq!(
        within.query,
        doc! {"$and": [
            or_missing("_c", doc! {"$lte": 50_i64}),
            or_missing("_c", doc! {"$gte": 0_i64}),
        ]}
    );

    let any = filter(&Filter::In {
        field: "_updated_ms".into(),
        values: vec![json!(0), json!(7)],
    });
    assert!(any.exact);
    assert_eq!(
        any.query,
        doc! {"$or": [
            or_missing("_u", doc! {"$eq": 0_i64}),
            {"_u": {"$eq": 7_i64}},
        ]}
    );
}

#[test]
fn metadata_predicates_that_zero_fails_keep_to_the_stored_field() {
    for (filter_, expected) in [
        (Filter::gt("_schema", 0), doc! {"_schema": {"$gt": 0_i64}}),
        (Filter::eq("_schema", 2), doc! {"_schema": {"$eq": 2_i64}}),
        (Filter::lt("_schema", 0), doc! {"_schema": {"$lt": 0_i64}}),
        (Filter::gte("_created_ms", 1), doc! {"_c": {"$gte": 1_i64}}),
    ] {
        let translated = filter(&filter_);
        assert!(translated.exact, "{filter_:?}");
        assert_eq!(translated.query, expected, "{filter_:?}");
    }
}

#[test]
fn a_negated_metadata_predicate_sees_a_legacy_record_as_zero() {
    let not_zero = filter(&Filter::eq("_schema", 0).negate());
    assert!(not_zero.exact);
    assert_eq!(
        not_zero.query,
        doc! {"$nor": [or_missing("_schema", doc! {"$eq": 0_i64})]}
    );
}

#[test]
fn an_expiry_stays_absent_when_missing() {
    let lte = filter(&Filter::lte("_expires_ms", 5));
    assert!(lte.exact);
    assert_eq!(lte.query, doc! {"_x": {"$lte": 5_i64}});
}

#[test]
fn metadata_sorts_read_a_missing_field_as_zero() {
    let plan = sort_plan(&[Sort::asc("_created_ms"), Sort::desc("_expires_ms")]).unwrap();
    let source = |name: &str| {
        plan.add_fields
            .get_document(name)
            .unwrap()
            .get_array("$ifNull")
            .unwrap()
            .clone()
    };
    assert_eq!(
        source("_tsd_k0"),
        [Bson::Document(doc! {"$ifNull": ["$_c", 0_i64]}), Bson::Null]
    );
    assert_eq!(
        source("_tsd_k1"),
        [Bson::String("$_x".into()), Bson::Null],
        "an absent expiry is still no expiry"
    );
    // The rank reads the same defaulted value.
    let rank = plan.add_fields.get_document("_tsd_r0").unwrap().to_string();
    assert!(rank.contains("$ifNull"), "{rank}");
}
