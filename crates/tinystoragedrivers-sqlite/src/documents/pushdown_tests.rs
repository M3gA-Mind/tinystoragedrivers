//! Only exact-enough clauses are pushed to SQL.

use serde_json::json;

use rusqlite::types::Value as SqlValue;

use super::*;

/// A collection that projects `state`, `done`, `a`, `rank`, `items.0` and `we"ird`.
fn projection() -> Projection {
    let spec = CollectionSpec::new("jobs").index(tinystoragedrivers_core::IndexSpec::new(
        "by_state",
        ["state", "done", "a", "rank", "items.0", "we\"ird"],
    ));
    let tables = crate::sql::Tables::new();
    Projection::new(
        Ctx {
            tables: &tables,
            scope: "local",
            now_ms: 0,
        },
        &spec,
        "jobs",
    )
}

fn clause(filter: &Filter) -> Option<Clause> {
    super::clause(filter, &projection())
}

const STATE_IDS: &str = "id IN (SELECT f.id FROM \"doc_fields\" AS f WHERE f.scope = 'local' AND f.coll = 'jobs' AND f.path = 'state' AND f.value";

#[test]
fn pushes_string_and_boolean_equality_through_the_side_table() {
    let pushed = clause(&Filter::eq("state", "queued")).unwrap();
    assert_eq!(pushed.sql, format!("{STATE_IDS} = ?)"));
    assert_eq!(pushed.params, vec![SqlValue::Text("queued".into())]);
    let flag = clause(&Filter::eq("done", true)).unwrap();
    assert_eq!(flag.params, vec![SqlValue::Integer(1)]);
    let id = clause(&Filter::eq(ID_FIELD, "a")).unwrap();
    assert_eq!(id.sql, "id = ?");
}

#[test]
fn pushes_membership_and_conjunctions() {
    let pushed = clause(&Filter::one_of("state", ["a", "b"])).unwrap();
    assert_eq!(pushed.sql, format!("{STATE_IDS} IN (?, ?))"));
    let both = clause(
        &Filter::eq("a", "x")
            .and(Filter::gt("n", 3))
            .and(Filter::one_of(ID_FIELD, ["i"])),
    )
    .unwrap();
    assert!(both.sql.ends_with(") AND (id IN (?))"), "{}", both.sql);
    assert_eq!(both.params.len(), 2);
}

#[test]
fn paths_without_a_projection_are_left_to_rust() {
    for filter in [
        Filter::eq("undeclared", "x"),
        Filter::one_of("undeclared", ["x"]),
    ] {
        assert_eq!(clause(&filter), None, "{filter:?}");
    }
    // The projected half of a conjunction still narrows the scan.
    let both = clause(&Filter::eq("undeclared", "x").and(Filter::eq("state", "q"))).unwrap();
    assert_eq!(both.params, vec![SqlValue::Text("q".into())]);
    // Awkward key names and array indexes are projected like any other path.
    assert!(clause(&Filter::eq("items.0", "x")).is_some());
    assert!(clause(&Filter::one_of("we\"ird", ["x"])).is_some());
}

#[test]
fn leaves_everything_else_to_rust() {
    for filter in [
        Filter::All,
        Filter::eq("n", 5),
        Filter::eq("rank", 5),
        Filter::eq("o", json!({"a": 1})),
        Filter::eq(ID_FIELD, true),
        Filter::one_of("n", [1, 2]),
        Filter::one_of(ID_FIELD, [json!(true)]),
        Filter::In {
            field: "x".into(),
            values: Vec::new(),
        },
        Filter::eq("a", "x").or(Filter::eq("state", "y")),
        Filter::eq("a", "x").negate(),
        Filter::gt("n", 1).and(Filter::lt("n", 3)),
        Filter::gt("rank", Value::Null),
        Filter::gt("rank", json!([1])),
        Filter::gt(ID_FIELD, 1),
    ] {
        assert_eq!(clause(&filter), None, "{filter:?}");
    }
}

#[test]
fn reserved_metadata_paths_push_down_to_their_columns() {
    let range = Filter::gte("_created_ms", 5).and(Filter::lt("_created_ms", 9));
    let pushed = clause(&range).unwrap();
    assert_eq!(pushed.sql, "(created_ms >= ?) AND (created_ms < ?)");
    let pushed = clause(&Filter::eq("_schema", 7)).unwrap();
    assert_eq!(pushed.sql, "schema = ?");
    assert_eq!(pushed.params, vec![SqlValue::Integer(7)]);
    assert_eq!(
        clause(&Filter::eq("_expires_ms", "text")),
        None,
        "not a number"
    );
    assert_eq!(
        clause(&Filter::eq("_updated_ms", 1.5)),
        None,
        "left to the Rust check"
    );
}

const RANK_ROWS: &str = "id IN (SELECT f.id FROM \"doc_fields\" AS f WHERE f.scope = 'local' AND f.coll = 'jobs' AND f.path = 'rank' AND";

#[test]
fn a_range_on_a_projected_field_tests_kind_and_value() {
    let number = clause(&Filter::gt("rank", 2)).unwrap();
    assert_eq!(
        number.sql,
        format!("{RANK_ROWS} f.kind = 2 AND f.value > ?)")
    );
    assert_eq!(number.params, vec![SqlValue::Integer(2)]);
    let text = clause(&Filter::gte("rank", "b").and(Filter::lt("rank", "d"))).unwrap();
    assert_eq!(
        text.params,
        vec![SqlValue::Text("b".into()), SqlValue::Text("d".into())]
    );
    let flag = clause(&Filter::lte("done", true)).unwrap();
    assert!(
        flag.sql.contains("f.kind = 1 AND f.value <= ?"),
        "{}",
        flag.sql
    );
}

#[test]
fn both_bounds_of_a_range_share_one_lookup() {
    let both = Filter::Range {
        field: "rank".into(),
        gt: Some(json!(1)),
        gte: None,
        lt: Some(json!(9.5)),
        lte: None,
    };
    let pushed = clause(&both).unwrap();
    assert_eq!(
        pushed.sql,
        format!("{RANK_ROWS} f.kind = 2 AND f.value > ? AND f.kind = 2 AND f.value < ?)")
    );
    assert_eq!(
        pushed.params,
        vec![SqlValue::Integer(1), SqlValue::Real(9.5)]
    );
    let clash = Filter::Range {
        field: "rank".into(),
        gt: Some(json!(1)),
        gte: None,
        lt: Some(json!("z")),
        lte: None,
    };
    assert!(
        clause(&clash)
            .unwrap()
            .sql
            .contains("f.kind = 2 AND f.value > ? AND f.kind = 3")
    );
}

#[test]
fn a_bound_that_may_be_rounded_is_compared_inclusively() {
    let big = clause(&Filter::gt("rank", u64::MAX - 1)).unwrap();
    assert!(big.sql.contains("f.value >= ?"), "{}", big.sql);
    let float = clause(&Filter::lt("rank", 1e19)).unwrap();
    assert!(float.sql.contains("f.value <= ?"), "{}", float.sql);
    let small = clause(&Filter::gt("rank", 1.5)).unwrap();
    assert!(small.sql.contains("f.value > ?"), "{}", small.sql);
}

#[test]
fn a_range_on_the_id_compares_bytes_of_string_bounds() {
    let pushed = clause(&Filter::gte(ID_FIELD, "b").and(Filter::lt(ID_FIELD, "d"))).unwrap();
    assert_eq!(pushed.sql, "(id >= ?) AND (id < ?)");
}
