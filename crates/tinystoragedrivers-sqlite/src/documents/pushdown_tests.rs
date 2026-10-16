//! Only exact-enough clauses are pushed to SQL.

use serde_json::json;

use rusqlite::types::Value as SqlValue;
use tinystoragedrivers_core::{CursorPosition, Meta, Sort};

use super::*;

/// A collection that projects `state`, `done`, `a`, `rank`, `items.0` and `we"ird`.
fn projection() -> Projection {
    let spec = CollectionSpec::new("jobs").index(tinystoragedrivers_core::IndexSpec::new(
        "by_state",
        ["state", "done", "a", "rank", "items.0", "we\"ird"],
    ));
    let tables = crate::sql::Tables::new("");
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

fn after(position: &CursorPosition, sorts: &[Sort]) -> Option<Clause> {
    super::after(position, sorts, &projection())
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
        Filter::gt("rank", 1),
        Filter::eq("a", "x").or(Filter::eq("state", "y")),
        Filter::eq("a", "x").negate(),
        Filter::gt("n", 1).and(Filter::lt("n", 3)),
    ] {
        assert_eq!(clause(&filter), None, "{filter:?}");
    }
}

#[test]
fn seeks_by_nullable_mixed_direction_sort_and_id() {
    let position = CursorPosition {
        id: "last".into(),
        values: vec![json!(2), json!("queued")],
    };
    let pushed = after(&position, &[Sort::asc("rank"), Sort::desc("state")]).unwrap();
    assert!(pushed.sql.contains("f.path = 'rank'"), "{}", pushed.sql);
    assert!(pushed.sql.contains("f.path = 'state'"));
    assert!(pushed.sql.contains("SELECT f.kind"));
    assert!(pushed.sql.contains("f.id = \"docs\".id"));
    assert!(pushed.sql.contains(") >= ?"));
    assert!(pushed.sql.contains(") <= ?"));
    assert!(pushed.sql.ends_with("id > ?)"));
    assert_eq!(pushed.params.last(), Some(&SqlValue::Text("last".into())));

    let null = CursorPosition {
        id: "n".into(),
        values: vec![Value::Null],
    };
    let nullable = after(&null, &[Sort::asc("rank")]).unwrap();
    assert!(!nullable.sql.contains(") >= ?"));
}

#[test]
fn leaves_structural_sort_values_to_reference_ordering() {
    for value in [json!([]), json!({"key": 1})] {
        let position = CursorPosition {
            id: "last".into(),
            values: vec![value],
        };
        assert_eq!(after(&position, &[Sort::asc("rank")]), None);
    }
}

#[test]
fn a_sort_on_an_unprojected_path_is_not_seeked_in_sql() {
    let position = CursorPosition {
        id: "last".into(),
        values: vec![json!(1)],
    };
    assert_eq!(after(&position, &[Sort::asc("undeclared")]), None);
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

#[test]
fn a_metadata_sort_seeks_through_its_column() {
    let sorts = [Sort::asc("_expires_ms")];
    let position = CursorPosition::for_record("a", &json!({}), &Meta::unknown(), &sorts);
    let pushed = after(&position, &sorts).unwrap();
    assert!(pushed.sql.contains("expires_ms IS NULL"), "{}", pushed.sql);
    assert!(!pushed.sql.contains("doc_fields"), "{}", pushed.sql);
}
