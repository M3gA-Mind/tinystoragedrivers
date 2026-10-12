//! Only exact-enough clauses are pushed to SQL.

use serde_json::json;

use super::*;

#[test]
fn pushes_string_and_boolean_equality() {
    let pushed = clause(&Filter::eq("state", "queued")).unwrap();
    assert_eq!(pushed.sql, "json_extract(doc, ?) = ?");
    assert_eq!(
        pushed.params,
        vec![
            SqlValue::Text("$.\"state\"".into()),
            SqlValue::Text("queued".into())
        ]
    );
    let flag = clause(&Filter::eq("done", true)).unwrap();
    assert_eq!(flag.params[1], SqlValue::Integer(1));
    let id = clause(&Filter::eq(ID_FIELD, "a")).unwrap();
    assert_eq!(id.sql, "id = ?");
}

#[test]
fn pushes_membership_and_conjunctions() {
    let pushed = clause(&Filter::one_of("state", ["a", "b"])).unwrap();
    assert_eq!(pushed.sql, "json_extract(doc, ?) IN (?, ?)");
    let both = clause(
        &Filter::eq("a", "x")
            .and(Filter::gt("n", 3))
            .and(Filter::one_of(ID_FIELD, ["i"])),
    )
    .unwrap();
    assert_eq!(both.sql, "(json_extract(doc, ?) = ?) AND (id IN (?))");
    assert_eq!(both.params.len(), 3);
}

#[test]
fn leaves_everything_else_to_rust() {
    for filter in [
        Filter::All,
        Filter::eq("n", 5),
        Filter::eq("o", json!({"a": 1})),
        Filter::eq(ID_FIELD, true),
        Filter::one_of("n", [1, 2]),
        Filter::one_of(ID_FIELD, [json!(true)]),
        Filter::In {
            field: "x".into(),
            values: Vec::new(),
        },
        Filter::gt("n", 1),
        Filter::eq("a", "x").or(Filter::eq("b", "y")),
        Filter::eq("a", "x").negate(),
        Filter::gt("n", 1).and(Filter::lt("n", 3)),
        Filter::eq("tags.0", "x"),
        Filter::one_of("we\"ird", ["x"]),
    ] {
        assert_eq!(clause(&filter), None, "{filter:?}");
    }
}

#[test]
fn seeks_by_nullable_mixed_direction_sort_and_id() {
    let position = CursorPosition {
        id: "last".into(),
        values: vec![json!(2), Value::Null],
    };
    let pushed = after(&position, &[Sort::asc("rank"), Sort::desc("state")]).unwrap();
    assert!(pushed.sql.contains("json_type(doc, '$.\"rank\"')"));
    assert!(pushed.sql.contains("json_type(doc, '$.\"state\"')"));
    assert!(pushed.sql.contains("json_extract(doc, '$.\"rank\"') >= ?"));
    assert!(pushed.sql.contains("json_extract(doc, '$.\"state\"') <= ?"));
    assert!(pushed.sql.ends_with("id > ?))"));
    assert_eq!(pushed.params.last(), Some(&SqlValue::Text("last".into())));

    let null = CursorPosition {
        id: "n".into(),
        values: vec![Value::Null],
    };
    let nullable = after(&null, &[Sort::asc("rank")]).unwrap();
    assert!(
        !nullable
            .sql
            .contains("json_extract(doc, '$.\"rank\"') >= ?")
    );
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
    assert_eq!(
        after(
            &CursorPosition {
                id: "last".into(),
                values: vec![json!(1)],
            },
            &[Sort::asc("items.0")],
        ),
        None
    );
}
