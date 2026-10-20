//! Only exact-enough clauses are pushed to SQL.

use super::*;
use rusqlite::types::Value as SqlValue;
use serde_json::json;
use tinystoragedrivers_types::IndexSpec;

fn projection() -> Projection {
    let spec = CollectionSpec::new("jobs").index(IndexSpec::new(
        "by_state",
        ["state", "done", "a", "rank", "items.0", "we\"ird"],
    ));
    let directory = tempfile::tempdir().unwrap();
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open(
        directory.path().join("pushdown.db"),
    )
    .unwrap();
    let tables = connection.tables().clone();
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

fn clause_for(filter: &Filter) -> Option<Clause> {
    super::clause(filter, &projection())
}

const STATE_IDS: &str = "id IN (SELECT f.id FROM \"doc_fields\" AS f WHERE f.scope = 'local' AND f.coll = 'jobs' AND f.path = 'state' AND f.value";

#[test]
fn pushes_string_boolean_membership_and_conjunctions() {
    let pushed = clause_for(&Filter::eq("state", "queued")).unwrap();
    assert_eq!(pushed.sql, format!("{STATE_IDS} = ?)"));
    assert_eq!(pushed.params, vec![SqlValue::Text("queued".into())]);
    assert_eq!(
        clause_for(&Filter::eq("done", true)).unwrap().params,
        vec![SqlValue::Integer(1)]
    );
    let id = clause_for(&Filter::eq(ID_FIELD, "a")).unwrap();
    assert_eq!(id.sql, "id = ?");
    assert_eq!(
        clause_for(&Filter::one_of("state", ["a", "b"]))
            .unwrap()
            .sql,
        format!("{STATE_IDS} IN (?, ?))")
    );
    let both = clause_for(
        &Filter::eq("a", "x")
            .and(Filter::gt("n", 3))
            .and(Filter::one_of(ID_FIELD, ["i"])),
    )
    .unwrap();
    assert!(both.sql.ends_with(") AND (id IN (?))"), "{}", both.sql);
    assert_eq!(both.params.len(), 2);
}

#[test]
fn leaves_unprojected_and_unsupported_values_for_rust() {
    for filter in [
        Filter::eq("undeclared", "x"),
        Filter::one_of("undeclared", ["x"]),
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
        assert_eq!(clause_for(&filter), None, "{filter:?}");
    }
    let partial = clause_for(&Filter::eq("undeclared", "x").and(Filter::eq("state", "q"))).unwrap();
    assert_eq!(partial.params, vec![SqlValue::Text("q".into())]);
    assert!(clause_for(&Filter::eq("items.0", "x")).is_some());
    assert!(clause_for(&Filter::one_of("we\"ird", ["x"])).is_some());
}

#[test]
fn pushes_metadata_and_typed_ranges_safely() {
    let range = Filter::gte("_created_ms", 5).and(Filter::lt("_created_ms", 9));
    assert_eq!(
        clause_for(&range).unwrap().sql,
        "(created_ms >= ?) AND (created_ms < ?)"
    );
    assert_eq!(
        clause_for(&Filter::eq("_schema", 7)).unwrap().sql,
        "schema = ?"
    );
    assert_eq!(clause_for(&Filter::eq("_expires_ms", "text")), None);
    assert_eq!(clause_for(&Filter::eq("_updated_ms", 1.5)), None);

    let number = clause_for(&Filter::gt("rank", 2)).unwrap();
    assert!(number.sql.contains("f.kind = 2 AND f.value > ?"));
    assert_eq!(number.params, vec![SqlValue::Integer(2)]);
    let text = clause_for(&Filter::gte("rank", "b").and(Filter::lt("rank", "d"))).unwrap();
    assert_eq!(
        text.params,
        vec![SqlValue::Text("b".into()), SqlValue::Text("d".into())]
    );
    let flag = clause_for(&Filter::lte("done", true)).unwrap();
    assert!(flag.sql.contains("f.kind = 1 AND f.value <= ?"));
}

#[test]
fn range_pushdown_handles_lossy_numbers_and_string_ids() {
    let big = clause_for(&Filter::gt("rank", u64::MAX - 1)).unwrap();
    assert!(big.sql.contains("f.value >= ?"));
    assert!(
        clause_for(&Filter::lt("rank", 1e19))
            .unwrap()
            .sql
            .contains("f.value <= ?")
    );
    assert!(
        clause_for(&Filter::gt("rank", 1.5))
            .unwrap()
            .sql
            .contains("f.value > ?")
    );
    assert_eq!(
        clause_for(&Filter::gte(ID_FIELD, "b").and(Filter::lt(ID_FIELD, "d")))
            .unwrap()
            .sql,
        "(id >= ?) AND (id < ?)"
    );
    assert_eq!(
        clause_for(&Filter::Range {
            field: "rank".into(),
            gt: Some(json!(1)),
            gte: None,
            lt: Some(json!(9.5)),
            lte: None,
        })
        .unwrap()
        .params,
        vec![SqlValue::Integer(1), SqlValue::Real(9.5)]
    );
}
