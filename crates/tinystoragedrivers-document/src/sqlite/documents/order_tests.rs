//! SQL order construction and keyset seeking.

use serde_json::json;
use tinystoragedrivers_types::{CollectionSpec, CursorPosition, Direction, IndexSpec, Meta, Sort};

use super::super::ops::Ctx;
use super::*;

fn projection() -> Projection {
    let directory = tempfile::tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(directory.path()).unwrap();
    let spec = CollectionSpec::new("jobs").index(IndexSpec::new("by", ["state", "rank"]));
    Projection::new(
        Ctx {
            tables: connection.tables(),
            scope: "local",
            now_ms: 0,
        },
        &spec,
        "jobs",
    )
}

fn ordered(sorts: &[Sort]) -> Option<Order> {
    super::order(sorts, &projection())
}

fn after(position: &CursorPosition, sorts: &[Sort]) -> Option<Clause> {
    super::after(position, &ordered(sorts)?)
}

#[test]
fn projected_keys_join_and_order_by_kind_then_value() {
    let order = ordered(&[Sort::desc("rank"), Sort::asc("state")]).unwrap();
    assert_eq!(order.joins.matches("LEFT JOIN").count(), 2);
    assert!(order.joins.contains("f.path = 'rank'"));
    assert!(order.joins.contains("ON s1.sid = \"docs\".id"));
    assert_eq!(
        order.terms,
        "COALESCE(s0.k, 0) DESC, s0.v DESC, COALESCE(s1.k, 0) ASC, s1.v ASC, id ASC"
    );
    assert_eq!(
        order.structural_ranks(),
        ["COALESCE(s0.k, 0)", "COALESCE(s1.k, 0)"]
    );
}

#[test]
fn id_and_metadata_order_without_projected_joins() {
    let order = ordered(&[Sort::desc("_updated_ms"), Sort::asc("_id")]).unwrap();
    assert_eq!(order.joins, "");
    assert_eq!(order.terms, "updated_ms DESC, id ASC, id ASC");
    assert!(order.structural_ranks().is_empty());
    assert!(
        ordered(&[Sort::asc("_expires_ms")])
            .unwrap()
            .terms
            .starts_with("CASE WHEN expires_ms IS NULL")
    );
}

#[test]
fn unprojected_and_structural_keys_fall_back_to_rust_ordering() {
    assert!(ordered(&[Sort::asc("rank"), Sort::asc("undeclared")]).is_none());
    for value in [json!([]), json!({"key": 1})] {
        let position = CursorPosition {
            id: "last".into(),
            values: vec![value],
        };
        assert!(after(&position, &[Sort::asc("rank")]).is_none());
    }
}

#[test]
fn keyset_seek_obeys_each_direction_and_null_rank() {
    let position = CursorPosition {
        id: "last".into(),
        values: vec![json!(2), json!("queued")],
    };
    let sorts = [Sort::asc("rank"), Sort::desc("state")];
    let clause = after(&position, &sorts).unwrap();
    assert!(clause.sql.contains("s0.v > ?"));
    assert!(clause.sql.contains("s1.v < ?"));
    assert!(!clause.sql.contains(">="));
    assert!(clause.sql.ends_with("id > ?)"));
    assert_eq!(
        clause.params.last(),
        Some(&rusqlite::types::Value::Text("last".into()))
    );

    let null = CursorPosition {
        id: "null".into(),
        values: vec![json!(null)],
    };
    let clause = after(&null, &[Sort::asc("rank")]).unwrap();
    assert!(!clause.sql.contains("s0.v > ?"));
    assert_eq!(Sort::desc("rank").direction, Direction::Desc);
}

#[test]
fn metadata_cursor_seeks_by_the_metadata_column() {
    let sorts = [Sort::asc("_expires_ms")];
    let position = CursorPosition::for_record("a", &json!({}), &Meta::unknown(), &sorts);
    let clause = after(&position, &sorts).unwrap();
    assert!(clause.sql.contains("expires_ms IS NULL"));
    assert!(!clause.sql.contains("doc_fields"));
}
