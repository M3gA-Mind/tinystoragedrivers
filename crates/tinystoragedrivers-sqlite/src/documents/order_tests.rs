//! Sort keys and cursor seeks as SQL.

use serde_json::json;
use tinystoragedrivers_core::{CollectionSpec, IndexSpec, Meta};

use super::super::ops::Ctx;
use super::*;

fn projection() -> Projection {
    let spec = CollectionSpec::new("jobs").index(IndexSpec::new("by", ["state", "rank"]));
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

fn order(sorts: &[Sort]) -> Option<Order> {
    super::order(sorts, &projection())
}

fn after(position: &CursorPosition, sorts: &[Sort]) -> Option<Clause> {
    super::after(position, &order(sorts)?)
}

#[test]
fn a_projected_key_is_a_join_ordered_by_kind_then_value() {
    let ordered = order(&[Sort::desc("rank"), Sort::asc("state")]).unwrap();
    assert_eq!(ordered.joins.matches("LEFT JOIN").count(), 2);
    assert!(
        ordered.joins.contains("f.path = 'rank'"),
        "{}",
        ordered.joins
    );
    assert!(ordered.joins.contains("ON s1.sid = \"docs\".id"));
    assert_eq!(
        ordered.terms,
        "COALESCE(s0.k, 0) DESC, s0.v DESC, COALESCE(s1.k, 0) ASC, s1.v ASC, id ASC"
    );
    assert_eq!(
        ordered.structural_ranks(),
        ["COALESCE(s0.k, 0)", "COALESCE(s1.k, 0)"]
    );
}

#[test]
fn the_id_and_metadata_need_no_join_and_no_constant_rank() {
    let ordered = order(&[Sort::desc("_updated_ms"), Sort::asc("_id")]).unwrap();
    assert_eq!(ordered.joins, "");
    assert_eq!(ordered.terms, "updated_ms DESC, id ASC, id ASC");
    assert_eq!(ordered.structural_ranks().len(), 0);
    let expiring = order(&[Sort::asc("_expires_ms")]).unwrap();
    assert!(expiring.terms.starts_with("CASE WHEN expires_ms IS NULL"));
}

#[test]
fn an_unprojected_key_is_left_to_rust() {
    assert!(order(&[Sort::asc("rank"), Sort::asc("undeclared")]).is_none());
}

#[test]
fn seeks_strictly_past_a_nullable_mixed_direction_position() {
    let position = CursorPosition {
        id: "last".into(),
        values: vec![json!(2), json!("queued")],
    };
    let pushed = after(&position, &[Sort::asc("rank"), Sort::desc("state")]).unwrap();
    assert!(pushed.sql.contains("s0.v > ?"), "{}", pushed.sql);
    assert!(pushed.sql.contains("s1.v < ?"), "{}", pushed.sql);
    assert!(!pushed.sql.contains(">="), "{}", pushed.sql);
    assert!(pushed.sql.ends_with("id > ?)"));
    assert_eq!(pushed.params.last(), Some(&SqlValue::Text("last".into())));

    let null = CursorPosition {
        id: "n".into(),
        values: vec![Value::Null],
    };
    let nullable = after(&null, &[Sort::asc("rank")]).unwrap();
    assert!(!nullable.sql.contains("s0.v >"), "{}", nullable.sql);
}

#[test]
fn leaves_structural_sort_values_to_reference_ordering() {
    for value in [json!([]), json!({"key": 1})] {
        let position = CursorPosition {
            id: "last".into(),
            values: vec![value],
        };
        assert!(after(&position, &[Sort::asc("rank")]).is_none());
    }
}

#[test]
fn a_metadata_sort_seeks_through_its_column() {
    let sorts = [Sort::asc("_expires_ms")];
    let position = CursorPosition::for_record("a", &json!({}), &Meta::unknown(), &sorts);
    let pushed = after(&position, &sorts).unwrap();
    assert!(pushed.sql.contains("expires_ms IS NULL"), "{}", pushed.sql);
    assert!(!pushed.sql.contains("doc_fields"), "{}", pushed.sql);
}
