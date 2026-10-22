use super::*;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::{IndexSpec, Precondition, Scope};

#[test]
fn projection_ranks_and_sql_values_cover_json_shapes() {
    let spec = CollectionSpec::new("items")
        .index(IndexSpec::new("by", ["nested.value", "items.0"]))
        .searchable(["text"]);
    assert_eq!(
        projected(&spec),
        ["items.0", "nested.value", "text"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
    let values = [
        (Value::Null, 0, SqlValue::Null),
        (Value::Bool(true), 1, SqlValue::Integer(1)),
        (serde_json::json!(7), 2, SqlValue::Integer(7)),
        (serde_json::json!(1.5), 2, SqlValue::Real(1.5)),
        (serde_json::json!("text"), 3, SqlValue::Text("text".into())),
        (serde_json::json!([1]), 4, SqlValue::Null),
        (serde_json::json!({"a":1}), 5, SqlValue::Null),
    ];
    for (value, expected_rank, expected_sql) in values {
        assert_eq!(rank(&value), expected_rank, "rank for {value}");
        assert_eq!(sql_value(&value), expected_sql, "SQL value for {value}");
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one test covers the full projection lifecycle"
)]
async fn writes_replaces_removes_and_rebuilds_projected_rows() {
    let directory = tempfile::tempdir().unwrap();
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open(
        directory.path().join("fields.db"),
    )
    .unwrap();
    let tables = connection.tables().clone();
    let documents =
        crate::sqlite::SqliteDocuments::new(connection.clone(), Scope::new("fields").unwrap());
    let spec = CollectionSpec::new("items")
        .index(IndexSpec::new("by", ["nested.value", "missing"]))
        .searchable(["text"]);
    documents.ensure_collection(&spec).await.unwrap();
    documents
        .put(
            "items",
            "one",
            serde_json::json!({"nested":{"value":"first"},"text":"hello"}),
            Precondition::None,
        )
        .await
        .unwrap();
    documents
        .put(
            "items",
            "two",
            serde_json::json!({"nested":{"value":2},"text":"world"}),
            Precondition::None,
        )
        .await
        .unwrap();

    let count = connection
        .run_now(|conn| {
            conn.query_row("SELECT COUNT(*) FROM doc_fields", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(tinystoragedrivers_connect::sqlite::error::during(
                "count fields",
            ))
        })
        .unwrap();
    assert_eq!(count, 4, "missing paths do not create projection rows");

    connection
        .run_now(|conn| {
            write(
                conn,
                &tables,
                &spec,
                "fields",
                "one",
                &serde_json::json!({"nested":{"value":"updated"}}),
            )?;
            Ok(())
        })
        .unwrap();
    let projected_value = connection
        .run_now(|conn| {
            conn.query_row(
                "SELECT value FROM doc_fields WHERE scope='fields' AND coll='items' AND id='one' AND path='nested.value'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(tinystoragedrivers_connect::sqlite::error::during("read field"))
        })
        .unwrap();
    assert_eq!(projected_value, "updated");

    connection
        .run_now(|conn| {
            remove(conn, &tables, "fields", "items", Some("one"))?;
            rebuild(conn, &tables, &spec)?;
            Ok(())
        })
        .unwrap();
    let rebuilt = connection
        .run_now(|conn| {
            conn.query_row(
                "SELECT value FROM doc_fields WHERE scope='fields' AND coll='items' AND id='one' AND path='nested.value'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map_err(tinystoragedrivers_connect::sqlite::error::during("read rebuilt field"))
        })
        .unwrap();
    assert_eq!(
        rebuilt, "first",
        "rebuild reads the canonical document body"
    );

    connection
        .run_now(|conn| {
            remove(conn, &tables, "fields", "items", None)?;
            rebuild(conn, &tables, &CollectionSpec::new("items"))?;
            Ok(())
        })
        .unwrap();
    let count = connection
        .run_now(|conn| {
            conn.query_row("SELECT COUNT(*) FROM doc_fields", [], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(tinystoragedrivers_connect::sqlite::error::during(
                "count cleared fields",
            ))
        })
        .unwrap();
    assert_eq!(count, 0);
}
