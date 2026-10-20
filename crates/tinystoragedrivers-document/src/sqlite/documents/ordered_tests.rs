use super::*;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_types::{IndexSpec, Precondition, Scope};

#[tokio::test]
async fn sql_ordering_pages_keysets_and_falls_back_when_it_cannot_be_exact() {
    let directory = tempfile::tempdir().unwrap();
    let connection = tinystoragedrivers_connect::sqlite::SqliteConnection::open(
        directory.path().join("ordered.db"),
    )
    .unwrap();
    let scope = Scope::new("ordered").unwrap();
    let documents = crate::sqlite::SqliteDocuments::new(connection.clone(), scope.clone());
    let spec = CollectionSpec::new("items").index(IndexSpec::new("by_rank", ["rank"]));
    documents.ensure_collection(&spec).await.unwrap();
    for (id, rank) in [("c", 3), ("a", 1), ("b", 2)] {
        documents
            .put(
                "items",
                id,
                serde_json::json!({"rank":rank}),
                Precondition::None,
            )
            .await
            .unwrap();
    }

    let tables = connection.tables().clone();
    let first_tables = tables.clone();
    let first = connection
        .run_now({
            let spec = spec.clone();
            let scope = scope.clone();
            move |conn| {
                matching_ordered(
                    conn,
                    Ctx {
                        tables: &first_tables,
                        scope: scope.as_str(),
                        now_ms: 0,
                    },
                    &spec,
                    &Filter::All,
                    &[Sort::asc("rank")],
                    None,
                    2,
                )
            }
        })
        .unwrap()
        .expect("projected scalar sort is SQL-compatible");
    assert_eq!(
        first.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );

    let cursor = CursorPosition::for_record(
        &first[1].id,
        &first[1].doc,
        &first[1].meta,
        &[Sort::asc("rank")],
    );
    let after_tables = tables.clone();
    let after = connection
        .run_now({
            let spec = spec.clone();
            let scope = scope.clone();
            move |conn| {
                matching_ordered(
                    conn,
                    Ctx {
                        tables: &after_tables,
                        scope: scope.as_str(),
                        now_ms: 0,
                    },
                    &spec,
                    &Filter::All,
                    &[Sort::asc("rank")],
                    Some(&cursor),
                    2,
                )
            }
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        after.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        ["c"]
    );

    let fallback_tables = tables;
    let fallback = connection
        .run_now(move |conn| {
            matching_ordered(
                conn,
                Ctx {
                    tables: &fallback_tables,
                    scope: scope.as_str(),
                    now_ms: 0,
                },
                &spec,
                &Filter::All,
                &[Sort::asc("not_projected")],
                None,
                1,
            )
        })
        .unwrap();
    assert!(fallback.is_none(), "unprojected key uses Rust ordering");
}
