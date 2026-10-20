//! Behavioral tests for the shared-connection SQLite KV engine.

use super::*;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::tempdir;

fn connection() -> (SqliteConnection, Arc<AtomicU64>, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let now = Arc::new(AtomicU64::new(2_000));
    let clock_value = Arc::clone(&now);
    let connection = SqliteConnection::open_with_clock(
        dir.path(),
        Arc::new(move || clock_value.load(Ordering::SeqCst)),
    )
    .unwrap();
    (connection, now, dir)
}

#[tokio::test]
async fn sqlite_round_trips_json_cas_and_delete_versions() {
    let (connection, _, _dir) = connection();
    let scope = Scope::new("tenant").unwrap();
    let store = SqliteKv::new(connection.clone(), scope.clone()).unwrap();
    let first = store.put("key", json!([1, null, "x"])).await.unwrap();
    assert_eq!(first, Version::FIRST);
    let read = store.get("key").await.unwrap().unwrap();
    let second = store
        .cas("key", json!(true), read.unchanged())
        .await
        .unwrap();
    assert_eq!(second, Version(2));
    assert!(
        store
            .delete("key", Precondition::Version(second))
            .await
            .unwrap()
    );
    assert_eq!(store.put("key", json!(false)).await.unwrap(), Version(4));
    let another_scope = SqliteKv::new(connection, Scope::new("other").unwrap()).unwrap();
    assert!(another_scope.get("key").await.unwrap().is_none());
}

#[tokio::test]
async fn sqlite_cas_conflicts_and_listing_pages_in_key_order() {
    let (connection, _, _dir) = connection();
    let store = SqliteKv::new(connection, Scope::new("tenant").unwrap()).unwrap();
    for key in ["other", "p/3", "p/1", "p/2"] {
        store.put(key, json!(key)).await.unwrap();
    }
    let error = store
        .cas("p/1", json!(0), Precondition::Absent)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    let first = store.list("p/", None, 2).await.unwrap();
    assert_eq!(
        first
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["p/1", "p/2"]
    );
    let second = store.list("p/", first.next, 2).await.unwrap();
    assert_eq!(
        second
            .items
            .iter()
            .map(|item| item.id.as_str())
            .collect::<Vec<_>>(),
        ["p/3"]
    );
}

#[tokio::test]
async fn sqlite_ttl_uses_the_connection_clock_and_expired_rows_stay_versioned() {
    let (connection, now, _dir) = connection();
    let store = SqliteKv::new(connection, Scope::new("tenant").unwrap()).unwrap();
    assert_eq!(
        store
            .put_with_ttl("expiring", json!(1), Duration::from_millis(50))
            .await
            .unwrap(),
        Version::FIRST
    );
    assert!(store.get("expiring").await.unwrap().is_some());
    now.fetch_add(50, Ordering::SeqCst);
    assert!(store.get("expiring").await.unwrap().is_none());
    assert_eq!(store.put("expiring", json!(2)).await.unwrap(), Version(2));
}

#[tokio::test]
async fn sqlite_writes_only_to_native_kv_table() {
    let (connection, _, _dir) = connection();
    connection
        .run_now(|conn| {
            conn.execute_batch(
                "CREATE TABLE kv (id TEXT PRIMARY KEY, payload TEXT NOT NULL); INSERT INTO kv VALUES ('legacy', 'preserve');",
            )
            .map_err(tinystoragedrivers_connect::sqlite::error::during(
                "seed a legacy table",
            ))
        })
        .unwrap();
    let store = SqliteKv::new(connection.clone(), Scope::new("tenant").unwrap()).unwrap();
    store.put("key", json!(1)).await.unwrap();
    let count: i64 = connection
        .run_now(|conn| {
            conn.query_row("SELECT count(*) FROM docs", [], |row| row.get(0))
                .map_err(tinystoragedrivers_connect::sqlite::error::during(
                    "count documents",
                ))
        })
        .unwrap();
    assert_eq!(count, 0);
    let legacy: String = connection
        .run_now(|conn| {
            conn.query_row("SELECT payload FROM kv WHERE id='legacy'", [], |row| {
                row.get(0)
            })
            .map_err(tinystoragedrivers_connect::sqlite::error::during(
                "read the legacy table",
            ))
        })
        .unwrap();
    assert_eq!(legacy, "preserve");
    let tables: i64 = connection
        .run_now(|conn| {
            conn.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='_tsd_kv'",
                [],
                |row| row.get(0),
            )
            .map_err(tinystoragedrivers_connect::sqlite::error::during(
                "inspect KV table",
            ))
        })
        .unwrap();
    assert_eq!(tables, 1);
}

#[tokio::test]
async fn independent_connections_serialize_compare_and_swap() {
    let (connection, _, dir) = connection();
    let second_connection = SqliteConnection::open(dir.path()).unwrap();
    let scope = Scope::new("tenant").unwrap();
    let first = SqliteKv::new(connection, scope.clone()).unwrap();
    let second = SqliteKv::new(second_connection, scope).unwrap();
    let version = first.put("key", json!(0)).await.unwrap();

    let (left, right) = tokio::join!(
        first.cas("key", json!(1), Precondition::Version(version)),
        second.cas("key", json!(2), Precondition::Version(version)),
    );
    let loser = match (left, right) {
        (Ok(_), Err(error)) | (Err(error), Ok(_)) => error,
        outcome => panic!("expected one successful CAS, got {outcome:?}"),
    };
    assert_eq!(loser.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    assert_eq!(first.get("key").await.unwrap().unwrap().version, Version(2));
}
