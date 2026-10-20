use super::*;
use tinystoragedrivers_api::StreamStore;

async fn open() -> (tempfile::TempDir, SqliteConnection, Scope, SqliteStreams) {
    let dir = tempfile::tempdir().unwrap();
    let connection = SqliteConnection::open(dir.path()).unwrap();
    let scope = Scope::new("stream-unit").unwrap();
    let streams = SqliteStreams::new(connection.clone(), scope.clone());
    (dir, connection, scope, streams)
}

#[tokio::test]
async fn stream_info_prefix_listing_truncation_and_delete_preserve_offsets() {
    let (_dir, _connection, _scope, streams) = open().await;
    streams
        .append_batch("events/a", vec![serde_json::json!(1), serde_json::json!(2)])
        .await
        .unwrap();
    streams
        .append("elsewhere", serde_json::json!(3))
        .await
        .unwrap();
    let info = streams.info("events/a").await.unwrap().unwrap();
    assert_eq!((info.base, info.len), (0, 2));
    assert!(info.sealed_ms.is_none());
    assert!(info.last_append_ms.is_some());
    assert_eq!(streams.info("missing").await.unwrap(), None);
    assert_eq!(
        streams.streams("events/").await.unwrap(),
        ["events/a".to_owned()]
    );
    assert_eq!(streams.truncate_before("events/a", 1).await.unwrap(), 1);
    assert_eq!(
        streams
            .read_window("events/a", 0, usize::MAX)
            .await
            .unwrap(),
        [StreamEntry {
            offset: 1,
            value: serde_json::json!(2)
        }]
    );
    assert_eq!(
        streams.truncate_before("events/a", u64::MAX).await.unwrap(),
        1
    );
    assert_eq!(streams.len("events/a").await.unwrap(), 2);
    assert!(streams.delete_stream("events/a").await.unwrap());
    assert!(!streams.delete_stream("events/a").await.unwrap());
    assert_eq!(streams.len("events/a").await.unwrap(), 0);
    assert!(format!("{streams:?}").contains("stream-unit"));
}

#[tokio::test]
async fn sealed_stream_delete_requires_the_inspected_timestamp() {
    let (_dir, _connection, _scope, streams) = open().await;
    streams
        .append("sealed", serde_json::json!("value"))
        .await
        .unwrap();
    assert_eq!(
        streams
            .append_batch_if_offset("sealed", 0, vec![])
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    let outcome = streams.seal("sealed").await.unwrap();
    assert_eq!(outcome.len, 1);
    let info = streams.info("sealed").await.unwrap().unwrap();
    let sealed_ms = info.sealed_ms.expect("sealed stream records its seal time");
    assert!(
        streams
            .delete_sealed_stream("sealed", sealed_ms + 1)
            .await
            .unwrap()
            == false
    );
    assert!(
        streams
            .delete_sealed_stream("sealed", sealed_ms)
            .await
            .unwrap()
    );
    assert_eq!(streams.info("sealed").await.unwrap(), None);
    assert!(
        !streams
            .streams("")
            .await
            .unwrap()
            .contains(&"sealed".to_owned())
    );
    assert_eq!(
        streams
            .append("sealed", serde_json::json!("new"))
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn validates_names_and_surfaces_corrupt_rows() {
    let (_dir, connection, _scope, streams) = open().await;
    assert_eq!(
        streams
            .append("", serde_json::json!(1))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert_eq!(
        streams.info("").await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert!(streams.read_window("bad", 0, 1).await.unwrap().is_empty());
    streams.append("bad", serde_json::json!(1)).await.unwrap();
    let tables = connection.tables().clone();
    connection
        .run_now(move |conn| {
            conn.execute(
                &format!(
                    "UPDATE {} SET body = 'not json' WHERE name = 'bad'",
                    ident(tables.streams())
                ),
                [],
            )
            .map_err(during("corrupt stream row"))?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        streams.read_window("bad", 0, 1).await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Serialization
    );
}

#[test]
fn offset_conversion_rejects_values_outside_the_sqlite_domain() {
    assert_eq!(to_offset(-1), 0);
    assert_eq!(
        to_stored(u64::MAX).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Backend
    );
}
