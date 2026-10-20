//! File stream recovery, corruption and filesystem-boundary cases.

use std::path::{Path, PathBuf};

use super::*;
use serde_json::json;
use tempfile::TempDir;
use tinystoragedrivers_api::StreamStore;
use tinystoragedrivers_types::ErrorKind;

fn open() -> (TempDir, FileConnection, Scope, FileStreams) {
    let dir = tempfile::tempdir().unwrap();
    let connection = FileConnection::open(dir.path()).unwrap();
    let scope = Scope::new("local").unwrap();
    let streams = FileStreams::new(connection.clone(), scope.clone());
    (dir, connection, scope, streams)
}

fn files(connection: &FileConnection, scope: &Scope, name: &str) -> (PathBuf, PathBuf) {
    let dir = connection
        .root()
        .join("scopes")
        .join(crate::file::encode::dir_components(scope.as_str()))
        .join("streams");
    let stem = file_stem(name);
    (
        dir.join(format!("{stem}.jsonl")),
        dir.join(format!("{stem}{META_SUFFIX}")),
    )
}

fn append_raw(path: &Path, bytes: &[u8]) {
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(bytes)
        .unwrap();
}

#[tokio::test]
async fn torn_final_line_is_ignored_and_replaced_on_append() {
    let (_dir, connection, scope, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, _) = files(&connection, &scope, "s");
    append_raw(&data, br#"{"offset":1,"val"#);
    assert_eq!(streams.len("s").await.unwrap(), 1);
    assert_eq!(streams.read_window("s", 0, 10).await.unwrap().len(), 1);
    streams.append("s", json!(1)).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(data).unwrap(),
        "{\"offset\":0,\"value\":0}\n{\"offset\":1,\"value\":1}\n"
    );
}

#[tokio::test]
async fn interrupted_truncation_keeps_the_meta_base_authoritative() {
    let (_dir, connection, scope, streams) = open();
    streams
        .append_batch("s", vec![json!(0), json!(1), json!(2)])
        .await
        .unwrap();
    let (_, meta) = files(&connection, &scope, "s");
    std::fs::write(meta, br#"{"name":"s","base":2}"#).unwrap();
    let entries = streams.read_window("s", 0, 10).await.unwrap();
    assert_eq!(
        entries.iter().map(|entry| entry.offset).collect::<Vec<_>>(),
        [2]
    );
    assert_eq!(streams.len("s").await.unwrap(), 3);
    assert_eq!(streams.truncate_before("s", 1).await.unwrap(), 0);
}

#[tokio::test]
async fn corrupt_lines_collisions_and_unopenable_files_are_reported() {
    let (_dir, connection, scope, streams) = open();
    streams.append("s", json!(0)).await.unwrap();
    let (data, meta) = files(&connection, &scope, "s");
    append_raw(&data, b"garbage\n");
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Serialization
    );
    assert_eq!(
        streams.read_window("s", 0, 10).await.unwrap_err().kind(),
        ErrorKind::Serialization
    );

    let another = "y".repeat(300);
    let (_, other_meta) = files(&connection, &scope, &another);
    std::fs::create_dir_all(other_meta.parent().unwrap()).unwrap();
    std::fs::write(&other_meta, br#"{"name":"other","base":0}"#).unwrap();
    assert_eq!(
        streams.len(&another).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(streams.streams("").await.unwrap(), ["other", "s"]);

    std::fs::remove_file(&data).unwrap();
    std::fs::create_dir(&data).unwrap();
    assert_eq!(
        streams.len("s").await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        streams.read_window("s", 0, 5).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(format!("{streams:?}").contains("FileStreams"));
    assert!(meta.exists());
}

#[test]
fn file_stream_length_uses_base_and_last_line_and_rejects_overflow() {
    let meta = StreamMeta {
        name: "s".into(),
        base: 5,
        sealed_len: None,
        sealed_ms: None,
        last_append_ms: None,
    };
    assert_eq!(length(&meta, None).unwrap(), 5);
    assert_eq!(
        length(&meta, Some(br#"{"offset":2,"value":0}"#)).unwrap(),
        5
    );
    assert_eq!(
        length(&meta, Some(br#"{"offset":7,"value":0}"#)).unwrap(),
        8
    );
    let overflow = br#"{"offset":18446744073709551615,"value":0}"#;
    assert!(length(&meta, Some(overflow)).is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_data_files_are_refused_without_touching_the_target() {
    let (dir, connection, scope, streams) = open();
    let (data, _) = files(&connection, &scope, "s");
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    let victim = dir.path().join("victim");
    std::fs::write(&victim, "{\"offset\":0,\"value\":\"private\"}\n").unwrap();
    std::os::unix::fs::symlink(&victim, &data).unwrap();
    assert_eq!(
        streams.append("s", json!(1)).await.unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        std::fs::read_to_string(&victim).unwrap(),
        "{\"offset\":0,\"value\":\"private\"}\n"
    );
    assert_eq!(streams.len("s").await.unwrap(), 0);
}

#[tokio::test]
async fn stream_files_retain_length_after_full_truncation_and_reopen() {
    let (_dir, connection, scope, streams) = open();
    streams
        .append_batch("s", vec![json!(0), json!(1)])
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("s", 9).await.unwrap(), 2);
    let (data, _) = files(&connection, &scope, "s");
    std::fs::remove_file(data).unwrap();
    assert_eq!(streams.len("s").await.unwrap(), 2);
    assert!(streams.read_window("s", 0, 5).await.unwrap().is_empty());
    assert_eq!(streams.append_batch("s", vec![]).await.unwrap(), 2);

    let reopened = FileStreams::new(connection, scope);
    assert_eq!(reopened.len("s").await.unwrap(), 2);
    assert_eq!(reopened.append("s", json!(2)).await.unwrap(), 2);
}

#[tokio::test]
async fn sealing_is_idempotent_and_conditional_delete_checks_the_seal_time() {
    let (_dir, _connection, _scope, streams) = open();
    streams
        .append_batch("sealed", vec![json!("a"), json!("b")])
        .await
        .unwrap();
    let first = streams.seal("sealed").await.unwrap();
    assert_eq!(first.base, 0);
    assert_eq!(first.len, 2);
    assert!(first.blob_bytes > 0);
    let sealed_ms = streams
        .info("sealed")
        .await
        .unwrap()
        .unwrap()
        .sealed_ms
        .unwrap();

    let again = streams.seal("sealed").await.unwrap();
    assert_eq!(again, first);
    assert!(
        !streams
            .delete_sealed_stream("sealed", sealed_ms.saturating_add(1))
            .await
            .unwrap()
    );
    assert!(streams.info("sealed").await.unwrap().is_some());
    assert!(
        streams
            .delete_sealed_stream("sealed", sealed_ms)
            .await
            .unwrap()
    );
    assert!(streams.info("sealed").await.unwrap().is_none());
}
