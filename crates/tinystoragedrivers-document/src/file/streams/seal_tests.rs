use super::*;

fn setup() -> (tempfile::TempDir, Scope, String) {
    let dir = tempfile::tempdir().unwrap();
    let scope = Scope::new("seal-tests").unwrap();
    let key = sealed_blob_key("stream").unwrap();
    (dir, scope, key)
}

fn store_sidecar(root: &Path, scope: &Scope, key: &str, expires_at_ms: Option<u64>) {
    let (data, sidecar) = blob_paths(root, scope, key);
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    std::fs::write(&data, b"payload").unwrap();
    write_json(
        &sidecar,
        &BlobSidecar {
            key: key.to_owned(),
            content_type: Some(CONTENT_TYPE.to_owned()),
            expires_at_ms,
        },
    )
    .unwrap();
}

#[test]
fn blob_lookup_distinguishes_missing_expired_and_live_data() {
    let (dir, scope, key) = setup();
    let root = dir.path();
    assert!(read_blob(root, &scope, &key).unwrap().is_none());

    store_sidecar(root, &scope, &key, Some(10));
    assert!(read_blob(root, &scope, &key).unwrap().is_none());

    store_sidecar(root, &scope, &key, None);
    assert_eq!(
        read_blob(root, &scope, &key).unwrap(),
        Some(b"payload".to_vec())
    );
}

#[test]
fn valid_sidecar_without_a_data_file_is_an_absent_blob() {
    let (dir, scope, key) = setup();
    let (data, sidecar) = blob_paths(dir.path(), &scope, &key);
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    write_json(
        &sidecar,
        &BlobSidecar {
            key: key.clone(),
            content_type: None,
            expires_at_ms: None,
        },
    )
    .unwrap();
    assert!(!data.exists());
    assert!(read_blob(dir.path(), &scope, &key).unwrap().is_none());
}

#[tokio::test]
async fn direct_blob_write_rejects_an_already_sealed_stream() {
    use serde_json::json;
    use tinystoragedrivers_api::StreamStore;

    let (dir, scope, _) = setup();
    let connection = tinystoragedrivers_connect::FileConnection::open(dir.path()).unwrap();
    let streams = FileStreams::new(connection, scope.clone());
    streams.append("stream", json!("entry")).await.unwrap();
    streams.seal("stream").await.unwrap();

    let paths = FileStreams::paths(dir.path(), &scope, "stream");
    assert_eq!(
        write_blob(dir.path(), &scope, &paths, "stream")
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
}

#[test]
fn blob_lookup_rejects_a_hashed_name_collision() {
    let (dir, scope, key) = setup();
    let (data, sidecar) = blob_paths(dir.path(), &scope, &key);
    std::fs::create_dir_all(data.parent().unwrap()).unwrap();
    write_json(
        &sidecar,
        &BlobSidecar {
            key: "different-key".into(),
            content_type: None,
            expires_at_ms: None,
        },
    )
    .unwrap();
    assert_eq!(
        read_blob(dir.path(), &scope, &key).unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Backend
    );
}

#[test]
fn deleting_a_blob_removes_sidecar_and_data_and_is_idempotent() {
    let (dir, scope, key) = setup();
    store_sidecar(dir.path(), &scope, &key, None);
    let paths = blob_paths(dir.path(), &scope, &key);
    assert!(paths.0.exists() && paths.1.exists());
    delete_blob(dir.path(), &scope, "stream").unwrap();
    assert!(!paths.0.exists() && !paths.1.exists());
    delete_blob(dir.path(), &scope, "stream").unwrap();
}
