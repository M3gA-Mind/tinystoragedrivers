//! Optional `MinIO` conformance and a large streaming roundtrip.
//! Set `TSD_S3_URL` plus `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to a local test bucket.

use std::sync::Arc;
use tinystoragedrivers_core::{BlobBackend, CompositeStorage, MemoryStorage, Scope};
use tinystoragedrivers_s3::{S3Config, S3Storage};
use tokio::io::AsyncReadExt;

fn driver() -> Result<Option<S3Storage>, tinystoragedrivers_core::StorageError> {
    let Ok(url) = std::env::var("TSD_S3_URL") else {
        return Ok(None);
    };
    Ok(Some(S3Storage::open(&S3Config::parse(&url)?)?))
}

#[tokio::test]
async fn live_minio_blob_and_isolation_conformance() {
    let Some(blobs) = driver().unwrap() else {
        return;
    };
    let backend = CompositeStorage::new(Arc::new(MemoryStorage::new()), Arc::new(blobs));
    tinystoragedrivers_core::conformance::run_with_process_capabilities(
        &backend, false, false, false,
    )
    .await;
}

#[tokio::test]
async fn live_minio_500_mib_streaming_roundtrip() {
    let Some(driver) = driver().unwrap() else {
        return;
    };
    let scope = driver.for_scope(&Scope::new("large").unwrap()).unwrap();
    let len = 500 * 1024 * 1024;
    let meta = scope
        .put_stream(
            "streaming",
            Box::pin(tokio::io::repeat(42).take(len)),
            len,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    assert_eq!(meta.len, len);
    let mut blob = scope.get_stream("streaming").await.unwrap().unwrap();
    assert_eq!(blob.meta, meta);
    let mut chunk = vec![0; 64 * 1024];
    let mut got = 0_u64;
    loop {
        let read = blob.reader.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        assert!(chunk[..read].iter().all(|b| *b == 42));
        got += read as u64;
    }
    assert_eq!(got, len);
    assert!(scope.delete("streaming").await.unwrap());
}

#[tokio::test]
async fn live_minio_direct_presigned_upload_and_download() {
    let Some(driver) = driver().unwrap() else {
        return;
    };
    let scope = driver.for_scope(&Scope::new("presigned").unwrap()).unwrap();
    let ttl = std::time::Duration::from_secs(60);
    let client = reqwest::Client::new();
    let put = scope.presign_put("direct", ttl).await.unwrap();
    client
        .put(put)
        .body("direct bytes")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let get = scope.presign_get("direct", ttl).await.unwrap();
    let body = client
        .get(get)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"direct bytes");
    assert!(scope.delete("direct").await.unwrap());
}
