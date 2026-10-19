//! Optional `MinIO` conformance and a large streaming roundtrip.
//! Set `TSD_S3_URL` plus `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` to a local test bucket.

use tinystoragedrivers_core::{BlobBackend, Scope};
use tinystoragedrivers_s3::{S3Config, S3Storage};
use tokio::io::AsyncReadExt;

fn driver() -> Result<Option<S3Storage>, tinystoragedrivers_core::StorageError> {
    let Ok(url) = std::env::var("TSD_S3_URL") else {
        return Ok(None);
    };
    Ok(Some(S3Storage::open(&S3Config::parse(&url)?)?))
}

#[tokio::test]
async fn live_minio_blob_reads_ranges_lists_and_isolates_scopes() {
    let Some(driver) = driver().unwrap() else {
        return;
    };
    let target_scope = Scope::new("live-blob-target").unwrap();
    let sibling_scope = Scope::new("live-blob-sibling").unwrap();
    let target = driver.for_scope(&target_scope).unwrap();
    let sibling = driver.for_scope(&sibling_scope).unwrap();

    target
        .put("records/one", b"target-bytes".to_vec(), Some("text/plain"))
        .await
        .unwrap();
    sibling
        .put("records/one", b"sibling-bytes".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        target
            .get_range("records/one", 7..12)
            .await
            .unwrap()
            .unwrap(),
        b"bytes"
    );
    assert_eq!(
        target.get("records/one").await.unwrap().unwrap().bytes,
        b"target-bytes"
    );
    assert_eq!(
        sibling.get("records/one").await.unwrap().unwrap().bytes,
        b"sibling-bytes"
    );
    assert_eq!(target.list("records/").await.unwrap().len(), 1);
    assert!(target.get("missing").await.unwrap().is_none());

    assert!(target.delete("records/one").await.unwrap());
    assert!(sibling.get("records/one").await.unwrap().is_some());
    assert!(sibling.delete("records/one").await.unwrap());
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
