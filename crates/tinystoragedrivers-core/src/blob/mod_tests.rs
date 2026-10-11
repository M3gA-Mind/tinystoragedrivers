//! Blob key validation and range clamping.

use super::*;

#[test]
fn keys_validate() {
    assert!(validate_blob_key("uploads/2026/a.png").is_ok());
    for bad in ["", "/a", "a/", "a//b", "./a", "a/../b", "nul\0"] {
        assert!(validate_blob_key(bad).is_err(), "{bad:?}");
    }
    assert!(validate_blob_key(&"a".repeat(MAX_BLOB_KEY_LEN + 1)).is_err());
}

#[test]
fn ranges_clamp_to_the_blob() {
    assert_eq!(clamp_range(&(2..5), 10).unwrap(), 2..5);
    assert_eq!(clamp_range(&(2..50), 10).unwrap(), 2..10);
    assert_eq!(clamp_range(&(20..50), 10).unwrap(), 10..10);
    assert_eq!(clamp_range(&(0..u64::MAX), 3).unwrap(), 0..3);
    #[allow(
        clippy::reversed_empty_ranges,
        reason = "the inverted range is the input under test"
    )]
    let inverted = 5..1;
    assert!(clamp_range(&inverted, 10).is_err());
}

#[test]
fn metadata_omits_a_missing_content_type() {
    let meta = BlobMeta {
        key: "k".into(),
        len: 1,
        content_type: None,
    };
    assert_eq!(
        serde_json::to_string(&meta).unwrap(),
        r#"{"key":"k","len":1}"#
    );
}

#[tokio::test]
async fn default_streaming_checks_length_before_publishing() {
    use crate::{MemoryStorage, Scope, StorageBackend};
    let storage = MemoryStorage::new().for_scope(&Scope::local()).unwrap();
    let blobs = storage.blobs();
    blobs.put("old", vec![9], None).await.unwrap();
    for len in [1, 3] {
        let result = blobs
            .put_stream("old", Box::pin(std::io::Cursor::new(vec![1, 2])), len, None)
            .await;
        assert!(result.is_err());
        assert_eq!(blobs.get("old").await.unwrap().unwrap().bytes, [9]);
    }
    assert!(!blobs.capabilities().contains(Capability::StreamingBlobs));
    assert_eq!(
        blobs
            .presign_put("old", Duration::from_secs(60))
            .await
            .unwrap_err()
            .kind(),
        crate::ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    let mut stream = blobs.get_stream("old").await.unwrap().unwrap();
    let mut bytes = Vec::new();
    stream.reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, [9]);
}
