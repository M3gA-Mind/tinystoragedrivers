use super::*;

#[test]
fn s3_configuration_requires_the_matching_facade_feature() {
    let parsed = BlobConfig::parse("s3://bucket/prefix?region=us-west-2");
    if cfg!(feature = "s3") {
        assert!(matches!(parsed.unwrap(), BlobConfig::S3 { .. }));
    } else {
        assert!(parsed.unwrap_err().message().contains("s3 feature"));
    }
}

#[tokio::test]
async fn backend_blob_selection_returns_a_real_scope_bound_port() {
    let scope = Scope::new("local").unwrap();
    let config = BlobConfig::Backend(StorageConfig::parse("memory").unwrap());
    let blobs = open_blobs(&config, &scope).await.unwrap();
    blobs.put("note", b"contents".to_vec(), None).await.unwrap();
    assert_eq!(blobs.get("note").await.unwrap().unwrap().bytes, b"contents");
}
