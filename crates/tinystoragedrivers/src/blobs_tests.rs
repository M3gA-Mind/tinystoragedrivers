use super::*;

#[test]
fn s3_configuration_requires_the_matching_facade_feature() {
    assert!(matches!(
        BlobConfig::parse("memory").unwrap(),
        BlobConfig::Backend(_)
    ));
    assert!(BlobConfig::parse("invalid-backend").is_err());
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

#[cfg(feature = "s3")]
#[tokio::test]
async fn configured_s3_blob_port_roundtrips_through_the_facade() {
    let Ok(url) = std::env::var("TSD_S3_URL") else {
        return;
    };
    let scope = Scope::new("facade-s3-coverage").unwrap();
    let blobs = open_blobs(&BlobConfig::parse(&url).unwrap(), &scope)
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let key = format!("facade-{}-{nonce}", std::process::id());
    blobs
        .put(&key, b"facade payload".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        blobs.get(&key).await.unwrap().unwrap().bytes,
        b"facade payload"
    );
    assert!(blobs.head(&key).await.unwrap().is_some());
    assert!(blobs.delete(&key).await.unwrap());
}
