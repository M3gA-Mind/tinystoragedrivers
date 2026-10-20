use super::{S3Connection, S3ConnectionConfig};
use std::sync::Arc;

#[test]
fn parses_bucket_prefix_and_compatible_endpoint() {
    let config = S3ConnectionConfig::parse(
        "s3://media/attachments?region=local&endpoint=http%3A%2F%2Flocalhost%3A9000",
    )
    .unwrap();
    assert_eq!(config.bucket, "media");
    assert_eq!(config.prefix, "attachments");
    assert_eq!(config.region, "local");
    assert_eq!(config.endpoint.as_deref(), Some("http://localhost:9000"));
}

#[test]
fn rejects_bad_storage_prefixes_and_credentials() {
    for value in [
        "s3://user:secret@bucket",
        "s3://bucket?secret=x",
        "s3://bucket/a//b",
    ] {
        assert!(S3ConnectionConfig::parse(value).is_err(), "{value}");
    }
}

#[test]
fn rejects_invalid_url_parts_endpoints_and_prefixes() {
    for value in [
        "https://bucket/path",
        "s3://bucket:9000/path",
        "s3://bucket/path#fragment",
        "s3:///missing-bucket",
        "s3://bucket?region=",
        "s3://bucket?unknown=value",
        "s3://bucket/path?endpoint=ftp%3A%2F%2Flocalhost",
        "s3://bucket/path?endpoint=http%3A%2F%2Fuser%3Asecret%40localhost",
        "s3://bucket/path?endpoint=http%3A%2F%2Flocalhost%3Fbad%3D1",
        "s3://bucket/a/../b",
    ] {
        assert!(S3ConnectionConfig::parse(value).is_err(), "{value}");
    }
    let error = S3ConnectionConfig::parse(&format!("s3://bucket/{}", "x".repeat(1025)));
    assert!(error.is_err());
}

#[test]
fn injected_object_store_is_shared_with_its_configured_prefix() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let connection = S3Connection::with_store(store.clone(), None, "bucket", "/base/path/");
    let cloned = connection.clone();
    assert!(Arc::ptr_eq(&connection.store(), &store));
    assert!(Arc::ptr_eq(&cloned.store(), &store));
    assert!(connection.signer().is_none());
    assert_eq!(connection.bucket(), "bucket");
    assert_eq!(connection.prefix(), "base/path");
    assert!(format!("{connection:?}").contains("S3Connection"));
}

#[test]
fn builds_s3_connection_without_contacting_the_service() {
    let config = S3ConnectionConfig::parse(
        "s3://test-bucket/prefix?region=us-east-1&endpoint=http%3A%2F%2F127.0.0.1%3A9000",
    )
    .unwrap();
    let connection = S3Connection::open(&config).unwrap();

    assert_eq!(connection.bucket(), "test-bucket");
    assert_eq!(connection.prefix(), "prefix");
    assert!(connection.signer().is_some());
}
