use super::S3ConnectionConfig;

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
