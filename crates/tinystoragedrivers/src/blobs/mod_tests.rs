//! Composite URL selection and missing-feature failures.
use super::*;
use crate::{ErrorKind, Scope};

#[tokio::test]
async fn composite_config_selects_independent_persistent_ports() {
    let config = BlobConfig::parse("memory").unwrap();
    let backend = open_composite(&StorageConfig::Memory, &config)
        .await
        .unwrap();
    let one = backend.for_scope(&Scope::local()).unwrap();
    one.blobs().put("x", vec![1], None).await.unwrap();
    let two = backend.for_scope(&Scope::new("other").unwrap()).unwrap();
    assert!(two.blobs().get("x").await.unwrap().is_none());
    crate::conformance::run(backend.as_ref(), false).await;
}

#[test]
fn s3_configuration_requires_its_feature() {
    let result = BlobConfig::parse("s3://bucket/prefix?region=us-east-1");
    #[cfg(feature = "s3")]
    assert!(matches!(result.unwrap(), BlobConfig::S3 { .. }));
    #[cfg(not(feature = "s3"))]
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
}

#[test]
fn s3_credentials_are_rejected_without_echoing_the_url() {
    let error = BlobConfig::parse("s3://user:secret@bucket").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!error.to_string().contains("secret"));
}
