//! Blob URL validation and missing-feature failures.
use super::*;
use crate::ErrorKind;

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
