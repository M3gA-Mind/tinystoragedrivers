//! Blob URL validation and missing-feature failures.
use super::*;
use crate::{ErrorKind, StorageConfig};
use tinystoragedrivers_core::{Capability, Scope};

#[tokio::test]
async fn opening_a_backend_blob_port_preserves_scope_and_capability_behavior() {
    let blobs = open_blobs(&BlobConfig::Backend(StorageConfig::Memory))
        .await
        .unwrap();
    let first = blobs.for_scope(&Scope::new("first").unwrap()).unwrap();
    let second = blobs.for_scope(&Scope::new("second").unwrap()).unwrap();

    first.put("item", b"first".to_vec(), None).await.unwrap();
    second.put("item", b"second".to_vec(), None).await.unwrap();
    assert_eq!(first.get("item").await.unwrap().unwrap().bytes, b"first");
    assert_eq!(second.get("item").await.unwrap().unwrap().bytes, b"second");

    let caps = blobs.capabilities();
    assert!(caps.contains(Capability::BlobTtl));
    assert!(!caps.contains(Capability::Transactions));
    assert!(!caps.contains(Capability::Fencing));
    assert!(!caps.contains(Capability::PresignedUrls));
    assert!(!caps.contains(Capability::StreamingBlobs));
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
