//! Independently configured blob storage and composite document/blob backends.
#[cfg(not(feature = "s3"))]
use crate::StorageError;
use crate::{BackendBlobs, BlobBackend, Result, StorageConfig};
use std::sync::Arc;

/// A blob location independent of the document/stream location.
#[derive(Debug, Clone)]
pub enum BlobConfig {
    /// The blob port from a complete backend.
    Backend(StorageConfig),
    /// An S3-compatible object-store URL. Credentials belong in the provider chain.
    S3 {
        /// Validated S3 location without credentials.
        url: String,
    },
}

impl BlobConfig {
    /// Parse a blob URL (s3://, or any [`StorageConfig`] form).
    ///
    /// # Errors
    ///
    /// Invalid URL, or an S3 URL when the s3 feature is absent.
    pub fn parse(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                tinystoragedrivers_s3::S3Config::parse(url)?;
                return Ok(Self::S3 {
                    url: url.to_owned(),
                });
            }
            #[cfg(not(feature = "s3"))]
            return Err(StorageError::invalid_input(
                "s3 blob URL needs the s3 feature",
            ));
        }
        Ok(Self::Backend(StorageConfig::parse(url)?))
    }
}

/// Open a scoped blob backend independent of the document backend.
///
/// # Errors
///
/// A missing feature, invalid configuration, or backend connection error.
pub async fn open_blobs(config: &BlobConfig) -> Result<Arc<dyn BlobBackend>> {
    match config {
        BlobConfig::Backend(config) => Ok(Arc::new(BackendBlobs::new(crate::open(config).await?))),
        #[cfg(feature = "s3")]
        BlobConfig::S3 { url } => Ok(Arc::new(tinystoragedrivers_s3::S3Storage::open(
            &tinystoragedrivers_s3::S3Config::parse(url)?,
        )?)),
        #[cfg(not(feature = "s3"))]
        BlobConfig::S3 { .. } => Err(StorageError::invalid_input(
            "s3 blob URL needs the s3 feature",
        )),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
