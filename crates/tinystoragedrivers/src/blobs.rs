//! Select and open a scope-bound blob port independently of the other ports.

use std::sync::Arc;

use tinystoragedrivers_api::BlobStore;
#[cfg(not(feature = "s3"))]
use tinystoragedrivers_types::StorageError;
use tinystoragedrivers_types::{Result, Scope};

use crate::StorageConfig;

/// A blob location independent of a complete backend's other primitives.
#[derive(Debug, Clone)]
pub enum BlobConfig {
    /// Reuse the blob port from a complete backend configuration.
    Backend(StorageConfig),
    /// S3-compatible object storage; credentials use the standard provider chain.
    S3 {
        /// Credential-free S3 location.
        url: String,
    },
}

impl BlobConfig {
    /// Parse an S3 URL or any [`StorageConfig`] form.
    ///
    /// # Errors
    /// Returns `InvalidInput` for malformed or disabled S3 configuration.
    pub fn parse(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                tinystoragedrivers_connect::S3ConnectionConfig::parse(url)?;
                return Ok(Self::S3 {
                    url: url.to_owned(),
                });
            }
            #[cfg(not(feature = "s3"))]
            {
                return Err(StorageError::invalid_input(
                    "s3 blob URL needs the s3 feature",
                ));
            }
        }
        Ok(Self::Backend(StorageConfig::parse(url)?))
    }
}

/// Open a blob port bound to `scope`.
///
/// # Errors
/// Returns a configuration, connection, or backend-opening error.
pub async fn open_blobs(config: &BlobConfig, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
    match config {
        BlobConfig::Backend(config) => {
            let backend = crate::open(config).await?;
            Ok(backend.for_scope(scope)?.blobs().clone())
        }
        #[cfg(feature = "s3")]
        BlobConfig::S3 { url } => {
            let parsed = tinystoragedrivers_connect::S3ConnectionConfig::parse(url)?;
            let connection = tinystoragedrivers_connect::S3Connection::open(&parsed)?;
            Ok(Arc::new(tinystoragedrivers_blob::S3BlobStore::new(
                &connection,
                scope,
            )?))
        }
        #[cfg(not(feature = "s3"))]
        BlobConfig::S3 { .. } => Err(StorageError::invalid_input(
            "s3 blob URL needs the s3 feature",
        )),
    }
}

#[cfg(test)]
#[path = "blobs_tests.rs"]
mod tests;
