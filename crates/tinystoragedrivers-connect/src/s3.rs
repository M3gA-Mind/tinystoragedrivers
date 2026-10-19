//! Shared S3-compatible object-store connection handles.

use std::sync::Arc;

use object_store::{ObjectStore, aws::AmazonS3Builder, signer::Signer};
use tinystoragedrivers_types::{Result, StorageError};

/// Credential-free S3 bucket and endpoint configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ConnectionConfig {
    /// Bucket name.
    pub bucket: String,
    /// Prefix within the bucket.
    pub prefix: String,
    /// AWS region, defaulting to `us-east-1`.
    pub region: String,
    /// Optional S3-compatible endpoint.
    pub endpoint: Option<String>,
}

impl S3ConnectionConfig {
    /// Parse an `s3://bucket/prefix` URL with optional `region` and `endpoint`.
    /// Credentials are supplied through the standard AWS provider chain.
    ///
    /// # Errors
    /// Returns `InvalidInput` for malformed URLs, embedded credentials, or
    /// unsupported parameters.
    pub fn parse(value: &str) -> Result<Self> {
        let url = url::Url::parse(value)
            .map_err(|_| StorageError::invalid_input("invalid s3 storage URL"))?;
        if url.scheme() != "s3"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.fragment().is_some()
        {
            return Err(StorageError::invalid_input(
                "s3 URL must name a bucket without credentials",
            ));
        }
        let bucket = url
            .host_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(|| StorageError::invalid_input("s3 URL requires a bucket"))?
            .to_owned();
        let prefix = url.path().trim_matches('/').to_owned();
        if !prefix.is_empty() {
            validate_prefix(&prefix)?;
        }
        let mut region = "us-east-1".to_owned();
        let mut endpoint = None;
        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "region" if !value.is_empty() => region = value.into_owned(),
                "endpoint" if !value.is_empty() => {
                    let parsed = url::Url::parse(&value)
                        .map_err(|_| StorageError::invalid_input("invalid s3 endpoint"))?;
                    if !matches!(parsed.scheme(), "http" | "https")
                        || parsed.host_str().is_none()
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                        || parsed.query().is_some()
                        || parsed.fragment().is_some()
                    {
                        return Err(StorageError::invalid_input("invalid s3 endpoint"));
                    }
                    endpoint = Some(value.into_owned());
                }
                _ => {
                    return Err(StorageError::invalid_input(
                        "unknown or empty s3 URL parameter",
                    ));
                }
            }
        }
        Ok(Self {
            bucket,
            prefix,
            region,
            endpoint,
        })
    }

    fn builder(&self) -> AmazonS3Builder {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region);
        if let Some(endpoint) = &self.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_config(object_store::aws::AmazonS3ConfigKey::S3Endpoint, endpoint)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false);
        }
        builder
    }
}

/// A configured object-store client shared by blob handles.
#[derive(Clone)]
pub struct S3Connection {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    bucket: String,
    prefix: String,
}

impl std::fmt::Debug for S3Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Connection")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl S3Connection {
    /// Build a client using the configuration's standard AWS credential providers.
    /// This does not contact the remote service.
    ///
    /// # Errors
    /// Returns `InvalidInput` when client configuration is invalid.
    pub fn open(config: &S3ConnectionConfig) -> Result<Self> {
        let client = Arc::new(config.builder().build().map_err(|error| {
            StorageError::invalid_input("invalid s3 client configuration").with_source(error)
        })?);
        Ok(Self {
            store: client.clone(),
            signer: Some(client),
            bucket: config.bucket.clone(),
            prefix: config.prefix.trim_matches('/').to_owned(),
        })
    }

    /// Construct a shared handle around an injected object store.
    #[must_use]
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            store,
            signer,
            bucket: bucket.into(),
            prefix: prefix.into().trim_matches('/').to_owned(),
        }
    }

    /// Return a clone of the shared object store.
    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Return an optional signer for presigned requests.
    #[must_use]
    pub fn signer(&self) -> Option<Arc<dyn Signer>> {
        self.signer.clone()
    }

    /// Return the configured bucket.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Return the configured object prefix.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

#[cfg(test)]
#[path = "s3_tests.rs"]
mod tests;

fn validate_prefix(prefix: &str) -> Result<()> {
    use tinystoragedrivers_types::Key;
    if prefix.len() > tinystoragedrivers_types::MAX_KEY_LEN || prefix.contains('\0') {
        return Err(StorageError::invalid_input("invalid s3 object prefix"));
    }
    if prefix
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(StorageError::invalid_input("invalid s3 object prefix"));
    }
    Key::new(prefix.to_owned())?;
    Ok(())
}
