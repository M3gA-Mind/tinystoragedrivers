//! Scoped S3 blobs, with bounded multipart writes and incremental reads.

use std::{ops::Range, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::TryStreamExt;
use http::Method;
use object_store::{
    Attribute, Attributes, ObjectStore, PutMultipartOptions, PutOptions, aws::AmazonS3Builder,
    path::Path, signer::Signer,
};
use tinystoragedrivers_core::{
    Blob, BlobBackend, BlobMeta, BlobReader, BlobStore, BlobStream, Capabilities, Capability,
    ErrorKind, Result, Scope, StorageError, async_trait, validate_blob_key, validate_database,
};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

/// Fixed multipart chunk size (8 MiB); at most one part is in flight.
const PART_SIZE: usize = 8 * 1024 * 1024;
const HEX: &[u8; 16] = b"0123456789abcdef";

/// S3 bucket, endpoint and object-key prefix. Credentials come from the
/// standard AWS environment/credential provider chain, never from the URL.
#[derive(Debug, Clone)]
pub struct S3Config {
    /// Bucket name.
    pub bucket: String,
    /// Prefix inside the bucket.
    pub prefix: String,
    /// AWS region (also supplied for compatible endpoints).
    pub region: String,
    /// Optional S3-compatible endpoint; HTTP is accepted for local `MinIO`.
    pub endpoint: Option<String>,
}

impl S3Config {
    /// Parse an S3 URL with optional region and endpoint query parameters.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for malformed URLs, credentials, or unknown parameters.
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
            .filter(|s| !s.is_empty())
            .ok_or_else(|| StorageError::invalid_input("s3 URL requires a bucket"))?
            .to_owned();
        let prefix = url.path().trim_matches('/').to_owned();
        if !prefix.is_empty() {
            validate_blob_key(&prefix)?;
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

    /// Construct a scoped blob handle, using path-style requests for compatible
    /// endpoints. Construction performs no network request.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an invalid database name, prefix, or client configuration.
    pub fn blobs(&self, scope: &Scope, database: &str) -> Result<S3Blobs> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region);
        if let Some(endpoint) = &self.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false);
        }
        let client = Arc::new(builder.build().map_err(map_error)?);
        S3Blobs::with_store(client.clone(), Some(client), &self.prefix, scope, database)
    }
}

/// One connected S3 client shared by all tenant/database blob handles.
#[derive(Debug)]
pub struct S3Storage {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    prefix: String,
    database: String,
}

impl S3Storage {
    /// Build a client using the configuration's standard credential providers.
    ///
    /// # Errors
    ///
    /// Invalid configuration or client construction failure.
    pub fn open(config: &S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region);
        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false);
        }
        let client = Arc::new(builder.build().map_err(map_error)?);
        Self::with_store(client.clone(), Some(client), &config.prefix)
    }

    /// Inject an object-store client for local conformance or custom providers.
    ///
    /// # Errors
    ///
    /// Invalid object prefix.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        prefix: &str,
    ) -> Result<Self> {
        if !prefix.is_empty() {
            validate_blob_key(prefix.trim_matches('/'))?;
        }
        Ok(Self {
            store,
            signer,
            prefix: prefix.trim_matches('/').to_owned(),
            database: "default".to_owned(),
        })
    }
}

impl BlobBackend for S3Storage {
    fn capabilities(&self) -> Capabilities {
        let caps = Capabilities::none().with(Capability::StreamingBlobs);
        if self.signer.is_some() {
            caps.with(Capability::PresignedUrls)
        } else {
            caps
        }
    }
    fn for_scope(&self, scope: &Scope) -> Result<Arc<dyn BlobStore>> {
        Ok(Arc::new(S3Blobs::with_store(
            self.store.clone(),
            self.signer.clone(),
            &self.prefix,
            scope,
            &self.database,
        )?))
    }
    fn database(&self, name: &str) -> Result<Arc<dyn BlobBackend>> {
        validate_database(name)?;
        Ok(Arc::new(Self {
            store: self.store.clone(),
            signer: self.signer.clone(),
            prefix: self.prefix.clone(),
            database: name.to_owned(),
        }))
    }
}

/// Blob port under an unambiguous database/tenant prefix.
///
/// Writes are not fenced with documents. Upload immutable blobs first, then
/// fence the document referencing them, and garbage-collect unreferenced blobs.
#[derive(Debug)]
pub struct S3Blobs {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    prefix: String,
}

impl S3Blobs {
    /// Inject an S3-compatible object store and optional signer. This also enables
    /// deterministic conformance tests against a local object store.
    ///
    /// # Errors
    ///
    /// `InvalidInput` for an invalid prefix or database name.
    pub fn with_store(
        store: Arc<dyn ObjectStore>,
        signer: Option<Arc<dyn Signer>>,
        prefix: &str,
        scope: &Scope,
        database: &str,
    ) -> Result<Self> {
        validate_database(database)?;
        let prefix = prefix.trim_matches('/');
        if !prefix.is_empty() {
            validate_blob_key(prefix)?;
        }
        // Hex encodes every UTF-8 byte, including slash, percent and colon. The
        // fixed separators therefore cannot be introduced by a tenant name.
        let tenant: String = scope
            .as_str()
            .bytes()
            .flat_map(|byte| {
                [
                    char::from(HEX[usize::from(byte >> 4)]),
                    char::from(HEX[usize::from(byte & 15)]),
                ]
            })
            .collect();
        let prefix = format!(
            "{}{database}/scopes/{tenant}/",
            if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            }
        );
        if prefix.len() >= 1024 {
            return Err(StorageError::invalid_input(
                "s3 tenant/database prefix leaves no room for a blob key",
            ));
        }
        Ok(Self {
            store,
            signer,
            prefix,
        })
    }

    fn path(&self, key: &str) -> Result<Path> {
        validate_blob_key(key)?;
        if key.len() > self.max_key_len() {
            return Err(StorageError::invalid_input(
                "blob key exceeds the scoped s3 key limit",
            ));
        }
        Path::parse(format!("{}{key}", self.prefix))
            .map_err(|_| StorageError::invalid_input("invalid object key"))
    }

    async fn signed(&self, key: &str, ttl: Duration, method: Method) -> Result<String> {
        let path = self.path(key)?;
        if ttl.as_secs() == 0 || ttl > Duration::from_secs(7 * 24 * 60 * 60) {
            return Err(StorageError::invalid_input(
                "presigned URL ttl must be between one second and seven days",
            ));
        }
        let signer = self.signer.as_ref().ok_or_else(|| {
            StorageError::unsupported(Capability::PresignedUrls, "this blob store has no signer")
        })?;
        signer
            .signed_url(method, &path, ttl)
            .await
            .map(|url| url.to_string())
            .map_err(map_error)
    }
}

fn attributes(content_type: Option<&str>) -> Attributes {
    let mut attributes = Attributes::default();
    if let Some(value) = content_type {
        attributes.insert(Attribute::ContentType, value.to_owned().into());
    }
    attributes
}

fn metadata(key: &str, len: u64, attributes: &Attributes) -> BlobMeta {
    BlobMeta {
        key: key.to_owned(),
        len,
        content_type: attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_owned()),
    }
}

fn map_error(error: object_store::Error) -> StorageError {
    let kind = match &error {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
        object_store::Error::Precondition { .. } => ErrorKind::Conflict,
        _ => ErrorKind::Backend,
    };
    // The original transport message can contain credential-bearing URLs.
    // Do not include it in diagnostics.
    drop(error);
    StorageError::new(kind, "object store operation failed")
}

#[async_trait]
impl BlobStore for S3Blobs {
    fn max_key_len(&self) -> usize {
        1024 - self.prefix.len()
    }
    fn capabilities(&self) -> Capabilities {
        let caps = Capabilities::none().with(Capability::StreamingBlobs);
        if self.signer.is_some() {
            caps.with(Capability::PresignedUrls)
        } else {
            caps
        }
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        self.put_stream(
            key,
            Box::pin(std::io::Cursor::new(bytes)),
            len,
            content_type,
        )
        .await
    }

    async fn put_stream(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        let path = self.path(key)?;
        let attrs = attributes(content_type);
        if len <= PART_SIZE as u64 {
            let mut data = Vec::new();
            (&mut reader)
                .take(len + 1)
                .read_to_end(&mut data)
                .await
                .map_err(|_| StorageError::backend("blob reader failed"))?;
            if data.len() as u64 != len {
                return Err(StorageError::invalid_input(
                    "blob reader length differs from declared length",
                ));
            }
            self.store
                .put_opts(
                    &path,
                    Bytes::from(data).into(),
                    PutOptions {
                        attributes: attrs.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(map_error)?;
        } else {
            if len > PART_SIZE as u64 * 10_000 {
                return Err(StorageError::invalid_input(
                    "blob exceeds multipart part limit",
                ));
            }
            let mut upload = self
                .store
                .put_multipart_opts(
                    &path,
                    PutMultipartOptions {
                        attributes: attrs.clone(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(map_error)?;
            let result: Result<()> = async {
                let mut remaining = len;
                while remaining > 0 {
                    let size = usize::try_from(remaining.min(PART_SIZE as u64))
                        .map_err(|_| StorageError::invalid_input("blob part size overflow"))?;
                    let mut part = vec![0; size];
                    reader.read_exact(&mut part).await.map_err(|_| {
                        StorageError::invalid_input("blob reader ended before declared length")
                    })?;
                    upload
                        .put_part(Bytes::from(part).into())
                        .await
                        .map_err(map_error)?;
                    remaining -= size as u64;
                }
                let mut extra = [0];
                if reader
                    .read(&mut extra)
                    .await
                    .map_err(|_| StorageError::backend("blob reader failed"))?
                    != 0
                {
                    return Err(StorageError::invalid_input(
                        "blob reader exceeds declared length",
                    ));
                }
                upload.complete().await.map_err(map_error)?;
                Ok(())
            }
            .await;
            if let Err(error) = result {
                // Lifecycle expiry is still required for abrupt process death or
                // cancellation, where asynchronous cleanup cannot run.
                let _ = upload.abort().await;
                return Err(error);
            }
        }
        Ok(metadata(key, len, &attrs))
    }

    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        let path = self.path(key)?;
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(map_error(error)),
        };
        let meta = metadata(key, result.meta.size, &result.attributes);
        let stream = result
            .into_stream()
            .map_err(|_| std::io::Error::other("object store read failed"));
        Ok(Some(BlobStream {
            meta,
            reader: Box::pin(StreamReader::new(stream)),
        }))
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        let Some(mut stream) = self.get_stream(key).await? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        stream
            .reader
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| StorageError::backend("object store read failed"))?;
        Ok(Some(Blob {
            meta: stream.meta,
            bytes,
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        let result = match self
            .store
            .get_opts(
                &self.path(key)?,
                object_store::GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(map_error(error)),
        };
        Ok(Some(metadata(key, result.meta.size, &result.attributes)))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        if range.start > range.end {
            return Err(StorageError::invalid_input(
                "blob range start is after its end",
            ));
        }
        let Some(meta) = self.head(key).await? else {
            return Ok(None);
        };
        let range = range.start.min(meta.len)..range.end.min(meta.len);
        if range.is_empty() {
            return Ok(Some(Vec::new()));
        }
        self.store
            .get_range(&path, range)
            .await
            .map(|b| Some(b.to_vec()))
            .map_err(map_error)
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        let path = self.path(key)?;
        if self.head(key).await?.is_none() {
            return Ok(false);
        }
        self.store.delete(&path).await.map_err(map_error)?;
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        // A partial key prefix need not itself be a valid full key.
        let root = Path::parse(self.prefix.trim_end_matches('/'))
            .map_err(|_| StorageError::invalid_input("invalid scope prefix"))?;
        let mut objects = self.store.list(Some(&root));
        let mut metas = Vec::new();
        while let Some(object) = objects.try_next().await.map_err(map_error)? {
            if let Some(key) = object.location.as_ref().strip_prefix(&self.prefix)
                && key.starts_with(prefix)
            {
                // S3 ListObjects omits content-type, so fetch HEAD for the
                // port's complete metadata contract.
                if let Some(meta) = self.head(key).await? {
                    metas.push(meta);
                }
            }
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        self.signed(key, ttl, Method::GET).await
    }

    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        self.signed(key, ttl, Method::PUT).await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
