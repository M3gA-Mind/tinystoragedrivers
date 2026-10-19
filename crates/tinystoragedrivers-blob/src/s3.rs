//! S3-compatible scoped blob adapter using bounded multipart writes.

use std::{collections::BTreeMap, io, ops::Range, sync::Arc, time::{Duration, SystemTime, UNIX_EPOCH}};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{TryStreamExt, io::AsyncReadExt};
use http::Method;
use object_store::{Attribute, Attributes, ObjectStore, PutMultipartOptions, PutOptions, path::Path, signer::Signer};
use tinystoragedrivers_api::{BlobReader, BlobStore, BlobStream};
use tinystoragedrivers_connect::{S3Connection, S3ConnectionConfig};
use tinystoragedrivers_types::{Blob, BlobListEntry, BlobListPage, BlobMeta, Capabilities, Capability, ErrorKind, Result, Scope, StorageError, MAX_BLOB_LIST_PAGE_ITEMS};

use crate::key::{validate_key, validate_prefix, MAX_KEY_LEN};

const PART_SIZE: usize = 8 * 1024 * 1024;
const WRITTEN_AT: &str = "tsd-written-at-ms";
const EXPIRES_AT: &str = "tsd-expires-at-ms";

/// A scope-bound adapter over the shared configured S3 object store.
#[derive(Debug, Clone)]
pub struct S3BlobStore {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
    prefix: String,
    max_key_len: usize,
}

impl S3BlobStore {
    /// Connect a scope using a configured storage URL and AWS credentials.
    ///
    /// # Errors
    /// Returns a configuration error when the URL or client cannot be built.
    pub fn open(url: &str, scope: Scope) -> Result<Self> {
        let config = S3ConnectionConfig::parse(url)?;
        let connection = S3Connection::open(&config)?;
        Self::new(connection, scope)
    }

    /// Bind a shared S3 connection to one scope.
    ///
    /// # Errors
    /// Returns `InvalidInput` when the effective key prefix exceeds the
    /// object-store key limit.
    pub fn new(connection: S3Connection, scope: Scope) -> Result<Self> {
        let tenant: String = scope.as_str().bytes().map(|byte| format!("{byte:02x}")).collect();
        let prefix = if connection.prefix().is_empty() {
            format!("scopes/{tenant}/")
        } else {
            format!("{}/scopes/{tenant}/", connection.prefix())
        };
        let max_key_len = 1024_usize.saturating_sub(prefix.len());
        if max_key_len == 0 { return Err(StorageError::invalid_input("S3 prefix leaves no room for blob keys")); }
        Ok(Self { store: connection.store(), signer: connection.signer(), prefix, max_key_len: max_key_len.min(MAX_KEY_LEN) })
    }

    /// Inject an object store for tests or custom object-store deployments.
    ///
    /// # Errors
    /// Returns `InvalidInput` when the prefix leaves no key capacity.
    pub fn with_store(store: Arc<dyn ObjectStore>, signer: Option<Arc<dyn Signer>>, prefix: &str, scope: Scope) -> Result<Self> {
        let connection = S3Connection::with_store(store, signer, "test", prefix);
        Self::new(connection, scope)
    }

    fn path(&self, key: &str) -> Result<Path> {
        validate_key(key)?;
        if key.len() > self.max_key_len { return Err(StorageError::invalid_input("blob key exceeds S3 key limit")); }
        Path::parse(format!("{}{key}", self.prefix)).map_err(|_| StorageError::invalid_input("invalid S3 object key"))
    }

    async fn signed(&self, key: &str, ttl: Duration, method: Method) -> Result<String> {
        if ttl.is_zero() || ttl > Duration::from_secs(7 * 24 * 60 * 60) { return Err(StorageError::invalid_input("presign lifetime must be between one second and seven days")); }
        let signer = self.signer.as_ref().ok_or_else(|| StorageError::unsupported(Capability::PresignedUrls, "S3 connection has no signer"))?;
        signer.signed_url(method, &self.path(key)?, ttl).await.map(|url| url.to_string()).map_err(map_error)
    }

    async fn upload(&self, key: &str, mut reader: BlobReader, len: u64, content_type: Option<&str>, ttl: Option<Duration>) -> Result<BlobMeta> {
        if len > (PART_SIZE as u64).saturating_mul(10_000) { return Err(StorageError::invalid_input("blob exceeds multipart part limit")); }
        let path = self.path(key)?;
        let now = now_ms();
        let expires = ttl.map(|ttl| now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)));
        let attrs = attributes(content_type, Some(now), expires);
        if len <= PART_SIZE as u64 {
            let mut bytes = Vec::with_capacity(usize::try_from(len).unwrap_or(0));
            while let Some(chunk) = reader.try_next().await? {
                if (bytes.len() as u64).saturating_add(chunk.len() as u64) > len { return Err(StorageError::invalid_input("blob stream exceeds declared length")); }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() as u64 != len { return Err(StorageError::invalid_input("blob stream is shorter than declared length")); }
            self.store.put_opts(&path, Bytes::from(bytes).into(), PutOptions { attributes: attrs.clone(), ..Default::default() }).await.map_err(map_error)?;
        } else {
            let mut upload = self.store.put_multipart_opts(&path, PutMultipartOptions { attributes: attrs.clone(), ..Default::default() }).await.map_err(map_error)?;
            let result: Result<()> = async {
                let input = reader.map_err(|error| io::Error::other(error.to_string()));
                let mut input = futures_util::io::StreamReader::new(input);
                let mut remaining = len;
                while remaining > 0 {
                    let size = usize::try_from(remaining.min(PART_SIZE as u64)).map_err(|_| StorageError::invalid_input("blob part size overflow"))?;
                    let mut chunk = vec![0; size];
                    input.read_exact(&mut chunk).await.map_err(reader_error)?;
                    upload.put_part(Bytes::from(chunk).into()).await.map_err(map_error)?;
                    remaining -= size as u64;
                }
                let mut extra = [0_u8; 1];
                if input.read(&mut extra).await.map_err(reader_error)? != 0 { return Err(StorageError::invalid_input("blob stream exceeds declared length")); }
                upload.complete().await.map_err(map_error)?;
                Ok(())
            }.await;
            if let Err(error) = result { let _ = upload.abort().await; return Err(error); }
        }
        Ok(BlobMeta { key: key.to_owned(), len, content_type: content_type.map(str::to_owned) })
    }

    async fn attributes_for(&self, path: &Path) -> Result<Option<(object_store::ObjectMeta, Attributes)>> {
        match self.store.get_opts(path, object_store::GetOptions { head: true, ..Default::default() }).await {
            Ok(result) => Ok(Some((result.meta, result.attributes))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(map_error(error)),
        }
    }
}

#[async_trait]
impl BlobStore for S3BlobStore {
    fn capabilities(&self) -> Capabilities {
        let caps = Capabilities::none().with(Capability::Blobs).with(Capability::BlobTtl).with(Capability::StreamingBlobs).with(Capability::BlobListing);
        if self.signer.is_some() { caps.with(Capability::PresignedUrls) } else { caps }
    }
    fn max_key_len(&self) -> usize { self.max_key_len }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        let reader: BlobReader = Box::pin(futures_util::stream::once(async move { Ok(Bytes::from(bytes)) }));
        self.upload(key, reader, len, content_type, None).await
    }
    async fn put_expiring(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>, ttl: Duration) -> Result<BlobMeta> {
        let len = bytes.len() as u64;
        let reader: BlobReader = Box::pin(futures_util::stream::once(async move { Ok(Bytes::from(bytes)) }));
        self.upload(key, reader, len, content_type, Some(ttl)).await
    }
    async fn put_stream(&self, key: &str, reader: BlobReader, len: u64, content_type: Option<&str>) -> Result<BlobMeta> {
        self.upload(key, reader, len, content_type, None).await
    }
    async fn put_stream_expiring(&self, key: &str, reader: BlobReader, len: u64, content_type: Option<&str>, ttl: Duration) -> Result<BlobMeta> {
        self.upload(key, reader, len, content_type, Some(ttl)).await
    }
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        let path = self.path(key)?;
        let Some(result) = self.attributes_for(&path).await? else { return Ok(None) };
        if is_expired(&result.1, now_ms()) { return Ok(None); }
        let meta = object_meta(key, result.0.size, &result.1);
        let content = self.store.get(&path).await.map_err(map_error)?;
        let reader = Box::pin(content.into_stream().map_err(map_error));
        Ok(Some(BlobStream { meta, reader }))
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        let Some(mut stream) = self.get_stream(key).await? else { return Ok(None) };
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.reader.try_next().await? { bytes.extend_from_slice(&chunk); }
        Ok(Some(Blob { meta: stream.meta, bytes }))
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        if range.start > range.end { return Err(StorageError::invalid_input("blob range starts after it ends")); }
        let path = self.path(key)?;
        let Some((meta, attributes)) = self.attributes_for(&path).await? else { return Ok(None) };
        if is_expired(&attributes, now_ms()) { return Ok(None); }
        let start = range.start.min(meta.size);
        let end = range.end.min(meta.size);
        self.store.get_range(&path, start..end).await.map(|bytes| Some(bytes.to_vec())).map_err(map_error)
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        let path = self.path(key)?;
        let Some((meta, attributes)) = self.attributes_for(&path).await? else { return Ok(None) };
        if is_expired(&attributes, now_ms()) { return Ok(None); }
        Ok(Some(object_meta(key, meta.size, &attributes)))
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        let path = self.path(key)?;
        let found = self.attributes_for(&path).await?;
        let existed = found.is_some();
        let live = found.is_some_and(|(_, attrs)| !is_expired(&attrs, now_ms()));
        match self.store.delete(&path).await {
            Ok(()) => Ok(existed && live),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(map_error(error)),
        }
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_prefix(prefix, None)?;
        let mut entries = Vec::new();
        let mut cursor = None;
        loop {
            let page = self.list_page(prefix, cursor.as_deref(), MAX_BLOB_LIST_PAGE_ITEMS).await?;
            for entry in page.items {
                if let Some(meta) = self.head(&entry.meta.key).await? { entries.push(meta); }
            }
            cursor = page.next_after;
            if cursor.is_none() { break; }
        }
        Ok(entries)
    }
    async fn list_page(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<BlobListPage> {
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS { return Err(StorageError::invalid_input("invalid blob listing page size")); }
        validate_prefix(prefix, after)?;
        let root = Path::parse(self.prefix.trim_end_matches('/')).map_err(|_| StorageError::invalid_input("invalid S3 scope prefix"))?;
        let mut listing = self.store.list(Some(&root));
        let mut keys = BTreeMap::new();
        while let Some(object) = listing.try_next().await.map_err(map_error)? {
            let full = object.location.as_ref();
            let Some(key) = full.strip_prefix(&self.prefix) else { continue };
            if key.starts_with(prefix) && after.is_none_or(|cursor| key.as_bytes() > cursor.as_bytes()) {
                keys.insert(key.to_owned(), ());
                if keys.len() > limit + 1 { keys.pop_last(); }
            }
        }
        let more = keys.len() > limit;
        let mut items = Vec::with_capacity(keys.len());
        for (key, _) in keys.into_iter().take(limit) {
            let path = self.path(&key)?;
            if let Some((meta, attrs)) = self.attributes_for(&path).await? {
                let age = attrs.get(&Attribute::Metadata(WRITTEN_AT.into())).and_then(|v| v.as_ref().parse::<u64>().ok());
                items.push(BlobListEntry { meta: object_meta(&key, meta.size, &attrs), written_at_ms: age });
            }
        }
        let next_after = if more { items.last().map(|entry| entry.meta.key.clone()) } else { None };
        Ok(BlobListPage { items, next_after })
    }
    async fn sweep_expired(&self) -> Result<u64> {
        let root = Path::parse(self.prefix.trim_end_matches('/')).map_err(|_| StorageError::invalid_input("invalid S3 scope prefix"))?;
        let mut listing = self.store.list(Some(&root));
        let mut deleted = 0;
        while let Some(object) = listing.try_next().await.map_err(map_error)? {
            if object.location.as_ref().strip_prefix(&self.prefix).is_none() { continue; }
            let Some((_, attributes)) = self.attributes_for(&object.location).await? else { continue };
            if !is_expired(&attributes, now_ms()) { continue; }
            let Some((_, latest)) = self.attributes_for(&object.location).await? else { continue };
            if is_expired(&latest, now_ms()) {
                self.store.delete(&object.location).await.map_err(map_error)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> { self.signed(key, ttl, Method::GET).await }
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> { self.signed(key, ttl, Method::PUT).await }
}

fn attributes(content_type: Option<&str>, written_at: Option<u64>, expires_at: Option<u64>) -> Attributes {
    let mut attributes = Attributes::default();
    if let Some(value) = content_type { attributes.insert(Attribute::ContentType, value.to_owned().into()); }
    if let Some(age) = written_at { attributes.insert(Attribute::Metadata(WRITTEN_AT.into()), age.to_string().into()); }
    if let Some(expiry) = expires_at { attributes.insert(Attribute::Metadata(EXPIRES_AT.into()), expiry.to_string().into()); }
    attributes
}

fn is_expired(attributes: &Attributes, now: u64) -> bool {
    attributes.get(&Attribute::Metadata(EXPIRES_AT.into())).and_then(|value| value.as_ref().parse::<u64>().ok()).is_some_and(|expiry| expiry <= now)
}

fn reader_error(error: io::Error) -> StorageError {
    let kind = if error.kind() == io::ErrorKind::UnexpectedEof { ErrorKind::InvalidInput } else { ErrorKind::Backend };
    StorageError::new(kind, "S3 blob input stream failed").with_source(error)
}

fn object_meta(key: &str, len: usize, attributes: &Attributes) -> BlobMeta {
    BlobMeta { key: key.to_owned(), len: len as u64, content_type: attributes.get(&Attribute::ContentType).map(|value| value.as_ref().to_owned()) }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

fn map_error(error: object_store::Error) -> StorageError {
    let kind = match error {
        object_store::Error::NotFound { .. } => ErrorKind::NotFound,
        object_store::Error::AlreadyExists { .. } => ErrorKind::AlreadyExists,
        object_store::Error::Precondition { .. } => ErrorKind::Conflict,
        _ => ErrorKind::Backend,
    };
    StorageError::new(kind, "S3 object operation failed")
}

#[cfg(test)]
#[path = "s3_tests.rs"]
mod tests;
