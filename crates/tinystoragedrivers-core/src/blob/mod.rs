//! The blob port: opaque byte objects addressed by key.
//!
//! Attachments, exported artifacts and large transcript bodies are blobs. Keys
//! are `/`-separated paths so [`BlobStore::list`] can enumerate a prefix, but no
//! driver promises directories exist.

use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};

use crate::Capability;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Result, StorageError};

/// The format limit for a blob key, in bytes.
/// Drivers may announce a smaller physical limit with [`BlobStore::max_key_len`].
pub const MAX_BLOB_KEY_LEN: usize = 1024;

/// What is known about a blob without reading it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    /// The blob's key.
    pub key: String,
    /// Size in bytes.
    pub len: u64,
    /// MIME type, when the writer gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// A blob's metadata and bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// Metadata.
    pub meta: BlobMeta,
    /// Contents.
    pub bytes: Vec<u8>,
}

/// An owned asynchronous blob reader. Drivers may deliver bytes incrementally.
pub type BlobReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Metadata and an incremental reader for a blob.
pub struct BlobStream {
    /// Metadata obtained before the body is consumed.
    pub meta: BlobMeta,
    /// Contents; a read failure is reported as an I/O error.
    pub reader: BlobReader,
}

impl std::fmt::Debug for BlobStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStream")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

/// Opaque byte objects, bound to one scope.
#[async_trait]
pub trait BlobStore: Send + Sync + std::fmt::Debug {
    /// Maximum logical key length in UTF-8 bytes for this scoped handle.
    ///
    /// Object stores count their tenant/database prefix against a physical key
    /// limit, so this may be smaller than the general format limit.
    fn max_key_len(&self) -> usize {
        MAX_BLOB_KEY_LEN
    }

    /// Store `bytes` under `key`, replacing any previous blob.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta>;

    /// Store exactly the declared bytes, replacing the old blob only on success.
    ///
    /// Object-store drivers override this to upload bounded chunks. The default
    /// buffers the body for existing drivers; use a streaming driver for large objects.
    /// A declared length mismatch fails before publishing the object.
    ///
    /// # Errors
    ///
    /// Invalid key or length, reader failure, or a backend error.
    async fn put_stream(
        &self,
        key: &str,
        mut reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        validate_blob_key(key)?;
        let mut bytes = Vec::new();
        (&mut reader)
            .take(len.saturating_add(1))
            .read_to_end(&mut bytes)
            .await
            .map_err(|error| StorageError::backend("blob reader failed").with_source(error))?;
        if bytes.len() as u64 != len {
            return Err(StorageError::invalid_input(
                "blob reader length differs from declared length",
            ));
        }
        self.put(key, bytes, content_type).await
    }

    /// Read a blob incrementally. The default wraps the existing buffered getter.
    ///
    /// # Errors
    ///
    /// An invalid key or backend error. Later reader failures are I/O errors.
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        Ok(self.get(key).await?.map(|blob| BlobStream {
            meta: blob.meta,
            reader: Box::pin(std::io::Cursor::new(blob.bytes)),
        }))
    }

    /// Optional abilities of this blob handle.
    fn capabilities(&self) -> crate::Capabilities {
        crate::Capabilities::none()
    }

    /// A bearer URL for a direct GET, valid for the given duration.
    ///
    /// URLs must be treated as credentials. Presigning does not check existence.
    ///
    /// # Errors
    ///
    /// Unsupported when this handle lacks [`Capability::PresignedUrls`], invalid key/TTL,
    /// or a backend error.
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        let _ = (key, ttl);
        Err(StorageError::unsupported(
            Capability::PresignedUrls,
            "this blob driver cannot presign URLs",
        ))
    }

    /// A bearer URL for a direct PUT, valid for the given duration.
    ///
    /// The URL permits replacing the addressed object without a document fence.
    ///
    /// # Errors
    ///
    /// Unsupported when this handle lacks [`Capability::PresignedUrls`], invalid key/TTL,
    /// or a backend error.
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        let _ = (key, ttl);
        Err(StorageError::unsupported(
            Capability::PresignedUrls,
            "this blob driver cannot presign URLs",
        ))
    }

    /// Read a whole blob.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn get(&self, key: &str) -> Result<Option<Blob>>;

    /// Read the bytes in `range`, clamped to the blob's length.
    ///
    /// # Errors
    ///
    /// An invalid key, an inverted range, or a backend error.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>>;

    /// Read a blob's metadata.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>>;

    /// Remove a blob and report whether it existed.
    ///
    /// # Errors
    ///
    /// An invalid key, or a backend error.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// Metadata of every blob whose key starts with `prefix`, sorted by key.
    ///
    /// # Errors
    ///
    /// A backend error.
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>>;
}

/// Validate a blob key: 1 to [`MAX_BLOB_KEY_LEN`] bytes, no NUL, no empty,
/// `.` or `..` path segment, and no leading `/`.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput).
pub fn validate_blob_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_BLOB_KEY_LEN || key.contains('\0') {
        return Err(StorageError::invalid_input(format!(
            "blob key must be 1 to {MAX_BLOB_KEY_LEN} bytes without NUL"
        )));
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(StorageError::invalid_input(
            "blob key has an empty, `.` or `..` segment",
        ));
    }
    Ok(())
}

/// Clamp `range` to a blob of `len` bytes.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when the range
/// is inverted.
pub fn clamp_range(range: &Range<u64>, len: usize) -> Result<Range<usize>> {
    if range.start > range.end {
        return Err(StorageError::invalid_input(
            "blob range start is after its end",
        ));
    }
    let clamp = |at: u64| usize::try_from(at).map_or(len, |at| at.min(len));
    Ok(clamp(range.start)..clamp(range.end))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
