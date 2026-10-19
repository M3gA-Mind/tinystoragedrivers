//! Opaque byte object port with runtime-neutral streaming.

use std::ops::Range;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use tinystoragedrivers_types::{
    Blob, BlobListPage, BlobMeta, Capabilities, Capability, MAX_BLOB_LIST_PAGE_ITEMS, Result,
    StorageError, TreeEntry, TreeFile,
};

/// An owned stream of blob chunks. Stream failures use the storage error vocabulary.
pub type BlobReader = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// Blob metadata and an incremental content stream.
pub struct BlobStream {
    /// Metadata available before consuming the body.
    pub meta: BlobMeta,
    /// The content bytes.
    pub reader: BlobReader,
}

impl std::fmt::Debug for BlobStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStream")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

/// Opaque byte objects bound to one scope.
#[async_trait]
pub trait BlobStore: Send + Sync + std::fmt::Debug {
    /// Optional abilities provided by this blob handle.
    fn capabilities(&self) -> Capabilities;

    /// Maximum logical key length in UTF-8 bytes.
    fn max_key_len(&self) -> usize;

    /// Store bytes, replacing any previous blob.
    ///
    /// # Errors
    /// Returns an error for an invalid key or a backend failure.
    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta>;

    /// Store exactly `len` bytes, publishing only after a successful complete read.
    ///
    /// # Errors
    /// Returns an error for an invalid key, a length mismatch, a stream failure,
    /// or a backend failure.
    async fn put_stream(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        let bytes = collect_exact(reader, len).await?;
        self.put(key, bytes, content_type).await
    }

    /// Store bytes with a lifetime. Unsupported unless `BlobTtl` is present.
    ///
    /// # Errors
    /// Returns `Unsupported(BlobTtl)` when expiry is unavailable, or a backend
    /// error.
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        let _ = (key, bytes, content_type, ttl);
        Err(StorageError::unsupported(
            Capability::BlobTtl,
            "blob expiry is unsupported",
        ))
    }

    /// Store exactly `len` streamed bytes with a lifetime.
    ///
    /// # Errors
    /// Returns an error for a length mismatch, stream failure, unsupported blob
    /// expiry, or a backend failure.
    async fn put_stream_expiring(
        &self,
        key: &str,
        reader: BlobReader,
        len: u64,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        if !self.capabilities().contains(Capability::BlobTtl) {
            return Err(StorageError::unsupported(
                Capability::BlobTtl,
                "blob expiry is unsupported",
            ));
        }
        let bytes = collect_exact(reader, len).await?;
        self.put_expiring(key, bytes, content_type, ttl).await
    }

    /// Read a complete object.
    ///
    /// # Errors
    /// Returns an error for an invalid key or a backend failure.
    async fn get(&self, key: &str) -> Result<Option<Blob>>;

    /// Read a complete object as a stream; the default emits one chunk.
    ///
    /// # Errors
    /// Returns the error from [`Self::get`].
    async fn get_stream(&self, key: &str) -> Result<Option<BlobStream>> {
        let Some(blob) = self.get(key).await? else {
            return Ok(None);
        };
        let meta = blob.meta;
        let chunk = Bytes::from(blob.bytes);
        let reader = Box::pin(OnceChunk {
            bytes: chunk,
            yielded: false,
        });
        Ok(Some(BlobStream { meta, reader }))
    }

    /// Read a byte range, with the same half-open range semantics as `Range`.
    ///
    /// # Errors
    /// Returns an error for an invalid key or a backend failure.
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>>;

    /// Read metadata without consuming the body.
    ///
    /// # Errors
    /// Returns an error for an invalid key or a backend failure.
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>>;

    /// Remove a blob.
    ///
    /// # Errors
    /// Returns an error for an invalid key or a backend failure.
    async fn delete(&self, key: &str) -> Result<bool>;

    /// List blobs whose keys start with `prefix`.
    ///
    /// # Errors
    /// Returns an error for an invalid prefix or a backend failure.
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>>;

    /// List physical blobs with bytewise key ordering and an exclusive cursor.
    /// Expired physical objects are included; blobs without recorded age have
    /// `written_at_ms: None`. The maximum page size is
    /// [`MAX_BLOB_LIST_PAGE_ITEMS`].
    ///
    /// # Errors
    /// Returns `InvalidInput` for a zero or oversized page and
    /// `Unsupported(BlobListing)` when pagination is unavailable.
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        let _ = (prefix, after);
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS {
            return Err(StorageError::invalid_input(
                "invalid blob listing page size",
            ));
        }
        Err(StorageError::unsupported(
            Capability::BlobListing,
            "paged blob listing is unsupported",
        ))
    }

    /// Reclaim expired blobs where supported. Defaults to zero.
    ///
    /// # Errors
    /// Returns an error if the backend cannot complete reclamation.
    async fn sweep_expired(&self) -> Result<u64> {
        Ok(0)
    }

    /// Create a short-lived read URL when `PresignedUrls` is available.
    ///
    /// # Errors
    /// Returns `Unsupported(PresignedUrls)` when URL signing is unavailable, or
    /// a backend error.
    async fn presign_get(&self, key: &str, ttl: Duration) -> Result<String> {
        let _ = (key, ttl);
        Err(StorageError::unsupported(
            Capability::PresignedUrls,
            "presigned URLs are unsupported",
        ))
    }

    /// Create a short-lived write URL when `PresignedUrls` is available.
    ///
    /// # Errors
    /// Returns `Unsupported(PresignedUrls)` when URL signing is unavailable, or
    /// a backend error.
    async fn presign_put(&self, key: &str, ttl: Duration) -> Result<String> {
        let _ = (key, ttl);
        Err(StorageError::unsupported(
            Capability::PresignedUrls,
            "presigned URLs are unsupported",
        ))
    }

    /// List the immediate directory children from the flat blob namespace.
    ///
    /// # Errors
    /// Returns an error from [`Self::list`].
    async fn list_dir(&self, dir: &str) -> Result<Vec<TreeEntry>> {
        if !dir.is_empty() {
            validate_tree_key(dir, self.max_key_len())?;
        }
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let listed = self.list(&prefix).await?;
        let mut entries = std::collections::BTreeMap::new();
        for meta in listed {
            let Some(rest) = meta.key.strip_prefix(&prefix) else {
                continue;
            };
            if let Some((name, _)) = rest.split_once('/') {
                entries
                    .entry((name.to_owned(), true))
                    .or_insert_with(|| TreeEntry::Dir {
                        name: name.to_owned(),
                        prefix: format!("{prefix}{name}"),
                    });
            } else {
                entries.insert(
                    (rest.to_owned(), false),
                    TreeEntry::Blob {
                        name: rest.to_owned(),
                        meta,
                    },
                );
            }
        }
        Ok(entries.into_values().collect())
    }

    /// Replace a directory tree with the supplied files.
    ///
    /// # Errors
    /// Returns the first error from a blob write.
    async fn put_tree(&self, dir: &str, files: Vec<TreeFile>) -> Result<()> {
        if !dir.is_empty() {
            validate_tree_key(dir, self.max_key_len())?;
        }
        let base = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let mut paths = std::collections::HashSet::with_capacity(files.len());
        let mut entries = Vec::with_capacity(files.len());
        for file in files {
            let key = format!("{base}{}", file.path);
            validate_tree_key(&key, self.max_key_len())?;
            if !paths.insert(key.clone()) {
                return Err(StorageError::invalid_input(
                    "tree contains a duplicate blob key",
                ));
            }
            entries.push((key, file));
        }
        for (key, file) in entries {
            self.put(&key, file.bytes, file.content_type.as_deref())
                .await?;
        }
        Ok(())
    }

    /// Delete all objects beneath a directory prefix.
    ///
    /// # Errors
    /// Returns an error from listing or deleting a blob.
    async fn delete_tree(&self, dir: &str) -> Result<u64> {
        if dir.is_empty() {
            return Err(StorageError::invalid_input(
                "delete_tree requires a directory",
            ));
        }
        validate_tree_key(dir, self.max_key_len())?;
        let prefix = if dir.is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let listed = self.list(&prefix).await?;
        let mut deleted = 0;
        for meta in listed {
            deleted += u64::from(self.delete(&meta.key).await?);
        }
        Ok(deleted)
    }
}

async fn collect_exact(mut reader: BlobReader, len: u64) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut total = 0_u64;
    while let Some(chunk) = std::future::poll_fn(|cx| reader.as_mut().poll_next(cx)).await {
        let chunk = chunk?;
        total = total.saturating_add(chunk.len() as u64);
        if total > len {
            return Err(StorageError::invalid_input(
                "blob stream exceeds declared length",
            ));
        }
        output.extend_from_slice(&chunk);
    }
    if total != len {
        return Err(StorageError::invalid_input(
            "blob stream is shorter than declared length",
        ));
    }
    Ok(output)
}

fn validate_tree_key(key: &str, max_key_len: usize) -> Result<()> {
    if key.is_empty() || key.len() > max_key_len || key.contains('\0') {
        return Err(StorageError::invalid_input(
            "invalid tree blob key length or NUL byte",
        ));
    }
    if key
        .split('/')
        .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(StorageError::invalid_input(
            "invalid tree blob key path segment",
        ));
    }
    Ok(())
}

struct OnceChunk {
    bytes: Bytes,
    yielded: bool,
}

impl Stream for OnceChunk {
    type Item = Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.yielded {
            std::task::Poll::Ready(None)
        } else {
            this.yielded = true;
            std::task::Poll::Ready(Some(Ok(this.bytes.clone())))
        }
    }
}

#[cfg(test)]
#[path = "blob_tests.rs"]
mod tests;
