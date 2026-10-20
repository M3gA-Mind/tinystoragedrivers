//! `Blobs` selectors, including the two streamed transfers.
use std::time::Duration;

use bytes::Bytes;
use futures_core::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::task::{Context, Poll};
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_bus::{MAX_INLINE_BLOB_BYTES, MAX_PAGE_ITEMS, MAX_PRESIGN_TTL_SECS};
use tinystoragedrivers_types::StorageError;
use tinystoragedrivers_types::{BlobMeta, Capability};

use super::{Reply, StorageServer, decode, encode, invalid, native};
use crate::transport::BlobBody;

pub(crate) fn info(meta: BlobMeta) -> wire::BlobInfo {
    wire::BlobInfo {
        key: meta.key,
        len: meta.len,
        content_type: meta.content_type,
    }
}

fn validate_inline_blob(meta: &BlobMeta, bytes: &[u8]) -> Result<(), wire::StorageFailure> {
    if meta.len > MAX_INLINE_BLOB_BYTES as u64
        || bytes.len() > MAX_INLINE_BLOB_BYTES
        || bytes.len() as u64 != meta.len
    {
        return Err(invalid());
    }
    Ok(())
}

/// Fails a read that would take more than the declared number of bytes, so a
/// body longer than `len` is an error rather than a silent truncation.
struct Exact {
    inner: BlobBody,
    remaining: u64,
    done: bool,
}
impl Stream for Exact {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) if self.remaining == 0 => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(Some(Err(StorageError::invalid_input(
                    "blob stream is shorter than declared length",
                ))))
            }
            Poll::Ready(Some(Err(error))) => {
                self.done = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(bytes))) if bytes.len() as u64 > self.remaining => {
                self.done = true;
                Poll::Ready(Some(Err(StorageError::invalid_input(
                    "blob stream exceeds declared length",
                ))))
            }
            Poll::Ready(Some(Ok(bytes))) => {
                self.remaining -= bytes.len() as u64;
                Poll::Ready(Some(Ok(bytes)))
            }
        }
    }
}

impl StorageServer {
    #[allow(
        clippy::too_many_lines,
        reason = "selector flow validates each inline and streamed operation in one place"
    )]
    pub(super) async fn blobs(&self, selector: &str, request: Value) -> Reply {
        let blobs = self.storage.blobs();
        match selector {
            "Put" => {
                let r: wire::PutBlobRequest = decode(request)?;
                if r.bytes.len() > MAX_INLINE_BLOB_BYTES {
                    return Err(invalid());
                }
                let content_type = r.content_type.as_deref();
                let meta = match r.ttl_ms {
                    None => native(blobs.put(&r.key, r.bytes, content_type).await)?,
                    Some(ttl_ms) => native(
                        blobs
                            .put_expiring(
                                &r.key,
                                r.bytes,
                                content_type,
                                Duration::from_millis(ttl_ms),
                            )
                            .await,
                    )?,
                };
                encode(&info(meta))
            }
            "Get" => {
                let r: wire::BlobKeyRequest = decode(request)?;
                let Some(meta) = native(blobs.head(&r.key).await)? else {
                    return encode(&None::<wire::BlobBody>);
                };
                if meta.len > MAX_INLINE_BLOB_BYTES as u64 {
                    return Err(invalid());
                }
                let found = native(blobs.get(&r.key).await)?;
                if let Some(blob) = &found {
                    validate_inline_blob(&blob.meta, &blob.bytes)?;
                    // `head` and `get` are separate backend calls. A replace
                    // between them must not let a now-oversized body escape
                    // the inline frame cap.
                }
                encode(&found.map(|blob| wire::BlobBody {
                    meta: info(blob.meta),
                    bytes: blob.bytes,
                }))
            }
            "GetRange" => {
                let r: wire::GetRangeRequest = decode(request)?;
                if r.end < r.start || r.end - r.start > MAX_INLINE_BLOB_BYTES as u64 {
                    return Err(invalid());
                }
                encode(&native(blobs.get_range(&r.key, r.start..r.end).await)?)
            }
            "Head" => {
                let r: wire::BlobKeyRequest = decode(request)?;
                encode(&native(blobs.head(&r.key).await)?.map(info))
            }
            "Delete" => {
                let r: wire::BlobKeyRequest = decode(request)?;
                encode(&native(blobs.delete(&r.key).await)?)
            }
            "List" => {
                let r: wire::ListBlobsRequest = decode(request)?;
                let listed = native(blobs.list(&r.prefix).await)?;
                if listed.len() > MAX_PAGE_ITEMS {
                    return Err(invalid());
                }
                encode(&listed.into_iter().map(info).collect::<Vec<_>>())
            }
            "ListPage" => {
                let r: wire::ListBlobsPageRequest = decode(request)?;
                if r.limit == 0 || r.limit > tinystoragedrivers_types::MAX_BLOB_LIST_PAGE_ITEMS {
                    return Err(invalid());
                }
                let page = native(
                    blobs
                        .list_page(&r.prefix, r.after.as_deref(), r.limit)
                        .await,
                )?;
                if page.items.len() > r.limit
                    || page.items.len() > tinystoragedrivers_types::MAX_BLOB_LIST_PAGE_ITEMS
                {
                    return Err(invalid());
                }
                let value = encode(&page)?;
                if value.to_string().len() > wire::MAX_INLINE_BYTES {
                    return Err(invalid());
                }
                Ok(value)
            }
            "PresignGet" | "PresignPut" => {
                let r: wire::PresignRequest = decode(request)?;
                if r.ttl_secs == 0 || r.ttl_secs > MAX_PRESIGN_TTL_SECS {
                    return Err(invalid());
                }
                let ttl = Duration::from_secs(r.ttl_secs);
                let url = if selector == "PresignGet" {
                    native(blobs.presign_get(&r.key, ttl).await)?
                } else {
                    native(blobs.presign_put(&r.key, ttl).await)?
                };
                encode(&wire::PresignedUrl { url })
            }
            "Capabilities" => {
                if !request.is_null() {
                    return Err(invalid());
                }
                encode(&wire::BlobCapabilities {
                    presigned_urls: blobs.capabilities().contains(Capability::PresignedUrls),
                    ttl: blobs.capabilities().contains(Capability::BlobTtl),
                    max_key_len: blobs.max_key_len(),
                })
            }
            // Streamed transfers carry a body, so they are not request/reply.
            _ => Err(invalid()),
        }
    }

    pub(super) async fn put_stream(
        &self,
        head: wire::PutBlobStreamRequest,
        body: BlobBody,
    ) -> Result<wire::BlobInfo, wire::StorageFailure> {
        let reader: BlobBody = Box::pin(Exact {
            inner: body,
            remaining: head.len,
            done: false,
        });
        let blobs = self.storage.blobs();
        let content_type = head.content_type.as_deref();
        let meta = match head.ttl_ms {
            None => native(
                blobs
                    .put_stream(&head.key, reader, head.len, content_type)
                    .await,
            )?,
            Some(ttl_ms) => native(
                blobs
                    .put_stream_expiring(
                        &head.key,
                        reader,
                        head.len,
                        content_type,
                        Duration::from_millis(ttl_ms),
                    )
                    .await,
            )?,
        };
        Ok(info(meta))
    }

    pub(super) async fn get_stream(
        &self,
        request: wire::BlobKeyRequest,
    ) -> Result<Option<(wire::BlobStreamHead, BlobBody)>, wire::StorageFailure> {
        let found = native(self.storage.blobs().get_stream(&request.key).await)?;
        Ok(found.map(|stream| {
            (
                wire::BlobStreamHead {
                    meta: info(stream.meta),
                },
                stream.reader,
            )
        }))
    }
}

#[cfg(test)]
#[path = "blobs_tests.rs"]
mod tests;
