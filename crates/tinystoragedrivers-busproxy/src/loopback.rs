//! In-process transport: a client wired straight to a [`StorageServer`].
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use serde_json::Value;
use tinystoragedrivers_bus::{
    BLOB_STREAM_CHUNK_BYTES, BlobKeyRequest, BlobStreamHead, MAX_FRAME_BYTES, PutBlobStreamRequest,
    WatchChanged,
};
use tokio::sync::broadcast;

use crate::server::StorageServer;
use crate::transport::{Admission, BlobBody, Object, StorageTransport, WireError};

/// A [`StorageTransport`] that calls a server in the same process.
///
/// Every request and reply is serialized to JSON text, checked against
/// `MAX_FRAME_BYTES` and parsed again, so what crosses is exactly what a bus
/// would carry. Blob bodies move in `BLOB_STREAM_CHUNK_BYTES` reads, as a bus
/// stream does.
#[derive(Debug)]
pub struct Loopback {
    server: Arc<StorageServer>,
    admission: Admission,
    largest_frame: AtomicUsize,
    streamed_bytes: Arc<AtomicU64>,
}

impl Loopback {
    /// Connect to `server` as a caller admitted with `admission`.
    #[must_use]
    pub fn new(server: Arc<StorageServer>, admission: Admission) -> Self {
        Self {
            server,
            admission,
            largest_frame: AtomicUsize::new(0),
            streamed_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The largest request or reply frame seen, in bytes.
    #[must_use]
    pub fn largest_frame(&self) -> usize {
        self.largest_frame.load(Ordering::Relaxed)
    }

    /// Total blob bytes that travelled over streams in either direction.
    #[must_use]
    pub fn streamed_bytes(&self) -> u64 {
        self.streamed_bytes.load(Ordering::Relaxed)
    }

    /// Serialize as a frame, enforce the cap and parse it back.
    fn frame(&self, value: &Value) -> Result<Value, WireError> {
        let text = serde_json::to_string(value)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        self.largest_frame.fetch_max(text.len(), Ordering::Relaxed);
        if text.len() > MAX_FRAME_BYTES {
            return Err(WireError::Transport("frame exceeds the 16 MiB cap".into()));
        }
        serde_json::from_str(&text).map_err(|_| WireError::Transport("frame decode failed".into()))
    }

    fn chunked(&self, body: BlobBody) -> BlobBody {
        Box::pin(Chunked {
            inner: body,
            moved: Arc::clone(&self.streamed_bytes),
            rest: None,
        })
    }
}

/// Reads at most one stream chunk at a time and counts the bytes moved.
struct Chunked {
    inner: BlobBody,
    moved: Arc<AtomicU64>,
    rest: Option<Bytes>,
}

impl Stream for Chunked {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let bytes = if let Some(rest) = self.rest.take() {
            rest
        } else {
            match self.inner.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Some(Ok(bytes))) => bytes,
            }
        };
        let take = bytes.len().min(BLOB_STREAM_CHUNK_BYTES);
        if take < bytes.len() {
            self.rest = Some(bytes.slice(take..));
        }
        let chunk = bytes.slice(..take);
        self.moved.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        Poll::Ready(Some(Ok(chunk)))
    }
}

fn wire_failure(failure: tinystoragedrivers_bus::StorageFailure) -> WireError {
    WireError::Storage(failure)
}

#[async_trait]
impl StorageTransport for Loopback {
    async fn call(
        &self,
        object: Object,
        selector: &str,
        request: Value,
    ) -> Result<Value, WireError> {
        let request = self.frame(&request)?;
        let reply = self
            .server
            .handle(self.admission, object.as_str(), selector, request)
            .await
            .map_err(wire_failure)?;
        self.frame(&reply)
    }

    async fn put_blob_stream(
        &self,
        head: PutBlobStreamRequest,
        body: BlobBody,
    ) -> Result<Value, WireError> {
        let head = serde_json::to_value(&head)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        let head = serde_json::from_value(self.frame(&head)?)
            .map_err(|_| WireError::Transport("frame decode failed".into()))?;
        let info = self
            .server
            .put_blob_stream(head, self.chunked(body))
            .await
            .map_err(wire_failure)?;
        let info = serde_json::to_value(&info)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        self.frame(&info)
    }

    async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobBody)>, WireError> {
        let found = self
            .server
            .get_blob_stream(request)
            .await
            .map_err(wire_failure)?;
        Ok(found.map(|(head, body)| (head, self.chunked(body))))
    }

    fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.server.signals()
    }
}

#[cfg(test)]
#[path = "loopback_tests.rs"]
mod tests;
