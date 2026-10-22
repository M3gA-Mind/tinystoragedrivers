//! Moving blob bytes between a `BlobReader` and a `TinyBus` stream.
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;
use tinybus::{MAX_CHUNK_LEN, StreamReader, StreamWriter};
use tinystoragedrivers_api::BlobReader;
use tokio::sync::mpsc;

/// How many received chunks wait for the consumer. Small on purpose: the
/// stream's own window is the buffer, this only decouples the two tasks.
const READ_AHEAD: usize = 2;

/// An `AsyncRead` over the chunks a spawned task pulls from a stream.
struct ChunkReader {
    rx: mpsc::Receiver<io::Result<Vec<u8>>>,
}
impl Stream for ChunkReader {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(Bytes::from(chunk)))),
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(
                tinystoragedrivers_types::StorageError::unavailable("storage blob stream failed"),
            ))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A `BlobReader` over a received stream. A stream that aborts or goes idle
/// surfaces as an I/O error on the reader, never as a short clean end.
pub(crate) fn blob_reader(mut stream: StreamReader) -> BlobReader {
    let (tx, rx) = mpsc::channel(READ_AHEAD);
    tokio::spawn(async move {
        loop {
            match stream.next_chunk().await {
                Ok(Some(chunk)) => {
                    if tx.send(Ok(chunk)).await.is_err() {
                        // The consumer is gone; dropping the reader aborts the
                        // sender's next write.
                        return;
                    }
                }
                Ok(None) => return,
                Err(_) => {
                    let _ = tx
                        .send(Err(io::Error::other("storage blob stream failed")))
                        .await;
                    return;
                }
            }
        }
    });
    Box::pin(ChunkReader { rx })
}

/// Why [`pump`] stopped.
#[derive(Debug)]
pub(crate) enum PumpError {
    /// The source ended before `len` bytes.
    Short,
    /// The source failed.
    Source,
    /// The bus refused a chunk.
    Bus(tinybus::Error),
}

/// Write exactly `len` bytes from `reader` into `writer` in stream-sized chunks
/// and close it. On failure the stream is aborted so the receiver stops
/// waiting.
///
/// # Errors
/// [`PumpError`] naming which side failed.
pub(crate) async fn pump(
    mut writer: StreamWriter,
    reader: &mut BlobReader,
    len: u64,
) -> Result<(), PumpError> {
    let mut remaining = len;
    let result = async {
        while let Some(item) = std::future::poll_fn(|cx| reader.as_mut().poll_next(cx)).await {
            let chunk = item.map_err(|_| PumpError::Source)?;
            if chunk.len() as u64 > remaining {
                return Err(PumpError::Short);
            }
            for part in chunk.chunks(MAX_CHUNK_LEN) {
                writer.write_chunk(part).await.map_err(PumpError::Bus)?;
            }
            remaining -= chunk.len() as u64;
        }
        if remaining != 0 {
            return Err(PumpError::Short);
        }
        Ok(())
    }
    .await;
    // `write_chunk` borrows the writer mutably above, so close it here.
    match result {
        Ok(()) => writer.finish().await.map(|_| ()).map_err(PumpError::Bus),
        Err(error) => {
            let _ = writer.abort().await;
            Err(error)
        }
    }
}

/// Adapt a `TinyBus` reader into the runtime-neutral API stream, with two chunks
/// of bounded read-ahead and transport failures preserved as stream errors.
#[cfg(test)]
#[path = "chunks_tests.rs"]
mod tests;
