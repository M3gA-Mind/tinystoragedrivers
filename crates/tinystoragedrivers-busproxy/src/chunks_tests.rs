use super::*;
use bytes::Bytes;
use futures_core::Stream;
use tinybus::{Connection, StreamDescriptor, broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_types::{Result, StorageError};

struct Source(Option<Bytes>);
impl Stream for Source {
    type Item = Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}

fn source(bytes: Vec<u8>) -> BlobReader {
    Box::pin(Source(Some(Bytes::from(bytes))))
}

async fn collect(mut reader: BlobReader) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|cx| reader.as_mut().poll_next(cx)).await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

/// Two connected peers on an in-process broker: `(sender, receiver)`.
async fn peers() -> (Connection, Connection) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let sender = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let receiver = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    (sender, receiver)
}

#[tokio::test(flavor = "multi_thread")]
async fn bytes_round_trip_through_a_stream_in_order() {
    let (sender, receiver) = peers().await;
    let to = receiver.unique_name().unwrap();
    let body: Vec<u8> = (0..1_300_000_u32).map(|at| (at % 253) as u8).collect();
    let writer = sender
        .open_stream(&to, StreamDescriptor::with_len(body.len() as u64))
        .await
        .unwrap();
    let reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source = source(body.clone());
    let len = body.len() as u64;
    let (pumped, collected) = tokio::join!(pump(writer, &mut source, len), collect(reader));
    pumped.unwrap();
    assert_eq!(collected.unwrap(), body);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_aborted_stream_is_an_error_on_the_reader_not_a_clean_end() {
    let (sender, receiver) = peers().await;
    let mut writer = sender
        .open_stream(
            &receiver.unique_name().unwrap(),
            StreamDescriptor::with_len(10),
        )
        .await
        .unwrap();
    let reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    writer.write_chunk(b"abc").await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        collect(reader).await.unwrap_err().message(),
        "storage blob stream failed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_short_source_aborts_the_stream_and_reports_short() {
    let (sender, receiver) = peers().await;
    let writer = sender
        .open_stream(
            &receiver.unique_name().unwrap(),
            StreamDescriptor::with_len(100),
        )
        .await
        .unwrap();
    let reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source = source(vec![1; 10]);
    assert!(matches!(
        pump(writer, &mut source, 100).await,
        Err(PumpError::Short)
    ));
    assert!(collect(reader).await.is_err());
}

struct Failing;
impl Stream for Failing {
    type Item = Result<Bytes>;
    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(Err(StorageError::unavailable("source failed"))))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_source_is_reported_as_the_sources_fault() {
    let (sender, receiver) = peers().await;
    let writer = sender
        .open_stream(
            &receiver.unique_name().unwrap(),
            StreamDescriptor::with_len(100),
        )
        .await
        .unwrap();
    let _reader = receiver.accept_stream(&writer.stream_ref()).unwrap();
    let mut source: BlobReader = Box::pin(Failing);
    assert!(matches!(
        pump(writer, &mut source, 100).await,
        Err(PumpError::Source)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receiver_that_walks_away_fails_the_writer_on_the_bus() {
    let (sender, receiver) = peers().await;
    let writer = sender
        .open_stream(
            &receiver.unique_name().unwrap(),
            StreamDescriptor::with_len(8 * 1024 * 1024),
        )
        .await
        .unwrap();
    drop(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source = source(vec![0; 8 * 1024 * 1024]);
    assert!(matches!(
        pump(writer, &mut source, 8 * 1024 * 1024).await,
        Err(PumpError::Bus(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_zero_length_stream_closes_without_chunks() {
    let (sender, receiver) = peers().await;
    let writer = sender
        .open_stream(
            &receiver.unique_name().unwrap(),
            StreamDescriptor::with_len(0),
        )
        .await
        .unwrap();
    let reader = blob_reader(receiver.accept_stream(&writer.stream_ref()).unwrap());
    let mut source = source(Vec::new());
    pump(writer, &mut source, 0).await.unwrap();
    assert_eq!(collect(reader).await.unwrap(), Vec::<u8>::new());
}
