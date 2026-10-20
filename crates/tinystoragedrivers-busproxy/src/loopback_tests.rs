use super::*;

struct Once(Option<Bytes>);
impl Stream for Once {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}

#[tokio::test]
async fn loopback_splits_large_blob_chunks_and_preserves_every_byte() {
    let size = 20 * 1024 * 1024 + 123;
    let source = Bytes::from(vec![0x5a; size]);
    let mut chunked = Box::pin(Chunked {
        inner: Box::pin(Once(Some(source))),
        moved: Arc::new(AtomicU64::new(0)),
        rest: None,
    });
    let mut received = 0;
    while let Some(part) = std::future::poll_fn(|cx| chunked.as_mut().poll_next(cx)).await {
        let part = part.unwrap();
        assert!(part.len() <= BLOB_STREAM_CHUNK_BYTES);
        assert!(part.iter().all(|byte| *byte == 0x5a));
        received += part.len();
    }
    assert_eq!(received, size);
}
