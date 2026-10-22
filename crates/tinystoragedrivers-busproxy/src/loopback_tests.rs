use super::*;

struct Once(Option<Bytes>);
impl Stream for Once {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.take().map(Ok))
    }
}

struct PendingThenReady {
    first: bool,
    bytes: Option<Bytes>,
}

impl Stream for PendingThenReady {
    type Item = tinystoragedrivers_types::Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.first {
            self.first = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        Poll::Ready(self.bytes.take().map(Ok))
    }
}

#[derive(serde::Deserialize)]
struct FailsSerialization;

impl Serialize for FailsSerialization {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(serde::ser::Error::custom("fixture serialization failure"))
    }
}

#[derive(Serialize)]
struct FailsDeserialization;

impl<'de> serde::Deserialize<'de> for FailsDeserialization {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(serde::de::Error::custom("fixture deserialization failure"))
    }
}

fn loopback() -> Loopback {
    let server = Arc::new(StorageServer::new(
        tinystoragedrivers_testkit::memory_storage(
            &tinystoragedrivers_types::Scope::new("loopback-codec").unwrap(),
        ),
    ));
    Loopback::new(server, crate::Admission::Private)
}

#[test]
fn loopback_maps_frame_and_value_codec_failures() {
    let loopback = loopback();
    assert!(matches!(
        loopback.frame(&FailsSerialization),
        Err(crate::WireError::Transport(_))
    ));
    assert!(matches!(
        loopback.frame(&FailsDeserialization),
        Err(crate::WireError::Transport(_))
    ));
    assert!(matches!(
        loopback.round_trip(&FailsSerialization),
        Err(crate::WireError::Transport(_))
    ));
    assert!(matches!(
        loopback.round_trip(&FailsDeserialization),
        Err(crate::WireError::Transport(_))
    ));
}

#[tokio::test]
async fn chunked_stream_propagates_pending_before_yielding_bytes() {
    let mut chunked = Box::pin(Chunked {
        inner: Box::pin(PendingThenReady {
            first: false,
            bytes: Some(Bytes::from_static(b"ready")),
        }),
        moved: Arc::new(AtomicU64::new(0)),
        rest: None,
    });
    let part = std::future::poll_fn(|cx| chunked.as_mut().poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(part.as_ref(), b"ready");
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

#[tokio::test]
async fn loopback_frames_calls_and_counts_streamed_blob_bytes() {
    let server = Arc::new(StorageServer::new(
        tinystoragedrivers_testkit::memory_storage(
            &tinystoragedrivers_types::Scope::new("loopback-frame-counters").unwrap(),
        ),
    ));
    let loopback = Loopback::new(server, crate::Admission::Private);
    let _signals = loopback.signals();
    let reply = loopback
        .call(
            crate::Object::Kv,
            "Get",
            serde_json::json!({"key":"missing"}),
        )
        .await
        .unwrap();
    assert_eq!(reply, serde_json::Value::Null);
    assert!(loopback.largest_frame() > 0);

    let head = tinystoragedrivers_bus::PutBlobStreamRequest {
        key: "streamed".into(),
        len: 3,
        content_type: None,
        ttl_ms: None,
    };
    loopback
        .put_blob_stream(head, Box::pin(Once(Some(Bytes::from_static(b"abc")))))
        .await
        .unwrap();
    assert_eq!(loopback.streamed_bytes(), 3);

    let (_, mut body) = loopback
        .get_blob_stream(tinystoragedrivers_bus::BlobKeyRequest {
            key: "streamed".into(),
        })
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = std::future::poll_fn(|cx| body.as_mut().poll_next(cx)).await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(bytes, b"abc");
    assert_eq!(loopback.streamed_bytes(), 6);
}

#[tokio::test]
async fn loopback_rejects_request_frames_over_the_contract_cap() {
    let server = Arc::new(StorageServer::new(
        tinystoragedrivers_testkit::memory_storage(
            &tinystoragedrivers_types::Scope::new("loopback-oversized-frame").unwrap(),
        ),
    ));
    let loopback = Loopback::new(server, crate::Admission::Private);
    let result = loopback
        .call(
            crate::Object::Kv,
            "Get",
            serde_json::json!({"key":"x".repeat(tinystoragedrivers_bus::MAX_FRAME_BYTES + 1)}),
        )
        .await;
    assert!(matches!(result, Err(crate::WireError::Transport(_))));
    assert_eq!(loopback.streamed_bytes(), 0);
}
