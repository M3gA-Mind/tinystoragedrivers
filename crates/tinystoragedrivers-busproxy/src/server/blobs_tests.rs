use super::*;
use bytes::Bytes;
use futures_core::Stream;
use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

struct Chunks(VecDeque<tinystoragedrivers_types::Result<Bytes>>);

impl Stream for Chunks {
    type Item = tinystoragedrivers_types::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.pop_front())
    }
}

fn make_exact(
    inner: impl Stream<Item = tinystoragedrivers_types::Result<Bytes>> + Send + 'static,
    len: u64,
) -> Exact {
    Exact {
        inner: Box::pin(inner),
        remaining: len,
        done: false,
    }
}

fn poll(exact: &mut Exact) -> Poll<Option<tinystoragedrivers_types::Result<Bytes>>> {
    let mut context = Context::from_waker(Waker::noop());
    Pin::new(exact).poll_next(&mut context)
}

#[test]
fn exact_stream_requires_the_declared_length_then_fuses() {
    let mut exact = make_exact(Chunks(VecDeque::from([Ok(Bytes::from_static(b"abc"))])), 3);
    assert!(matches!(poll(&mut exact), Poll::Ready(Some(Ok(bytes))) if bytes == "abc"));
    assert!(matches!(poll(&mut exact), Poll::Ready(None)));
    assert!(matches!(poll(&mut exact), Poll::Ready(None)));

    let mut short = make_exact(Chunks(VecDeque::new()), 1);
    assert!(
        matches!(poll(&mut short), Poll::Ready(Some(Err(error))) if error.kind() == tinystoragedrivers_types::ErrorKind::InvalidInput)
    );
    assert!(matches!(poll(&mut short), Poll::Ready(None)));

    let mut long = make_exact(Chunks(VecDeque::from([Ok(Bytes::from_static(b"ab"))])), 1);
    assert!(
        matches!(poll(&mut long), Poll::Ready(Some(Err(error))) if error.kind() == tinystoragedrivers_types::ErrorKind::InvalidInput)
    );
}

#[derive(Debug)]
struct ReplacedBlob {
    head_len: u64,
}

#[tinystoragedrivers_api::async_trait]
impl tinystoragedrivers_api::BlobStore for ReplacedBlob {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        tinystoragedrivers_types::Capabilities::none()
    }
    fn max_key_len(&self) -> usize {
        1024
    }
    async fn put(
        &self,
        _key: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> tinystoragedrivers_types::Result<BlobMeta> {
        unreachable!()
    }
    async fn get(
        &self,
        key: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::Blob>> {
        Ok(Some(tinystoragedrivers_types::Blob {
            meta: BlobMeta {
                key: key.into(),
                len: (MAX_INLINE_BLOB_BYTES + 1) as u64,
                content_type: None,
            },
            bytes: vec![0x5a; MAX_INLINE_BLOB_BYTES + 1],
        }))
    }
    async fn get_range(
        &self,
        _key: &str,
        _range: std::ops::Range<u64>,
    ) -> tinystoragedrivers_types::Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn head(&self, key: &str) -> tinystoragedrivers_types::Result<Option<BlobMeta>> {
        Ok(Some(BlobMeta {
            key: key.into(),
            len: self.head_len,
            content_type: None,
        }))
    }
    async fn delete(&self, _key: &str) -> tinystoragedrivers_types::Result<bool> {
        Ok(false)
    }
    async fn list(&self, _prefix: &str) -> tinystoragedrivers_types::Result<Vec<BlobMeta>> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn inline_get_refuses_a_blob_replaced_after_the_head_check() {
    use tinystoragedrivers_api::Storage;
    let base = tinystoragedrivers_testkit::memory_storage(
        &tinystoragedrivers_types::Scope::new("race").unwrap(),
    );
    let storage = Storage::new(
        base.documents().clone(),
        base.streams().clone(),
        base.kv().clone(),
        std::sync::Arc::new(ReplacedBlob { head_len: 4 }),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    match server
        .handle(
            crate::Admission::Private,
            "Blobs",
            "Get",
            serde_json::json!({"key":"race"}),
        )
        .await
    {
        Err(error) => assert_eq!(error.kind, wire::StorageKind::InvalidInput),
        Ok(_) => panic!("oversized replacement was returned inline"),
    }
}

#[tokio::test]
async fn inline_get_rejects_a_head_that_already_exceeds_the_inline_cap() {
    use tinystoragedrivers_api::Storage;
    let base = tinystoragedrivers_testkit::memory_storage(
        &tinystoragedrivers_types::Scope::new("oversized-head").unwrap(),
    );
    let storage = Storage::new(
        base.documents().clone(),
        base.streams().clone(),
        base.kv().clone(),
        std::sync::Arc::new(ReplacedBlob {
            head_len: (MAX_INLINE_BLOB_BYTES + 1) as u64,
        }),
        base.queues().clone(),
        base.secrets().clone(),
    );
    let server = StorageServer::new(storage);
    match server
        .handle(
            crate::Admission::Private,
            "Blobs",
            "Get",
            serde_json::json!({"key":"large"}),
        )
        .await
    {
        Err(error) => assert_eq!(error.kind, wire::StorageKind::InvalidInput),
        Ok(_) => panic!("oversized blob head was returned inline"),
    }
}

#[test]
fn inline_get_rejects_blob_replaced_after_small_head_check() {
    let mut meta = BlobMeta {
        key: "race".into(),
        len: 4,
        content_type: None,
    };
    // `head` observed four bytes, then a concurrent writer replaced the value
    // before `get` returned this snapshot.
    let replaced = vec![0x5a; MAX_INLINE_BLOB_BYTES + 1];
    meta.len = replaced.len() as u64;
    assert_eq!(validate_inline_blob(&meta, &replaced), Err(invalid()));
}

#[test]
fn inline_get_rejects_metadata_and_body_length_mismatch() {
    let meta = BlobMeta {
        key: "race".into(),
        len: 5,
        content_type: None,
    };
    assert_eq!(validate_inline_blob(&meta, b"four"), Err(invalid()));
}
