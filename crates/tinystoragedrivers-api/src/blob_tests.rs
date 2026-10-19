use super::*;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

use tinystoragedrivers_types::{Blob, ErrorKind, StorageError};

#[derive(Debug, Default)]
struct MemoryBlobs {
    values: Mutex<BTreeMap<String, StoredBlob>>,
    put_calls: Mutex<usize>,
    capabilities: Capabilities,
}

type StoredBlob = (Vec<u8>, Option<String>);

impl MemoryBlobs {
    fn with_capabilities(capabilities: Capabilities) -> Self {
        Self {
            capabilities,
            ..Self::default()
        }
    }

    fn bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.values
            .lock()
            .unwrap()
            .get(key)
            .map(|(bytes, _)| bytes.clone())
    }

    fn put_count(&self) -> usize {
        *self.put_calls.lock().unwrap()
    }
}

#[async_trait]
impl BlobStore for MemoryBlobs {
    fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    fn max_key_len(&self) -> usize {
        128
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        *self.put_calls.lock().unwrap() += 1;
        self.values.lock().unwrap().insert(
            key.to_owned(),
            (bytes.clone(), content_type.map(str::to_owned)),
        );
        Ok(BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .get(key)
            .map(|(bytes, content_type)| Blob {
                meta: BlobMeta {
                    key: key.to_owned(),
                    len: bytes.len() as u64,
                    content_type: content_type.clone(),
                },
                bytes: bytes.clone(),
            }))
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        Ok(self.bytes(key).map(|bytes| {
            let start = usize::try_from(range.start)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let end = usize::try_from(range.end)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            if start >= end {
                Vec::new()
            } else {
                bytes[start..end].to_vec()
            }
        }))
    }

    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .get(key)
            .map(|(bytes, content_type)| BlobMeta {
                key: key.to_owned(),
                len: bytes.len() as u64,
                content_type: content_type.clone(),
            }))
    }

    async fn delete(&self, key: &str) -> Result<bool> {
        Ok(self.values.lock().unwrap().remove(key).is_some())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, (bytes, content_type))| BlobMeta {
                key: key.clone(),
                len: bytes.len() as u64,
                content_type: content_type.clone(),
            })
            .collect())
    }
}

struct Chunks(std::collections::VecDeque<Result<Bytes>>);

impl Stream for Chunks {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.pop_front())
    }
}

fn reader(chunks: impl IntoIterator<Item = Result<Bytes>>) -> BlobReader {
    Box::pin(Chunks(chunks.into_iter().collect()))
}

#[test]
fn default_put_stream_buffers_chunks_and_publishes_only_exact_success() {
    let store = MemoryBlobs::default();
    let meta = run_ready(store.put_stream(
        "item",
        reader([Ok(Bytes::from_static(b"ab")), Ok(Bytes::from_static(b"cd"))]),
        4,
        Some("text/plain"),
    ))
    .unwrap();

    assert_eq!(meta.len, 4);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
    assert_eq!(store.bytes("item").as_deref(), Some(&b"abcd"[..]));
    assert_eq!(store.put_count(), 1);
}

#[test]
fn default_put_stream_rejects_short_long_and_failed_inputs_without_replacing() {
    let store = MemoryBlobs::default();
    run_ready(store.put("item", b"old".to_vec(), None)).unwrap();
    let initial_puts = store.put_count();

    for (input, declared, expected) in [
        (
            reader([Ok(Bytes::from_static(b"ab"))]),
            3,
            ErrorKind::InvalidInput,
        ),
        (
            reader([Ok(Bytes::from_static(b"abcd"))]),
            3,
            ErrorKind::InvalidInput,
        ),
        (
            reader([Err(StorageError::backend("source failed"))]),
            3,
            ErrorKind::Backend,
        ),
    ] {
        let error = run_ready(store.put_stream("item", input, declared, None)).unwrap_err();
        assert_eq!(error.kind(), expected);
        assert_eq!(store.bytes("item").as_deref(), Some(&b"old"[..]));
        assert_eq!(store.put_count(), initial_puts);
    }
}

#[test]
fn default_put_stream_accepts_zero_length_and_stream_expiry_is_capability_gated() {
    let store = MemoryBlobs::default();
    let meta = run_ready(store.put_stream("empty", reader([]), 0, None)).unwrap();
    assert_eq!(meta.len, 0);
    assert_eq!(store.bytes("empty").as_deref(), Some(&b""[..]));

    let error = run_ready(store.put_stream_expiring(
        "expires",
        reader([Ok(Bytes::from_static(b"x"))]),
        1,
        None,
        Duration::from_secs(1),
    ))
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::BlobTtl));
    assert_eq!(store.bytes("expires"), None);

    let enabled = MemoryBlobs::with_capabilities(Capabilities::none().with(Capability::BlobTtl));
    let error = run_ready(enabled.put_stream_expiring(
        "expires",
        reader([Ok(Bytes::from_static(b"x"))]),
        1,
        None,
        Duration::from_secs(1),
    ))
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::BlobTtl));
    assert_eq!(enabled.bytes("expires"), None);
}

#[test]
fn default_get_stream_preserves_metadata_and_bytes_including_empty_and_missing() {
    let store = MemoryBlobs::default();
    run_ready(store.put("empty", Vec::new(), None)).unwrap();
    run_ready(store.put("filled", b"body".to_vec(), Some("text/plain"))).unwrap();

    for (key, expected, expected_type) in [
        ("empty", b"".as_slice(), None),
        ("filled", b"body".as_slice(), Some("text/plain")),
    ] {
        let mut body = run_ready(store.get_stream(key)).unwrap().unwrap();
        assert!(format!("{body:?}").contains("BlobStream"));
        assert_eq!(body.meta.len, expected.len() as u64);
        assert_eq!(body.meta.content_type.as_deref(), expected_type);
        let first = poll_next(body.reader.as_mut()).map(|item| item.unwrap().unwrap());
        let Poll::Ready(bytes) = first else {
            panic!("default stream should emit one chunk")
        };
        assert_eq!(bytes.as_ref(), expected);
        assert!(matches!(poll_next(body.reader.as_mut()), Poll::Ready(None)));
    }
    assert!(run_ready(store.get_stream("missing")).unwrap().is_none());
}

#[test]
fn default_blob_optional_methods_and_tree_helpers_preserve_contract() {
    let store = MemoryBlobs::default();
    assert_eq!(run_ready(store.sweep_expired()).unwrap(), 0);
    assert_eq!(
        run_ready(store.presign_get("x", Duration::from_secs(1)))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    assert_eq!(
        run_ready(store.presign_put("x", Duration::from_secs(1)))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
    assert_eq!(
        run_ready(store.put_expiring("x", vec![1], None, Duration::from_secs(1)))
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::BlobTtl)
    );

    run_ready(store.put_tree(
        "root",
        vec![
            TreeFile::new("top.txt", b"top".to_vec()),
            TreeFile::new("nested/child.txt", b"child".to_vec()),
        ],
    ))
    .unwrap();
    let root_entries = run_ready(store.list_dir("")).unwrap();
    assert_eq!(
        root_entries.iter().map(TreeEntry::name).collect::<Vec<_>>(),
        ["root"]
    );
    let entries = run_ready(store.list_dir("root")).unwrap();
    assert_eq!(
        entries.iter().map(TreeEntry::name).collect::<Vec<_>>(),
        ["nested", "top.txt"]
    );
    assert!(matches!(&entries[0], TreeEntry::Dir { prefix, .. } if prefix == "root/nested"));
    assert!(matches!(&entries[1], TreeEntry::Blob { meta, .. } if meta.key == "root/top.txt"));
    assert_eq!(
        run_ready(store.get_range("root/top.txt", 1..3)).unwrap(),
        Some(b"op".to_vec())
    );
    assert_eq!(
        run_ready(store.head("root/top.txt")).unwrap().unwrap().len,
        3
    );
    assert_eq!(run_ready(store.list("root/")).unwrap().len(), 2);
    assert_eq!(run_ready(store.delete_tree("root")).unwrap(), 2);
    assert!(!run_ready(store.delete("root/top.txt")).unwrap());

    let duplicate = run_ready(store.put_tree(
        "root",
        vec![TreeFile::new("same", vec![]), TreeFile::new("same", vec![])],
    ))
    .unwrap_err();
    assert_eq!(duplicate.kind(), ErrorKind::InvalidInput);
    let traversal =
        run_ready(store.put_tree("root", vec![TreeFile::new("../escape", vec![])])).unwrap_err();
    assert_eq!(traversal.kind(), ErrorKind::InvalidInput);
    assert_eq!(
        run_ready(store.delete_tree("")).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    for invalid_path in ["", "a//b", ".", "a/./b", "a/../b", "bad\0name"] {
        assert_eq!(
            run_ready(store.put_tree("root", vec![TreeFile::new(invalid_path, vec![])]))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput,
            "path should be rejected: {invalid_path:?}"
        );
    }
    let too_long = "x".repeat(129);
    assert_eq!(
        run_ready(store.put_tree("root", vec![TreeFile::new(too_long, vec![])]))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        run_ready(store.list_dir("../escape")).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

fn poll_next(
    stream: Pin<&mut (dyn Stream<Item = Result<Bytes>> + Send)>,
) -> Poll<Option<Result<Bytes>>> {
    let mut context = Context::from_waker(Waker::noop());
    stream.poll_next(&mut context)
}

fn run_ready<F: Future>(future: F) -> F::Output {
    let mut context = Context::from_waker(Waker::noop());
    let mut future = Box::pin(future);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly pended"),
    }
}

#[test]
fn default_page_listing_validates_size_then_reports_precise_capability() {
    let store = MemoryBlobs::default();
    let error = run_ready(store.list_page("", None, 1)).unwrap_err();
    assert_eq!(
        error.kind(),
        ErrorKind::Unsupported(Capability::BlobListing)
    );
    assert_eq!(
        run_ready(store.list_page("", None, 0)).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        run_ready(store.list_page("", None, MAX_BLOB_LIST_PAGE_ITEMS + 1))
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
}
