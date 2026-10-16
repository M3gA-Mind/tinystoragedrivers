//! Large synthetic uploads into a multipart recorder without retaining the body.

use super::*;
use futures_util::stream::BoxStream;
use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Totals {
    bytes: AtomicU64,
    parts: AtomicUsize,
    maximum: AtomicUsize,
    completed: AtomicUsize,
    list_prefix: std::sync::Mutex<Option<String>>,
}

#[derive(Debug)]
struct Recorder {
    inner: object_store::memory::InMemory,
    totals: Arc<Totals>,
}

impl std::fmt::Display for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("recorder")
    }
}

#[derive(Debug)]
struct Upload(Arc<Totals>);

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let totals = self.0.clone();
        Box::pin(async move {
            let size = payload.content_length();
            totals.bytes.fetch_add(size as u64, Ordering::Relaxed);
            totals.parts.fetch_add(1, Ordering::Relaxed);
            totals.maximum.fetch_max(size, Ordering::Relaxed);
            Ok(())
        })
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        self.0.completed.fetch_add(1, Ordering::Relaxed);
        Ok(PutResult {
            e_tag: None,
            version: None,
            extensions: http::Extensions::default(),
        })
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for Recorder {
    async fn put_opts(
        &self,
        path: &Path,
        data: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(path, data, options).await
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(Upload(self.totals.clone())))
    }
    async fn get_opts(
        &self,
        path: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(path, options).await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        *self.totals.list_prefix.lock().unwrap() = prefix.map(ToString::to_string);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn a_500_mib_source_is_uploaded_in_bounded_parts() {
    let totals = Arc::new(Totals::default());
    let recorder = Arc::new(Recorder {
        inner: object_store::memory::InMemory::new(),
        totals: totals.clone(),
    });
    let blobs = S3Blobs::with_store(recorder, None, "test", &Scope::local()).unwrap();
    let len = 500 * 1024 * 1024;
    blobs
        .put_stream(
            "large",
            Box::pin(tokio::io::repeat(42).take(len)),
            len,
            None,
        )
        .await
        .unwrap();
    assert_eq!(totals.bytes.load(Ordering::Relaxed), len);
    assert_eq!(totals.parts.load(Ordering::Relaxed), 63);
    assert_eq!(totals.maximum.load(Ordering::Relaxed), PART_SIZE);
    assert_eq!(totals.completed.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn listing_pushes_complete_requested_directories_into_the_backend_query() {
    let totals = Arc::new(Totals::default());
    let recorder = Arc::new(Recorder {
        inner: object_store::memory::InMemory::new(),
        totals: totals.clone(),
    });
    let blobs = S3Blobs::with_store(recorder, None, "root", &Scope::new("alice").unwrap()).unwrap();
    blobs.put("folder/file-one", vec![1], None).await.unwrap();
    blobs.put("folder/file-two", vec![2], None).await.unwrap();
    blobs.put("elsewhere/file", vec![3], None).await.unwrap();
    let listed = blobs.list("folder/fil").await.unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        totals.list_prefix.lock().unwrap().as_deref(),
        Some(format!("{}folder", blobs.prefix).as_str())
    );
}
