//! Scoped blob conformance and multipart publication guarantees.

use super::*;
use tinystoragedrivers_core::{MemoryStorage, ScopedStorage, StorageBackend};

#[derive(Debug)]
struct Composite {
    documents: Arc<dyn StorageBackend>,
    objects: Arc<object_store::memory::InMemory>,
    database: String,
}

impl StorageBackend for Composite {
    fn driver(&self) -> &'static str {
        "test-composite"
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }
    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        let local = self.documents.for_scope(scope)?;
        Ok(ScopedStorage::new(
            scope.clone(),
            self.driver(),
            local.documents().clone(),
            local.streams().clone(),
            Arc::new(S3Blobs::with_store(
                self.objects.clone(),
                None,
                "test",
                scope,
                &self.database,
            )?),
        ))
    }
    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        validate_database(name)?;
        Ok(Arc::new(Self {
            documents: self.documents.database(name)?,
            objects: self.objects.clone(),
            database: name.to_owned(),
        }))
    }
}

fn handle(objects: Arc<object_store::memory::InMemory>, scope: &str) -> S3Blobs {
    S3Blobs::with_store(objects, None, "test", &Scope::new(scope).unwrap(), "blobs").unwrap()
}

#[tokio::test]
async fn blob_contract_and_scope_database_isolation() {
    let backend = Composite {
        documents: Arc::new(MemoryStorage::new()),
        objects: Arc::new(object_store::memory::InMemory::new()),
        database: "default".into(),
    };
    tinystoragedrivers_core::conformance::run(&backend, false).await;
}

#[tokio::test]
async fn tenant_path_characters_cannot_alias_another_scope() {
    let objects = Arc::new(object_store::memory::InMemory::new());
    let a = handle(objects.clone(), "alice/bob");
    let b = handle(objects.clone(), "alice%2Fbob");
    let c = handle(objects, "alice");
    a.put("x", vec![1], None).await.unwrap();
    b.put("x", vec![2], None).await.unwrap();
    c.put("bob/x", vec![3], None).await.unwrap();
    assert_eq!(a.get("x").await.unwrap().unwrap().bytes, [1]);
    assert_eq!(b.get("x").await.unwrap().unwrap().bytes, [2]);
    assert_eq!(c.get("bob/x").await.unwrap().unwrap().bytes, [3]);
    assert_eq!(a.list("").await.unwrap().len(), 1);
}

#[tokio::test]
async fn multipart_roundtrip_and_length_mismatch_preserve_previous_object() {
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let data = vec![42; PART_SIZE * 2 + 123];
    store
        .put_stream(
            "large",
            Box::pin(std::io::Cursor::new(data.clone())),
            data.len() as u64,
            Some("application/octet-stream"),
        )
        .await
        .unwrap();
    let mut result = store.get_stream("large").await.unwrap().unwrap();
    assert_eq!(result.meta.len, data.len() as u64);
    let mut got = Vec::new();
    result.reader.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, data);

    for len in [data.len() as u64 - 1, data.len() as u64 + 1] {
        let err = store
            .put_stream(
                "large",
                Box::pin(std::io::Cursor::new(data.clone())),
                len,
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
        assert_eq!(store.get("large").await.unwrap().unwrap().bytes, data);
    }
}

#[tokio::test]
async fn buffered_length_mismatch_does_not_publish() {
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    for len in [1, 3] {
        assert!(
            store
                .put_stream(
                    "small",
                    Box::pin(std::io::Cursor::new(vec![1, 2])),
                    len,
                    None
                )
                .await
                .is_err()
        );
        assert!(store.head("small").await.unwrap().is_none());
    }
    assert!(!store.capabilities().contains(Capability::PresignedUrls));
    assert_eq!(
        store
            .presign_get("x", Duration::from_secs(60))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::PresignedUrls)
    );
}

#[test]
fn parses_compatible_endpoint_and_rejects_embedded_credentials() {
    let config =
        S3Config::parse("s3://bucket/prefix?region=local&endpoint=http%3A%2F%2Flocalhost%3A9000")
            .unwrap();
    assert_eq!(config.bucket, "bucket");
    assert_eq!(config.prefix, "prefix");
    assert_eq!(config.region, "local");
    assert_eq!(config.endpoint.as_deref(), Some("http://localhost:9000"));
    for bad in [
        "s3://user:secret@bucket",
        "s3://bucket?secret=x",
        "s3://bucket?endpoint=https%3A%2F%2Fuser%3Asecret%40host",
    ] {
        let error = S3Config::parse(bad).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(!error.to_string().contains("secret"));
    }
}

#[tokio::test]
async fn presigning_binds_scope_method_and_expiry_without_network() {
    let client = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("bucket")
            .with_region("us-east-1")
            .with_access_key_id("test-access")
            .with_secret_access_key("test-secret")
            .with_endpoint("http://localhost:9000")
            .with_allow_http(true)
            .build()
            .unwrap(),
    );
    let blobs = S3Blobs::with_store(
        client.clone(),
        Some(client),
        "prefix",
        &Scope::new("alice/bob").unwrap(),
        "media",
    )
    .unwrap();
    assert!(blobs.capabilities().contains(Capability::StreamingBlobs));
    assert!(blobs.capabilities().contains(Capability::PresignedUrls));
    let get = blobs
        .presign_get("hello world.txt", Duration::from_secs(60))
        .await
        .unwrap();
    let put = blobs
        .presign_put("hello world.txt", Duration::from_secs(60))
        .await
        .unwrap();
    let get = url::Url::parse(&get).unwrap();
    let put = url::Url::parse(&put).unwrap();
    assert_eq!(
        get.path(),
        "/bucket/prefix/media/scopes/616c6963652f626f62/hello%20world.txt"
    );
    let query: std::collections::HashMap<_, _> = get.query_pairs().collect();
    assert_eq!(query.get("X-Amz-Expires").unwrap(), "60");
    assert_ne!(get.query(), put.query());
    assert!(blobs.presign_get("x", Duration::ZERO).await.is_err());
    assert!(
        blobs
            .presign_get("x", Duration::from_secs(604_801))
            .await
            .is_err()
    );
    assert!(
        blobs
            .presign_get("../x", Duration::from_secs(60))
            .await
            .is_err()
    );
}

#[path = "bounded_tests.rs"]
mod bounded;

#[tokio::test]
async fn scoped_key_limit_counts_utf8_and_rejects_overflow_without_network() {
    let store = handle(Arc::new(object_store::memory::InMemory::new()), "alice");
    let limit = store.max_key_len();
    let ascii = "x".repeat(limit);
    store.put(&ascii, vec![1], None).await.unwrap();
    let mut utf8 = "é".repeat(limit / 2);
    if !limit.is_multiple_of(2) {
        utf8.push('x');
    }
    store.put(&utf8, vec![2], None).await.unwrap();
    assert_eq!(store.get(&utf8).await.unwrap().unwrap().bytes, [2]);
    assert_eq!(
        store
            .put(&(ascii + "x"), vec![], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidInput
    );
    assert!(
        S3Blobs::with_store(
            Arc::new(object_store::memory::InMemory::new()),
            None,
            &"x".repeat(1000),
            &Scope::new("alice").unwrap(),
            "blobs"
        )
        .is_err()
    );
}
