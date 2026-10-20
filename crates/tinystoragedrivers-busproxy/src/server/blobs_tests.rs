use super::*;

#[derive(Debug)]
struct ReplacedBlob;

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
            len: 4,
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
        std::sync::Arc::new(ReplacedBlob),
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
