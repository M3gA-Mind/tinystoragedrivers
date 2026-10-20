use super::*;
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    ops::Range,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_api::StreamStore;
use tinystoragedrivers_connect::MongoConnection;
use tinystoragedrivers_types::{
    Capability, CollectionSpec, ExpectedDocument, Filter, Precondition, ReadCheck, Scope,
    WatchNotice, WatchOptions, WriteOp, WriteResult,
};

#[derive(Debug, Default)]
pub(crate) struct TestBlobs {
    values: Mutex<BTreeMap<String, Vec<u8>>>,
    put_started: Option<Arc<tokio::sync::Notify>>,
    allow_put: Option<Arc<tokio::sync::Notify>>,
    put_order: Option<Arc<AtomicUsize>>,
    first_put_started: Option<Arc<tokio::sync::Notify>>,
    allow_first_put: Option<Arc<tokio::sync::Notify>>,
    fail_next_put: Option<Arc<AtomicBool>>,
    delete_started: Option<Arc<tokio::sync::Notify>>,
    allow_delete: Option<Arc<tokio::sync::Notify>>,
    delete_order: Option<Arc<AtomicUsize>>,
}

#[tinystoragedrivers_api::async_trait]
impl BlobStore for TestBlobs {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        tinystoragedrivers_types::Capabilities::none()
    }
    fn max_key_len(&self) -> usize {
        tinystoragedrivers_types::MAX_KEY_LEN
    }
    fn supports_sealed_blobs(&self) -> bool {
        true
    }
    async fn put(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::BlobMeta> {
        if let Some(order) = &self.put_order {
            if order.fetch_add(1, Ordering::SeqCst) == 0 {
                if let Some(started) = &self.first_put_started {
                    started.notify_one();
                }
                if let Some(allow) = &self.allow_first_put {
                    allow.notified().await;
                }
            }
        } else {
            if let Some(started) = &self.put_started {
                started.notify_one();
            }
            if let Some(allow) = &self.allow_put {
                allow.notified().await;
            }
        }
        if self
            .fail_next_put
            .as_ref()
            .is_some_and(|fail| fail.swap(false, Ordering::SeqCst))
        {
            return Err(tinystoragedrivers_types::StorageError::backend(
                "injected blob upload failure",
            ));
        }
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), bytes.clone());
        Ok(tinystoragedrivers_types::BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: content_type.map(str::to_owned),
        })
    }
    async fn get(
        &self,
        key: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::Blob>> {
        Ok(self.values.lock().unwrap().get(key).cloned().map(|bytes| {
            tinystoragedrivers_types::Blob {
                meta: tinystoragedrivers_types::BlobMeta {
                    key: key.to_owned(),
                    len: bytes.len() as u64,
                    content_type: Some("application/zstd".to_owned()),
                },
                bytes,
            }
        }))
    }
    async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> tinystoragedrivers_types::Result<Option<Vec<u8>>> {
        Ok(self.values.lock().unwrap().get(key).map(|bytes| {
            let start = usize::try_from(range.start.min(bytes.len() as u64)).unwrap();
            let end = usize::try_from(range.end.min(bytes.len() as u64)).unwrap();
            bytes[start..end].to_vec()
        }))
    }
    async fn head(
        &self,
        key: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::BlobMeta>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .get(key)
            .map(|bytes| tinystoragedrivers_types::BlobMeta {
                key: key.to_owned(),
                len: bytes.len() as u64,
                content_type: Some("application/zstd".to_owned()),
            }))
    }
    async fn delete(&self, key: &str) -> tinystoragedrivers_types::Result<bool> {
        if self
            .delete_order
            .as_ref()
            .is_none_or(|order| order.fetch_add(1, Ordering::SeqCst) == 0)
        {
            if let Some(started) = &self.delete_started {
                started.notify_one();
            }
            if let Some(allow) = &self.allow_delete {
                allow.notified().await;
            }
        }
        Ok(self.values.lock().unwrap().remove(key).is_some())
    }
    async fn list(
        &self,
        prefix: &str,
    ) -> tinystoragedrivers_types::Result<Vec<tinystoragedrivers_types::BlobMeta>> {
        Ok(self
            .values
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, bytes)| tinystoragedrivers_types::BlobMeta {
                key: key.clone(),
                len: bytes.len() as u64,
                content_type: Some("application/zstd".to_owned()),
            })
            .collect())
    }
}

#[tokio::test]
async fn deleting_an_old_stream_generation_preserves_a_recreated_sealed_blob() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_delete_recreate_race").await;
    let delete_started = Arc::new(tokio::sync::Notify::new());
    let allow_delete = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        values: Mutex::default(),
        put_started: None,
        allow_put: None,
        delete_started: Some(Arc::clone(&delete_started)),
        allow_delete: Some(Arc::clone(&allow_delete)),
        delete_order: Some(Arc::new(AtomicUsize::new(0))),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append("events", serde_json::json!("old"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();

    let deleting = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.delete_stream("events").await })
    };
    delete_started.notified().await;
    streams
        .append("events", serde_json::json!("new generation"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();
    allow_delete.notify_one();
    assert!(deleting.await.unwrap().unwrap());

    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("new generation"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn deleting_generation_one_does_not_delete_generation_ten_blob() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_delete_prefix_collision").await;
    let blobs = Arc::new(TestBlobs::default());
    let blob_store: Arc<dyn BlobStore> = blobs.clone();
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blob_store);
    streams
        .append("events", serde_json::json!("first incarnation"))
        .await
        .unwrap();
    assert!(streams.delete_stream("events").await.unwrap());
    streams
        .append("events", serde_json::json!("generation one"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();

    let generation_ten = format!("{}/g10", crate::sealed_blob_key("events").unwrap());
    blobs
        .values
        .lock()
        .unwrap()
        .insert(generation_ten.clone(), vec![1, 2, 3]);
    assert!(streams.delete_stream("events").await.unwrap());
    assert!(blobs.head(&generation_ten).await.unwrap().is_some());
    connection.database().drop().await.unwrap();
}

async fn test_connection(uri: &str, stem: &str) -> MongoConnection {
    let database = format!(
        "{stem}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |time| time.as_millis())
    );
    MongoConnection::connect(uri, &database)
        .await
        .expect("test MongoDB is available")
}

#[tokio::test]
async fn mongo_stream_seal_uses_the_real_scope_bound_blob_engine() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_real_blob_seal").await;
    let scope = Scope::new("scope-a").unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(tinystoragedrivers_blob::MongoBlobStore::new(
        connection.clone(),
        scope.clone(),
    ));
    let streams = MongoStreams::new(connection.clone(), scope).with_blob_store(Arc::clone(&blobs));
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("first"), serde_json::json!("second")],
        )
        .await
        .unwrap();
    assert!(streams.capabilities().contains(Capability::StreamSeal));
    let sealed = streams.seal("events").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (0, 2));
    assert_eq!(streams.seal("events").await.unwrap(), sealed);
    assert_eq!(streams.read_window("events", 0, 10).await.unwrap().len(), 2);

    let key = format!("{}/g0", crate::sealed_blob_key("events").unwrap());
    assert_eq!(
        blobs.get(&key).await.unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );
    assert!(blobs.list("").await.unwrap().is_empty());
    assert!(
        blobs
            .list_page("", None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    let other_scope = tinystoragedrivers_blob::MongoBlobStore::new(
        connection.clone(),
        Scope::new("scope-b").unwrap(),
    );
    assert_eq!(other_scope.get_sealed(&key).await.unwrap(), None);
    assert!(streams.delete_stream("events").await.unwrap());
    assert_eq!(blobs.head_sealed(&key).await.unwrap(), None);
    connection.database().drop().await.unwrap();
}

/// Multiple append batches keep the stream's first retained offset at zero.
#[tokio::test]
async fn stream_info_base_survives_multiple_append_batches() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_base").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap());

    streams
        .append_batch("events", vec![serde_json::json!("first")])
        .await
        .unwrap();
    streams
        .append_batch("events", vec![serde_json::json!("second")])
        .await
        .unwrap();

    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!(info.base, 0);
    assert_eq!(info.len, 2);

    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn truncating_entire_mongo_stream_preserves_its_next_append_offset() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_truncate").await;
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap());
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();

    assert_eq!(streams.truncate_before("events", 20).await.unwrap(), 2);
    assert_eq!(streams.len("events").await.unwrap(), 2);
    assert!(
        streams
            .read_window("events", 0, 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        streams
            .append("events", serde_json::json!("c"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap()[0].offset,
        2
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn mongo_seal_recovers_from_marker_and_preserves_blob_backed_offsets() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stream_seal").await;
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();
    assert_eq!(streams.truncate_before("events", 1).await.unwrap(), 1);
    let sealed = streams.seal("events").await.unwrap();
    assert_eq!(sealed.base, 1);
    assert_eq!(sealed.len, 2);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("b"),
        }]
    );
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!(info.base, 1);
    assert!(info.sealed_ms.is_some());
    connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_streams")
        .insert_one(mongodb::bson::doc! {
            "_scope": "scope-a", "s": "events", "g": 0_i64,
            "o": 0_i64, "n": 1_i64, "e": 1_i64, "t": 0_i64,
            "vs": ["left by interrupted seal"],
        })
        .await
        .unwrap();
    assert_eq!(
        streams
            .append("events", serde_json::json!("c"))
            .await
            .unwrap_err()
            .kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(streams.seal("events").await.unwrap(), sealed);
    assert!(
        streams
            .read_window("events", 0, 10)
            .await
            .unwrap()
            .iter()
            .all(|entry| entry.value != serde_json::json!("left by interrupted seal"))
    );
    assert!(
        streams
            .delete_sealed_stream("events", info.sealed_ms.unwrap())
            .await
            .unwrap()
    );
    assert_eq!(streams.len("events").await.unwrap(), 0);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn failed_blob_upload_leaves_stream_unsealed_and_retryable() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_upload_failure").await;
    let blobs = Arc::new(TestBlobs {
        fail_next_put: Some(Arc::new(AtomicBool::new(true))),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append("events", serde_json::json!("still readable"))
        .await
        .unwrap();

    let failure = streams.seal("events").await.unwrap_err();
    assert!(failure.to_string().contains("injected blob upload failure"));
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("still readable"),
        }]
    );
    assert_eq!(
        streams.info("events").await.unwrap().unwrap().sealed_ms,
        None
    );

    assert_eq!(streams.seal("events").await.unwrap().len, 1);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn standalone_truncate_and_seal_linearize_without_losing_the_truncated_view() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_standalone_truncate_seal").await;
    let truncate_started = Arc::new(tokio::sync::Notify::new());
    let resume_truncate = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs::default());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs)
        .without_transactions_for_test()
        .pause_truncate_for_test(Arc::clone(&truncate_started), Arc::clone(&resume_truncate));
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("discard"), serde_json::json!("keep")],
        )
        .await
        .unwrap();

    let truncating = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.truncate_before("events", 1).await })
    };
    truncate_started.notified().await;
    let sealed = streams.seal("events").await.unwrap();
    assert_eq!((sealed.base, sealed.len), (1, 2));
    resume_truncate.notify_one();
    assert_eq!(
        truncating.await.unwrap().unwrap_err().kind(),
        tinystoragedrivers_types::ErrorKind::Conflict
    );
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("keep"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn transactional_truncation_invalidates_a_seal_snapshot_taken_before_blob_publish() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_truncate_race").await;
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        values: Mutex::default(),
        put_started: Some(Arc::clone(&put_started)),
        allow_put: Some(Arc::clone(&allow_put)),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append_batch(
            "events",
            vec![serde_json::json!("a"), serde_json::json!("b")],
        )
        .await
        .unwrap();

    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    put_started.notified().await;
    assert_eq!(streams.truncate_before("events", 1).await.unwrap(), 1);
    allow_put.notify_one();

    let error = sealing.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!((info.base, info.len, info.sealed_ms), (1, 2, None));
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 1,
            value: serde_json::json!("b"),
        }]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn concurrent_sealers_keep_the_published_generation_blob_readable() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_race").await;
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(Arc::new(TestBlobs {
            values: Mutex::default(),
            put_started: Some(Arc::clone(&put_started)),
            allow_put: Some(Arc::clone(&allow_put)),
            ..Default::default()
        }));
    streams
        .append_batch("events", vec![serde_json::json!("first")])
        .await
        .unwrap();

    let first = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    let second = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    put_started.notified().await;
    put_started.notified().await;
    allow_put.notify_waiters();
    let first = first.await.unwrap();
    let second = second.await.unwrap();
    assert!(
        first.is_ok() || second.is_ok(),
        "first={first:?}, second={second:?}"
    );
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![tinystoragedrivers_types::StreamEntry {
            offset: 0,
            value: serde_json::json!("first"),
        }]
    );
    let info = streams.info("events").await.unwrap().unwrap();
    assert!(info.sealed_ms.is_some());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn append_winning_the_end_offset_prevents_a_stale_seal_marker() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_seal_append_race").await;
    let put_started = Arc::new(tokio::sync::Notify::new());
    let allow_put = Arc::new(tokio::sync::Notify::new());
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(Arc::new(TestBlobs {
            values: Mutex::default(),
            put_started: Some(Arc::clone(&put_started)),
            allow_put: Some(Arc::clone(&allow_put)),
            ..Default::default()
        }));
    streams
        .append("events", serde_json::json!("first"))
        .await
        .unwrap();

    let sealing = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    put_started.notified().await;
    assert_eq!(
        streams
            .append("events", serde_json::json!("second"))
            .await
            .unwrap(),
        1
    );
    allow_put.notify_one();

    let error = sealing.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    let info = streams.info("events").await.unwrap().unwrap();
    assert_eq!((info.base, info.len, info.sealed_ms), (0, 2, None));
    assert_eq!(streams.read_window("events", 0, 10).await.unwrap().len(), 2);
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn stale_seal_attempt_cannot_replace_the_newer_published_blob() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_stale_seal_blob").await;
    let first_put_started = Arc::new(tokio::sync::Notify::new());
    let allow_first_put = Arc::new(tokio::sync::Notify::new());
    let blobs = Arc::new(TestBlobs {
        values: Mutex::default(),
        put_order: Some(Arc::new(AtomicUsize::new(0))),
        first_put_started: Some(Arc::clone(&first_put_started)),
        allow_first_put: Some(Arc::clone(&allow_first_put)),
        ..Default::default()
    });
    let streams = MongoStreams::new(connection.clone(), Scope::new("scope-a").unwrap())
        .with_blob_store(blobs);
    streams
        .append("events", serde_json::json!("first snapshot"))
        .await
        .unwrap();
    let stale = {
        let streams = streams.clone();
        tokio::spawn(async move { streams.seal("events").await })
    };
    first_put_started.notified().await;

    streams
        .append("events", serde_json::json!("newer snapshot"))
        .await
        .unwrap();
    streams.seal("events").await.unwrap();
    allow_first_put.notify_one();
    let error = stale.await.unwrap().unwrap_err();
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    assert_eq!(
        streams.read_window("events", 0, 10).await.unwrap(),
        vec![
            tinystoragedrivers_types::StreamEntry {
                offset: 0,
                value: serde_json::json!("first snapshot"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 1,
                value: serde_json::json!("newer snapshot"),
            },
        ]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn collection_declarations_accumulate_and_enumeration_is_scope_bound() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_document_specs").await;
    let scope_a = Scope::new("scope-a").unwrap();
    let scope_b = Scope::new("scope-b").unwrap();
    let docs_a = MongoDocuments::new(connection.clone(), scope_a);
    let docs_b = MongoDocuments::new(connection.clone(), scope_b);
    let broad = CollectionSpec::new("records").searchable(["name"]);
    let narrow = CollectionSpec::new("records");

    docs_a.ensure_collection(&broad).await.unwrap();
    docs_a.ensure_collection(&narrow).await.unwrap();
    docs_a.validate_collection_spec(&broad).await.unwrap();
    docs_a
        .put(
            "records",
            "one",
            serde_json::json!({"name":"alpha"}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_b
        .put(
            "records",
            "two",
            serde_json::json!({"name":"beta"}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_b
        .put(
            "other_only",
            "three",
            serde_json::json!({"name":"gamma"}),
            Precondition::None,
        )
        .await
        .unwrap();

    assert_eq!(
        docs_a.collections("").await.unwrap(),
        vec!["records".to_owned()]
    );
    assert_eq!(
        docs_b.collections("").await.unwrap(),
        vec!["other_only".to_owned(), "records".to_owned()]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn reads_the_legacy_mongodb_document_shape() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_legacy_document")
        .await
        .with_clock(Arc::new(|| 1_000));
    let scope = Scope::new("legacy-scope").unwrap();
    let collection = connection
        .database()
        .collection::<mongodb::bson::Document>("legacy");
    collection
        .insert_one(mongodb::bson::doc! {
            "_id": {"s": scope.as_str(), "k": "known"},
            "_scope": scope.as_str(),
            "_key": "known",
            "_v": 7_i64,
            "d": {"value": "from-1.0"},
            "_schema": 3_i64,
            "_c": 100_i64,
            "_u": 125_i64,
        "_x": 9_999_i64,
        })
        .await
        .unwrap();
    let docs = MongoDocuments::new(connection.clone(), scope);

    let found = docs.get("legacy", "known").await.unwrap().unwrap();
    assert_eq!(found.version.0, 7);
    assert_eq!(found.doc, serde_json::json!({"value":"from-1.0"}));
    assert_eq!(found.meta.schema, 3);
    assert_eq!(found.meta.created_ms, 100);
    assert_eq!(found.meta.updated_ms, 125);
    assert_eq!(found.meta.expires_ms, Some(9_999));
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn reads_legacy_mongodb_stream_segments_in_place() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_legacy_stream").await;
    let scope = Scope::new("legacy-scope").unwrap();
    let segments = connection
        .database()
        .collection::<mongodb::bson::Document>("_tsd_streams");
    segments
        .insert_one(mongodb::bson::doc! {
            "_id": mongodb::bson::oid::ObjectId::new(),
            "_scope": scope.as_str(),
            "s": "legacy/events",
            "g": 0_i64,
            "o": 4_i64,
            "n": 2_i64,
            "e": 6_i64,
            "t": 0_i64,
            "vs": ["from-1.0-a", "from-1.0-b"],
        })
        .await
        .unwrap();

    let streams = MongoStreams::new(connection.clone(), scope);
    assert_eq!(streams.len("legacy/events").await.unwrap(), 6);
    assert_eq!(
        streams.read_window("legacy/events", 0, 10).await.unwrap(),
        vec![
            tinystoragedrivers_types::StreamEntry {
                offset: 4,
                value: serde_json::json!("from-1.0-a"),
            },
            tinystoragedrivers_types::StreamEntry {
                offset: 5,
                value: serde_json::json!("from-1.0-b"),
            },
        ]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn ttl_fields_expire_and_declared_text_search_is_scoped() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_ttl_search")
        .await
        .with_clock(Arc::new(|| 100));
    let scope_a = Scope::new("scope-a").unwrap();
    let scope_b = Scope::new("scope-b").unwrap();
    let docs_a = MongoDocuments::new(connection.clone(), scope_a);
    let docs_b = MongoDocuments::new(connection.clone(), scope_b);
    let spec = CollectionSpec::new("items")
        .ttl("expires_at")
        .searchable(["name"]);
    docs_a.ensure_collection(&spec).await.unwrap();
    docs_a
        .put(
            "items",
            "live",
            serde_json::json!({"name":"blue comet","expires_at":101}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_a
        .put(
            "items",
            "expired",
            serde_json::json!({"name":"blue comet","expires_at":100}),
            Precondition::None,
        )
        .await
        .unwrap();
    docs_b
        .put(
            "items",
            "other-scope",
            serde_json::json!({"name":"blue comet","expires_at":101}),
            Precondition::None,
        )
        .await
        .unwrap();

    assert!(docs_a.get("items", "expired").await.unwrap().is_none());
    assert_eq!(docs_a.count("items", &Filter::All).await.unwrap(), 1);
    assert_eq!(
        docs_a
            .search("items", "blue comet", 10)
            .await
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect::<Vec<_>>(),
        vec!["live"]
    );
    connection.database().drop().await.unwrap();
}

#[tokio::test]
async fn validated_batches_commit_together_and_roll_back_on_conflict() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_atomic_batch").await;
    assert!(connection.transactions_available());
    let docs = MongoDocuments::new(connection.clone(), Scope::new("scope-a").unwrap());
    let spec = CollectionSpec::new("records");
    docs.ensure_collection(&spec).await.unwrap();
    let version = docs
        .put(
            "records",
            "anchor",
            serde_json::json!({"state":"before"}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    let results = docs
        .atomic_batch_checked(
            vec![ReadCheck {
                collection: "records".to_owned(),
                id: "anchor".to_owned(),
                expected: ExpectedDocument::Version(version),
            }],
            vec![
                WriteOp::Put {
                    collection: "records".to_owned(),
                    id: "anchor".to_owned(),
                    doc: serde_json::json!({"state":"after"}),
                    precondition: Precondition::Version(version),
                },
                WriteOp::Put {
                    collection: "records".to_owned(),
                    id: "created".to_owned(),
                    doc: serde_json::json!({"state":"created"}),
                    precondition: Precondition::Absent,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 2);
    assert!(matches!(results[0], WriteResult::Put { .. }));
    assert!(matches!(results[1], WriteResult::Put { .. }));
    let anchor = docs.get("records", "anchor").await.unwrap().unwrap();
    assert_eq!(anchor.doc, serde_json::json!({"state":"after"}));

    let failed = docs
        .atomic_batch(vec![
            WriteOp::Put {
                collection: "records".to_owned(),
                id: "rolled-back".to_owned(),
                doc: serde_json::json!({"state":"temporary"}),
                precondition: Precondition::Absent,
            },
            WriteOp::Put {
                collection: "records".to_owned(),
                id: "anchor".to_owned(),
                doc: serde_json::json!({"state":"conflict"}),
                precondition: Precondition::Absent,
            },
        ])
        .await;
    assert!(failed.is_err());
    assert!(docs.get("records", "rolled-back").await.unwrap().is_none());
    connection.database().drop().await.unwrap();
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the watch regression pins scope filtering, resume binding, and reconnect behavior together"
)]
async fn native_watch_invalidates_for_scope_mutations_and_rejects_cross_binding_resume() {
    let Ok(uri) = std::env::var("TSD_MONGO_URL") else {
        return;
    };
    let connection = test_connection(&uri, "tsd26_watch").await;
    assert!(connection.transactions_available());
    let scope_a = Scope::new("watch-a").unwrap();
    let docs_a = MongoDocuments::new(connection.clone(), scope_a.clone());
    let docs_b = MongoDocuments::new(connection.clone(), Scope::new("watch-b").unwrap());
    let mut watch = docs_a
        .watch("records", &Filter::All, &WatchOptions::default())
        .await
        .unwrap();

    docs_b
        .put(
            "records",
            "other-scope",
            serde_json::json!({"value":1}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), watch.next())
            .await
            .is_err()
    );

    docs_a
        .put(
            "records",
            "same-scope",
            serde_json::json!({"value":2}),
            Precondition::None,
        )
        .await
        .unwrap();
    let notice = tokio::time::timeout(std::time::Duration::from_secs(5), watch.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let WatchNotice::Invalidated {
        cursor: Some(cursor),
    } = notice
    else {
        panic!("expected a resumable scope invalidation");
    };

    let other_connection = MongoConnection::connect(&uri, connection.database().name())
        .await
        .unwrap();
    let other_docs = MongoDocuments::new(other_connection, scope_a.clone());
    let rejected_other_connection = other_docs
        .watch(
            "records",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor.clone()),
                interval: std::time::Duration::from_secs(1),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        rejected_other_connection.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );

    let mut resumed = docs_a
        .watch(
            "records",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor.clone()),
                interval: std::time::Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    let rejected = docs_a
        .watch(
            "other",
            &Filter::All,
            &WatchOptions {
                after: Some(cursor),
                interval: std::time::Duration::from_secs(1),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        rejected.kind(),
        tinystoragedrivers_types::ErrorKind::InvalidInput
    );

    docs_a
        .put(
            "records",
            "after-resume",
            serde_json::json!({"value":3}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_secs(5), resumed.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        WatchNotice::Invalidated { .. }
    ));
    connection.database().drop().await.unwrap();
}
