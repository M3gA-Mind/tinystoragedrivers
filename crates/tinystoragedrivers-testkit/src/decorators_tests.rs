use super::*;
use std::{sync::Arc, time::Duration};
use tinystoragedrivers_api::{
    BlobStore, DocumentStore, KvStore, QueueStore, SecretStore, StreamStore,
};
use tinystoragedrivers_types::{ErrorKind, Scope};

#[test]
fn conflict_once_fails_exactly_the_first_checked_operation() {
    let decorator = ConflictOnce::new("adapter");
    assert_eq!(decorator.check().unwrap_err().kind(), ErrorKind::Conflict);
    assert!(decorator.check().is_ok());
    assert!(decorator.check().is_ok());
}

#[tokio::test(start_paused = true)]
async fn slow_runs_the_operation_after_its_configured_delay_hook() {
    let decorator = Slow::new(7, Duration::from_millis(20));
    let operation = tokio::spawn(async move { decorator.run(async { decorator.inner + 1 }).await });
    tokio::task::yield_now().await;
    assert!(
        !operation.is_finished(),
        "delay hook waits before delegation"
    );
    tokio::time::advance(Duration::from_millis(20)).await;
    assert_eq!(operation.await.unwrap(), 8);
}

#[tokio::test]
async fn conflict_once_call_injects_then_delegates() {
    let decorator = ConflictOnce::new(());
    assert_eq!(
        decorator.call(async { Ok(1) }).await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    assert_eq!(decorator.call(async { Ok(2) }).await.unwrap(), 2);
}

#[tokio::test]
async fn failing_is_a_kv_trait_object_decorator_and_preserves_capabilities() {
    let scope = Scope::new("failing_adapter").unwrap();
    let storage = memory_storage(&scope);
    let inner = Arc::clone(storage.kv());
    let capabilities = inner.capabilities();
    let failing = Failing::new(inner);
    failing
        .faults
        .fail_operation_nth("kv.put", 1, ErrorKind::Unavailable);
    let port: &dyn KvStore = &failing;
    assert_eq!(port.capabilities(), capabilities);
    assert_eq!(
        port.put("k", serde_json::json!(1))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unavailable
    );
    assert!(port.put("k", serde_json::json!(2)).await.is_ok());
}

#[tokio::test]
async fn failing_trait_object_clone_shares_backend_and_fault_schedule() {
    let scope = Scope::new("failing_clone_adapter").unwrap();
    let storage = memory_storage(&scope);
    let inner: Arc<dyn KvStore> = Arc::clone(storage.kv()) as Arc<dyn KvStore>;
    let failing: Failing<dyn KvStore> = Failing::new(inner);
    let clone = failing.clone();
    failing
        .faults
        .fail_operation_nth("kv.put", 1, ErrorKind::Unavailable);
    assert_eq!(
        clone
            .put("k", serde_json::json!(1))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unavailable
    );
    assert!(failing.put("k", serde_json::json!(2)).await.is_ok());
    assert_eq!(
        storage.kv().get("k").await.unwrap().unwrap().doc,
        serde_json::json!(2)
    );
}

#[tokio::test(start_paused = true)]
async fn slow_is_a_kv_trait_object_decorator() {
    let scope = Scope::new("slow_adapter").unwrap();
    let storage = memory_storage(&scope);
    let inner = Arc::clone(storage.kv());
    inner.put("k", serde_json::json!(1)).await.unwrap();
    let slow = Slow::new(inner, Duration::from_millis(10));
    let read = tokio::spawn(async move { slow.get("k").await });
    tokio::task::yield_now().await;
    assert!(!read.is_finished());
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(read.await.unwrap().unwrap().is_some());
}

#[tokio::test]
async fn slow_gate_holds_and_releases_a_kv_operation_deterministically() {
    let scope = Scope::new("slow_gate_adapter").unwrap();
    let storage = memory_storage(&scope);
    let inner = Arc::clone(storage.kv());
    inner.put("k", serde_json::json!(1)).await.unwrap();
    let slow = Arc::new(Slow::gated(inner));
    let operation = {
        let slow = Arc::clone(&slow);
        tokio::spawn(async move { slow.get("k").await })
    };
    tokio::task::yield_now().await;
    assert!(
        !operation.is_finished(),
        "operation waits at the adapter gate"
    );
    slow.release_one();
    assert!(operation.await.unwrap().unwrap().is_some());
}

#[tokio::test]
async fn conflict_once_is_a_kv_trait_object_decorator() {
    let scope = Scope::new("conflict_once_adapter").unwrap();
    let storage = memory_storage(&scope);
    let conflict = ConflictOnce::new(Arc::clone(storage.kv()));
    let port: &dyn KvStore = &conflict;
    assert_eq!(
        port.put("k", serde_json::json!(1))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    assert!(port.put("k", serde_json::json!(2)).await.is_ok());
}

#[tokio::test]
async fn slow_and_conflict_once_forward_the_complete_kv_surface() {
    let scope = Scope::new("decorated_kv_surface").unwrap();
    let storage = memory_storage(&scope);
    let slow: Arc<dyn KvStore> = Arc::new(Slow::new(Arc::clone(storage.kv()), Duration::ZERO));
    conformance::kv::run(slow.as_ref()).await;

    let conflict = ConflictOnce::new(Arc::clone(storage.kv()));
    assert_eq!(
        conflict
            .put("prime", serde_json::json!(0))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    conformance::kv::run(&conflict).await;
}

#[tokio::test]
async fn slow_and_conflict_once_forward_queue_stream_and_secret_ports() {
    let scope = Scope::new("decorated_other_ports").unwrap();
    let storage = memory_storage(&scope);

    let slow_queue: Arc<dyn QueueStore> =
        Arc::new(Slow::new(Arc::clone(storage.queues()), Duration::ZERO));
    conformance::queue::run(slow_queue.as_ref()).await;
    let conflict_queue = ConflictOnce::new(Arc::clone(storage.queues()));
    assert_eq!(
        conflict_queue.purge("initial").await.unwrap_err().kind(),
        ErrorKind::Conflict
    );
    conformance::queue::run(&conflict_queue).await;

    let slow_stream: Arc<dyn StreamStore> =
        Arc::new(Slow::new(Arc::clone(storage.streams()), Duration::ZERO));
    conformance::streams::run(slow_stream.as_ref()).await;
    let conflict_stream = ConflictOnce::new(Arc::clone(storage.streams()));
    assert_eq!(
        conflict_stream
            .append("initial", serde_json::json!(0))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    conformance::streams::run(&conflict_stream).await;

    let slow_secrets: Arc<dyn SecretStore> =
        Arc::new(Slow::new(Arc::clone(storage.secrets()), Duration::ZERO));
    conformance::secrets::run(slow_secrets.as_ref()).await;
    let conflict_secrets = ConflictOnce::new(Arc::clone(storage.secrets()));
    assert_eq!(
        conflict_secrets
            .set("initial", b"x")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    conformance::secrets::run(&conflict_secrets).await;
}

#[tokio::test]
async fn slow_and_conflict_once_forward_document_port() {
    let scope = Scope::new("decorated_documents").unwrap();
    let storage = memory_storage(&scope);
    let slow: Arc<dyn DocumentStore> =
        Arc::new(Slow::new(Arc::clone(storage.documents()), Duration::ZERO));
    conformance::documents::run(slow.as_ref()).await;
    conformance::validated::run(slow.as_ref()).await;
    conformance::semantics::run(slow.as_ref()).await;
    conformance::watch::run(slow.as_ref()).await;
    let conflict = ConflictOnce::new(Arc::clone(storage.documents()));
    assert_eq!(
        conflict
            .ensure_collection(&tinystoragedrivers_types::CollectionSpec::new("initial"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    conformance::documents::run(&conflict).await;
    conformance::validated::run(&conflict).await;
    conformance::semantics::run(&conflict).await;
    conformance::watch::run(&conflict).await;
}

#[tokio::test]
async fn slow_and_conflict_once_forward_blob_port() {
    let scope = Scope::new("decorated_blobs").unwrap();
    let storage = memory_storage(&scope);
    let slow: Arc<dyn BlobStore> = Arc::new(Slow::new(Arc::clone(storage.blobs()), Duration::ZERO));
    conformance::blobs::run(slow.as_ref()).await;
    let _ = slow
        .put_expiring("ttl", vec![2], None, Duration::from_millis(10))
        .await;
    let _ = slow
        .put_stream_expiring(
            "stream-ttl",
            Box::pin(futures_util::stream::empty::<
                tinystoragedrivers_types::Result<bytes::Bytes>,
            >()),
            0,
            None,
            Duration::from_millis(10),
        )
        .await;
    let _ = slow
        .put_tree(
            "tree",
            vec![tinystoragedrivers_types::TreeFile::new("child", vec![3])],
        )
        .await;
    let _ = slow.delete_tree("tree").await;
    let conflict = ConflictOnce::new(Arc::clone(storage.blobs()));
    assert_eq!(
        conflict
            .put("initial", vec![1], None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    conformance::blobs::run(&conflict).await;
    let _ = conflict
        .put_stream_expiring(
            "stream-ttl",
            Box::pin(futures_util::stream::empty::<
                tinystoragedrivers_types::Result<bytes::Bytes>,
            >()),
            0,
            None,
            Duration::from_millis(10),
        )
        .await;
    let _ = conflict.sweep_expired().await;
}

#[tokio::test]
async fn blob_decorators_forward_sealed_hooks_and_injected_failures() {
    let scope = Scope::new("decorated_sealed_blobs").unwrap();
    let storage = memory_storage(&scope);
    let key = "_tsd/sealed/events/g0";

    let failing = Failing::new(Arc::clone(storage.blobs()));
    assert!(failing.supports_sealed_blobs());
    failing
        .faults
        .fail_operation_nth("blobs.put_sealed", 1, ErrorKind::Unavailable);
    assert_eq!(
        failing
            .put_sealed(key, b"sealed".to_vec(), Some("application/zstd"))
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unavailable
    );
    failing
        .put_sealed(key, b"sealed".to_vec(), Some("application/zstd"))
        .await
        .unwrap();
    assert_eq!(
        failing.get_sealed(key).await.unwrap().unwrap().bytes,
        b"sealed"
    );
    assert_eq!(failing.head_sealed(key).await.unwrap().unwrap().len, 6);
    assert_eq!(
        failing
            .list_sealed("_tsd/sealed/events/")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(failing.delete_sealed(key).await.unwrap());

    let slow = Slow::new(Arc::clone(storage.blobs()), Duration::ZERO);
    slow.put_sealed(key, b"slow".to_vec(), None).await.unwrap();
    assert_eq!(slow.get_sealed(key).await.unwrap().unwrap().bytes, b"slow");
    assert_eq!(slow.head_sealed(key).await.unwrap().unwrap().len, 4);
    assert_eq!(
        slow.list_sealed("_tsd/sealed/events/").await.unwrap().len(),
        1
    );
    assert!(slow.delete_sealed(key).await.unwrap());

    let conflict = ConflictOnce::new(Arc::clone(storage.blobs()));
    assert_eq!(
        conflict
            .put_sealed(key, b"retry".to_vec(), None)
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Conflict
    );
    conflict
        .put_sealed(key, b"retry".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        conflict.get_sealed(key).await.unwrap().unwrap().bytes,
        b"retry"
    );
    assert_eq!(conflict.head_sealed(key).await.unwrap().unwrap().len, 5);
    assert_eq!(
        conflict
            .list_sealed("_tsd/sealed/events/")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(conflict.delete_sealed(key).await.unwrap());
    assert_eq!(
        storage.blobs().list("").await.unwrap(),
        Vec::<tinystoragedrivers_types::BlobMeta>::new()
    );
}

#[test]
fn decorator_port_metadata_matches_the_wrapped_engines() {
    let scope = Scope::new("decorated_port_metadata").unwrap();
    let storage = memory_storage(&scope);
    let kv_caps = storage.kv().capabilities();
    assert_eq!(
        Slow::new(Arc::clone(storage.kv()), Duration::ZERO).capabilities(),
        kv_caps
    );
    assert_eq!(
        ConflictOnce::new(Arc::clone(storage.kv())).capabilities(),
        kv_caps
    );
    let stream_caps = storage.streams().capabilities();
    assert_eq!(
        Slow::new(Arc::clone(storage.streams()), Duration::ZERO).capabilities(),
        stream_caps
    );
    assert_eq!(
        ConflictOnce::new(Arc::clone(storage.streams())).capabilities(),
        stream_caps
    );
    let document_caps = storage.documents().capabilities();
    assert_eq!(
        Slow::new(Arc::clone(storage.documents()), Duration::ZERO).capabilities(),
        document_caps
    );
    assert_eq!(
        ConflictOnce::new(Arc::clone(storage.documents())).capabilities(),
        document_caps
    );
    let blob_caps = storage.blobs().capabilities();
    assert_eq!(
        Slow::new(Arc::clone(storage.blobs()), Duration::ZERO).capabilities(),
        blob_caps
    );
    assert_eq!(
        ConflictOnce::new(Arc::clone(storage.blobs())).capabilities(),
        blob_caps
    );
    assert_eq!(
        Slow::new(Arc::clone(storage.queues()), Duration::ZERO).features(),
        storage.queues().features()
    );
    assert_eq!(
        ConflictOnce::new(Arc::clone(storage.queues())).features(),
        storage.queues().features()
    );
    assert_eq!(
        Slow::new(Arc::clone(storage.secrets()), Duration::ZERO).backend_name(),
        storage.secrets().backend_name()
    );
    assert_eq!(
        ConflictOnce::new(Arc::clone(storage.secrets())).backend_name(),
        storage.secrets().backend_name()
    );
}
