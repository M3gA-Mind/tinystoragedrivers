use super::*;
use std::{sync::Arc, time::Duration};
use tinystoragedrivers_api::KvStore;
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
