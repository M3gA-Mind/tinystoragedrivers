//! Revocation drains real native work; fencing rejects its late publication.
use super::*;
use std::{future::Future, task::Poll};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{CollectionSpec, Result};
use tinystoragedrivers_secrets::{DocumentSecrets, SecretStore, StaticKey, Zeroizing};

#[derive(Debug)]
struct GatedSecrets {
    inner: Arc<dyn SecretStore>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl SecretStore for GatedSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.inner.get(name).await
    }
    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        self.entered.notify_one();
        let _permit = self.release.acquire().await.unwrap();
        self.inner.set(name, value).await
    }
    async fn delete(&self, name: &str) -> Result<bool> {
        self.inner.delete(name).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    fn backend_name(&self) -> &'static str {
        "gated-document-proof"
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoke_waits_for_native_callback_and_fence_blocks_late_secret_write() {
    let backend = MemoryStorage::new();
    let guard_scope = Scope::new("guard").unwrap();
    let guard = backend.for_scope(&guard_scope).unwrap();
    guard
        .documents()
        .put("leases", "active", json!({"epoch":1}), Precondition::None)
        .await
        .unwrap();
    let storage = backend
        .for_scope_fenced(
            &Scope::new("alice").unwrap(),
            &Fence::epoch(guard_scope, "leases", "active", "epoch", 1),
        )
        .unwrap();
    storage
        .documents()
        .ensure_collection(&CollectionSpec::new("secrets"))
        .await
        .unwrap();
    let inner = Arc::new(DocumentSecrets::new(
        &storage,
        Arc::new(StaticKey::new(Zeroizing::new([7; 32]))),
    ));
    let gated = Arc::new(GatedSecrets {
        inner: inner.clone(),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let server = Arc::new(StorageServer::new(storage.clone()).with_secrets(gated.clone()));
    let fixture = Fixture::with_server(storage.documents().clone(), Some(server)).await;
    let proxy = fixture.proxy();
    let routing = route(&fixture);
    let call = tokio::spawn(async move {
        proxy
            .call_confidential::<serde_json::Value>(
                "ScopedCall",
                (
                    routing,
                    "Secrets",
                    "Set",
                    json!({"name":"late","value":[1,2,3]}),
                ),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), gated.entered.notified())
        .await
        .unwrap();
    assert_eq!(fixture.binding.guard().running(), 1);
    guard
        .documents()
        .put("leases", "active", json!({"epoch":2}), Precondition::None)
        .await
        .unwrap();
    fixture.binding.revoke();
    let live_guard = fixture.binding.guard();
    let mut drained = Box::pin(live_guard.drained());
    std::future::poll_fn(|cx| {
        assert!(drained.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    gated.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), drained)
        .await
        .unwrap();
    assert_eq!(live_guard.running(), 0);
    assert!(call.await.unwrap().is_err());
    assert!(inner.get("late").await.unwrap().is_none());
}
