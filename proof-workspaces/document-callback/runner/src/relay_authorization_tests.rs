//! An unrelated confidential caller cannot borrow the fixture module's authority.
use super::*;
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn outsider_cannot_relay_secret_reads_plain_calls_or_native_roundtrips() {
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::new("owner").unwrap()).unwrap();
    let secrets = Arc::new(MemorySecrets::new());
    secrets
        .set("cache_key", b"known-existing-cache-key")
        .await
        .unwrap();
    let server = Arc::new(StorageServer::new(storage.clone()).with_secrets(secrets.clone()));
    let fixture = Fixture::with_server(storage.documents().clone(), Some(server)).await;
    let outsider = fixture
        .attacker
        .proxy(
            fixture.binding.guard().module_peer().unwrap().as_str(),
            "/proof",
            fixture.interface,
        )
        .unwrap();
    let read = (
        route(&fixture),
        "Secrets",
        "Get",
        json!({"name":"cache_key"}),
    );
    let result = outsider
        .call_confidential::<serde_json::Value>("ScopedCall", read.clone())
        .await;
    assert!(
        result.is_err(),
        "outsider borrowed module authority to read the existing cache key"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("proof loading host required")
    );
    let error = outsider
        .call_confidential::<serde_json::Value>("ScopedPlainCall", read)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("proof loading host required"));
    let error = outsider
        .call_confidential::<serde_json::Value>("NativeRoundtrip", (route(&fixture),))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("proof loading host required"));
    let attempts_after: u64 = fixture
        .proxy()
        .call_confidential("CallbackAttempts", ())
        .await
        .unwrap();
    assert_eq!(
        attempts_after, 0,
        "outsider calls must be rejected before relay dispatch"
    );
    assert_eq!(
        secrets.get("cache_key").await.unwrap().unwrap().as_slice(),
        b"known-existing-cache-key"
    );
    assert_eq!(storage.streams().len("audit").await.unwrap(), 0);
    assert!(
        storage
            .documents()
            .get("cache", "key")
            .await
            .unwrap()
            .is_none()
    );
}

#[derive(Debug)]
struct CountedSecrets {
    inner: MemorySecrets,
    reads: std::sync::atomic::AtomicU64,
}
#[async_trait::async_trait]
impl SecretStore for CountedSecrets {
    async fn get(
        &self,
        name: &str,
    ) -> tinystoragedrivers_core::Result<Option<tinystoragedrivers_secrets::Zeroizing<Vec<u8>>>>
    {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(name).await
    }
    async fn set(&self, name: &str, value: &[u8]) -> tinystoragedrivers_core::Result<()> {
        self.inner.set(name, value).await
    }
    async fn delete(&self, name: &str) -> tinystoragedrivers_core::Result<bool> {
        self.inner.delete(name).await
    }
    async fn list(&self, prefix: &str) -> tinystoragedrivers_core::Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    fn backend_name(&self) -> &'static str {
        "counted-memory-proof"
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loading_host_retarget_reaches_callback_guard_without_borrowing_replacement_authority() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let backend = MemoryStorage::new();
    let storage = backend.for_scope(&Scope::new("owner").unwrap()).unwrap();
    let original = Fixture::with_server(
        storage.documents().clone(),
        Some(Arc::new(StorageServer::new(storage.clone()))),
    )
    .await;
    let native = Arc::new(CountedSecrets {
        inner: MemorySecrets::new(),
        reads: AtomicU64::new(0),
    });
    native
        .set("cache_key", b"replacement-existing-key")
        .await
        .unwrap();
    let replacement = Fixture::on_network(
        storage.documents().clone(),
        Some(Arc::new(
            StorageServer::new(storage.clone()).with_secrets(native.clone()),
        )),
        original.network.clone(),
        true,
    )
    .await;
    let request = json!({"name":"cache_key"});
    let positive = scoped_call(&replacement, "Secrets", "Get", request.clone())
        .await
        .unwrap();
    let positive: tinystoragedrivers_bus::SecretValue = serde_json::from_value(positive).unwrap();
    assert_eq!(positive.value, b"replacement-existing-key");
    assert_eq!(native.reads.load(Ordering::SeqCst), 1);
    let before: u64 = original
        .proxy()
        .call_confidential("CallbackAttempts", ())
        .await
        .unwrap();
    let failure = original
        .proxy()
        .call_confidential::<serde_json::Value>(
            "ScopedCall",
            (route(&replacement), "Secrets", "Get", request),
        )
        .await
        .unwrap_err();
    let after: u64 = original
        .proxy()
        .call_confidential("CallbackAttempts", ())
        .await
        .unwrap();
    assert_eq!(
        after,
        before + 1,
        "the authorized loading host must actually dispatch the cross-instance attempt"
    );
    assert!(!failure.to_string().contains("proof loading host required"));
    assert!(
        matches!(failure, tinybus::Error::MethodFailed { ref name, .. }
        if name == "ai.tinyhumans.tinybus.Error.NotAttested"),
        "the SDK callback capability must reject the wrong source: {failure}"
    );
    assert_eq!(
        native.reads.load(Ordering::SeqCst),
        1,
        "the lower callback guard must reject before native secret I/O"
    );
    // The replacement stays usable after rejecting the real cross-instance call.
    scoped_call(&replacement, "Secrets", "Get", json!({"name":"cache_key"}))
        .await
        .unwrap();
    assert_eq!(native.reads.load(Ordering::SeqCst), 2);
}
