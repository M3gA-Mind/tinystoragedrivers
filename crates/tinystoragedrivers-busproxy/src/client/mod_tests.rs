//! Client bundle behavior over a local transport.
use std::sync::Arc;

use tinystoragedrivers_api::{SecretStore, Storage, async_trait};
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{
    Capability, ErrorKind, Result, Scope, SecretPrecondition, SecretVersion, StorageError,
};
use zeroize::Zeroizing;

use super::*;
use crate::{Admission, Loopback, StorageServer};

async fn storage() -> Storage {
    let server = StorageServer::new(memory_storage(&Scope::new("local").unwrap()));
    BusStorage::connect(Arc::new(Loopback::new(
        Arc::new(server),
        Admission::Private,
    )))
    .await
    .unwrap()
}

#[derive(Debug)]
struct NoSecretExtras(Arc<dyn SecretStore>);

#[async_trait]
impl SecretStore for NoSecretExtras {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        self.0.get(name).await
    }

    async fn get_versioned(
        &self,
        _name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "versioned reads unavailable in test store",
        ))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        self.0.set(name, value).await
    }

    async fn set_if(
        &self,
        _name: &str,
        _value: &[u8],
        _condition: SecretPrecondition,
    ) -> Result<SecretVersion> {
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "conditional writes unavailable in test store",
        ))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        self.0.delete(name).await
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
        Err(StorageError::unsupported(
            Capability::Secrets,
            "listing unavailable in test store",
        ))
    }

    fn enumerable(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "no-secret-extras"
    }
}

#[tokio::test]
async fn secret_enumerability_is_learned_only_over_the_confidential_route() {
    let backend = memory_storage(&Scope::new("secret-capabilities").unwrap());
    let server = Arc::new(StorageServer::new(backend));
    let storage = BusStorage::connect(Arc::new(Loopback::new(
        server.clone(),
        Admission::Confidential,
    )))
    .await
    .unwrap();
    assert!(storage.secrets().enumerable());
    storage.secrets().set("visible", b"value").await.unwrap();
    assert_eq!(storage.secrets().list("vis").await.unwrap(), ["visible"]);
    let secret_caps = server
        .handle(
            Admission::Private,
            "Secrets",
            "Capabilities",
            serde_json::Value::Null,
        )
        .await
        .unwrap_err();
    assert_eq!(
        secret_caps.kind,
        tinystoragedrivers_bus::StorageKind::Unsupported
    );

    let untrusted = BusStorage::connect(Arc::new(Loopback::new(server, Admission::Private)))
        .await
        .unwrap();
    assert!(!untrusted.secrets().enumerable());
    assert_eq!(
        untrusted.secrets().list("vis").await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Secrets)
    );
    assert_eq!(
        untrusted.secrets().get("visible").await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Secrets)
    );
}

#[tokio::test]
async fn secret_optional_operations_keep_typed_unsupported_categories() {
    let base = memory_storage(&Scope::new("secret-unsupported").unwrap());
    let storage = Storage::new(
        base.documents().clone(),
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        Arc::new(NoSecretExtras(base.secrets().clone())),
    );
    let server = StorageServer::new(storage);
    let client = BusStorage::connect(Arc::new(Loopback::new(
        Arc::new(server),
        Admission::Confidential,
    )))
    .await
    .unwrap();
    assert!(!client.secrets().enumerable());
    assert_eq!(
        client
            .secrets()
            .get_versioned("name")
            .await
            .unwrap_err()
            .kind(),
        ErrorKind::Unsupported(Capability::ConditionalSecretWrites)
    );
    assert_eq!(
        client.secrets().list("prefix").await.unwrap_err().kind(),
        ErrorKind::Unsupported(Capability::Secrets)
    );
}

#[tokio::test]
async fn unsupported_refusals_name_the_capability_the_call_needs() {
    let storage = storage().await;
    let error = storage
        .blobs()
        .presign_get("k", std::time::Duration::from_secs(60))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unsupported(Capability::PresignedUrls)
    );
}

#[tokio::test]
async fn connected_bundle_exposes_all_six_ports() {
    let storage = storage().await;
    let _ = storage.documents();
    let _ = storage.streams();
    let _ = storage.kv();
    let _ = storage.blobs();
    let _ = storage.queues();
    let _ = storage.secrets();
}

#[tokio::test]
async fn a_closed_transport_is_reported_as_unavailable() {
    #[derive(Debug)]
    struct Closed(tokio::sync::broadcast::Sender<tinystoragedrivers_bus::WatchChanged>);

    #[async_trait::async_trait]
    impl crate::StorageTransport for Closed {
        async fn call(
            &self,
            _: crate::Object,
            _: &str,
            _: serde_json::Value,
        ) -> std::result::Result<serde_json::Value, crate::WireError> {
            Err(crate::WireError::Transport("bus closed".into()))
        }
        async fn put_blob_stream(
            &self,
            _: tinystoragedrivers_bus::PutBlobStreamRequest,
            _: crate::BlobBody,
        ) -> std::result::Result<serde_json::Value, crate::WireError> {
            Err(crate::WireError::Transport("bus closed".into()))
        }
        async fn get_blob_stream(
            &self,
            _: tinystoragedrivers_bus::BlobKeyRequest,
        ) -> std::result::Result<
            Option<(tinystoragedrivers_bus::BlobStreamHead, crate::BlobBody)>,
            crate::WireError,
        > {
            Err(crate::WireError::Transport("bus closed".into()))
        }
        fn signals(
            &self,
        ) -> tokio::sync::broadcast::Receiver<tinystoragedrivers_bus::WatchChanged> {
            self.0.subscribe()
        }
    }

    let (sender, _) = tokio::sync::broadcast::channel(1);
    let error = BusStorage::connect(Arc::new(Closed(sender)))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        tinystoragedrivers_types::ErrorKind::Unavailable
    );
}

#[test]
fn only_an_explicit_confidential_admission_denial_is_optional_secrets() {
    let denied = wire_error(crate::WireError::NotAttested, Some(Capability::Secrets));
    assert_eq!(denied.kind(), ErrorKind::Unsupported(Capability::Secrets));
    let unexpected = wire_error(
        crate::WireError::Transport("probe failed".into()),
        Some(Capability::Secrets),
    );
    assert_eq!(unexpected.kind(), ErrorKind::Unavailable);
    let denial_on_an_unclassified_operation = wire_error(crate::WireError::NotAttested, None);
    assert_eq!(
        denial_on_an_unclassified_operation.kind(),
        ErrorKind::Unavailable
    );
}
