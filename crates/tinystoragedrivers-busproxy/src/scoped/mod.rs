//! Exact-instance, confidential transport for the three-port callback profile.
use crate::{BlobBody, BusStoragePorts, Object, StorageTransport, WireError};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tinybus::{BusName, Connection, ObjectPath};
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageFailure, StorageKind, WatchChanged,
};
use tinystoragedrivers_types::{ScopedStorageProfile, ScopedStorageRoute};
use tokio::sync::broadcast;

struct ScopedTransport {
    connection: Connection,
    destination: BusName,
    root: ObjectPath,
    signals: broadcast::Sender<WatchChanged>,
}
impl std::fmt::Debug for ScopedTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedTransport").finish_non_exhaustive()
    }
}
fn scoped_wire_error(error: tinybus::Error) -> WireError {
    match error {
        tinybus::Error::MethodFailed { name, message } if name == crate::ERROR_STORAGE => {
            let kind = serde_json::from_value::<StorageKind>(Value::String(message))
                .unwrap_or(StorageKind::Backend);
            WireError::Storage(StorageFailure { kind })
        }
        _ => WireError::Transport("storage callback unavailable".into()),
    }
}
fn unsupported() -> WireError {
    WireError::Storage(StorageFailure {
        kind: StorageKind::Unsupported,
    })
}
#[async_trait]
impl StorageTransport for ScopedTransport {
    async fn call(
        &self,
        object: Object,
        selector: &str,
        request: Value,
    ) -> Result<Value, WireError> {
        if object == Object::Blobs || matches!(selector, "WatchOpen" | "WatchClose") {
            return Err(unsupported());
        }
        let path = ObjectPath::new(format!(
            "{}/{}",
            self.root.as_str().trim_end_matches('/'),
            object.as_str()
        ))
        .map_err(|_| WireError::Transport("invalid storage callback route".into()))?;
        let proxy = tinybus::Proxy::new(
            self.connection.clone(),
            self.destination.clone(),
            path,
            super::names::interface(),
        )
        .map_err(|_| WireError::Transport("invalid storage callback route".into()))?;
        proxy
            .call_confidential(selector, (request,))
            .await
            .map_err(scoped_wire_error)
    }
    async fn put_blob_stream(
        &self,
        _: PutBlobStreamRequest,
        _: BlobBody,
    ) -> Result<Value, WireError> {
        Err(unsupported())
    }
    async fn get_blob_stream(
        &self,
        _: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobBody)>, WireError> {
        Err(unsupported())
    }
    fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.signals.subscribe()
    }
}
/// Connect only to an exact unique callback destination using confidential calls.
///
/// The route carries no authority: only the module instance admitted with the
/// host binding can use it. This partial client exposes no blob backend.
///
/// # Errors
/// Refuses unsupported versions, malformed paths, well-known destinations,
/// unavailable bindings and native capability failures.
pub async fn connect_scoped_storage(
    connection: Connection,
    route: ScopedStorageRoute,
) -> tinystoragedrivers_types::Result<BusStoragePorts> {
    let invalid =
        || tinystoragedrivers_types::StorageError::invalid_input("invalid storage callback route");
    let destination = BusName::new(route.destination).map_err(|_| invalid())?;
    let root = ObjectPath::new(route.object_root).map_err(|_| invalid())?;
    if route.version != 1
        || !destination.is_unique()
        || route.profile != ScopedStorageProfile::DocumentsStreamsSecretsV1
    {
        return Err(invalid());
    }
    let (signals, _) = broadcast::channel(1);
    BusStoragePorts::connect(Arc::new(ScopedTransport {
        connection,
        destination,
        root,
        signals,
    }))
    .await
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
