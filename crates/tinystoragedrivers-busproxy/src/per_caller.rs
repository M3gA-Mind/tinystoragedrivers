//! A ready-made [`StorageEndpoint`] that gives each authenticated caller its
//! own [`StorageServer`].
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;
use tinybus::BusName;
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageFailure, WatchChanged,
};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::BlobReader;
use tokio::sync::broadcast;

use crate::host::{HostFailure, StorageCall, StorageEndpoint};

/// Serves each caller from the [`StorageServer`] `bind` returns for it.
///
/// `bind` is the host's whole policy: it maps the broker-authenticated sender
/// to a server over that caller's scope, grant and fence, or `None` to refuse.
/// Servers are created on a caller's first call and kept, so a caller's watches
/// and signals live until [`PerCallerEndpoint::release`] drops it.
pub struct PerCallerEndpoint<F> {
    bind: F,
    servers: Mutex<HashMap<BusName, Arc<StorageServer>>>,
}

impl<F> std::fmt::Debug for PerCallerEndpoint<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerCallerEndpoint").finish_non_exhaustive()
    }
}

impl<F> PerCallerEndpoint<F>
where
    F: Fn(&BusName) -> Option<StorageServer> + Send + Sync + 'static,
{
    /// An endpoint that binds callers with `bind`.
    #[must_use]
    pub fn new(bind: F) -> Self {
        Self {
            bind,
            servers: Mutex::new(HashMap::new()),
        }
    }

    /// Drop the server bound to `caller`, for when it leaves the bus, and
    /// return whether it had one. The caller's watches and signals end with
    /// it; a later call from the same name binds afresh through `bind`.
    pub fn release(&self, caller: &BusName) -> bool {
        self.servers
            .lock()
            .is_ok_and(|mut servers| servers.remove(caller).is_some())
    }

    fn server(&self, sender: Option<&BusName>) -> Result<Arc<StorageServer>, HostFailure> {
        let sender = sender.ok_or(HostFailure::Denied("unauthenticated-caller"))?;
        let mut servers = self
            .servers
            .lock()
            .map_err(|_| HostFailure::Denied("host-unavailable"))?;
        if let Some(server) = servers.get(sender) {
            return Ok(Arc::clone(server));
        }
        let server = Arc::new((self.bind)(sender).ok_or(HostFailure::Denied("unknown-caller"))?);
        servers.insert(sender.clone(), Arc::clone(&server));
        Ok(server)
    }
}

fn refused(failure: StorageFailure) -> HostFailure {
    HostFailure::Storage(failure)
}

#[async_trait]
impl<F> StorageEndpoint for PerCallerEndpoint<F>
where
    F: Fn(&BusName) -> Option<StorageServer> + Send + Sync + 'static,
{
    async fn call(&self, call: StorageCall<'_>) -> Result<Value, HostFailure> {
        self.server(call.sender)?
            .handle(
                call.admission,
                call.object.as_str(),
                call.selector,
                call.request,
            )
            .await
            .map_err(refused)
    }

    async fn put_blob_stream(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure> {
        let info = self
            .server(sender)?
            .put_blob_stream(head, body)
            .await
            .map_err(refused)?;
        serde_json::to_value(info).map_err(|_| HostFailure::Denied("encode-failed"))
    }

    async fn get_blob_stream(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure> {
        self.server(sender)?
            .get_blob_stream(request)
            .await
            .map_err(refused)
    }

    fn watch_signals(&self, sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>> {
        self.server(Some(sender))
            .ok()
            .map(|server| server.signals())
    }
}

#[cfg(test)]
#[path = "per_caller_tests.rs"]
mod tests;
