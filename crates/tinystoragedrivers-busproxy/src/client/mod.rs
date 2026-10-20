//! The client: native port traits over a [`StorageTransport`].
mod blobs;
mod documents;
mod kv;
mod queues;
mod secrets;
mod streams;
mod watch;

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tinystoragedrivers_api::Storage;
use tinystoragedrivers_bus::{
    BlobCapabilities, CapabilitiesReply, KvCapabilitiesReply, QueueFeaturesReply,
    SecretsCapabilitiesReply,
};
use tinystoragedrivers_types::{
    Capabilities, Capability, ErrorKind, QueueFeatures, Result, StorageError,
};

use crate::convert::queues::features_from_wire;
use crate::convert::{HOST_ONLY, capabilities_of, error_of};
use crate::transport::{Object, StorageTransport, WireError};

pub use blobs::BusBlobs;
pub use documents::BusDocuments;
pub use kv::BusKv;
pub use queues::BusQueues;
pub use secrets::BusSecrets;
pub use streams::BusStreams;

/// Shared request plumbing of every port handle.
#[derive(Clone)]
pub(crate) struct Wire {
    transport: Arc<dyn StorageTransport>,
}

impl std::fmt::Debug for Wire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wire").finish_non_exhaustive()
    }
}

impl Wire {
    pub(crate) fn new(transport: Arc<dyn StorageTransport>) -> Self {
        Self { transport }
    }

    pub(crate) fn transport(&self) -> &Arc<dyn StorageTransport> {
        &self.transport
    }

    /// Call `selector` and decode the reply. `unsupported_as` names the
    /// capability a server `Unsupported` refusal stands for.
    pub(crate) async fn call<Req: Serialize, Resp: DeserializeOwned>(
        &self,
        object: Object,
        selector: &str,
        request: &Req,
        unsupported_as: Option<Capability>,
    ) -> Result<Resp> {
        let request = serde_json::to_value(request)?;
        let reply = self
            .transport
            .call(object, selector, request)
            .await
            .map_err(|error| wire_error(error, unsupported_as))?;
        Ok(serde_json::from_value(reply)?)
    }
}

pub(crate) fn wire_error(error: WireError, unsupported_as: Option<Capability>) -> StorageError {
    match error {
        WireError::Storage(failure) => error_of(failure, unsupported_as),
        WireError::NotAttested if unsupported_as == Some(Capability::Secrets) => {
            StorageError::unsupported(
                Capability::Secrets,
                "confidential secret route is not admitted",
            )
        }
        WireError::NotAttested => {
            StorageError::unavailable("confidential route requires an attested peer")
        }
        WireError::Transport(reason) => StorageError::unavailable(reason),
    }
}

/// Collects the six caller-bound ports into a stable storage bundle.
#[derive(Clone)]
pub struct BusStorage {
    wire: Wire,
    capabilities: Capabilities,
    kv_capabilities: Capabilities,
    blob_capabilities: BlobCapabilities,
    queue_features: QueueFeatures,
}

impl std::fmt::Debug for BusStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusStorage")
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl BusStorage {
    /// Connect over `transport` and learn the admitted backend's capabilities
    /// and queue features.
    ///
    /// # Errors
    /// Transport failures, or a server that refuses the capability calls.
    pub async fn connect(transport: Arc<dyn StorageTransport>) -> Result<Storage> {
        let wire = Wire::new(transport);
        let documents: CapabilitiesReply = wire
            .call(Object::Documents, "Capabilities", &(), None)
            .await?;
        let streams: CapabilitiesReply = wire
            .call(Object::Streams, "Capabilities", &(), None)
            .await?;
        let blob_capabilities: BlobCapabilities =
            wire.call(Object::Blobs, "Capabilities", &(), None).await?;
        let kv_capabilities: KvCapabilitiesReply =
            wire.call(Object::Kv, "Capabilities", &(), None).await?;
        let secrets_enumerable = match wire
            .call::<_, SecretsCapabilitiesReply>(
                Object::Secrets,
                "Capabilities",
                &(),
                Some(Capability::Secrets),
            )
            .await
        {
            Ok(reply) => reply.enumerable,
            Err(error) if error.kind() == ErrorKind::Unsupported(Capability::Secrets) => false,
            Err(error) => return Err(error),
        };
        let features: QueueFeaturesReply = wire.call(Object::Queues, "Features", &(), None).await?;
        let mut capabilities = capabilities_of(&documents.capabilities);
        for capability in HOST_ONLY {
            capabilities = capabilities.without(capability);
        }
        let bus = Self {
            wire,
            capabilities,
            kv_capabilities: capabilities_of(&kv_capabilities.capabilities),
            blob_capabilities,
            queue_features: features_from_wire(features),
        };
        Ok(Storage::new(
            Arc::new(BusDocuments::new(bus.wire.clone(), bus.capabilities)),
            Arc::new(BusStreams::new(
                bus.wire.clone(),
                capabilities_of(&streams.capabilities),
            )),
            Arc::new(BusKv::new(bus.wire.clone(), bus.kv_capabilities)),
            Arc::new(BusBlobs::new(bus.wire.clone(), bus.blob_capabilities)),
            Arc::new(BusQueues::new(bus.wire.clone(), bus.queue_features)),
            Arc::new(BusSecrets::new(bus.wire, secrets_enumerable)),
        ))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
