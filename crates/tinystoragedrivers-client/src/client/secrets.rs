//! `SecretStore` over the confidential `Secrets` object.
use tinystoragedrivers_api::types::{Capability, ErrorKind, Result, StorageError};
use tinystoragedrivers_api::{SecretStore, async_trait};
use tinystoragedrivers_bus as wire;
use zeroize::Zeroizing;

use super::Wire;
use crate::transport::Object;

/// The secret port over a bus. Its debug output carries nothing.
#[derive(Clone)]
pub struct BusSecrets {
    wire: Wire,
}

impl std::fmt::Debug for BusSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusSecrets").finish_non_exhaustive()
    }
}

impl BusSecrets {
    pub(super) fn new(wire: Wire) -> Self {
        Self { wire }
    }
}

#[async_trait]
impl SecretStore for BusSecrets {
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        let found: Option<wire::SecretValue> = self
            .wire
            .call(Object::Secrets, "Get", &request, None)
            .await
            .map_err(|error| secret_error(&error))?;
        Ok(found.map(|secret| Zeroizing::new(secret.value)))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        let request = wire::SetSecretRequest {
            name: name.to_owned(),
            value: value.to_vec(),
        };
        self.wire
            .call(Object::Secrets, "Set", &request, None)
            .await
            .map_err(|error| secret_error(&error))
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        self.wire
            .call(Object::Secrets, "Delete", &request, None)
            .await
            .map_err(|error| secret_error(&error))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let request = wire::ListSecretsRequest {
            prefix: prefix.to_owned(),
        };
        self.wire
            .call(Object::Secrets, "List", &request, None)
            .await
            .map_err(|error| secret_error(&error))
    }

    fn backend_name(&self) -> &'static str {
        "bus"
    }

    fn enumerable(&self) -> bool {
        true
    }
}

fn secret_error(error: &tinystoragedrivers_core::StorageError) -> StorageError {
    let message = "the storage host refused the secret operation";
    match error.kind() {
        tinystoragedrivers_core::ErrorKind::NotFound => StorageError::not_found(message),
        tinystoragedrivers_core::ErrorKind::Conflict => StorageError::conflict(message),
        tinystoragedrivers_core::ErrorKind::AlreadyExists => StorageError::already_exists(message),
        tinystoragedrivers_core::ErrorKind::Unsupported(_) => {
            StorageError::unsupported(Capability::Secrets, message)
        }
        tinystoragedrivers_core::ErrorKind::InvalidInput => StorageError::invalid_input(message),
        tinystoragedrivers_core::ErrorKind::Serialization => StorageError::serialization(message),
        tinystoragedrivers_core::ErrorKind::Crypto => StorageError::crypto(message),
        tinystoragedrivers_core::ErrorKind::Unavailable => StorageError::unavailable(message),
        _ => StorageError::new(ErrorKind::Backend, message),
    }
}
