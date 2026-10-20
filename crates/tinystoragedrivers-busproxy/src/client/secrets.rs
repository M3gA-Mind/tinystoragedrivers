//! `SecretStore` over the confidential `Secrets` object.
use tinystoragedrivers_api::{SecretStore, async_trait};
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_types::{Result, SecretPrecondition, SecretVersion};
use zeroize::Zeroizing;

use super::Wire;
use crate::transport::Object;

/// The secret port over a bus. Its debug output carries nothing.
#[derive(Clone)]
pub struct BusSecrets {
    wire: Wire,
    enumerable: bool,
}

impl std::fmt::Debug for BusSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusSecrets").finish_non_exhaustive()
    }
}

impl BusSecrets {
    pub(super) fn new(wire: Wire, enumerable: bool) -> Self {
        Self { wire, enumerable }
    }
}

#[async_trait]
impl SecretStore for BusSecrets {
    fn backend_name(&self) -> &'static str {
        "tinybus"
    }
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        let found: Option<wire::SecretValue> = self
            .wire
            .call(
                Object::Secrets,
                "Get",
                &request,
                Some(tinystoragedrivers_types::Capability::Secrets),
            )
            .await?;
        Ok(found.map(|secret| Zeroizing::new(secret.value)))
    }

    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        let found: Option<wire::VersionedSecretValue> = self
            .wire
            .call(
                Object::Secrets,
                "GetVersioned",
                &request,
                Some(tinystoragedrivers_types::Capability::ConditionalSecretWrites),
            )
            .await?;
        Ok(found.map(|secret| (Zeroizing::new(secret.value), secret.version)))
    }

    async fn set(&self, name: &str, value: &[u8]) -> Result<()> {
        let request = wire::SetSecretRequest {
            name: name.to_owned(),
            value: value.to_vec(),
        };
        self.wire
            .call(
                Object::Secrets,
                "Set",
                &request,
                Some(tinystoragedrivers_types::Capability::Secrets),
            )
            .await
    }

    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: SecretPrecondition,
    ) -> Result<SecretVersion> {
        let request = wire::SetSecretIfRequest {
            name: name.to_owned(),
            value: value.to_vec(),
            condition,
        };
        let version: SecretVersion = self
            .wire
            .call(
                Object::Secrets,
                "SetIf",
                &request,
                Some(tinystoragedrivers_types::Capability::ConditionalSecretWrites),
            )
            .await?;
        Ok(version)
    }

    async fn delete(&self, name: &str) -> Result<bool> {
        let request = wire::SecretNameRequest {
            name: name.to_owned(),
        };
        self.wire
            .call(
                Object::Secrets,
                "Delete",
                &request,
                Some(tinystoragedrivers_types::Capability::Secrets),
            )
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let request = wire::ListSecretsRequest {
            prefix: prefix.to_owned(),
        };
        self.wire
            .call(
                Object::Secrets,
                "List",
                &request,
                Some(tinystoragedrivers_types::Capability::Secrets),
            )
            .await
    }

    fn enumerable(&self) -> bool {
        self.enumerable
    }
}

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
