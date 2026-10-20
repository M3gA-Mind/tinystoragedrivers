//! `Secrets` selectors; only reached on the confidential route.
use serde_json::Value;
use tinystoragedrivers_api::types::ErrorKind;
use tinystoragedrivers_bus as wire;

use super::documents::bounded_list;
use super::{Reply, StorageServer, decode, encode, invalid};

impl StorageServer {
    pub(super) async fn secrets(&self, selector: &str, request: Value) -> Reply {
        let secrets = self.storage.secrets();
        match selector {
            "Capabilities" => encode(&wire::SecretsCapabilitiesReply {
                enumerable: secrets.enumerable(),
            }),
            "Get" => {
                let r: wire::SecretNameRequest = decode(request)?;
                let found = native_secret(secrets.get(&r.name).await)?;
                encode(&found.map(|value| wire::SecretValue {
                    value: value.to_vec(),
                }))
            }
            "GetVersioned" => {
                let r: wire::SecretNameRequest = decode(request)?;
                let found = native_secret(secrets.get_versioned(&r.name).await)?;
                encode(&found.map(|(value, version)| wire::VersionedSecretValue {
                    value: value.to_vec(),
                    version,
                }))
            }
            "Set" => {
                let r: wire::SetSecretRequest = decode(request)?;
                native_secret(secrets.set(&r.name, &r.value).await)?;
                encode(&())
            }
            "SetIf" => {
                let r: wire::SetSecretIfRequest = decode(request)?;
                let version = native_secret(secrets.set_if(&r.name, &r.value, r.condition).await)?;
                encode(&version)
            }
            "Delete" => {
                let r: wire::SecretNameRequest = decode(request)?;
                encode(&native_secret(secrets.delete(&r.name).await)?)
            }
            "List" => {
                let r: wire::ListSecretsRequest = decode(request)?;
                bounded_list(&native_secret(secrets.list(&r.prefix).await)?)
            }
            _ => Err(invalid()),
        }
    }
}

fn native_secret<T>(
    result: tinystoragedrivers_api::types::Result<T>,
) -> Result<T, wire::StorageFailure> {
    result.map_err(|error| wire::StorageFailure {
        kind: match error.kind() {
            ErrorKind::NotFound => wire::StorageKind::NotFound,
            ErrorKind::Conflict => wire::StorageKind::Conflict,
            ErrorKind::AlreadyExists => wire::StorageKind::AlreadyExists,
            ErrorKind::Unsupported(_) => wire::StorageKind::Unsupported,
            ErrorKind::InvalidInput => wire::StorageKind::InvalidInput,
            ErrorKind::Serialization => wire::StorageKind::Serialization,
            ErrorKind::Crypto => wire::StorageKind::Crypto,
            ErrorKind::Unavailable => wire::StorageKind::Unavailable,
            _ => wire::StorageKind::Backend,
        },
    })
}

#[cfg(test)]
#[path = "secrets_tests.rs"]
mod tests;
