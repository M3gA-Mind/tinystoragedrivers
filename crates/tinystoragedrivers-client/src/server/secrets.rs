//! `Secrets` selectors; only reached on the confidential route.
use serde_json::Value;
use tinystoragedrivers_bus as wire;

use super::documents::bounded_list;
use super::{Reply, StorageServer, decode, encode, invalid, native};

impl StorageServer {
    pub(super) async fn secrets(&self, selector: &str, request: Value) -> Reply {
        let Some(secrets) = &self.secrets else {
            return Err(wire::StorageFailure {
                kind: wire::StorageKind::Unsupported,
            });
        };
        match selector {
            "Get" => {
                let r: wire::SecretNameRequest = decode(request)?;
                let found = native(secrets.get(&r.name).await)?;
                encode(&found.map(|value| wire::SecretValue {
                    value: value.to_vec(),
                }))
            }
            "Set" => {
                let r: wire::SetSecretRequest = decode(request)?;
                native(secrets.set(&r.name, &r.value).await)?;
                encode(&())
            }
            "Delete" => {
                let r: wire::SecretNameRequest = decode(request)?;
                encode(&native(secrets.delete(&r.name).await)?)
            }
            "List" => {
                let r: wire::ListSecretsRequest = decode(request)?;
                bounded_list(&native(secrets.list(&r.prefix).await)?)
            }
            _ => Err(invalid()),
        }
    }
}
