//! A real SDK module that relays typed document requests to its exact caller.
use async_trait::async_trait;
use serde_json::Value;
use tinybus::{CallContext, Connection, Error, Interface, InterfaceName, MemberName, Result};
use tinystoragedrivers_bus::{DeleteRequest, DocumentClient, GetRequest, PutRequest};
struct Relay {
    connection: Connection,
    name: InterfaceName,
    members: Vec<MemberName>,
}
#[async_trait]
impl Interface for Relay {
    fn name(&self) -> InterfaceName {
        self.name.clone()
    }
    fn members(&self) -> Vec<MemberName> {
        self.members.clone()
    }
    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }
    async fn call(&self, _: &MemberName, _: Value) -> Result<Value> {
        Err(Error::failed("authenticated confidential caller required"))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> Result<Value> {
        let caller = context
            .authenticated_sender()
            .filter(|_| context.is_confidential())
            .ok_or_else(|| Error::failed("authenticated confidential caller required"))?;
        if matches!(member.as_str(), "ScopedCall" | "ScopedPlainCall") {
            let (route, object, selector, request): (
                tinystoragedrivers_schema::ScopedStorageRoute,
                String,
                String,
                Value,
            ) = serde_json::from_value(args)?;
            let proxy = self.connection.proxy(
                &route.destination,
                format!("{}/{}", route.object_root, object),
                tinystoragedrivers_bus::STORAGE_INTERFACE,
            )?;
            return if member.as_str() == "ScopedPlainCall" {
                proxy.call(&selector, (request,)).await
            } else {
                proxy.call_confidential(&selector, (request,)).await
            };
        }
        if member.as_str() == "NativeRoundtrip" {
            use tinystoragedrivers_core::{
                CollectionSpec, DocumentStore, Precondition, StreamStore, WriteOptions,
            };
            use tinystoragedrivers_secrets::SecretStore;
            let (route,): (tinystoragedrivers_schema::ScopedStorageRoute,) =
                serde_json::from_value(args)?;
            let ports =
                tinystoragedrivers_busproxy::connect_scoped_storage(self.connection.clone(), route)
                    .await
                    .map_err(|_| Error::failed("callback connect failed"))?;
            let run = async {
                let docs = ports.documents();
                docs.ensure_collection(&CollectionSpec::new("cache")).await?;
                docs.put_with("cache", "key", serde_json::json!({"cache":true}), Precondition::None,
                    WriteOptions { schema: 7, ..Default::default() }).await?;
                let found = docs.get("cache", "key").await?.ok_or_else(|| tinystoragedrivers_core::StorageError::not_found("missing callback document"))?;
                let secrets = ports.secrets();
                let before = secrets.get("cache_key").await?;
                secrets.set("cache_key", b"private-cache-key").await?;
                let after = secrets.get("cache_key").await?;
                secrets.set("env/DEMO", b"private-env").await?;
                let env = secrets.get("env/DEMO").await?;
                let removed = secrets.delete("env/DEMO").await?;
                let streams = ports.streams();
                let offset = streams.append("audit", serde_json::json!({"event":"private-audit"})).await?;
                let audit = streams.read_window("audit", offset, 1).await?;
                Ok::<_, tinystoragedrivers_core::StorageError>(serde_json::json!({
                    "schema":found.meta.schema, "version":found.version.0,
                    "before":before.is_some(), "after":after.as_deref().map(|v| v.as_slice()) == Some(b"private-cache-key".as_slice()),
                    "env":env.as_deref().map(|v| v.as_slice()) == Some(b"private-env".as_slice()), "removed":removed,
                    "offset":offset, "audit":audit, "len": streams.len("audit").await?,
                }))
            }.await;
            return run.map_err(|_| Error::failed("native callback failed"));
        }
        let client = DocumentClient::new(self.connection.proxy(
            caller.as_str(),
            "/documents",
            tinystoragedrivers_bus::INTERFACE,
        )?)?;
        // Input is decoded to the bus-only contract, not interpreted as authority.
        match member.as_str() {
            "Get" => {
                let (request,): (GetRequest,) = serde_json::from_value(args)?;
                Ok(serde_json::to_value(client.get(request).await?)?)
            }
            "Put" => {
                let (request,): (PutRequest,) = serde_json::from_value(args)?;
                Ok(serde_json::to_value(client.put(request).await?)?)
            }
            "Delete" => {
                let (request,): (DeleteRequest,) = serde_json::from_value(args)?;
                Ok(serde_json::to_value(client.delete(request).await?)?)
            }
            _ => Err(Error::failed("unknown document fixture member")),
        }
    }
}
async fn setup(connection: Connection) -> Result<()> {
    connection
        .serve_at(
            "/proof".parse()?,
            Relay {
                connection: connection.clone(),
                name: "ai.tinyhumans.storage.DocumentProof1".parse()?,
                members: [
                    "Get",
                    "Put",
                    "Delete",
                    "ScopedCall",
                    "ScopedPlainCall",
                    "NativeRoundtrip",
                ]
                .iter()
                .map(|member| MemberName::new(*member))
                .collect::<Result<_>>()?,
            },
        )
        .await
}
// The canonical SDK owns native ABI unsafe code. No local raw ABI implementation.
tinybus_module::module_export! {
    setup = setup,
    worker_threads = 1,
    provides = ["ai.tinyhumans.storage.DocumentProof1"],
    methods = ["Get", "Put", "Delete", "ScopedCall", "ScopedPlainCall", "NativeRoundtrip"],
    signals = [], requires = [], optional = [], lazy = false,
}
