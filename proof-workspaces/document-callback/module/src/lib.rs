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
                members: ["Get", "Put", "Delete"]
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
    methods = ["Get", "Put", "Delete"],
    signals = [], requires = [], optional = [], lazy = false,
}
