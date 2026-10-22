//! Confidential callbacks over captured native ports, with no backend opening.
use crate::{Admission, Object, StorageServer};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tinybus::module::{HostCallbackBinding, HostCallbackEndpoint, HostCallbackGuard};
use tinybus::{CallContext, Interface, InterfaceName, MemberName, ObjectPath};

fn selectors(object: Object) -> Vec<&'static str> {
    match object {
        Object::Documents => tinystoragedrivers_bus::DOCUMENTS_SELECTORS
            .iter()
            .copied()
            .filter(|name| !matches!(*name, "WatchOpen" | "WatchClose"))
            .collect(),
        Object::Streams => tinystoragedrivers_bus::STREAMS_SELECTORS.to_vec(),
        Object::Secrets => tinystoragedrivers_bus::SECRETS_SELECTORS.to_vec(),
        Object::Blobs | Object::Queues | Object::Kv => vec![],
    }
}
fn path(root: &ObjectPath, object: Object) -> tinybus::Result<ObjectPath> {
    ObjectPath::new(format!(
        "{}/{}",
        root.as_str().trim_end_matches('/'),
        object.as_str()
    ))
    .map_err(|_| tinybus::Error::failed("invalid storage callback root"))
}
/// Declare the three finite, confidential native callback endpoints.
///
/// # Errors
/// Refuses invalid root paths.
pub fn scoped_storage_endpoints(root: &str) -> tinybus::Result<Vec<HostCallbackEndpoint>> {
    let root = ObjectPath::new(root)
        .map_err(|_| tinybus::Error::failed("invalid storage callback root"))?;
    [Object::Documents, Object::Streams, Object::Secrets]
        .into_iter()
        .map(|object| {
            Ok(HostCallbackEndpoint {
                path: path(&root, object)?,
                interface: InterfaceName::new(tinystoragedrivers_bus::STORAGE_INTERFACE)?,
                members: selectors(object)
                    .into_iter()
                    .map(MemberName::new)
                    .collect::<tinybus::Result<_>>()?,
            })
        })
        .collect()
}
/// Capture an already scoped, admitted, fenced and generation-bound native server.
///
/// Registration neither opens storage nor declares schema. Every call must pass
/// the exact-instance binding guard and the confidential route before reaching
/// `StorageServer`. Native admission remains authoritative, including expiry.
/// No raw connection, blob streams or watch subscription is exposed.
///
/// # Errors
/// Refuses undeclared endpoints, duplicate registration or a revoked binding.
pub async fn serve_scoped_storage(
    binding: &HostCallbackBinding,
    root: ObjectPath,
    server: Arc<StorageServer>,
) -> tinybus::Result<()> {
    for (object, endpoint) in [Object::Documents, Object::Streams, Object::Secrets]
        .into_iter()
        .zip(scoped_storage_endpoints(root.as_str())?)
    {
        binding
            .serve_at(
                endpoint.path,
                ScopedService {
                    server: server.clone(),
                    guard: binding.guard(),
                    object,
                    interface: endpoint.interface,
                    members: endpoint.members,
                },
            )
            .await?;
    }
    Ok(())
}
struct ScopedService {
    server: Arc<StorageServer>,
    guard: HostCallbackGuard,
    object: Object,
    interface: InterfaceName,
    members: Vec<MemberName>,
}
#[async_trait]
impl Interface for ScopedService {
    fn name(&self) -> InterfaceName {
        self.interface.clone()
    }
    fn members(&self) -> Vec<MemberName> {
        self.members.clone()
    }
    fn requires_confidential(&self, _: &MemberName) -> bool {
        true
    }
    async fn call(&self, _: &MemberName, _: Value) -> tinybus::Result<Value> {
        Err(tinybus::Error::failed(
            "authenticated storage callback required",
        ))
    }
    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> tinybus::Result<Value> {
        if !self.guard.authorizes(context) {
            return Err(tinybus::Error::failed(
                "storage callback authority unavailable",
            ));
        }
        self.dispatch(member, args).await
    }
}
impl ScopedService {
    async fn dispatch(&self, member: &MemberName, args: Value) -> tinybus::Result<Value> {
        if !self.members.contains(member) {
            return Err(tinybus::Error::failed(
                "unsupported storage callback member",
            ));
        }
        let (request,): (Value,) = serde_json::from_value(args)
            .map_err(|_| tinybus::Error::failed("invalid storage callback arguments"))?;
        let mut reply = self
            .server
            .handle(
                Admission::Confidential,
                self.object.as_str(),
                member.as_str(),
                request,
            )
            .await
            .map_err(|failure| tinybus::Error::MethodFailed {
                name: "ai.tinyhumans.tinystorage.Error.Storage".into(),
                message: serde_json::to_value(failure.kind)
                    .ok()
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "backend".into()),
            })?;
        if self.object == Object::Documents && member.as_str() == "Capabilities" {
            let mut caps: tinystoragedrivers_bus::CapabilitiesReply = serde_json::from_value(reply)
                .map_err(|_| tinybus::Error::failed("invalid storage capabilities"))?;
            caps.capabilities.retain(|cap| {
                !matches!(
                    cap.as_str(),
                    "watch" | "streaming_blobs" | "presigned_urls" | "blob_ttl"
                )
            });
            reply = serde_json::to_value(caps)
                .map_err(|_| tinybus::Error::failed("invalid storage capabilities"))?;
        }
        Ok(reply)
    }
}

#[cfg(test)]
#[path = "scoped_tests.rs"]
mod tests;
