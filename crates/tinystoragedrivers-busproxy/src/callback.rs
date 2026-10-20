//! Confined document callbacks for compiled `TinyBus` modules.
//!
//! The host captures an already-admitted store and one collection before
//! module initialization. The callback exposes only Get, Put, and Delete.
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tinybus::module::{HostCallbackBinding, HostCallbackEndpoint, HostCallbackGuard};
use tinybus::{CallContext, Interface, InterfaceName, MemberName, ObjectPath};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_bus::{DeleteRequest, GetRequest, PutRequest, StorageFailure, StorageKind};
use tinystoragedrivers_types::{ErrorKind, StorageError, Versioned};

/// Declare the exact callback interface and finite member allowlist.
///
/// # Errors
/// Returns an error if `path` or the interface declaration is invalid.
pub fn endpoint(path: &str) -> tinybus::Result<HostCallbackEndpoint> {
    Ok(HostCallbackEndpoint {
        path: ObjectPath::new(path)?,
        interface: InterfaceName::new(tinystoragedrivers_bus::INTERFACE)?,
        members: tinystoragedrivers_bus::MEMBERS
            .iter()
            .map(|member| MemberName::new(*member))
            .collect::<tinybus::Result<_>>()?,
    })
}

/// Serve a fixed collection through a private host callback binding.
///
/// The binding supplies the caller guard and collection authority. This
/// function neither declares a collection nor changes the store's admission.
///
/// # Errors
/// Rejects invalid collection names, undeclared endpoints, duplicate
/// registration, or a revoked/used binding.
pub async fn serve_collection(
    binding: &HostCallbackBinding,
    path: ObjectPath,
    documents: Arc<dyn DocumentStore>,
    collection: impl Into<String>,
) -> tinybus::Result<()> {
    let collection = collection.into();
    tinystoragedrivers_types::validate_collection(&collection)
        .map_err(|_| tinybus::Error::failed("invalid document callback collection"))?;
    let declaration = endpoint(path.as_str())?;
    binding
        .serve_at(
            path,
            DocumentService {
                documents,
                collection,
                guard: binding.guard(),
                interface: declaration.interface,
                members: declaration.members,
            },
        )
        .await
}

struct DocumentService {
    documents: Arc<dyn DocumentStore>,
    collection: String,
    guard: HostCallbackGuard,
    interface: InterfaceName,
    members: Vec<MemberName>,
}

impl DocumentService {
    async fn dispatch(&self, member: &MemberName, args: Value) -> tinybus::Result<Value> {
        match member.as_str() {
            "Get" => {
                let (request,): (GetRequest,) = decode(args)?;
                let outcome: Result<Option<Versioned<Value>>, StorageFailure> = self
                    .documents
                    .get(&self.collection, &request.id)
                    .await
                    .map(|document| {
                        document.map(|document| Versioned {
                            id: document.id,
                            version: document.version,
                            doc: document.doc,
                            meta: document.meta,
                        })
                    })
                    .map_err(|error| storage_failure(&error));
                encode(outcome)
            }
            "Put" => {
                let (request,): (PutRequest,) = decode(args)?;
                let condition = request.precondition;
                encode(
                    self.documents
                        .put(&self.collection, &request.id, request.doc, condition)
                        .await
                        .map_err(|error| storage_failure(&error)),
                )
            }
            "Delete" => {
                let (request,): (DeleteRequest,) = decode(args)?;
                encode(
                    self.documents
                        .delete(&self.collection, &request.id, request.precondition)
                        .await
                        .map_err(|error| storage_failure(&error)),
                )
            }
            _ => Err(tinybus::Error::failed("unknown document callback member")),
        }
    }
}

fn decode<T: serde::de::DeserializeOwned>(args: Value) -> tinybus::Result<T> {
    serde_json::from_value(args)
        .map_err(|_| tinybus::Error::failed("invalid document callback arguments"))
}

fn encode<T: serde::Serialize>(outcome: T) -> tinybus::Result<Value> {
    serde_json::to_value(outcome)
        .map_err(|_| tinybus::Error::failed("document callback serialization failed"))
}

fn storage_failure(error: &StorageError) -> StorageFailure {
    let kind = match error.kind() {
        ErrorKind::NotFound => StorageKind::NotFound,
        ErrorKind::Conflict => StorageKind::Conflict,
        ErrorKind::AlreadyExists => StorageKind::AlreadyExists,
        ErrorKind::Unsupported(_) => StorageKind::Unsupported,
        ErrorKind::InvalidInput => StorageKind::InvalidInput,
        ErrorKind::Serialization => StorageKind::Serialization,
        ErrorKind::Crypto => StorageKind::Crypto,
        ErrorKind::Unavailable => StorageKind::Unavailable,
        ErrorKind::Backend => StorageKind::Backend,
        _ => StorageKind::Backend,
    };
    StorageFailure { kind }
}

#[async_trait]
impl Interface for DocumentService {
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
            "authenticated document callback required",
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
                "document callback authority unavailable",
            ));
        }
        self.dispatch(member, args).await
    }
}

#[cfg(test)]
#[path = "callback_tests.rs"]
mod tests;
