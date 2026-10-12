//! The host side: register the four `Storage` objects on a `TinyBus`
//! connection and hand every call to a [`StorageEndpoint`].
//!
//! The host decides everything. The endpoint is given the broker-authenticated
//! sender and must derive the caller's scope, grant and audit from it; nothing
//! in a request can name a scope (the wire types reject unknown fields).
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{
    BusName, CallContext, Connection, Interface, InterfaceName, MemberName, StreamDescriptor,
    StreamRef, WeakConnection,
};
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageFailure, StorageKind, WatchChanged,
};
use tinystoragedrivers_client::{Admission, Object};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::chunks::{blob_reader, pump};
use crate::names::{
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, WATCH_MEMBER, interface, member, object_path,
    watch_path, wire_value,
};
use tinystoragedrivers_core::BlobReader;

/// How a call failed, before it becomes a bus error.
#[derive(Debug, PartialEq, Eq)]
pub enum HostFailure {
    /// Not allowed. The label says why and must hold no request content.
    Denied(&'static str),
    /// The backend refused or failed, as a content-free category.
    Storage(StorageFailure),
}

impl From<StorageFailure> for HostFailure {
    fn from(failure: StorageFailure) -> Self {
        Self::Storage(failure)
    }
}

impl From<HostFailure> for tinybus::Error {
    fn from(error: HostFailure) -> Self {
        match error {
            HostFailure::Denied(label) => Self::MethodFailed {
                name: ERROR_DENIED.to_owned(),
                message: label.to_owned(),
            },
            HostFailure::Storage(failure) => Self::MethodFailed {
                name: ERROR_STORAGE.to_owned(),
                message: serde_json::to_value(failure.kind)
                    .ok()
                    .and_then(|kind| kind.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "backend".to_owned()),
            },
        }
    }
}

/// One request/reply call, as the bus delivered it.
#[derive(Debug)]
pub struct StorageCall<'a> {
    /// Which of the four objects.
    pub object: Object,
    /// The member name; the endpoint must treat unknown ones as invalid.
    pub selector: &'a str,
    /// The single request argument (`null` when there is none).
    pub request: Value,
    /// The broker-authenticated unique sender; `None` when the delivery path
    /// cannot prove one, in which case the endpoint must refuse.
    pub sender: Option<&'a BusName>,
    /// `Confidential` only for `Secrets`, which the bus delivers on a
    /// confidential route only.
    pub admission: Admission,
}

/// What a host serves the `Storage` objects from.
#[async_trait]
pub trait StorageEndpoint: Send + Sync + 'static {
    /// Answer one request/reply call.
    ///
    /// # Errors
    /// A refusal or a backend failure category.
    async fn call(&self, call: StorageCall<'_>) -> Result<Value, HostFailure>;

    /// `Blobs.PutStream`: store `body`, which must be exactly `head.len`
    /// bytes. Replies with the stored `BlobInfo`.
    ///
    /// # Errors
    /// A refusal or a backend failure category.
    async fn put_blob_stream(
        &self,
        sender: Option<&BusName>,
        head: PutBlobStreamRequest,
        body: BlobReader,
    ) -> Result<Value, HostFailure>;

    /// `Blobs.GetStream`: the blob's metadata and body, or `None`.
    ///
    /// # Errors
    /// A refusal or a backend failure category.
    async fn get_blob_stream(
        &self,
        sender: Option<&BusName>,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, HostFailure>;

    /// The watch invalidations of `sender`'s binding. The host calls this just
    /// before it forwards a caller's first `WatchOpen`, so no signal is missed;
    /// `None` means the endpoint has no signals for that caller.
    fn watch_signals(&self, sender: &BusName) -> Option<broadcast::Receiver<WatchChanged>>;
}

struct Shared {
    connection: WeakConnection,
    endpoint: Arc<dyn StorageEndpoint>,
    forwarders: Mutex<HashMap<BusName, JoinHandle<()>>>,
}

impl Shared {
    /// Start relaying `sender`'s watch signals if they are not already.
    fn forward_watch_signals(&self, sender: &BusName) {
        let Ok(mut forwarders) = self.forwarders.lock() else {
            return;
        };
        forwarders.retain(|_, task| !task.is_finished());
        if forwarders.contains_key(sender) {
            return;
        }
        let (Some(mut signals), Ok(path)) =
            (self.endpoint.watch_signals(sender), watch_path(sender))
        else {
            return;
        };
        let (interface, member) = (interface(), member(WATCH_MEMBER));
        let connection = self.connection.clone();
        let task = tokio::spawn(async move {
            loop {
                match signals.recv().await {
                    Ok(signal) => {
                        let Some(connection) = connection.upgrade() else {
                            return;
                        };
                        let emitted = connection
                            .emit(path.clone(), interface.clone(), member.clone(), (signal,))
                            .await;
                        if emitted.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        forwarders.insert(sender.clone(), task);
    }
}

/// One storage object on the bus.
struct ObjectService {
    object: Object,
    shared: Arc<Shared>,
}

fn selectors(object: Object) -> &'static [&'static str] {
    use tinystoragedrivers_bus as wire;
    match object {
        Object::Documents => &wire::DOCUMENTS_SELECTORS,
        Object::Streams => &wire::STREAMS_SELECTORS,
        Object::Blobs => &wire::BLOBS_SELECTORS,
        Object::Secrets => &wire::SECRETS_SELECTORS,
    }
}

fn invalid() -> HostFailure {
    HostFailure::Storage(StorageFailure {
        kind: StorageKind::InvalidInput,
    })
}

/// The positional arguments of a call as a list.
fn arguments(args: Value) -> Result<Vec<Value>, HostFailure> {
    match args {
        Value::Array(items) => Ok(items),
        Value::Null => Ok(Vec::new()),
        _ => Err(invalid()),
    }
}

/// The request out of positional bus arguments: none is `null`, one is itself.
fn request_of(args: Value) -> Result<Value, HostFailure> {
    let mut items = arguments(args)?;
    if items.len() > 1 {
        return Err(invalid());
    }
    Ok(items.pop().unwrap_or(Value::Null))
}

impl ObjectService {
    fn connection(&self) -> Result<Connection, HostFailure> {
        self.shared
            .connection
            .upgrade()
            .ok_or(HostFailure::Storage(StorageFailure {
                kind: StorageKind::Unavailable,
            }))
    }

    /// `PutStream`: `[PutBlobStreamRequest, StreamRef]`.
    async fn put_stream(
        &self,
        args: Value,
        sender: Option<&BusName>,
    ) -> Result<Value, HostFailure> {
        let mut items = arguments(args)?;
        let (Some(stream), Some(head), None) = (items.pop(), items.pop(), items.pop()) else {
            return Err(invalid());
        };
        let head: PutBlobStreamRequest = serde_json::from_value(head).map_err(|_| invalid())?;
        let stream: StreamRef = serde_json::from_value(stream).map_err(|_| invalid())?;
        if stream.len.is_some_and(|declared| declared != head.len) {
            return Err(invalid());
        }
        let reader = self
            .connection()?
            .accept_stream(&stream)
            .map_err(|_| invalid())?;
        self.shared
            .endpoint
            .put_blob_stream(sender, head, blob_reader(reader))
            .await
    }

    /// `GetStream`: `[BlobKeyRequest]`, replying `null` or `[head, StreamRef]`
    /// with the body streamed to the caller.
    async fn get_stream(
        &self,
        args: Value,
        sender: Option<&BusName>,
    ) -> Result<Value, HostFailure> {
        let request: BlobKeyRequest =
            serde_json::from_value(request_of(args)?).map_err(|_| invalid())?;
        // The body is sent to the broker-authenticated caller, so without one
        // there is nowhere safe to send it.
        let Some(caller) = sender else {
            return Err(HostFailure::Denied("unauthenticated-caller"));
        };
        let found = self
            .shared
            .endpoint
            .get_blob_stream(sender, request)
            .await?;
        let Some((head, mut body)) = found else {
            return Ok(Value::Null);
        };
        let len = head.meta.len;
        let writer = self
            .connection()?
            .open_stream(caller, StreamDescriptor::with_len(len))
            .await
            .map_err(|_| {
                HostFailure::Storage(StorageFailure {
                    kind: StorageKind::Unavailable,
                })
            })?;
        let reply = json!([wire_value(&head), wire_value(&writer.stream_ref())]);
        // The body follows the reply: the caller accepts the stream only once it
        // has the reference, and the stream's window holds the first chunks.
        tokio::spawn(async move {
            let _ = pump(writer, &mut body, len).await;
        });
        Ok(reply)
    }
}

#[async_trait]
impl Interface for ObjectService {
    fn name(&self) -> InterfaceName {
        interface()
    }

    fn members(&self) -> Vec<MemberName> {
        selectors(self.object)
            .iter()
            .map(|selector| member(selector))
            .collect()
    }

    fn requires_confidential(&self, _member: &MemberName) -> bool {
        self.object == Object::Secrets
    }

    async fn call(&self, _member: &MemberName, _args: Value) -> tinybus::Result<Value> {
        // Without the broker's context there is no caller to authorize.
        Err(HostFailure::Denied("unauthenticated-caller").into())
    }

    async fn call_with_context(
        &self,
        member: &MemberName,
        args: Value,
        context: &CallContext,
    ) -> tinybus::Result<Value> {
        let sender = context.authenticated_sender();
        let selector = member.as_str();
        let outcome = match (self.object, selector) {
            (Object::Blobs, "PutStream") => self.put_stream(args, sender).await,
            (Object::Blobs, "GetStream") => self.get_stream(args, sender).await,
            _ => {
                if let (Object::Documents, "WatchOpen", Some(sender)) =
                    (self.object, selector, sender)
                {
                    self.shared.forward_watch_signals(sender);
                }
                let admission = if self.object == Object::Secrets {
                    Admission::Confidential
                } else {
                    Admission::Private
                };
                match request_of(args) {
                    Ok(request) => {
                        self.shared
                            .endpoint
                            .call(StorageCall {
                                object: self.object,
                                selector,
                                request,
                                sender,
                                admission,
                            })
                            .await
                    }
                    Err(error) => Err(error),
                }
            }
        };
        outcome.map_err(Into::into)
    }
}

/// The registered `Storage` objects. Dropping it leaves them registered; call
/// [`ServedStorage::shutdown`] to withdraw them.
pub struct ServedStorage {
    connection: Connection,
    shared: Arc<Shared>,
}

impl std::fmt::Debug for ServedStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServedStorage").finish_non_exhaustive()
    }
}

impl ServedStorage {
    /// Withdraw the objects, release the name and stop relaying signals.
    ///
    /// # Errors
    /// The bus refuses to release the name.
    pub async fn shutdown(self) -> tinybus::Result<()> {
        for object in ALL {
            self.connection.unserve(&object_path(object)).await;
        }
        if let Ok(mut forwarders) = self.shared.forwarders.lock() {
            for (_, task) in forwarders.drain() {
                task.abort();
            }
        }
        self.connection.release_name(HOST_NAME).await
    }
}

const ALL: [Object; 4] = [
    Object::Documents,
    Object::Streams,
    Object::Blobs,
    Object::Secrets,
];

/// Serve the `Storage` objects on `connection` and claim [`HOST_NAME`].
///
/// # Errors
/// The name is taken or the bus refuses a registration.
pub async fn serve(
    connection: &Connection,
    endpoint: Arc<dyn StorageEndpoint>,
) -> tinybus::Result<ServedStorage> {
    let shared = Arc::new(Shared {
        connection: connection.downgrade(),
        endpoint,
        forwarders: Mutex::new(HashMap::new()),
    });
    for object in ALL {
        connection
            .serve_at(
                object_path(object),
                ObjectService {
                    object,
                    shared: Arc::clone(&shared),
                },
            )
            .await?;
    }
    if let Err(error) = connection.request_name(HOST_NAME).await {
        // Do not leave unreachable objects registered behind a name we lack.
        for object in ALL {
            connection.unserve(&object_path(object)).await;
        }
        return Err(error);
    }
    Ok(ServedStorage {
        connection: connection.clone(),
        shared,
    })
}

#[cfg(test)]
#[path = "host_tests.rs"]
mod tests;
