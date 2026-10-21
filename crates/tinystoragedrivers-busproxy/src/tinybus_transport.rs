//! The module-side [`StorageTransport`] over a real `TinyBus` connection.
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::address::StorageAddress;
use crate::transport::{Object, StorageTransport, WireError};
use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{BusName, Connection, MatchRule, Message, StreamDescriptor, StreamRef};
use tinystoragedrivers_api::BlobReader;
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, StorageFailure, StorageKind, WatchChanged,
};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::chunks::{PumpError, blob_reader, pump};
use crate::names::{
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, WATCH_MEMBER, callback_watch_path, host_name,
    interface, member, object_path, watch_path, wire_value,
};

/// Signals buffered for a slow watcher before the oldest are dropped.
const SIGNAL_BUFFER: usize = 256;
/// How long one call, or one stream chunk, may take. A large streamed blob is
/// bounded per chunk, not as a whole.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A [`StorageTransport`] that calls the `Storage` objects of the host that
/// owns [`HOST_NAME`], over one `TinyBus` connection.
///
/// * Documents, Streams, Blobs and Secrets calls are bus method calls; Secrets
///   use the confidential route, which the broker only completes to an
///   attested host.
/// * `PutStream` opens a bus stream on the host and writes the body while the
///   call is outstanding; `GetStream` accepts the stream the host opens on this
///   connection. Neither buffers a whole blob.
/// * Watch invalidations arrive as signals on a path derived from this
///   connection's unique name, accepted only from the host's unique name.
pub struct BusTransport {
    connection: Connection,
    host: BusName,
    /// The unique name of the peer that owns [`HOST_NAME`]; streams are
    /// accepted only from it.
    owner: BusName,
    callback: bool,
    address: Option<StorageAddress>,
    timeout: Duration,
    signals: broadcast::Sender<WatchChanged>,
    forwarder: JoinHandle<()>,
}

impl fmt::Debug for BusTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BusTransport")
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl Drop for BusTransport {
    fn drop(&mut self) {
        self.forwarder.abort();
    }
}

impl BusTransport {
    /// Attach to the storage host on `connection`.
    ///
    /// # Errors
    /// No peer owns [`HOST_NAME`], the connection has no unique name yet, or
    /// the broker refuses the signal subscription.
    pub async fn connect(connection: Connection) -> tinybus::Result<Self> {
        let owner = connection
            .name_owner(HOST_NAME)
            .await?
            .ok_or_else(|| tinybus::Error::NameHasNoOwner(host_name()))?;
        Self::connect_to(connection, host_name(), owner, false, None, None).await
    }

    /// Attach to the exact unique host-callback destination supplied by the
    /// loader. There is no well-known-name fallback in this mode.
    ///
    /// Port RPCs use the confidential callback route; blob stream payloads
    /// remain ordinary `TinyBus` streams. Watch signals use a fixed path because
    /// the unicast destination is already this exact callback-bound module.
    ///
    /// # Errors
    /// `destination` is not a unique name, the connection has no unique name,
    /// or the broker refuses the signal subscription.
    pub async fn connect_at(connection: Connection, destination: BusName) -> tinybus::Result<Self> {
        if !destination.is_unique() {
            return Err(tinybus::Error::failed(
                "storage callback destination must be a unique bus name",
            ));
        }
        Self::connect_to(
            connection,
            destination.clone(),
            destination,
            true,
            None,
            None,
        )
        .await
    }

    /// Attach to a host's invocation-scoped storage endpoint.
    ///
    /// Calls are routed only to the unique host and paths in `address`; this
    /// never looks up or falls back to the global storage service.
    ///
    /// # Errors
    /// Returns an error when the address is invalid for the connection or a
    /// required endpoint cannot be reached.
    pub async fn connect_at_address(
        connection: Connection,
        address: StorageAddress,
    ) -> tinybus::Result<Self> {
        if !address.host().is_unique() {
            return Err(tinybus::Error::failed(
                "storage address host must be a unique bus name",
            ));
        }
        Self::connect_to(
            connection,
            address.host().clone(),
            address.host().clone(),
            false,
            Some(address.clone()),
            Some(address.watch_path().clone()),
        )
        .await
    }

    async fn connect_to(
        connection: Connection,
        host: BusName,
        owner: BusName,
        callback: bool,
        address: Option<StorageAddress>,
        addressed_watch_path: Option<tinybus::ObjectPath>,
    ) -> tinybus::Result<Self> {
        let me = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("the connection has no unique name"))?;
        let mut rule = MatchRule::new()
            .signals()
            .sender(owner.clone())
            .interface(interface())
            .member(member(WATCH_MEMBER));
        rule.path = Some(match addressed_watch_path {
            Some(path) => path,
            None if callback => callback_watch_path(),
            None => watch_path(&me)?,
        });
        // Subscribe before returning so no watch signal can be missed.
        let mut received = connection.add_match(rule).await?;
        let (signals, _) = broadcast::channel(SIGNAL_BUFFER);
        let relay = signals.clone();
        let forwarder = tokio::spawn(async move {
            loop {
                match received.recv().await {
                    Ok(message) => {
                        if let Some(signal) = decode_signal(&message) {
                            // No receiver just means nobody is watching.
                            let _ = relay.send(signal);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Ok(Self {
            connection,
            host,
            owner,
            callback,
            address,
            timeout: CALL_TIMEOUT,
            signals,
            forwarder,
        })
    }

    /// Use `timeout` for calls and for each stream chunk instead of 30 s.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn proxy(&self, object: Object) -> Result<tinybus::Proxy, WireError> {
        tinybus::Proxy::new(
            self.connection.clone(),
            self.host.clone(),
            self.port_path(object),
            interface(),
        )
        .map(|proxy| proxy.with_timeout(self.timeout))
        .map_err(|error| transport_error(&error))
    }

    fn port_path(&self, object: Object) -> tinybus::ObjectPath {
        self.address.as_ref().map_or_else(
            || object_path(object),
            |address| address.port_path(object).clone(),
        )
    }

    fn streamed_call(&self, selector: &str, body: Value) -> Message {
        Message::method_call(
            self.host.clone(),
            self.port_path(Object::Blobs),
            interface(),
            member(selector),
            body,
        )
    }
}

fn transport_error(error: &tinybus::Error) -> WireError {
    WireError::Transport(error.to_string())
}

/// A failed call as the client sees it: a backend category stays a category,
/// everything else is a transport failure.
fn wire_error(error: tinybus::Error) -> WireError {
    match error {
        tinybus::Error::NotAttested { .. } => WireError::NotAttested,
        tinybus::Error::MethodFailed { name, .. } if name == tinybus::Error::NOT_ATTESTED => {
            WireError::NotAttested
        }
        tinybus::Error::MethodFailed { name, message } if name == ERROR_STORAGE => {
            let kind = serde_json::from_value::<StorageKind>(Value::String(message))
                .unwrap_or(StorageKind::Backend);
            WireError::Storage(StorageFailure { kind })
        }
        tinybus::Error::MethodFailed { name, message } if name == ERROR_DENIED => {
            WireError::Transport(format!("the storage host denied the call: {message}"))
        }
        other => transport_error(&other),
    }
}

fn decode_signal(message: &Message) -> Option<WatchChanged> {
    let first = match &message.body {
        Value::Array(items) if items.len() == 1 => items.first()?.clone(),
        _ => return None,
    };
    serde_json::from_value(first).ok()
}

/// The two-element `[head, StreamRef]` reply of `GetStream`.
fn decode_stream_reply(reply: Value) -> Result<Option<(BlobStreamHead, StreamRef)>, WireError> {
    if reply.is_null() {
        return Ok(None);
    }
    let Value::Array(mut items) = reply else {
        return Err(WireError::Transport("malformed GetStream reply".into()));
    };
    let (Some(stream), Some(head), None) = (items.pop(), items.pop(), items.pop()) else {
        return Err(WireError::Transport("malformed GetStream reply".into()));
    };
    let malformed = |_| WireError::Transport("malformed GetStream reply".into());
    Ok(Some((
        serde_json::from_value(head).map_err(malformed)?,
        serde_json::from_value(stream).map_err(malformed)?,
    )))
}

#[async_trait]
impl StorageTransport for BusTransport {
    async fn call(
        &self,
        object: Object,
        selector: &str,
        request: Value,
    ) -> Result<Value, WireError> {
        let proxy = self.proxy(object)?;
        let reply = if self.callback || object == Object::Secrets {
            proxy.call_confidential::<Value>(selector, (request,)).await
        } else {
            proxy.call::<Value>(selector, (request,)).await
        };
        reply.map_err(wire_error)
    }

    async fn put_blob_stream(
        &self,
        head: PutBlobStreamRequest,
        mut body: BlobReader,
    ) -> Result<Value, WireError> {
        let len = head.len;
        let mut descriptor = StreamDescriptor::with_len(len);
        if let Some(content_type) = &head.content_type {
            descriptor = descriptor.content_type(content_type.clone());
        }
        let writer = self
            .connection
            .open_stream_with_timeout(&self.host, descriptor, self.timeout)
            .await
            .map_err(|error| transport_error(&error))?;
        let message = self.streamed_call(
            "PutStream",
            json!([wire_value(&head), wire_value(&writer.stream_ref())]),
        );
        // The host reads the stream while this call is outstanding. Keep the
        // pump error typed until both sides' completion ordering is resolved.
        let sending = pump(writer, &mut body, len);
        let calling = async {
            self.connection
                .call_raw(message, self.timeout)
                .await
                .map_err(wire_error)
        };
        // Prefer a locally known source failure when both halves are ready,
        // but keep host rejections prompt when the source is still pending.
        resolve_put_stream(calling, sending).await
    }

    async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, WireError> {
        // Stream endpoints are explicitly ordinary even when the RPC methods
        // on this callback binding use confidential delivery.
        let reply = self
            .proxy(Object::Blobs)?
            .call::<Value>("GetStream", (wire_value(&request),))
            .await
            .map_err(wire_error)?;
        let Some((head, stream)) = decode_stream_reply(reply)? else {
            return Ok(None);
        };
        let reader = self
            .connection
            .accept_stream_from(&stream, &self.owner)
            .map_err(|error| transport_error(&error))?;
        Ok(Some((head, blob_reader(reader))))
    }

    fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.signals.subscribe()
    }
}

async fn resolve_put_stream<C, S>(calling: C, sending: S) -> Result<Value, WireError>
where
    C: Future<Output = Result<Value, WireError>>,
    S: Future<Output = Result<(), PumpError>>,
{
    tokio::pin!(calling);
    tokio::pin!(sending);
    tokio::select! {
        biased;
        sent = &mut sending => match sent {
            Err(PumpError::Bus(error)) => match poll_once(calling.as_mut()).await {
                Some(Err(host_error)) => Err(host_error),
                Some(Ok(_)) | None => Err(transport_error(&error)),
            },
            Err(error) => Err(pump_error(error)),
            Ok(()) => calling.await,
        },
        reply = &mut calling => match reply {
            Err(error) => Err(error),
            Ok(reply) => match sending.await {
                Ok(()) => Ok(reply),
                Err(error) => Err(pump_error(error)),
            },
        },
    }
}

async fn poll_once<F: Future>(mut future: std::pin::Pin<&mut F>) -> Option<F::Output> {
    std::future::poll_fn(|cx| {
        std::task::Poll::Ready(match future.as_mut().poll(cx) {
            std::task::Poll::Ready(output) => Some(output),
            std::task::Poll::Pending => None,
        })
    })
    .await
}

fn pump_error(error: PumpError) -> WireError {
    match error {
        PumpError::Short => WireError::Storage(StorageFailure {
            kind: StorageKind::InvalidInput,
        }),
        PumpError::Source => WireError::Storage(StorageFailure {
            kind: StorageKind::Backend,
        }),
        PumpError::Bus(error) => transport_error(&error),
    }
}

/// Storage for a module: connect to the host's `Storage` objects over
/// `connection` and return the one scoped handle the host bound this module to.
///
/// The returned [`BusStorage`](crate::client::BusStorage) also
/// gives `secrets()` and the capabilities the host admitted.
///
/// # Errors
/// The host is absent, refuses the capability calls, or the bus fails.
pub async fn connect_storage(
    connection: Connection,
) -> tinystoragedrivers_types::Result<tinystoragedrivers_api::Storage> {
    let transport = BusTransport::connect(connection)
        .await
        .map_err(|error| tinystoragedrivers_types::StorageError::unavailable(error.to_string()))?;
    crate::client::BusStorage::connect(Arc::new(transport)).await
}

/// Build the six client ports addressed to a unique host-callback destination.
///
/// Unlike [`connect_storage`], this never looks up the global well-known host
/// name or falls back to it. A module should pass the unique name supplied by
/// its loader for the matching `HostCallbackBinding`.
///
/// # Errors
/// The destination is not unique or `TinyBus` refuses the watch subscription.
pub async fn connect_storage_at(
    connection: Connection,
    destination: BusName,
) -> tinystoragedrivers_types::Result<tinystoragedrivers_api::Storage> {
    let transport = BusTransport::connect_at(connection, destination)
        .await
        .map_err(|error| tinystoragedrivers_types::StorageError::unavailable(error.to_string()))?;
    crate::client::BusStorage::connect(Arc::new(transport)).await
}

/// Build storage ports addressed to one invocation-rooted endpoint.
///
/// This is distinct from [`connect_storage_at`], which addresses an SDK
/// callback binding with fixed object paths.
///
/// # Errors
/// Returns an error if the address cannot be reached or a required port fails
/// to initialize.
pub async fn connect_storage_at_address(
    connection: Connection,
    address: StorageAddress,
) -> tinystoragedrivers_types::Result<tinystoragedrivers_api::Storage> {
    let transport = BusTransport::connect_at_address(connection, address)
        .await
        .map_err(|error| tinystoragedrivers_types::StorageError::unavailable(error.to_string()))?;
    crate::client::BusStorage::connect(Arc::new(transport)).await
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
