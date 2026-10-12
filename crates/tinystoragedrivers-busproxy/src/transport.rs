//! The module-side [`StorageTransport`] over a real `TinyBus` connection.
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinybus::{
    BusName, Connection, InterfaceName, MatchRule, MemberName, Message, StreamDescriptor, StreamRef,
};
use tinystoragedrivers_bus::{
    BlobKeyRequest, BlobStreamHead, PutBlobStreamRequest, STORAGE_INTERFACE, StorageFailure,
    StorageKind, WatchChanged,
};
use tinystoragedrivers_client::{Object, StorageTransport, WireError};
use tinystoragedrivers_core::{BlobReader, Scope};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::chunks::{PumpError, blob_reader, pump};
use crate::names::{ERROR_DENIED, ERROR_STORAGE, HOST_NAME, WATCH_MEMBER, object_path, watch_path};

/// Signals buffered for a slow watcher before the oldest are dropped.
const SIGNAL_BUFFER: usize = 256;
/// How long one call, or one stream chunk, may take. A large streamed blob is
/// bounded per chunk, not as a whole.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// The label a module gives the one scope its host bound it to. The host never
/// reads it; the scope itself comes from the broker-authenticated caller.
#[must_use]
pub fn module_scope_label() -> Scope {
    Scope::local()
}

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
            .ok_or_else(|| tinybus::Error::NameHasNoOwner(destination()))?;
        let me = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("the connection has no unique name"))?;
        let mut rule = MatchRule::new()
            .signals()
            .sender(owner)
            .interface(InterfaceName::new(STORAGE_INTERFACE)?)
            .member(MemberName::new(WATCH_MEMBER)?);
        rule.path = Some(watch_path(&me)?);
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
            host: destination(),
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
        let path = object_path(object).map_err(|error| transport_error(&error))?;
        self.connection
            .proxy(HOST_NAME, path.as_str(), STORAGE_INTERFACE)
            .map(|proxy| proxy.with_timeout(self.timeout))
            .map_err(|error| transport_error(&error))
    }

    fn streamed_call(member: &str, body: Value) -> Result<Message, WireError> {
        Ok(Message::method_call(
            destination(),
            object_path(Object::Blobs).map_err(|error| transport_error(&error))?,
            InterfaceName::new(STORAGE_INTERFACE).map_err(|error| transport_error(&error))?,
            MemberName::new(member).map_err(|error| transport_error(&error))?,
            body,
        ))
    }
}

fn destination() -> BusName {
    BusName::new(HOST_NAME).unwrap_or_else(|_| unreachable!("HOST_NAME is a valid bus name"))
}

fn transport_error(error: &tinybus::Error) -> WireError {
    WireError::Transport(error.to_string())
}

/// A failed call as the client sees it: a backend category stays a category,
/// everything else is a transport failure.
fn wire_error(error: tinybus::Error) -> WireError {
    match error {
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
        let reply = if object == Object::Secrets {
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
            .open_stream_with_timeout(&destination(), descriptor, self.timeout)
            .await
            .map_err(|error| transport_error(&error))?;
        let head = serde_json::to_value(&head)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        let stream = serde_json::to_value(writer.stream_ref())
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        let message = Self::streamed_call("PutStream", json!([head, stream]))?;
        // The call and the body are in flight together: the host reads the
        // stream while it answers, so neither can wait for the other. The
        // first failure wins and the other half is dropped.
        let sending = async {
            pump(writer, &mut body, len)
                .await
                .map_err(|error| match error {
                    PumpError::Short => WireError::Storage(StorageFailure {
                        kind: StorageKind::InvalidInput,
                    }),
                    PumpError::Source => WireError::Transport("blob source failed".into()),
                    PumpError::Bus(error) => transport_error(&error),
                })
        };
        let calling = async {
            self.connection
                .call_raw(message, self.timeout)
                .await
                .map_err(wire_error)
        };
        let (reply, ()) = tokio::try_join!(calling, sending)?;
        Ok(reply)
    }

    async fn get_blob_stream(
        &self,
        request: BlobKeyRequest,
    ) -> Result<Option<(BlobStreamHead, tinystoragedrivers_core::BlobReader)>, WireError> {
        let request = serde_json::to_value(&request)
            .map_err(|_| WireError::Transport("frame encode failed".into()))?;
        let reply = self
            .proxy(Object::Blobs)?
            .call::<Value>("GetStream", (request,))
            .await
            .map_err(wire_error)?;
        let Some((head, stream)) = decode_stream_reply(reply)? else {
            return Ok(None);
        };
        let reader = self
            .connection
            .accept_stream(&stream)
            .map_err(|error| transport_error(&error))?;
        Ok(Some((head, blob_reader(reader))))
    }

    fn signals(&self) -> broadcast::Receiver<WatchChanged> {
        self.signals.subscribe()
    }
}

/// Storage for a module: connect to the host's `Storage` objects over
/// `connection` and return the one scoped handle the host bound this module to.
///
/// The returned [`BusStorage`](tinystoragedrivers_client::BusStorage) also
/// gives `secrets()` and the capabilities the host admitted.
///
/// # Errors
/// The host is absent, refuses the capability calls, or the bus fails.
pub async fn connect_storage(
    connection: Connection,
) -> tinystoragedrivers_core::Result<tinystoragedrivers_client::BusStorage> {
    let transport = BusTransport::connect(connection)
        .await
        .map_err(|error| tinystoragedrivers_core::StorageError::unavailable(error.to_string()))?;
    tinystoragedrivers_client::BusStorage::connect(Arc::new(transport), module_scope_label()).await
}
