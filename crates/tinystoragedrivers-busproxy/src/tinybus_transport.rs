//! The module-side [`StorageTransport`] over a real `TinyBus` connection.
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

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
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, WATCH_MEMBER, host_name, interface, member,
    object_path, watch_path, wire_value,
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
        let me = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("the connection has no unique name"))?;
        let mut rule = MatchRule::new()
            .signals()
            .sender(owner.clone())
            .interface(interface())
            .member(member(WATCH_MEMBER));
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
            host: host_name(),
            owner,
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
            host_name(),
            object_path(object),
            interface(),
        )
        .map(|proxy| proxy.with_timeout(self.timeout))
        .map_err(|error| transport_error(&error))
    }

    fn streamed_call(selector: &str, body: Value) -> Message {
        Message::method_call(
            host_name(),
            object_path(Object::Blobs),
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
            .open_stream_with_timeout(&host_name(), descriptor, self.timeout)
            .await
            .map_err(|error| transport_error(&error))?;
        let message = Self::streamed_call(
            "PutStream",
            json!([wire_value(&head), wire_value(&writer.stream_ref())]),
        );
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
                    PumpError::Source => WireError::Storage(StorageFailure {
                        kind: StorageKind::Backend,
                    }),
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
    ) -> Result<Option<(BlobStreamHead, BlobReader)>, WireError> {
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

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
