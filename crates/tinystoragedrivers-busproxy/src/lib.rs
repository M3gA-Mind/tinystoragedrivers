//! The real `TinyBus` transport for the storage bus.
//!
//! This crate adapts the shared storage ports to `TinyBus` on both ends of a
//! connection:
//!
//! * **Module side.** [`BusTransport`] implements
//!   [`StorageTransport`] over a [`tinybus::Connection`]: async JSON calls to the
//!   `ai.tinyhumans.tinystorage.Storage` objects, blobs over `tinybus` streams
//!   in bounded chunks, watch invalidations as signals. [`connect_storage`]
//!   is the one call a module makes to obtain its six-port [`tinystoragedrivers_api::Storage`].
//!   [`connect_scoped_storage`] connects to an exact-instance route using only
//!   the documents, native JSON streams and secrets callback profile.
//! * **Host side.** [`serve`] registers the six objects on the host's
//!   connection and hands every call, with the broker-authenticated sender, to
//!   a [`StorageEndpoint`]. [`PerCallerEndpoint`] is the ready-made endpoint
//!   that binds each caller to its own `StorageServer` over a host-composed
//!   `Storage` bundle.
//!
//! ```no_run
//! # async fn module(connection: tinybus::Connection) -> tinystoragedrivers_types::Result<()> {
//! use tinystoragedrivers_types::CollectionSpec;
//!
//! let storage = tinystoragedrivers_busproxy::connect_storage(connection).await?;
//! storage.documents().ensure_collection(&CollectionSpec::new("notes")).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Wire conventions
//!
//! * Request/reply members take one positional argument (`[request]`) and reply
//!   with the typed reply as JSON. `Secrets` is served on the confidential
//!   route only.
//! * `Blobs.PutStream` takes `[PutBlobStreamRequest, StreamRef]`: the caller
//!   opens a stream on the host and writes the body while the call is
//!   outstanding. `Blobs.GetStream` takes `[BlobKeyRequest]` and replies `null`
//!   or `[BlobStreamHead, StreamRef]`; the host opens the stream on the caller.
//! * Watch invalidations are the `WatchChanged` signal carrying `[WatchChanged]`
//!   on `/ai/tinyhumans/tinystorage/Watch/<caller>`; see [`watch_path`].
//! * Failures are `MethodFailed` errors named [`ERROR_DENIED`] (a label, no
//!   content) or [`ERROR_STORAGE`] (a `snake_case` `StorageKind`).
//!
//! ## Peer binding
//!
//! * Streams are taken with `Connection::accept_stream_from`: the host reads an
//!   upload only from the authenticated caller that opened it, and a client
//!   reads a download only from the host's unique name. A guessed stream id of
//!   another peer is refused and stays available to its owner.
//! * Watch signals are unicast with `Connection::emit_to` to the subscribing
//!   caller, so a peer that matches the same path or interface never sees them.
mod address;
mod chunks;
mod client;
mod convert;
mod host;
mod loopback;
mod names;
mod per_caller;
mod scoped;
mod server;
mod tinybus_transport;
mod transport;

pub use address::{INVOCATION_OBJECT_ROOT, StorageAddress};
pub use client::{
    BusBlobs, BusDocuments, BusKv, BusQueues, BusSecrets, BusStorage, BusStoragePorts, BusStreams,
};
pub use host::{
    HostFailure, ServedStorage, StorageCall, StorageEndpoint, prepare_storage_host_callbacks,
    serve, serve_at_address, serve_with_host_callbacks,
};
pub use loopback::Loopback;
pub use names::{
    ERROR_DENIED, ERROR_STORAGE, HOST_NAME, OBJECT_ROOT, WATCH_MEMBER, object_path, watch_path,
};
pub use per_caller::PerCallerEndpoint;
pub use scoped::connect_scoped_storage;
pub use server::StorageServer;
pub use tinybus_transport::{
    BusTransport, connect_storage, connect_storage_at, connect_storage_at_address,
};
pub use transport::{Admission, BlobBody, Object, StorageTransport, WireError};

#[cfg(test)]
#[path = "bus_tests.rs"]
mod bus_tests;

mod callback;
pub use callback::{endpoint, serve_collection};
pub use callback::{scoped_storage_endpoints, serve_scoped_storage};

#[cfg(test)]
#[path = "ops_tests.rs"]
mod ops_tests;
