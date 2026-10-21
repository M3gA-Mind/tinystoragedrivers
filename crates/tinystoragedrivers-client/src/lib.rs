//! Storage bus client and reference server adapter.
//!
//! [`BusStorage`] implements the same ports as a native driver
//! ([`StorageBackend`](tinystoragedrivers_core::StorageBackend),
//! `DocumentStore`, `StreamStore`, `BlobStore`, `QueueStore`, `SecretStore`) by turning each
//! call into a typed `tinystoragedrivers-bus` message sent through a
//! [`StorageTransport`]. A transport is the only thing that knows about
//! `TinyBus`; this crate has no dependency on it.
//!
//! When an embedding supplies a transport for only documents, native JSON
//! streams and secrets, [`BusStoragePorts`] connects those three handles
//! without probing blobs or queues. It deliberately has no watch interface and
//! does not implement [`StorageBackend`](tinystoragedrivers_core::StorageBackend),
//! whose baseline includes blobs.
//!
//! [`StorageServer`] is the host side: it is generic over an installed
//! backend, holds the caller's scope, and answers the same messages. The scope
//! lives in the server and never in a request: every request type rejects
//! unknown fields, so a forged `scope` is a decode failure. [`Loopback`] joins
//! a client to a server in one process through a JSON text frame, the way a
//! bus does, so the shared conformance suite can run against the client.
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use std::sync::Arc;
//! use tinystoragedrivers_client::{Admission, BusStorage, Loopback, StorageServer};
//! use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
//!
//! let backend = MemoryStorage::new();
//! let storage = backend.for_scope(&Scope::local())?;
//! let server = Arc::new(StorageServer::new(storage));
//! let bus = BusStorage::connect(
//!     Arc::new(Loopback::new(server, Admission::Private)),
//!     Scope::local(),
//! )
//! .await?;
//! let docs = bus.for_scope(&Scope::local())?;
//! docs.documents().ensure_collection(&tinystoragedrivers_core::CollectionSpec::new("notes")).await?;
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! An embedding with a partial transport can connect only the ports it serves:
//!
//! ```no_run
//! use std::sync::Arc;
//! use tinystoragedrivers_client::{BusStoragePorts, StorageTransport};
//! # async fn module(transport: Arc<dyn StorageTransport>) -> tinystoragedrivers_core::Result<()> {
//! let ports = BusStoragePorts::connect(transport).await?;
//! let _documents = ports.documents();
//! let _streams = ports.streams();
//! let _secrets = ports.secrets();
//! # Ok(())
//! # }
//! ```
mod client;
mod convert;
mod loopback;
mod server;
mod transport;

pub use client::{
    BusBlobs, BusDocuments, BusQueues, BusSecrets, BusStorage, BusStoragePorts, BusStreams,
};
pub use loopback::Loopback;
pub use server::StorageServer;
pub use transport::{Admission, BlobBody, Object, StorageTransport, WireError};

#[cfg(test)]
#[path = "ops_tests.rs"]
mod ops_tests;
