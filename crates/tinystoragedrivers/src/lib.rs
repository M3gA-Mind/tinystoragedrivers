//! Composition facade for Tiny Storage Drivers v2.
//!
//! This crate parses storage URLs and combines primitive engines for an
//! application's composition root. Consumer libraries should depend on
//! [`tinystoragedrivers_api`] and [`tinystoragedrivers_types`] directly.
//!
//! Memory storage is always available. Durable document backends are enabled
//! explicitly through `file`, `sqlite`, or `mongodb` features. S3 blobs and
//! `RabbitMQ` queues have independent [`BlobConfig`] and [`QueueConfig`] helpers,
//! because they are per-port engines. Secrets are unsupported by durable
//! document configurations until the host supplies its own store and key provider.
//!
//! ```no_run
//! # async fn example() -> Result<(), tinystoragedrivers_types::StorageError> {
//! use tinystoragedrivers::{Scope, StorageConfig, open};
//!
//! let backend = open(&StorageConfig::parse("memory")?).await?;
//! let storage = backend.for_scope(&Scope::new("local")?)?;
//! let _documents = storage.documents();
//! # Ok(())
//! # }
//! ```

mod blobs;
mod config;
mod queues;

pub use blobs::{BlobConfig, open_blobs};
pub use config::{StorageConfig, open, redact_url};
pub use queues::{QueueConfig, open_queues};
pub use tinystoragedrivers_api as api;
pub use tinystoragedrivers_api::{Storage, StorageBackend};
pub use tinystoragedrivers_secrets as secrets;
pub use tinystoragedrivers_types as types;
pub use tinystoragedrivers_types::*;
