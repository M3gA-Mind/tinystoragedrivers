//! Native engine implementations of the scope-bound JSON [`KvStore`] port.
//!
//! The default `memory` feature is useful for ephemeral state and tests. The
//! optional `file`, `sqlite`, and `mongodb` features persist values using
//! engine-native KV storage without routing through document records.
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
//! use serde_json::json;
//! use tinystoragedrivers_api::KvStore;
//! use tinystoragedrivers_types::{Precondition, Scope};
//! use tinystoragedrivers_kv::MemoryKv;
//!
//! let store = MemoryKv::new(Scope::new("tenant")?);
//! let version = store.put("settings/theme", json!("dark")).await?;
//! let current = store.get("settings/theme").await?.unwrap();
//! assert_eq!(current.version, version);
//! store.cas("settings/theme", json!("light"), current.unchanged()).await?;
//! # Ok::<(), tinystoragedrivers_types::StorageError>(())
//! # }).unwrap();
//! ```
//!
//! This crate deliberately contains no document-backed KV model, composite
//! storage policy, or host-specific namespace helpers.

mod common;

use std::sync::Arc;

/// Injectable Unix epoch-millisecond clock for deterministic TTL behavior.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "memory")]
pub use memory::MemoryKv;

#[cfg(feature = "file")]
mod file;
#[cfg(feature = "file")]
pub use file::FileKv;

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteKv;

#[cfg(feature = "mongodb")]
mod mongodb;
#[cfg(feature = "mongodb")]
pub use mongodb::MongoKv;
