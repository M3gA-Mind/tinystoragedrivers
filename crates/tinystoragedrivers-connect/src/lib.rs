//! Cloneable shared connections used by storage primitive engines.
//!
//! This crate owns connection setup and engine-neutral connection handles. It
//! does not implement storage operations or choose a primitive engine.

use std::sync::Arc;

/// Clock injected into local database handles, in Unix epoch milliseconds.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[cfg(feature = "sqlite")]
pub mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::{SqliteConnection, SqliteTables};

#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mongodb")]
pub use mongodb::MongoConnection;

#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
pub use s3::{S3Connection, S3ConnectionConfig};

#[cfg(feature = "file")]
mod file;
#[cfg(feature = "file")]
pub use file::FileConnection;

#[cfg(feature = "amqp")]
mod amqp;
#[cfg(feature = "amqp")]
pub use amqp::AmqpConnection;
