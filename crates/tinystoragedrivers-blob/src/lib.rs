//! Standalone scoped blob engines.
//!
//! This crate implements only the opaque blob port. It does not own storage
//! URL resolution, composite routing, orphan cleanup policy, or stream seals.

mod key;
mod memory;

pub use memory::MemoryBlobStore;
pub use tinystoragedrivers_api::{BlobReader, BlobStore, BlobStream};
pub use tinystoragedrivers_types::{Blob, BlobListEntry, BlobListPage, BlobMeta, Scope, TreeEntry, TreeFile};

#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
pub use fs::FileBlobStore;

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteBlobStore;

#[cfg(feature = "mongodb")]
mod mongodb;
#[cfg(feature = "mongodb")]
pub use mongodb::MongoBlobStore;

#[cfg(feature = "s3")]
mod s3;
#[cfg(feature = "s3")]
pub use s3::S3BlobStore;
