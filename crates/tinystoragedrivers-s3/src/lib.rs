//! Streaming S3-compatible blob storage without an AWS SDK dependency.
//!
//! This crate supplies only blobs: pair it with a document/stream backend.
//! Each scope occupies a distinct object key prefix.
//!
//! ```
//! use tinystoragedrivers_s3::S3Config;
//! let config = S3Config::parse("s3://media/attachments?region=us-east-1")?;
//! assert_eq!(config.bucket, "media");
//! # Ok::<(), tinystoragedrivers_core::StorageError>(())
//! ```
mod blobs;
pub use blobs::{S3Blobs, S3Config, S3Storage};
