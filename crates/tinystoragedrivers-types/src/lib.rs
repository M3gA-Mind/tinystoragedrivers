//! Serializable values shared by storage primitive ports and their consumers.
//!
//! This crate contains data contracts and runtime-independent helpers only. It
//! deliberately owns no storage engines, model registry, or host policy.
//!
//! ```
//! use tinystoragedrivers_types::{Filter, Scope};
//!
//! let scope = Scope::new("legacy:owner")?;
//! let child = scope.child("agent/one")?;
//! assert_eq!(child.as_str(), "legacy:owner/agent%2Fone");
//! assert!(Filter::eq("state", "ready").matches("a", &serde_json::json!({"state":"ready"})));
//! # Ok::<(), tinystoragedrivers_types::StorageError>(())
//! ```

mod blob;
mod capabilities;
mod document;
mod error;
mod filter;
mod key;
mod queue;
mod scope;
mod secret;
mod stream;
pub mod value;
mod watch;

pub use blob::{Blob, BlobMeta, SEALED_BLOB_PREFIX, TreeEntry, TreeFile, is_reserved_blob_key};
pub use capabilities::{Capabilities, Capability};
pub use document::{
    CollectionSpec, Cursor, CursorPosition, ExpectedDocument, IndexSpec, MAX_COLLECTION_LEN,
    MAX_ID_LEN, Meta, Page, Precondition, Query, RESERVED_PREFIX, ReadCheck, SearchHit, SearchSpec,
    Version, Versioned, WriteOp, WriteOptions, WriteResult, validate_collection, validate_doc,
    validate_field_path, validate_id,
};
pub use error::{ErrorKind, Result, StorageError};
pub use filter::{
    CREATED_FIELD, Direction, EXPIRES_FIELD, Filter, ID_FIELD, SCHEMA_FIELD, Sort, UPDATED_FIELD,
    sort_documents, sort_documents_with_meta,
};
pub use key::{Key, MAX_KEY_LEN};
pub use queue::{
    DeadLetter, Delivery, Enqueued, MAX_PRIORITY, MAX_QUEUE_NAME_LEN, NewMessage, QueueFeatures,
    QueueSpec, QueueStats, Receipt, Retry, validate_queue_name,
};
pub use scope::{MAX_SCOPE_LEN, Scope};
pub use secret::{
    MAX_SECRET_NAME_LEN, SecretPrecondition, SecretVersion, validate_secret_name,
    validate_secret_prefix,
};
pub use stream::{SealOutcome, StreamEntry, StreamInfo};
pub use watch::{MAX_WATCH_CURSOR_BYTES, WatchCursor, WatchNotice, WatchOptions, WatchReset};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
