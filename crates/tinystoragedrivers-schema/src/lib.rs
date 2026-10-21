//! Pure storage declarations and versioned transformation wire contracts.
//!
//! Owners describe stored items with [`SchemaDeclaration`] and name deterministic
//! transforms using [`TransformDescriptor`]. This crate contains no execution
//! engine, storage driver, key hashing, clock, or transport. Runtime users can
//! retain the core crate's closure-based `Schema` and `Step` API.
//!
//! [`MigrationExchangeRequest`] and [`MigrationExchangeReply`] describe the
//! confidential fragmented exchange for stateless owner transforms, including
//! before storage initialization. They are wire contracts only: adapters supply
//! trusted peer context and confidential, monitor-excluded transport. Core's
//! optional `migration-delivery` feature implements assembly and lifecycle;
//! neither crate supplies a `TinyBus` adapter.
//!
//! [`ScopedStorageRoute`] and [`ScopedStorageProfile`] describe the finite
//! documents, native JSON streams and secrets callback profile. They are
//! routing data only: scope, admission, fences and generations remain with the
//! host and its native storage adapter.
//!
//! ```
//! use tinystoragedrivers_schema::{ScopedStorageProfile, ScopedStorageRoute};
//! let route = ScopedStorageRoute {
//!     version: 1,
//!     destination: ":1.42".into(),
//!     object_root: "/storage".into(),
//!     profile: ScopedStorageProfile::DocumentsStreamsSecretsV1,
//! };
//! let wire = serde_json::to_value(route)?;
//! assert_eq!(wire["version"], 1);
//! assert!(wire.get("scope").is_none());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ```
//! use tinystoragedrivers_schema::{Owner, SchemaDeclaration};
//! let schema = SchemaDeclaration {
//!     owner: Owner::new("orders")?, items: vec![], steps: vec![],
//! };
//! assert_eq!(serde_json::to_value(schema)?["owner"], "orders");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod capabilities;
mod collection;
mod declaration;
mod error;
mod migration;
mod record;
pub use migration::*;
mod seal;
mod transform;

pub use capabilities::{Capabilities, Capability};
pub use collection::{
    CollectionSpec, IndexSpec, MAX_COLLECTION_LEN, RESERVED_PREFIX, SearchSpec,
    validate_collection, validate_field_path,
};
pub use declaration::{Expiry, Item, Owner, ScopeClass, SourceRecord, TargetRecord};
pub use error::{ErrorKind, Result, StorageError};
pub use record::Meta;
pub use seal::{SealPolicy, SealWhen};
pub use transform::{
    BlobTransformOutput, BlobTransformRequest, DocumentsTransformRequest, SchemaDeclaration,
    StepDeclaration, StreamTransformOutput, StreamTransformRequest, TransformDescriptor,
    TransformError, TransformErrorKind, TransformKind, TransformResult,
};

mod callback;
pub use callback::{ScopedStorageProfile, ScopedStorageRoute};
