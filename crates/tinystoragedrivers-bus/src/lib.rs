//! Confidential document data and CAS requests for compiled modules.
//!
//! Scope and collection authority remain captured by the host, outside the wire.
//! This contract links no native storage ports or implementations.
//!
//! ```
//! use tinystoragedrivers_bus::{Precondition, PutRequest};
//! let request = PutRequest {
//!     id: "index".into(),
//!     doc: serde_json::json!({"nested": {"id": "business-data"}}),
//!     precondition: Precondition::Absent,
//! };
//! let wire = serde_json::to_value(request)?;
//! assert_eq!(wire["document_id"], "index");
//! assert!(wire["document_json"].is_string());
//! # Ok::<(), serde_json::Error>(())
//! ```
mod client;
mod documents;
mod inline_json;
mod types;
pub use client::DocumentClient;
pub use documents::{
    BatchRequest, BatchResponse, ClaimRequest, CollectionRequest, CollectionSpec,
    CollectionsRequest, CountRequest, DOCUMENTS_OBJECT, DOCUMENTS_SELECTORS, DeleteDocumentRequest,
    DeleteWhereRequest, Direction, EnsureCollectionRequest, ExpectedDocument, Filter,
    GetDocumentRequest, IndexSpec, PutDocumentRequest, Query, QueryRequest, QueryResponse,
    ReadCheck, STORAGE_INTERFACE, SearchHit, SearchRequest, SearchSpec, Sort,
    ValidateCollectionSpecRequest, WatchChanged, WatchCloseRequest, WatchNotice, WatchOpenRequest,
    WatchReset, WriteOp, WriteResult, is_documents_selector,
};
pub use types::{
    DeleteRequest, GetRequest, Precondition, PutRequest, StorageFailure, StorageKind, Version,
    Versioned,
};
/// Fixed callback interface.
pub const INTERFACE: &str = "ai.tinyhumans.storage.DocumentCollection1";
/// Finite callback member allowlist.
pub const MEMBERS: [&str; 3] = ["Get", "Put", "Delete"];
/// Native outcome, distinct from transport refusal.
pub type Outcome<T> = Result<T, StorageFailure>;
