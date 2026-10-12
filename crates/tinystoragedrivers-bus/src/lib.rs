//! Confidential document data and CAS requests for compiled modules.
//!
//! Scope and collection authority remain captured by the host, outside the wire.
//! This contract links no native storage ports or implementations.
mod client;
mod inline_json;
mod types;
pub use client::DocumentClient;
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
