//! Optional port behaviors and capability sets.

use crate::{Result, StorageError};

use std::fmt;

/// An optional behavior exposed by one or more primitive ports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Capability {
    /// The backend has a document port.
    Documents,
    /// The backend has an append-only stream port.
    Streams,
    /// The backend has a key/value port.
    Kv,
    /// The backend has a blob port.
    Blobs,
    /// The backend has a queue port.
    Queues,
    /// The backend has a secret port.
    Secrets,
    /// Read-only validation of collection declarations.
    CollectionValidation,
    /// Full-text search over declared fields.
    FullText,
    /// Atomic multi-write document transactions.
    Transactions,
    /// Expiry for documents or key/value entries.
    Ttl,
    /// Presigned blob upload or download URLs.
    PresignedUrls,
    /// Native blob streaming.
    StreamingBlobs,
    /// Collection enumeration.
    CollectionEnumeration,
    /// Atomic append conditional on a stream's next offset.
    ConditionalStreamAppend,
    /// Atomic validation of read dependencies with a document batch.
    ValidatedBatch,
    /// Document change watches.
    Watch,
    /// Expiring blobs.
    BlobTtl,
    /// Persisted document metadata and metadata query paths.
    RecordMetadata,
    /// Stream sealing and stream timing information.
    StreamSeal,
    /// Independently opened handles share immediately visible writes.
    SharedAcrossProcesses,
    /// Conditional writes arbitrate atomically across processes.
    CrossProcessCas,
}

impl Capability {
    const ALL: [Self; 21] = [
        Self::Documents,
        Self::Streams,
        Self::Kv,
        Self::Blobs,
        Self::Queues,
        Self::Secrets,
        Self::CollectionValidation,
        Self::FullText,
        Self::Transactions,
        Self::Ttl,
        Self::PresignedUrls,
        Self::StreamingBlobs,
        Self::CollectionEnumeration,
        Self::ConditionalStreamAppend,
        Self::ValidatedBatch,
        Self::Watch,
        Self::BlobTtl,
        Self::RecordMetadata,
        Self::StreamSeal,
        Self::SharedAcrossProcesses,
        Self::CrossProcessCas,
    ];
    const fn bit(self) -> u32 {
        match self {
            Self::Documents => 1 << 0,
            Self::Streams => 1 << 1,
            Self::Kv => 1 << 2,
            Self::Blobs => 1 << 3,
            Self::Queues => 1 << 4,
            Self::Secrets => 1 << 5,
            Self::CollectionValidation => 1 << 6,
            Self::FullText => 1 << 7,
            Self::Transactions => 1 << 8,
            Self::Ttl => 1 << 9,
            Self::PresignedUrls => 1 << 10,
            Self::StreamingBlobs => 1 << 11,
            Self::CollectionEnumeration => 1 << 12,
            Self::ConditionalStreamAppend => 1 << 13,
            Self::ValidatedBatch => 1 << 14,
            Self::Watch => 1 << 15,
            Self::BlobTtl => 1 << 16,
            Self::RecordMetadata => 1 << 17,
            Self::StreamSeal => 1 << 18,
            Self::SharedAcrossProcesses => 1 << 19,
            Self::CrossProcessCas => 1 << 20,
        }
    }
}
impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Documents => "documents",
            Self::Streams => "streams",
            Self::Kv => "kv",
            Self::Blobs => "blobs",
            Self::Queues => "queues",
            Self::Secrets => "secrets",
            Self::CollectionValidation => "collection_validation",
            Self::FullText => "full_text",
            Self::Transactions => "transactions",
            Self::Ttl => "ttl",
            Self::PresignedUrls => "presigned_urls",
            Self::StreamingBlobs => "streaming_blobs",
            Self::CollectionEnumeration => "collection_enumeration",
            Self::ConditionalStreamAppend => "conditional_stream_append",
            Self::ValidatedBatch => "validated_batch",
            Self::Watch => "watch",
            Self::BlobTtl => "blob_ttl",
            Self::RecordMetadata => "record_metadata",
            Self::StreamSeal => "stream_seal",
            Self::SharedAcrossProcesses => "shared_across_processes",
            Self::CrossProcessCas => "cross_process_cas",
        })
    }
}

/// A compact set of optional port behaviors.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Capabilities(u32);
impl Capabilities {
    /// Returns the empty capability set.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }
    /// Returns every capability defined by this crate version.
    #[must_use]
    pub const fn all() -> Self {
        Self((1 << 21) - 1)
    }
    /// Returns this set with one capability added.
    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability.bit())
    }
    /// Returns this set with one capability removed.
    #[must_use]
    pub const fn without(self, capability: Capability) -> Self {
        Self(self.0 & !capability.bit())
    }
    /// Tests whether one capability is present.
    #[must_use]
    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }
    /// Returns an error if the requested capability is missing.
    ///
    /// # Errors
    /// Returns `Unsupported` when the capability is absent.
    pub fn require(self, capability: Capability) -> Result<()> {
        if self.contains(capability) {
            Ok(())
        } else {
            Err(StorageError::unsupported(
                capability,
                format!("this storage driver does not provide {capability}"),
            ))
        }
    }
    /// Iterates over capabilities in a stable order.
    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL
            .into_iter()
            .filter(move |c| self.contains(*c))
    }
}
impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
#[path = "capabilities_tests.rs"]
mod tests;
