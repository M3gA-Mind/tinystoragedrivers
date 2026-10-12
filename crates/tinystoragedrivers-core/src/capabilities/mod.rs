//! What a driver can do beyond the baseline every driver must implement.
//!
//! The baseline is the whole [`DocumentStore`](crate::DocumentStore),
//! [`StreamStore`](crate::StreamStore) and [`BlobStore`](crate::BlobStore)
//! surface except the operations named here. A repository that needs one of
//! these checks [`Capabilities::require`] when it opens, so a deployment on the
//! wrong backend fails at boot rather than on the first request that needs it.

use std::fmt;

use crate::error::{Result, StorageError};

/// One optional ability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Capability {
    /// The backend can issue native handles that never initialize or repair schema.
    ExistingSchema,
    /// Read-only validation of persisted document collection declarations.
    CollectionValidation,
    /// [`DocumentStore::search`](crate::DocumentStore::search) over the
    /// fields a collection's [`SearchSpec`](crate::SearchSpec) names.
    FullText,
    /// [`DocumentStore::atomic_batch`](crate::DocumentStore::atomic_batch):
    /// several writes across documents and collections that land together or
    /// not at all.
    Transactions,
    /// Documents expire on their collection's
    /// [`ttl_field`](crate::CollectionSpec::ttl_field).
    Ttl,
    /// [`StorageBackend::for_scope_fenced`](crate::StorageBackend::for_scope_fenced):
    /// handles whose writes the driver checks against a
    /// [`Fence`](crate::Fence) in the same atomic step.
    Fencing,
    /// Direct object-store GET and PUT URLs, on the blob handle.
    PresignedUrls,
    /// Blob reads and writes use bounded buffers rather than whole objects.
    StreamingBlobs,
    /// `DocumentStore::collections` enumerates collections with records in
    /// the handle's scope.
    CollectionEnumeration,
    /// Atomic stream append guarded by its persistent next offset.
    ConditionalStreamAppend,
    /// Native fenced writes whose guard lives in another database.
    CrossDatabaseFencing,
    /// Document batches validate read-only dependencies atomically with writes.
    ValidatedBatch,
    /// Scoped reset with persistent generation admission.
    ScopePurge,
    /// Every storage port is purged atomically.
    AtomicScopePurge,
    /// Native scoped document invalidation subscriptions.
    Watch,
}

impl Capability {
    const ALL: [Self; 15] = [
        Self::ExistingSchema,
        Self::CollectionValidation,
        Self::FullText,
        Self::Transactions,
        Self::Ttl,
        Self::Fencing,
        Self::PresignedUrls,
        Self::StreamingBlobs,
        Self::CollectionEnumeration,
        Self::ConditionalStreamAppend,
        Self::CrossDatabaseFencing,
        Self::ValidatedBatch,
        Self::ScopePurge,
        Self::AtomicScopePurge,
        Self::Watch,
    ];

    const fn bit(self) -> u32 {
        match self {
            Self::ExistingSchema => 1 << 13,
            Self::CollectionValidation => 1 << 14,
            Self::FullText => 1,
            Self::Transactions => 1 << 1,
            Self::Ttl => 1 << 2,
            Self::Fencing => 1 << 3,
            Self::PresignedUrls => 1 << 4,
            Self::StreamingBlobs => 1 << 5,
            Self::CollectionEnumeration => 1 << 6,
            Self::ConditionalStreamAppend => 1 << 7,
            Self::CrossDatabaseFencing => 1 << 8,
            Self::ValidatedBatch => 1 << 9,
            Self::ScopePurge => 1 << 10,
            Self::AtomicScopePurge => 1 << 11,
            Self::Watch => 1 << 12,
        }
    }
}

impl fmt::Display for Capability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ExistingSchema => "existing_schema",
            Self::CollectionValidation => "collection_validation",
            Self::FullText => "full_text",
            Self::Transactions => "transactions",
            Self::Ttl => "ttl",
            Self::Fencing => "fencing",
            Self::PresignedUrls => "presigned_urls",
            Self::StreamingBlobs => "streaming_blobs",
            Self::CollectionEnumeration => "collection_enumeration",
            Self::ConditionalStreamAppend => "conditional_stream_append",
            Self::CrossDatabaseFencing => "cross_database_fencing",
            Self::ValidatedBatch => "validated_batch",
            Self::ScopePurge => "scope_purge",
            Self::AtomicScopePurge => "atomic_scope_purge",
            Self::Watch => "watch",
        })
    }
}

/// A set of [`Capability`] values.
///
/// ```
/// use tinystoragedrivers_core::{Capabilities, Capability};
///
/// let caps = Capabilities::none().with(Capability::Ttl);
/// assert!(caps.contains(Capability::Ttl));
/// assert!(caps.require(Capability::FullText).is_err());
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Capabilities(u32);

impl Capabilities {
    /// The empty set: a baseline driver.
    #[must_use]
    pub const fn none() -> Self {
        Self(0)
    }

    /// Every capability this version of the crate defines.
    ///
    /// The set grows in later releases, so a backend should list the
    /// capabilities it actually has rather than report `all()`. In
    /// particular, a backend that reports [`Capability::Fencing`] must
    /// implement (or forward)
    /// [`StorageBackend::for_scope_fenced`](crate::StorageBackend::for_scope_fenced);
    /// the trait's default refuses with `Unsupported(Fencing)`.
    #[must_use]
    pub const fn all() -> Self {
        Self(
            Capability::ExistingSchema.bit()
                | Capability::CollectionValidation.bit()
                | Capability::FullText.bit()
                | Capability::Transactions.bit()
                | Capability::Ttl.bit()
                | Capability::Fencing.bit()
                | Capability::PresignedUrls.bit()
                | Capability::StreamingBlobs.bit()
                | Capability::CollectionEnumeration.bit()
                | Capability::ConditionalStreamAppend.bit()
                | Capability::CrossDatabaseFencing.bit()
                | Capability::ValidatedBatch.bit()
                | Capability::ScopePurge.bit()
                | Capability::AtomicScopePurge.bit()
                | Capability::Watch.bit(),
        )
    }

    /// This set plus `capability`.
    #[must_use]
    pub const fn with(self, capability: Capability) -> Self {
        Self(self.0 | capability.bit())
    }

    /// This set minus `capability`.
    #[must_use]
    pub const fn without(self, capability: Capability) -> Self {
        Self(self.0 & !capability.bit())
    }

    /// Whether `capability` is in the set.
    #[must_use]
    pub const fn contains(self, capability: Capability) -> bool {
        self.0 & capability.bit() != 0
    }

    /// Succeed when `capability` is in the set.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Unsupported`](crate::ErrorKind::Unsupported) naming the
    /// missing capability.
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

    /// The capabilities in the set, in a stable order.
    pub fn iter(self) -> impl Iterator<Item = Capability> {
        Capability::ALL
            .into_iter()
            .filter(move |capability| self.contains(*capability))
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
