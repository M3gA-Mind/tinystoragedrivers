//! Stream sealing types: the outcome of a seal and the policy that states
//! when an owner seals.

use serde::{Deserialize, Serialize};

/// What [`StreamStore::seal`](crate::StreamStore::seal) leaves behind.
///
/// Sealing twice returns equal outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SealOutcome {
    /// The first retained offset: everything below it was truncated away
    /// before the seal.
    pub base: u64,
    /// The stream's length, the offset an append would have received. A
    /// stream truncated to nothing seals with `len == base`.
    pub len: u64,
    /// Size of the compressed blob holding the entries, in bytes.
    pub blob_bytes: u64,
}

impl SealOutcome {
    /// An outcome for a stream retaining offsets `base..len`, stored in a
    /// blob of `blob_bytes` bytes.
    #[must_use]
    pub const fn new(base: u64, len: u64, blob_bytes: u64) -> Self {
        Self {
            base,
            len,
            blob_bytes,
        }
    }

    /// How many entries the sealed stream holds.
    #[must_use]
    pub const fn retained(&self) -> u64 {
        self.len.saturating_sub(self.base)
    }
}

/// When the owner of a stream seals it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SealWhen {
    /// The owner calls [`StreamStore::seal`](crate::StreamStore::seal) when
    /// the stream's producer finishes.
    OnFinish,
    /// Maintenance seals the stream once it has gone this many milliseconds
    /// without an append.
    Idle(u64),
    /// The stream is never sealed.
    Never,
}

/// The sealing and retention policy a `Stream` schema item states.
///
/// This is only the declaration. Nothing applies it yet: the maintenance pass
/// that seals idle streams and deletes expired ones arrives with
/// `Maintenance::run`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SealPolicy {
    /// When to seal.
    pub when: SealWhen,
    /// Delete the stream (`delete_stream`) this many milliseconds after it
    /// was sealed. `None` keeps it until the owner deletes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_ms: Option<i64>,
}

impl SealPolicy {
    /// A policy that seals `when` and keeps sealed streams.
    #[must_use]
    pub const fn new(when: SealWhen) -> Self {
        Self {
            when,
            retain_ms: None,
        }
    }

    /// The same policy, deleting a stream `retain_ms` after it was sealed.
    #[must_use]
    pub const fn retaining_ms(mut self, retain_ms: i64) -> Self {
        self.retain_ms = Some(retain_ms);
        self
    }
}

impl Default for SealPolicy {
    /// Never seal, keep forever.
    fn default() -> Self {
        Self::new(SealWhen::Never)
    }
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
