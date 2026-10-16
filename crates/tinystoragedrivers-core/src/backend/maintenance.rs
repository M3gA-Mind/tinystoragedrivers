//! Space reclamation and write-ahead-log checkpoints.
//!
//! [`StorageBackend::maintain`](crate::StorageBackend::maintain) is the hook a
//! host schedules (on idle, on shutdown) to give a driver the chance to return
//! space it no longer needs. Seal and retention policies are applied by
//! [`Registry::maintain`](crate::schema::Registry::maintain), which takes the
//! same [`MaintenanceBudget`] (its `max_streams` limit) and reports what it did
//! in a [`PolicyReport`](crate::schema::PolicyReport). The types here are
//! `#[non_exhaustive]` so new limits and report fields are additions, not
//! breaking changes.

use crate::capabilities::Capability;
use crate::error::StorageError;

/// How much work one [`maintain`](crate::StorageBackend::maintain) call may do.
///
/// Build one with [`MaintenanceBudget::unlimited`],
/// [`MaintenanceBudget::reclaim_up_to`] or [`MaintenanceBudget::streams_up_to`];
/// further limits (a time bound, for instance) arrive as new fields.
///
/// ```
/// use tinystoragedrivers_core::MaintenanceBudget;
///
/// assert_eq!(MaintenanceBudget::unlimited().max_reclaim_bytes, None);
/// assert_eq!(MaintenanceBudget::reclaim_up_to(1 << 20).max_reclaim_bytes, Some(1 << 20));
/// assert_eq!(MaintenanceBudget::streams_up_to(8).max_streams, Some(8));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MaintenanceBudget {
    /// Stop reclaiming once about this many bytes have been returned. A driver
    /// that reclaims in fixed units (SQLite pages) rounds down to whole units,
    /// so a budget under one unit reclaims nothing, and `Some(0)` does no
    /// work. `None` means reclaim everything reclaimable.
    pub max_reclaim_bytes: Option<u64>,
    /// Seal or delete at most this many streams in one
    /// [`Registry::maintain`](crate::schema::Registry::maintain) call (a
    /// conflicted attempt counts), then stop and report `exhausted`.
    /// `Some(0)` does no work; `None` applies every due policy. Backends'
    /// `maintain` ignores it.
    pub max_streams: Option<u64>,
}

impl MaintenanceBudget {
    /// No limit: reclaim everything the driver can.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self {
            max_reclaim_bytes: None,
            max_streams: None,
        }
    }

    /// Reclaim at most about `bytes`.
    #[must_use]
    pub const fn reclaim_up_to(bytes: u64) -> Self {
        Self {
            max_reclaim_bytes: Some(bytes),
            max_streams: None,
        }
    }

    /// Seal or delete at most `streams` streams per policy pass.
    #[must_use]
    pub const fn streams_up_to(streams: u64) -> Self {
        Self::unlimited().with_max_streams(streams)
    }

    /// This budget, also limited to `streams` streams per policy pass.
    #[must_use]
    pub const fn with_max_streams(mut self, streams: u64) -> Self {
        self.max_streams = Some(streams);
        self
    }
}

/// What one [`maintain`](crate::StorageBackend::maintain) call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// Bytes the driver gave back to the filesystem.
    pub reclaimed_bytes: u64,
}

impl MaintenanceReport {
    /// A report of `reclaimed_bytes` returned.
    #[must_use]
    pub const fn new(reclaimed_bytes: u64) -> Self {
        Self { reclaimed_bytes }
    }
}

/// How hard [`checkpoint`](crate::StorageBackend::checkpoint) flushes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Checkpoint {
    /// Copy what can be copied without waiting for readers or writers.
    Passive,
    /// Copy everything and empty the log, so the database file alone holds the
    /// data. Call it before moving or copying a database file.
    Truncate,
}

pub(super) fn unsupported_maintenance() -> StorageError {
    StorageError::unsupported(
        Capability::Maintenance,
        "this storage driver has no maintenance to run",
    )
}

pub(super) fn unsupported_scopes() -> StorageError {
    StorageError::unsupported(
        Capability::ScopeEnumeration,
        "this storage driver cannot list scopes",
    )
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod tests;
