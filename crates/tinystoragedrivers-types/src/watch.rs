//! Payload-free document invalidation subscriptions.
//!
//! Subscribe before reading authoritative state. Notifications only ask the
//! caller to reread; they confer no authority and are not mutation history.

use std::fmt;
use std::time::Duration;

use crate::{Result, StorageError};

#[cfg(test)]
#[path = "watch_tests.rs"]
mod tests;

/// Maximum encoded opaque cursor size accepted at the port boundary.
pub const MAX_WATCH_CURSOR_BYTES: usize = 65_536;

/// An opaque native resume cursor. Debug never exposes its contents.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct WatchCursor(Vec<u8>);

impl WatchCursor {
    /// Wrap a driver's encoded cursor or a previously persisted cursor.
    ///
    /// # Errors
    /// Empty or oversized cursors return `InvalidInput`. Binding and native
    /// token validation remain the issuing driver's responsibility.
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        let cursor = Self(bytes);
        cursor.validate()?;
        Ok(cursor)
    }

    /// Encoded bytes for persistence or native driver decoding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Validate size, including cursors reconstructed by serde.
    ///
    /// # Errors
    /// `InvalidInput` for empty or oversized cursors.
    pub fn validate(&self) -> Result<()> {
        if self.0.is_empty() || self.0.len() > MAX_WATCH_CURSOR_BYTES {
            return Err(StorageError::invalid_input("invalid watch cursor size"));
        }
        Ok(())
    }
}

impl fmt::Debug for WatchCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("WatchCursor([REDACTED])")
    }
}

/// Resume selection and maximum quiet interval between conservative hints.
#[derive(Debug, Clone)]
pub struct WatchOptions {
    /// Resume the exact issuing database, scope, and query binding.
    pub after: Option<WatchCursor>,
    /// Upper bound on quiet waiting, including logical expiry invalidation.
    /// Drivers may emit earlier. Must be between one millisecond and one minute.
    pub interval: Duration,
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            after: None,
            interval: Duration::from_secs(1),
        }
    }
}

impl WatchOptions {
    /// Validate bounded waiting and cursor size before backend access.
    ///
    /// # Errors
    /// `InvalidInput` for intervals outside 1 ms..=60 s or invalid cursor sizes.
    pub fn validate(&self) -> Result<()> {
        if self.interval < Duration::from_millis(1) || self.interval > Duration::from_secs(60) {
            return Err(StorageError::invalid_input("invalid watch interval"));
        }
        if let Some(cursor) = &self.after {
            cursor.validate()?;
        }
        Ok(())
    }
}

/// Why a subscription can no longer preserve its invalidation boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WatchReset {
    /// The issuing backend can no longer resume the cursor's history.
    ResumeLost,
    /// The native collection or subscription was invalidated.
    Invalidated,
    /// A bounded buffer overflowed; missed hints cannot be reconstructed.
    Overflow,
}

/// A coalesced hint with no document body or decision payload.
pub enum WatchNotice {
    /// Reread authoritative state; conservative extra hints are permitted.
    Invalidated {
        /// Latest native resume boundary, if the source supports resumption.
        cursor: Option<WatchCursor>,
    },
    /// Terminal loss of the boundary. Drop and subscribe before rereading.
    ResetRequired {
        /// The loss category, never a new scope generation value.
        reason: WatchReset,
        /// Original typed backend failure, when a native error caused the loss.
        source: Option<StorageError>,
    },
}

impl fmt::Debug for WatchNotice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalidated { cursor } => f
                .debug_struct("Invalidated")
                .field("cursor", cursor)
                .finish(),
            Self::ResetRequired { reason, source } => f
                .debug_struct("ResetRequired")
                .field("reason", reason)
                .field("source_kind", &source.as_ref().map(StorageError::kind))
                .finish(),
        }
    }
}
