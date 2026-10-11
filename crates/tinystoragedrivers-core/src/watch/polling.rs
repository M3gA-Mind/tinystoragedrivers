//! Explicit conservative polling, without native history or resume promises.
use std::fmt;

use super::{DocumentWatch, WatchNotice, WatchOptions};
use crate::{Filter, Result, ScopedStorage, StorageError, validate_collection};

/// An explicitly selected polling subscription over one admitted handle.
///
/// This does not advertise native Watch support. Every tick conservatively
/// invalidates the query, including changes that entered and left between
/// reads. No document values are cached and no detached task is spawned.
pub struct PollingWatch {
    storage: ScopedStorage,
    interval: std::time::Duration,
}

impl fmt::Debug for PollingWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollingWatch").finish_non_exhaustive()
    }
}

impl PollingWatch {
    /// Open before the initial authoritative read. Only the explicit handle
    /// is used; this adapter never finds or installs a default backend.
    ///
    /// # Errors
    /// InvalidInput for names, filters, options or any resume cursor;
    /// Unsupported without native validated batches; native admission failures
    /// retain their type and source, including stale fence refusal.
    pub async fn open(
        storage: ScopedStorage,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Self> {
        validate_collection(collection)?;
        filter.validate()?;
        options.validate()?;
        if options.after.is_some() {
            return Err(StorageError::invalid_input(
                "polling watch cannot resume native history",
            ));
        }
        storage
            .documents()
            .atomic_batch_checked(vec![], vec![])
            .await?;
        Ok(Self {
            storage,
            interval: options.interval,
        })
    }
}

#[crate::async_trait]
impl DocumentWatch for PollingWatch {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        tokio::time::sleep(self.interval).await;
        self.storage
            .documents()
            .atomic_batch_checked(vec![], vec![])
            .await?;
        Ok(Some(WatchNotice::Invalidated { cursor: None }))
    }
}

#[cfg(test)]
#[path = "polling_tests.rs"]
mod tests;
