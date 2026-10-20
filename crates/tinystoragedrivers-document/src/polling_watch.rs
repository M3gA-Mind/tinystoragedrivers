//! Explicit conservative polling over a caller-provided document handle.

use std::{fmt, sync::Arc};

use tinystoragedrivers_api::{DocumentStore, DocumentWatch};
use tinystoragedrivers_types::{
    Filter, Result, StorageError, WatchNotice, WatchOptions, validate_collection,
};

/// Poll a document engine and emit conservative invalidation hints.
///
/// Polling does not advertise native watch support or retain document values.
/// Every tick asks the caller to reread authoritative state, even if no
/// mutation occurred between ticks.
pub struct PollingWatch {
    documents: Arc<dyn DocumentStore>,
    interval: std::time::Duration,
}

impl fmt::Debug for PollingWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollingWatch").finish_non_exhaustive()
    }
}

impl PollingWatch {
    /// Open a polling subscription over the explicitly supplied document port.
    ///
    /// # Errors
    /// Invalid collection, filter, options, or resume cursors return
    /// `InvalidInput`. A backend failure during the initial admission check is
    /// returned unchanged.
    pub async fn open(
        documents: Arc<dyn DocumentStore>,
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
        documents.atomic_batch_checked(vec![], vec![]).await?;
        Ok(Self {
            documents,
            interval: options.interval,
        })
    }
}

#[tinystoragedrivers_api::async_trait]
impl DocumentWatch for PollingWatch {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        tokio::time::sleep(self.interval).await;
        self.documents.atomic_batch_checked(vec![], vec![]).await?;
        Ok(Some(WatchNotice::Invalidated { cursor: None }))
    }
}

#[cfg(test)]
#[path = "polling_watch_tests.rs"]
mod tests;
