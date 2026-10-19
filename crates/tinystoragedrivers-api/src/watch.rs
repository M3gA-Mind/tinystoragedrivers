//! Native document invalidation subscriptions.

use std::fmt;

use async_trait::async_trait;
use tinystoragedrivers_types::{Result, WatchNotice};

/// Owned asynchronous invalidation subscription with no detached worker.
#[async_trait]
pub trait DocumentWatch: Send + fmt::Debug {
    /// Wait for a hint, or return `None` after a terminal reset.
    ///
    /// # Errors
    /// Returns a native backend error if waiting fails.
    async fn next(&mut self) -> Result<Option<WatchNotice>>;
}
