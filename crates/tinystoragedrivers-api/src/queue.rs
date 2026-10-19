//! At-least-once queue port.

use async_trait::async_trait;
use tinystoragedrivers_types::{
    DeadLetter, Delivery, Enqueued, NewMessage, QueueFeatures, QueueSpec, QueueStats, Receipt,
    Result, Retry,
};

/// Durable at-least-once queue operations bound to one scope.
#[async_trait]
pub trait QueueStore: Send + Sync + std::fmt::Debug {
    /// Optional queue features supported by this handle.
    fn features(&self) -> QueueFeatures;

    /// Declare a queue and its behavior.
    ///
    /// # Errors
    /// Returns an error for an invalid declaration or backend failure.
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()>;

    /// Enqueue messages and return assigned ids.
    ///
    /// # Errors
    /// Returns an error for invalid messages, an unknown queue, or backend
    /// failure.
    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>>;

    /// Receive messages, leasing them to the named consumer.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>>;

    /// Acknowledge a delivery receipt.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool>;

    /// Return a delivery for retry or move it to dead letters.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool>;

    /// Extend a delivery lease.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool>;

    /// Read queue counts.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn stats(&self, queue: &str) -> Result<QueueStats>;

    /// Read dead-lettered messages.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>>;

    /// Redrive one dead-lettered message.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn redrive(&self, queue: &str, id: &str) -> Result<bool>;

    /// Remove queued and dead-lettered messages.
    ///
    /// # Errors
    /// Returns an error for an unknown queue or backend failure.
    async fn purge(&self, queue: &str) -> Result<u64>;
}
