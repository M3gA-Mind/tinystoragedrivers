//! Shared AMQP broker connection handles.

use std::sync::Arc;

use lapin::{Channel, Connection, ConnectionProperties};
use tinystoragedrivers_types::{Result, StorageError};

/// One live AMQP connection that can create channels for independent stores.
pub struct AmqpConnection {
    connection: Arc<Connection>,
}

impl std::fmt::Debug for AmqpConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpConnection")
            .field("connected", &self.connection.status().connected())
            .finish()
    }
}

impl AmqpConnection {
    /// Connect to an AMQP broker.
    ///
    /// # Errors
    /// Returns `Unavailable` when the connection cannot be established. The
    /// URL and its credentials are not included in the error message.
    pub async fn connect(url: &str) -> Result<Self> {
        let connection = Connection::connect(url, ConnectionProperties::default())
            .await
            .map_err(|error| {
                StorageError::unavailable("amqp broker connection failed").with_source(error)
            })?;
        Ok(Self {
            connection: Arc::new(connection),
        })
    }

    /// Create a channel on the shared connection.
    ///
    /// # Errors
    /// Returns `Unavailable` if the broker cannot create a channel.
    pub async fn create_channel(&self) -> Result<Channel> {
        self.connection.create_channel().await.map_err(|error| {
            StorageError::unavailable("amqp channel creation failed").with_source(error)
        })
    }

    /// Whether the underlying AMQP connection is open.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.connection.status().connected()
    }
}

#[cfg(test)]
#[path = "amqp_tests.rs"]
mod tests;
