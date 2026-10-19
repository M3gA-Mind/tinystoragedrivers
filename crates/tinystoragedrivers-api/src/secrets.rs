//! Secret storage port.

use async_trait::async_trait;
use tinystoragedrivers_types::Result;
use zeroize::Zeroizing;

/// Secret values bound to one scope.
#[async_trait]
pub trait SecretStore: Send + Sync + std::fmt::Debug {
    /// Read a secret, returning plaintext in a zeroizing buffer.
    ///
    /// # Errors
    /// Returns an error for an invalid name or backend failure.
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>>;

    /// Store plaintext secret bytes.
    ///
    /// # Errors
    /// Returns an error for an invalid name or backend failure.
    async fn set(&self, name: &str, value: &[u8]) -> Result<()>;

    /// Delete a secret.
    ///
    /// # Errors
    /// Returns an error for an invalid name or backend failure.
    async fn delete(&self, name: &str) -> Result<bool>;

    /// List names beginning with a prefix.
    ///
    /// # Errors
    /// Returns an error for an invalid prefix or backend failure.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Whether this backend can enumerate names.
    fn enumerable(&self) -> bool;

    /// Stable implementation label for diagnostics.
    fn backend_name(&self) -> &'static str;
}
