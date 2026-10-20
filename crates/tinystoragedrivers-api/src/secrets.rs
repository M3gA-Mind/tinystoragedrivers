//! Secret storage port.

use async_trait::async_trait;
use tinystoragedrivers_types::{
    Capability, Result, SecretPrecondition, SecretVersion, StorageError, validate_secret_name,
};
use zeroize::Zeroizing;

/// Secret values bound to one scope.
#[async_trait]
pub trait SecretStore: Send + Sync + std::fmt::Debug {
    /// Read a secret, returning plaintext in a zeroizing buffer.
    ///
    /// # Errors
    /// Returns an error for an invalid name or backend failure.
    async fn get(&self, name: &str) -> Result<Option<Zeroizing<Vec<u8>>>>;

    /// Read one secret and the opaque revision of the same snapshot.
    ///
    /// # Errors
    /// Returns `InvalidInput` for an invalid name and `Unsupported` when this
    /// store cannot atomically compare revisions.
    async fn get_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Zeroizing<Vec<u8>>, SecretVersion)>> {
        validate_secret_name(name)?;
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "this secret store does not support versioned reads",
        ))
    }

    /// Store plaintext secret bytes.
    ///
    /// # Errors
    /// Returns an error for an invalid name or backend failure.
    async fn set(&self, name: &str, value: &[u8]) -> Result<()>;

    /// Store `value` only while the precondition still holds, atomically.
    ///
    /// # Errors
    /// Returns `Conflict` when the condition no longer holds, `InvalidInput`
    /// for an invalid name or value, and `Unsupported` when this store cannot
    /// compare and write atomically.
    async fn set_if(
        &self,
        name: &str,
        value: &[u8],
        condition: SecretPrecondition,
    ) -> Result<SecretVersion> {
        validate_secret_name(name)?;
        let _ = (value, condition);
        Err(StorageError::unsupported(
            Capability::ConditionalSecretWrites,
            "this secret store does not support conditional writes",
        ))
    }

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
