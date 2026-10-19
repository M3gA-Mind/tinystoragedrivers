//! Errors returned by primitive ports.

use std::{error::Error as StdError, fmt};

use crate::Capability;

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;

/// The category of a storage failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The requested value does not exist.
    NotFound,
    /// A write precondition failed.
    Conflict,
    /// A unique value already exists.
    AlreadyExists,
    /// An optional capability is unavailable.
    Unsupported(Capability),
    /// Input violates the primitive contract.
    InvalidInput,
    /// Serialization or deserialization failed.
    Serialization,
    /// Cryptographic processing failed.
    Crypto,
    /// The backend may become available after retrying.
    Unavailable,
    /// An otherwise uncategorized backend error occurred.
    Backend,
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("not found"),
            Self::Conflict => f.write_str("conflict"),
            Self::AlreadyExists => f.write_str("already exists"),
            Self::Unsupported(cap) => write!(f, "unsupported capability {cap}"),
            Self::InvalidInput => f.write_str("invalid input"),
            Self::Serialization => f.write_str("serialization"),
            Self::Crypto => f.write_str("crypto"),
            Self::Unavailable => f.write_str("unavailable"),
            Self::Backend => f.write_str("backend"),
        }
    }
}

/// A typed port error with an optional underlying cause.
#[derive(Debug, thiserror::Error)]
#[error("{kind}: {message}")]
pub struct StorageError {
    kind: ErrorKind,
    message: String,
    #[source]
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl StorageError {
    /// Creates an error with the supplied kind and diagnostic message.
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }
    /// Attaches the backend error that caused this error.
    #[must_use]
    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }
    /// Creates a not-found error.
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }
    /// Creates a failed-precondition error.
    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, message)
    }
    /// Creates a duplicate-value error.
    #[must_use]
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::AlreadyExists, message)
    }
    /// Creates an unsupported-capability error.
    #[must_use]
    pub fn unsupported(capability: Capability, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported(capability), message)
    }
    /// Creates an invalid-input error.
    #[must_use]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, message)
    }
    /// Creates a serialization error.
    #[must_use]
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Serialization, message)
    }
    /// Creates a cryptographic error.
    #[must_use]
    pub fn crypto(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Crypto, message)
    }
    /// Creates a transient backend error.
    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }
    /// Creates a generic backend error.
    #[must_use]
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Backend, message)
    }
    /// Returns the stable error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
    /// Returns the diagnostic message without the category prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
    /// Returns whether retrying without caller changes may succeed.
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.kind == ErrorKind::Unavailable
    }
}

impl From<serde_json::Error> for StorageError {
    fn from(error: serde_json::Error) -> Self {
        Self::serialization(error.to_string()).with_source(error)
    }
}

/// The common result type returned by primitive ports.
pub type Result<T> = std::result::Result<T, StorageError>;
