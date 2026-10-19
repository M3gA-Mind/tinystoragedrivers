//! Plain blob and tree values shared by blob engines.

use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "blob_tests.rs"]
mod tests;

/// Metadata for a stored blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobMeta {
    /// Opaque blob key.
    pub key: String,
    /// Stored body length in bytes.
    pub len: u64,
    /// Media type, when known.
    pub content_type: Option<String>,
}

/// A fully buffered blob value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    /// Metadata describing the body.
    pub meta: BlobMeta,
    /// Stored body bytes.
    pub bytes: Vec<u8>,
}

/// One child in a flat-key tree view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeEntry {
    /// A directory prefix containing at least one blob.
    Dir {
        /// Name of the child directory.
        name: String,
        /// Full child prefix without a trailing slash.
        prefix: String,
    },
    /// A blob directly inside the directory.
    Blob {
        /// Name of this child.
        name: String,
        /// Blob metadata; its key is the full key.
        meta: BlobMeta,
    },
}
impl TreeEntry {
    /// Returns this child's name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Dir { name, .. } | Self::Blob { name, .. } => name,
        }
    }
}

/// A file to add through a blob tree operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeFile {
    /// Relative path beneath the tree prefix.
    pub path: String,
    /// File bytes.
    pub bytes: Vec<u8>,
    /// Media type, when known.
    pub content_type: Option<String>,
}
impl TreeFile {
    /// Creates a file with no declared media type.
    #[must_use]
    pub fn new(path: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            bytes: bytes.into(),
            content_type: None,
        }
    }
}
