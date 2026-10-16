//! The tree view over the flat blob namespace.
//!
//! Blob keys are `/`-separated paths, but no driver stores directories. A
//! skill bundle (a `SKILL.md` plus scripts and references) is a set of keys
//! under one prefix; this module gives that set a directory-like shape without
//! asking any driver for anything beyond [`BlobStore::list`], so every driver
//! and the bus client agree on it by construction.

use std::collections::BTreeMap;

use super::{BlobMeta, MAX_BLOB_KEY_LEN, validate_user_blob_key};
use crate::error::{Result, StorageError};

/// One child of a directory in the tree view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeEntry {
    /// A sub-directory: at least one blob lives below it.
    Dir {
        /// The child's name, one path segment.
        name: String,
        /// The directory's full prefix, without a trailing `/`.
        prefix: String,
    },
    /// A blob directly in the directory.
    Blob {
        /// The blob's name, one path segment.
        name: String,
        /// The blob's metadata; `meta.key` is the full key.
        meta: BlobMeta,
    },
}

impl TreeEntry {
    /// The entry's one-segment name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Dir { name, .. } | Self::Blob { name, .. } => name,
        }
    }
}

/// A file to write with [`BlobStore::put_tree`](super::BlobStore::put_tree).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeFile {
    /// Path relative to the tree's prefix; validated like a blob key.
    pub path: String,
    /// The file's bytes.
    pub bytes: Vec<u8>,
    /// MIME type, when known.
    pub content_type: Option<String>,
}

impl TreeFile {
    /// A file with no content type.
    #[must_use]
    pub fn new(path: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            bytes: bytes.into(),
            content_type: None,
        }
    }
}

/// The key prefix `list` is asked for to enumerate directory `dir`:
/// `""` for the root, otherwise `dir` plus a `/` so that `skills/a` never
/// matches `skills/ab/x`.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when `dir` is
/// not a valid blob key (and not the root).
pub(super) fn list_prefix(dir: &str) -> Result<String> {
    if dir.is_empty() {
        return Ok(String::new());
    }
    validate_user_blob_key(dir)?;
    Ok(format!("{dir}/"))
}

/// Fold a flat listing under `list_prefix` into the directory's children,
/// sorted by name. A name that is both a blob and a directory (`a` and
/// `a/b`) yields both entries, the blob first.
pub(super) fn children(list_prefix: &str, listed: Vec<BlobMeta>) -> Vec<TreeEntry> {
    let mut out: BTreeMap<(String, bool), TreeEntry> = BTreeMap::new();
    for meta in listed {
        let Some(rest) = meta.key.strip_prefix(list_prefix) else {
            continue;
        };
        match rest.split_once('/') {
            None => {
                out.insert(
                    (rest.to_owned(), false),
                    TreeEntry::Blob {
                        name: rest.to_owned(),
                        meta,
                    },
                );
            }
            Some((name, _)) => {
                let prefix = format!("{list_prefix}{name}");
                out.entry((name.to_owned(), true))
                    .or_insert_with(|| TreeEntry::Dir {
                        name: name.to_owned(),
                        prefix,
                    });
            }
        }
    }
    out.into_values().collect()
}

/// Validate a whole tree before anything is written: every full key is a valid
/// blob key within `max_key_len`, and no two files share a path.
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) naming the first
/// offending path.
pub(super) fn plan(dir: &str, files: &[TreeFile], max_key_len: usize) -> Result<Vec<String>> {
    let base = list_prefix(dir)?;
    let mut seen = std::collections::HashSet::new();
    let mut keys = Vec::with_capacity(files.len());
    for file in files {
        let key = format!("{base}{}", file.path);
        validate_user_blob_key(&key)?;
        if key.len() > max_key_len.min(MAX_BLOB_KEY_LEN) {
            return Err(StorageError::invalid_input(format!(
                "tree path {:?} exceeds the blob key limit",
                file.path
            )));
        }
        if !seen.insert(key.clone()) {
            return Err(StorageError::invalid_input(format!(
                "tree path {:?} appears twice",
                file.path
            )));
        }
        keys.push(key);
    }
    Ok(keys)
}

#[cfg(test)]
#[path = "tree_tests.rs"]
mod tests;
