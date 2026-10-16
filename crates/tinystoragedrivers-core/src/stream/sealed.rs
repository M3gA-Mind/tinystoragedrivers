//! The sealed form of a stream: one compressed blob.
//!
//! [`StreamStore::seal`](crate::StreamStore::seal) moves a stream's retained
//! entries out of its per-entry storage into one blob in the same scope's blob
//! store, under [`sealed_blob_key`]. The blob is a zstd frame (level 3) around
//! a JSON object:
//!
//! ```json
//! {"version": 1, "base": 5, "values": [ ... ]}
//! ```
//!
//! `values[i]` is the entry at offset `base + i`. The encoding is
//! deterministic, so sealing the same entries twice writes the same bytes.
//!
//! Drivers share this module so every one writes and reads the same blob:
//! a database moved between drivers by an importer keeps its sealed streams.
//!
//! Reading a sealed stream decodes the whole blob on every call. A decoded
//! cache would have to be invalidated by `delete_stream` followed by a new
//! stream of the same name, across handles and processes, so it is left out
//! until a measurement asks for one.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{StreamEntry, validate_stream};
use crate::error::{Result, StorageError};
use crate::key::Key;

/// The blob-key prefix reserved for sealed streams. Nothing else may write
/// under it: the blob ports refuse it
/// ([`validate_user_blob_key`](crate::validate_user_blob_key)) and hide it from
/// listings, and only a driver's own sealing code reaches it.
pub const SEALED_BLOB_PREFIX: &str = "_tsd/sealed/";

/// Whether `key` is under [`SEALED_BLOB_PREFIX`].
#[must_use]
pub fn is_reserved_blob_key(key: &str) -> bool {
    key.starts_with(SEALED_BLOB_PREFIX)
}

/// The blob format version [`SealedLog`] writes and accepts.
const VERSION: u32 = 1;

/// zstd's default compression level: a good ratio at JSON-journal speeds.
const LEVEL: i32 = 3;

/// The blob key holding sealed stream `stream`.
///
/// The name becomes one key segment: `%` and `/` are escaped as in
/// [`Key::of`] (so `a/b` is `a%2Fb` and never splits into two segments), a
/// name that is only dots is escaped so no segment is `.` or `..`, and a name
/// whose escaped form passes 512 bytes is replaced by its digest. Distinct
/// names always get distinct keys, and the result always passes
/// [`validate_blob_key`](crate::validate_blob_key).
///
/// ```
/// use tinystoragedrivers_core::sealed_blob_key;
///
/// assert_eq!(sealed_blob_key("run/1")?, "_tsd/sealed/run%2F1");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
///
/// # Errors
///
/// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) for an invalid
/// stream name.
pub fn sealed_blob_key(stream: &str) -> Result<String> {
    validate_stream(stream)?;
    let segment = Key::of([stream])?;
    let segment = match segment.as_str() {
        // A segment of `.` or `..` is refused by blob keys; `%2E` cannot start
        // any other escaped name, because a literal `%` escapes to `%25`.
        "." => "%2E".to_owned(),
        ".." => "%2E.".to_owned(),
        other => other.to_owned(),
    };
    Ok(format!("{SEALED_BLOB_PREFIX}{segment}"))
}

/// The entries of a sealed stream, held in memory.
#[derive(Debug, Clone, PartialEq)]
pub struct SealedLog {
    base: u64,
    values: Vec<Value>,
}

#[derive(Serialize)]
struct WireRef<'a> {
    version: u32,
    base: u64,
    values: &'a [Value],
}

#[derive(Deserialize)]
struct Wire {
    version: u32,
    base: u64,
    values: Vec<Value>,
}

impl SealedLog {
    /// Entries `values`, the first at offset `base`.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::InvalidInput`](crate::ErrorKind::InvalidInput) when
    /// `base` plus the number of entries does not fit a `u64`: such a log has
    /// no representable length.
    pub fn new(base: u64, values: Vec<Value>) -> Result<Self> {
        if base.checked_add(values.len() as u64).is_none() {
            return Err(StorageError::invalid_input(
                "a sealed log's base plus its entries overflows u64",
            ));
        }
        Ok(Self { base, values })
    }

    /// The first retained offset.
    #[must_use]
    pub fn base(&self) -> u64 {
        self.base
    }

    /// The stream's length: one past the last retained offset.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.base + self.values.len() as u64
    }

    /// Whether the log retains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The blob bytes for this log.
    ///
    /// # Errors
    ///
    /// A backend error if compression fails.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec(&WireRef {
            version: VERSION,
            base: self.base,
            values: &self.values,
        })?;
        zstd::encode_all(json.as_slice(), LEVEL).map_err(|error| {
            StorageError::backend("cannot compress a sealed stream").with_source(error)
        })
    }

    /// Read a log back from blob bytes.
    ///
    /// # Errors
    ///
    /// A backend error when the bytes are not a sealed stream blob this
    /// version understands: the blob is corrupt, or was written by a newer
    /// format.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let json = zstd::decode_all(bytes).map_err(|error| {
            StorageError::backend("cannot decompress a sealed stream").with_source(error)
        })?;
        let wire: Wire = serde_json::from_slice(&json).map_err(|error| {
            StorageError::backend("sealed stream blob is malformed").with_source(error)
        })?;
        if wire.version != VERSION {
            return Err(StorageError::backend(format!(
                "sealed stream blob has unsupported format version {}",
                wire.version
            )));
        }
        Self::new(wire.base, wire.values).map_err(|error| {
            StorageError::backend("sealed stream blob is malformed").with_source(error)
        })
    }

    /// Up to `limit` entries starting at offset `from`, in offset order;
    /// offsets below the base are skipped, as in
    /// [`StreamStore::read_window`](crate::StreamStore::read_window).
    #[must_use]
    pub fn window(&self, from: u64, limit: usize) -> Vec<StreamEntry> {
        let start = from.max(self.base);
        let Ok(skip) = usize::try_from(start - self.base) else {
            return Vec::new();
        };
        self.values
            .iter()
            .enumerate()
            .skip(skip)
            .take(limit)
            .map(|(index, value)| StreamEntry {
                offset: self.base + index as u64,
                value: value.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
#[path = "sealed_tests.rs"]
mod tests;
