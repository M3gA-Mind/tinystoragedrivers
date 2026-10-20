//! How a document body sits in the `docs.body` column.
//!
//! A body is the document's compact JSON. When that text is longer than
//! [`COMPRESS_OVER`] bytes it is stored as a `BLOB` instead: one marker byte
//! ([`MARKER`]) followed by a zstd frame (level [`LEVEL`]) of the same JSON.
//! Smaller bodies (and any that would not shrink) stay `TEXT`, so the column
//! holds either type and a reader tells them apart by the value's SQLite type.
//! Column affinity does not interfere: `TEXT` affinity leaves a `BLOB` alone.
//!
//! SQL cannot see inside a compressed body, so nothing in SQL reads `body`;
//! indexed fields are projected into a side table (see [`crate::sqlite::fields`]).

use rusqlite::types::Value as SqlValue;
use serde_json::Value;
use tinystoragedrivers_types::{Result, StorageError};

/// Serialized JSON longer than this many bytes is compressed.
pub(crate) const COMPRESS_OVER: usize = 4096;

/// zstd's default level: a good ratio at document-write speeds.
const LEVEL: i32 = 3;

/// The first byte of a compressed body. It versions the encoding: a later
/// change picks another marker rather than guessing from the frame.
const MARKER: u8 = 1;

/// The column value for `doc`: `TEXT` for a small body, `BLOB` for a large one.
pub(crate) fn encode(doc: &Value) -> Result<SqlValue> {
    let text = serde_json::to_string(doc)?;
    if text.len() <= COMPRESS_OVER {
        return Ok(SqlValue::Text(text));
    }
    let frame = zstd::encode_all(text.as_bytes(), LEVEL)
        .map_err(|error| StorageError::backend("compress document body").with_source(error))?;
    if frame.len() >= text.len() {
        return Ok(SqlValue::Text(text));
    }
    let mut blob = Vec::with_capacity(frame.len() + 1);
    blob.push(MARKER);
    blob.extend_from_slice(&frame);
    Ok(SqlValue::Blob(blob))
}

#[cfg(test)]
thread_local! {
    /// Bodies decoded on this thread, for tests of how many rows a query reads.
    pub(crate) static DECODED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The document held by a `docs.body` value.
pub(crate) fn decode(stored: &SqlValue) -> Result<Value> {
    #[cfg(test)]
    DECODED.with(|count| count.set(count.get() + 1));
    match stored {
        SqlValue::Text(text) => Ok(serde_json::from_str(text)?),
        SqlValue::Blob(bytes) => match bytes.split_first() {
            Some((&MARKER, frame)) => {
                let json = zstd::decode_all(frame).map_err(|error| {
                    StorageError::backend("corrupt sqlite storage: bad compressed body")
                        .with_source(error)
                })?;
                Ok(serde_json::from_slice(&json)?)
            }
            _ => Err(StorageError::backend(
                "corrupt sqlite storage: unknown body encoding",
            )),
        },
        _ => Err(StorageError::backend(
            "corrupt sqlite storage: body is neither text nor a blob",
        )),
    }
}
