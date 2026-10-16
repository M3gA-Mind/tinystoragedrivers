//! Sealing a SQLite stream: the three steps and the blob they share.
//!
//! The steps run in the order that keeps a crash between any two
//! recoverable, and [`SqliteStreams::seal_stream`] runs all three in one
//! transaction, so an append cannot slip between the snapshot and the mark.
//! Tests run them as separate transactions to leave the state a crash there
//! would:
//!
//! 1. [`SqliteStreams::seal_write_blob`] writes the compressed entries to the
//!    blob table. A reader sees nothing different: the head still says
//!    unsealed, so the rows are still the stream.
//! 2. [`SqliteStreams::seal_mark`] sets the head's `sealed` flag, provided the
//!    stream is still what the blob was written from. From here on reads
//!    decode the blob.
//! 3. [`SqliteStreams::seal_drop_rows`] deletes the rows the blob replaces.
//!
//! On a stream that is already sealed, sealing only finishes step 3.

use rusqlite::{Connection, OptionalExtension, params};
use tinystoragedrivers_core::{
    Result, SealOutcome, SealedLog, StorageError, sealed_blob_key, validate_stream,
};

use super::{SqliteStreams, head, to_offset};
use crate::error::during;
use crate::sql::{Tables, ident};

/// The blob's `content_type`.
const CONTENT_TYPE: &str = "application/zstd";

/// The sealed blob's bytes, `None` when there is no blob.
pub(super) fn blob_bytes(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    stream: &str,
) -> Result<Option<Vec<u8>>> {
    let sql = format!(
        "SELECT bytes FROM {} WHERE scope = ?1 AND key = ?2",
        ident(&tables.blobs)
    );
    conn.query_row(&sql, params![scope, sealed_blob_key(stream)?], |row| {
        row.get(0)
    })
    .optional()
    .map_err(during("read sealed stream"))
}

/// The size of the sealed blob, `None` when there is no blob.
fn blob_len(conn: &Connection, tables: &Tables, scope: &str, stream: &str) -> Result<Option<u64>> {
    let sql = format!(
        "SELECT length(bytes) FROM {} WHERE scope = ?1 AND key = ?2",
        ident(&tables.blobs)
    );
    let len: Option<i64> = conn
        .query_row(&sql, params![scope, sealed_blob_key(stream)?], |row| {
            row.get(0)
        })
        .optional()
        .map_err(during("read sealed stream"))?;
    Ok(len.map(to_offset))
}

fn missing_blob() -> StorageError {
    StorageError::backend("sealed stream has no blob")
}

/// The entries of a sealed stream, decoded from its blob.
pub(super) fn read_sealed(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    stream: &str,
) -> Result<SealedLog> {
    let bytes = blob_bytes(conn, tables, scope, stream)?.ok_or_else(missing_blob)?;
    SealedLog::decode(&bytes)
}

/// Remove the sealed blob, if there is one.
pub(super) fn delete_blob(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    stream: &str,
) -> Result<()> {
    let sql = format!(
        "DELETE FROM {} WHERE scope = ?1 AND key = ?2",
        ident(&tables.blobs)
    );
    conn.execute(&sql, params![scope, sealed_blob_key(stream)?])
        .map_err(during("delete sealed stream blob"))?;
    Ok(())
}

/// Step 1: write the compressed entries as the stream's blob.
fn write_blob(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    stream: &str,
) -> Result<SealOutcome> {
    let head = head(conn, tables, scope, stream)?
        .ok_or_else(|| StorageError::not_found("stream does not exist"))?;
    if head.sealed {
        return Err(StorageError::conflict("stream is already sealed"));
    }
    let sql = format!(
        "SELECT seq, body FROM {} WHERE scope = ?1 AND name = ?2 ORDER BY seq",
        ident(&tables.streams)
    );
    let mut statement = conn.prepare(&sql).map_err(during("read stream"))?;
    let rows: Vec<(i64, String)> = statement
        .query_map(params![scope, stream], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(during("read stream"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("read stream"))?;
    let dense = rows.len() as u64 == head.len - head.base
        && rows
            .iter()
            .zip(head.base..)
            .all(|((seq, _), expected)| to_offset(*seq) == expected);
    if !dense {
        return Err(StorageError::backend(
            "stream rows do not match the stream head; refusing to seal",
        ));
    }
    let values = rows
        .iter()
        .map(|(_, value)| serde_json::from_str(value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let log = SealedLog::new(head.base, values)?;
    let bytes = log.encode()?;
    let outcome = SealOutcome::new(log.base(), log.len(), bytes.len() as u64);
    let upsert = format!(
        "INSERT INTO {} (scope, key, content_type, size, sha256, bytes, expires_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)
         ON CONFLICT (scope, key) DO UPDATE SET content_type = excluded.content_type, size = excluded.size,
             sha256 = excluded.sha256, bytes = excluded.bytes, expires_ms = NULL",
        ident(&tables.blobs)
    );
    conn.execute(
        &upsert,
        params![
            scope,
            sealed_blob_key(stream)?,
            CONTENT_TYPE,
            i64::try_from(bytes.len()).unwrap_or(i64::MAX),
            crate::blobs::digest(&bytes),
            bytes
        ],
    )
    .map_err(during("write sealed stream"))?;
    Ok(outcome)
}

/// Step 2: mark the stream sealed, provided it is still the stream the blob
/// was written from and the blob is still there.
fn mark(
    conn: &Connection,
    tables: &Tables,
    scope: &str,
    stream: &str,
    written: SealOutcome,
    now_ms: i64,
) -> Result<()> {
    let head = head(conn, tables, scope, stream)?
        .ok_or_else(|| StorageError::conflict("stream was deleted while sealing"))?;
    if head.base != written.base || head.len != written.len {
        return Err(StorageError::conflict("stream changed while sealing"));
    }
    if blob_len(conn, tables, scope, stream)? != Some(written.blob_bytes) {
        return Err(StorageError::conflict("sealed blob changed while sealing"));
    }
    let update = format!(
        "UPDATE {} SET sealed = 1, sealed_ms = ?3 WHERE scope = ?1 AND name = ?2",
        ident(&tables.stream_heads)
    );
    conn.execute(&update, params![scope, stream, now_ms])
        .map_err(during("seal stream"))?;
    Ok(())
}

/// Step 3: delete the rows the blob replaces. Idempotent, and a no-op for a
/// stream that is not sealed.
fn drop_rows(conn: &Connection, tables: &Tables, scope: &str, stream: &str) -> Result<()> {
    if head(conn, tables, scope, stream)?.is_some_and(|head| head.sealed) {
        let delete = format!(
            "DELETE FROM {} WHERE scope = ?1 AND name = ?2",
            ident(&tables.streams)
        );
        conn.execute(&delete, params![scope, stream])
            .map_err(during("seal stream"))?;
    }
    Ok(())
}

impl SqliteStreams {
    /// Seal `stream`; see [`StreamStore::seal`](tinystoragedrivers_core::StreamStore::seal).
    ///
    /// The three steps run in one immediate transaction, so an append cannot
    /// land between the snapshot and the mark: it commits before the seal and
    /// is in the blob, or after it and is refused. A crash rolls the whole
    /// transaction back.
    pub(super) async fn seal_stream(&self, stream: &str) -> Result<SealOutcome> {
        validate_stream(stream)?;
        let name = stream.to_owned();
        let now_ms = self.now_ms();
        self.with(true, move |conn, tables, scope| {
            let Some(found) = head(conn, tables, scope, &name)? else {
                return Err(StorageError::not_found("stream does not exist"));
            };
            let outcome = if found.sealed {
                let bytes = blob_len(conn, tables, scope, &name)?.ok_or_else(missing_blob)?;
                SealOutcome::new(found.base, found.len, bytes)
            } else {
                let written = write_blob(conn, tables, scope, &name)?;
                mark(conn, tables, scope, &name, written, now_ms)?;
                written
            };
            drop_rows(conn, tables, scope, &name)?;
            Ok(outcome)
        })
        .await
    }

    /// Step 1 on its own, for tests that stop the protocol after it.
    #[cfg(test)]
    pub(super) async fn seal_write_blob(&self, stream: &str) -> Result<SealOutcome> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(true, move |conn, tables, scope| {
            write_blob(conn, tables, scope, &stream)
        })
        .await
    }

    /// Step 2 on its own, for tests that stop the protocol after it.
    #[cfg(test)]
    pub(super) async fn seal_mark(&self, stream: &str, written: SealOutcome) -> Result<()> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        let now_ms = self.now_ms();
        self.with(true, move |conn, tables, scope| {
            mark(conn, tables, scope, &stream, written, now_ms)
        })
        .await
    }
}
