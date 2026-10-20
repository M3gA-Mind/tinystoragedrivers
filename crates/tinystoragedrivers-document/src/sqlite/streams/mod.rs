//! [`StreamStore`] on SQLite.
//!
//! Records live in one table keyed by `(scope, stream, offset)`; a metadata
//! row per stream holds the first retained offset (`base`) and the next offset
//! to allocate (`len`). Allocating and inserting happen in one immediate
//! transaction, so offsets are dense even with concurrent appenders.
//!
//! A sealed stream (see [`StreamStore::seal`]) keeps its entries as one
//! compressed blob in the blob table instead of rows, and its head row's
//! `sealed` flag says to read the blob. The private `seal` module holds the protocol.

use std::fmt;

use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use tinystoragedrivers_api::{StreamStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, Capability, Result, Scope, SealOutcome, StorageError, StreamEntry, StreamInfo,
};

use crate::sqlite::{SqliteTables, ident};
use crate::validate_stream;
use tinystoragedrivers_connect::sqlite::SqliteConnection;
use tinystoragedrivers_connect::sqlite::error::during;

/// SQLite streams bound to one scope.
#[derive(Clone)]
pub struct SqliteStreams {
    connection: SqliteConnection,
    scope: Scope,
}

impl fmt::Debug for SqliteStreams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStreams")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

mod seal;

/// A stream's head row.
struct Head {
    /// The first retained offset.
    base: u64,
    /// The next offset to allocate.
    len: u64,
    /// Whether the entries are in the sealed blob rather than in rows.
    sealed: bool,
    /// When the stream was sealed, if known.
    sealed_ms: Option<i64>,
    /// When a record was last appended, if known.
    last_append_ms: Option<i64>,
}

/// The head of a stream, if it exists.
fn head(
    conn: &Connection,
    tables: &SqliteTables,
    scope: &str,
    stream: &str,
) -> Result<Option<Head>> {
    let sql = format!(
        "SELECT base, len, sealed, sealed_ms, last_append_ms FROM {}
         WHERE scope = ?1 AND name = ?2",
        ident(tables.stream_heads())
    );
    conn.query_row(&sql, params![scope, stream], |row| {
        Ok(Head {
            base: to_offset(row.get(0)?),
            len: to_offset(row.get(1)?),
            sealed: row.get::<_, i64>(2)? != 0,
            sealed_ms: row.get(3)?,
            last_append_ms: row.get(4)?,
        })
    })
    .optional()
    .map_err(during("read stream"))
}

/// Remove a stream's entries, head and sealed blob inside the caller's
/// transaction; whether it existed.
fn delete_in(conn: &Connection, tables: &SqliteTables, scope: &str, stream: &str) -> Result<bool> {
    conn.execute(
        &format!(
            "DELETE FROM {} WHERE scope = ?1 AND name = ?2",
            ident(tables.streams())
        ),
        params![scope, stream],
    )
    .map_err(during("delete stream"))?;
    let removed = conn
        .execute(
            &format!(
                "DELETE FROM {} WHERE scope = ?1 AND name = ?2",
                ident(tables.stream_heads())
            ),
            params![scope, stream],
        )
        .map_err(during("delete stream"))?;
    // Even when the stream is gone, clear a blob a crashed seal left.
    seal::delete_blob(conn, tables, scope, stream)?;
    Ok(removed > 0)
}

fn to_offset(stored: i64) -> u64 {
    u64::try_from(stored).unwrap_or(0)
}

fn to_stored(offset: u64) -> Result<i64> {
    i64::try_from(offset).map_err(|_| StorageError::backend("stream offset space is exhausted"))
}

impl SqliteStreams {
    /// Bind a stream adapter to a shared SQLite connection and opaque scope.
    #[must_use]
    pub fn new(connection: SqliteConnection, scope: Scope) -> Self {
        Self { connection, scope }
    }

    /// Run `f` in a read or immediate write transaction on the shared connection.
    async fn with<T, F>(&self, write: bool, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Connection, &SqliteTables, &str) -> Result<T> + Send + 'static,
    {
        let tables = self.connection.tables().clone();
        let scope = self.scope.clone();
        self.connection
            .run(move |conn| {
                let tx = if write {
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                } else {
                    conn.transaction()
                }
                .map_err(during(if write {
                    "start stream write transaction"
                } else {
                    "start stream read transaction"
                }))?;
                let value = f(&tx, &tables, scope.as_str())?;
                tx.commit().map_err(during("finish stream transaction"))?;
                Ok(value)
            })
            .await
    }
}

impl SqliteStreams {
    async fn append_inner(
        &self,
        stream: &str,
        expected: Option<u64>,
        values: Vec<Value>,
    ) -> Result<u64> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        let now_ms = i64::try_from((self.connection.clock())()).unwrap_or(i64::MAX);
        self.with(true, move |conn, tables, scope| {
            let (base, first) = match head(conn, tables, scope, &stream)? {
                Some(head) if head.sealed => {
                    return Err(StorageError::conflict("stream is sealed"));
                }
                Some(head) => (head.base, head.len),
                None => (0, 0),
            };
            if expected.is_some_and(|offset| offset != first) {
                return Err(StorageError::conflict("stream next offset changed"));
            }
            first
                .checked_add(values.len() as u64)
                .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
            if values.is_empty() {
                return Ok(first);
            }
            let insert = format!(
                "INSERT INTO {} (scope, name, seq, body) VALUES (?1, ?2, ?3, ?4)",
                ident(tables.streams())
            );
            let mut next = first;
            for value in &values {
                conn.execute(
                    &insert,
                    params![
                        scope,
                        stream,
                        to_stored(next)?,
                        serde_json::to_string(value)?
                    ],
                )
                .map_err(during("append"))?;
                next += 1;
            }
            let upsert = format!(
                "INSERT INTO {} (scope, name, base, len, last_append_ms) VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (scope, name) DO UPDATE SET len = excluded.len,
                     last_append_ms = excluded.last_append_ms",
                ident(tables.stream_heads())
            );
            conn.execute(
                &upsert,
                params![scope, stream, to_stored(base)?, to_stored(next)?, now_ms],
            )
            .map_err(during("append"))?;
            Ok(first)
        })
        .await
    }
}

#[async_trait]
impl StreamStore for SqliteStreams {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Streams)
            .with(Capability::ConditionalStreamAppend)
            .with(Capability::StreamSeal)
    }

    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.append_inner(stream, None, values).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        self.append_inner(stream, Some(expected_next_offset), values)
            .await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(false, move |conn, tables, scope| {
            if head(conn, tables, scope, &stream)?.is_some_and(|head| head.sealed) {
                return Ok(seal::read_sealed(conn, tables, scope, &stream)?.window(from, limit));
            }
            let sql = format!(
                "SELECT seq, body FROM {} WHERE scope = ?1 AND name = ?2 AND seq >= ?3
                 ORDER BY seq LIMIT ?4",
                ident(tables.streams())
            );
            let from = i64::try_from(from).unwrap_or(i64::MAX);
            let limit = i64::try_from(limit).unwrap_or(i64::MAX);
            let mut statement = conn.prepare(&sql).map_err(during("read stream"))?;
            let rows: Vec<(i64, String)> = statement
                .query_map(params![scope, stream, from, limit], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(during("read stream"))?
                .collect::<rusqlite::Result<_>>()
                .map_err(during("read stream"))?;
            rows.into_iter()
                .map(|(offset, value)| {
                    Ok(StreamEntry {
                        offset: to_offset(offset),
                        value: serde_json::from_str(&value)?,
                    })
                })
                .collect()
        })
        .await
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(false, move |conn, tables, scope| {
            Ok(head(conn, tables, scope, &stream)?.map_or(0, |head| head.len))
        })
        .await
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(true, move |conn, tables, scope| {
            let Some(Head {
                base, len, sealed, ..
            }) = head(conn, tables, scope, &stream)?
            else {
                return Ok(0);
            };
            if sealed {
                return Err(StorageError::conflict("stream is sealed"));
            }
            let cut = offset.clamp(base, len);
            let delete = format!(
                "DELETE FROM {} WHERE scope = ?1 AND name = ?2 AND seq < ?3",
                ident(tables.streams())
            );
            conn.execute(&delete, params![scope, stream, to_stored(cut)?])
                .map_err(during("truncate stream"))?;
            let update = format!(
                "UPDATE {} SET base = ?3 WHERE scope = ?1 AND name = ?2",
                ident(tables.stream_heads())
            );
            conn.execute(&update, params![scope, stream, to_stored(cut)?])
                .map_err(during("truncate stream"))?;
            Ok(cut - base)
        })
        .await
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(true, move |conn, tables, scope| {
            delete_in(conn, tables, scope, &stream)
        })
        .await
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        // The check and the delete share the write transaction, so the stream
        // cannot be replaced between them.
        self.with(true, move |conn, tables, scope| {
            let sealed = head(conn, tables, scope, &stream)?
                .is_some_and(|head| head.sealed_ms == Some(sealed_ms));
            if !sealed {
                return Ok(false);
            }
            delete_in(conn, tables, scope, &stream)
        })
        .await
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        self.seal_stream(stream).await
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        validate_stream(stream)?;
        let stream = stream.to_owned();
        self.with(false, move |conn, tables, scope| {
            Ok(head(conn, tables, scope, &stream)?.map(|head| {
                StreamInfo::new(head.base, head.len, head.sealed_ms, head.last_append_ms)
            }))
        })
        .await
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.with(false, move |conn, tables, scope| {
            let sql = format!(
                "SELECT name FROM {} WHERE scope = ?1 ORDER BY name",
                ident(tables.stream_heads())
            );
            let mut statement = conn.prepare(&sql).map_err(during("list streams"))?;
            let names: Vec<String> = statement
                .query_map(params![scope], |row| row.get(0))
                .map_err(during("list streams"))?
                .collect::<rusqlite::Result<_>>()
                .map_err(during("list streams"))?;
            Ok(names
                .into_iter()
                .filter(|name| name.starts_with(&prefix))
                .collect())
        })
        .await
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
