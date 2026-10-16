//! Listing the scopes that hold records.

use rusqlite::Connection;
use tinystoragedrivers_core::{Result, Scope, StorageError};

use crate::error::during;
use crate::sql::{Tables, ident};

/// The scopes with a document (live or deleted), stream or blob whose text starts
/// with `prefix`, in byte order.
///
/// The prefix is compared with `substr`, not `LIKE`, so `%` and `_` are
/// literal. The `scope >= prefix` term lets SQLite start each primary-key scan
/// at the prefix instead of at the first row.
pub(crate) fn list(conn: &Connection, tables: &Tables, prefix: &str) -> Result<Vec<Scope>> {
    let branches = [
        &tables.docs,
        &tables.streams,
        &tables.stream_heads,
        &tables.blobs,
    ]
    .map(|table| {
        format!(
            "SELECT scope FROM {} WHERE scope >= ?1 AND substr(scope, 1, length(?1)) = ?1",
            ident(table)
        )
    });
    let sql = format!("{} ORDER BY 1", branches.join(" UNION "));
    let mut statement = conn.prepare(&sql).map_err(during("list scopes"))?;
    let rows = statement
        .query_map([prefix], |row| row.get::<_, String>(0))
        .map_err(during("list scopes"))?;
    rows.map(|row| {
        let text = row.map_err(during("list scopes"))?;
        Scope::new(text).map_err(|error| {
            StorageError::backend("a stored scope is not a valid scope").with_source(error)
        })
    })
    .collect()
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
