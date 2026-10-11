//! SQL text the driver builds: quoting, JSON paths, and the generic schema.
//!
//! Every name that reaches SQL text is either validated by the core (collection
//! and database names are `[A-Za-z0-9_.-]`) or quoted here; values are always
//! bound as parameters. Index and partial-index expressions are the one place
//! a value is inlined, because SQLite only uses a partial index when the query
//! repeats its `WHERE` term literally.

use rusqlite::{Connection, OptionalExtension};
use tinystoragedrivers_core::{Result, StorageError};

use crate::error::during;

/// Quote an SQL identifier.
pub(crate) fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote an SQL string literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The SQLite JSON path of a dotted field path (`owner.name` becomes
/// `$."owner"."name"`), or `None` when SQLite cannot address it the way
/// [`value::lookup`](tinystoragedrivers_core::value::lookup) does:
///
/// - a segment of digits is an array index on arrays and a key on objects,
///   which no single SQLite path expresses;
/// - SQLite's quoted path labels have no escape for `"` or `\`.
///
/// Callers fall back to evaluating such paths in Rust.
pub(crate) fn json_path(field: &str) -> Option<String> {
    let mut path = String::from("$");
    for segment in field.split('.') {
        if segment.bytes().all(|b| b.is_ascii_digit()) || segment.contains(['"', '\\']) {
            return None;
        }
        path.push_str(".\"");
        path.push_str(segment);
        path.push('"');
    }
    Some(path)
}

/// The table names of one (possibly prefixed) database.
#[derive(Debug, Clone)]
pub(crate) struct Tables {
    pub(crate) docs: String,
    pub(crate) admissions: String,
    pub(crate) purge_receipts: String,
    pub(crate) specs: String,
    pub(crate) fts: String,
    pub(crate) streams: String,
    pub(crate) stream_meta: String,
    pub(crate) blobs: String,
    /// The last version of removed documents, so an id never reuses one.
    pub(crate) tombstones: String,
    /// Prefix used for index names.
    pub(crate) prefix: String,
}

impl Tables {
    /// Tables for a database whose names start with `prefix` (empty for a
    /// database that has its file to itself).
    pub(crate) fn new(prefix: &str) -> Self {
        let name = |table: &str| format!("{prefix}_tsd_{table}");
        Self {
            docs: name("docs"),
            admissions: name("scope_admissions"),
            purge_receipts: name("purge_receipts"),
            specs: name("collections"),
            fts: name("fts"),
            streams: name("streams"),
            stream_meta: name("stream_meta"),
            blobs: name("blobs"),
            tombstones: name("tombstones"),
            prefix: prefix.to_owned(),
        }
    }

    /// Create every table if missing.
    fn declaration_sql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {admissions} (
                 scope TEXT PRIMARY KEY,
                 generation INTEGER NOT NULL,
                 operation TEXT NOT NULL
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {purge_receipts} (
                 scope TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 PRIMARY KEY (scope, operation)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {docs} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 doc TEXT NOT NULL,
                 PRIMARY KEY (scope, coll, id)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {specs} (
                 coll TEXT PRIMARY KEY,
                 spec TEXT NOT NULL
             ) WITHOUT ROWID;
             CREATE VIRTUAL TABLE IF NOT EXISTS {fts} USING fts5(
                 scope UNINDEXED, coll UNINDEXED, id UNINDEXED, body,
                 tokenize = 'unicode61'
             );
             CREATE TABLE IF NOT EXISTS {streams} (
                 scope TEXT NOT NULL,
                 stream TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 value TEXT NOT NULL,
                 PRIMARY KEY (scope, stream, seq)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {stream_meta} (
                 scope TEXT NOT NULL,
                 stream TEXT NOT NULL,
                 base INTEGER NOT NULL,
                 len INTEGER NOT NULL,
                 PRIMARY KEY (scope, stream)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {blobs} (
                 scope TEXT NOT NULL,
                 key TEXT NOT NULL,
                 content_type TEXT,
                 bytes BLOB NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS {tombstones} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 PRIMARY KEY (scope, coll, id)
             ) WITHOUT ROWID;",
            docs = ident(&self.docs),
            admissions = ident(&self.admissions),
            purge_receipts = ident(&self.purge_receipts),
            specs = ident(&self.specs),
            fts = ident(&self.fts),
            streams = ident(&self.streams),
            stream_meta = ident(&self.stream_meta),
            blobs = ident(&self.blobs),
            tombstones = ident(&self.tombstones),
        )
    }

    pub(crate) fn ensure(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(&self.declaration_sql())
            .map_err(during("create tables"))?;
        conn.execute_batch(&format!(
            "INSERT OR IGNORE INTO {} (scope, operation, generation) SELECT scope, operation, generation FROM {}",
            ident(&self.purge_receipts), ident(&self.admissions),
        )).map_err(during("migrate purge receipts"))
    }
    /// Inspect the exact driver layout without creating or migrating tables.
    pub(crate) fn validate(&self, conn: &Connection) -> Result<()> {
        let names = [
            &self.admissions,
            &self.purge_receipts,
            &self.docs,
            &self.specs,
            &self.fts,
            &self.streams,
            &self.stream_meta,
            &self.blobs,
            &self.tombstones,
        ];
        let declaration = self.declaration_sql();
        for (name, expected) in names.into_iter().zip(
            declaration
                .split(';')
                .filter(|sql| sql.trim_start().starts_with("CREATE")),
        ) {
            let stored: Option<String> = conn
                .query_row(
                    "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                    [name],
                    |row| row.get(0),
                )
                .optional()
                .map_err(during("inspect existing schema"))?;
            let stored =
                stored.ok_or_else(|| StorageError::not_found("driver table does not exist"))?;
            let normalize = |sql: &str| {
                sql.to_ascii_uppercase()
                    .replace("IF NOT EXISTS", "")
                    .chars()
                    .filter(|ch| !ch.is_whitespace())
                    .collect::<String>()
            };
            if normalize(&stored) != normalize(expected) {
                return Err(StorageError::invalid_input(
                    "existing driver table has incompatible schema",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
