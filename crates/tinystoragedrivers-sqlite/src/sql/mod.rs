//! SQL text the driver builds: quoting, JSON paths, and the generic schema.
//!
//! # Layout 2
//!
//! One database holds these tables (names carry the database prefix, empty for
//! a database that has its file to itself; the three `_tsd_` tables keep the
//! names they had before layout 2):
//!
//! ```text
//! docs(scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
//! streams(scope, name, seq, body)            stream_heads(scope, name, base, len, sealed)
//! blobs(scope, key, content_type, size, sha256, expires_ms, bytes)
//! fts                                         FTS5, searchable collections only
//! meta(key, value)                            `layout` = 2
//! _tsd_collections, _tsd_scope_admissions, _tsd_purge_receipts
//! ```
//!
//! A removed document keeps its row with `deleted = 1` and the body `{}`, so
//! its version continues if the id is written again. Every read skips deleted
//! rows. [`upgrade`] converts a v0.x file (`_tsd_docs` and friends) in place.
//!
//! Every name that reaches SQL text is either validated by the core (collection
//! and database names are `[A-Za-z0-9_.-]`) or quoted here; values are always
//! bound as parameters. Index and partial-index expressions are the one place
//! a value is inlined, because SQLite only uses a partial index when the query
//! repeats its `WHERE` term literally.

use rusqlite::{Connection, OptionalExtension};
use tinystoragedrivers_core::{CollectionSpec, Result, StorageError};

use crate::error::during;

mod upgrade;

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

/// The `meta.layout` value this driver writes and understands.
pub(crate) const LAYOUT: &str = "2";

/// The table names of one (possibly prefixed) database.
#[derive(Debug, Clone)]
pub(crate) struct Tables {
    pub(crate) docs: String,
    pub(crate) admissions: String,
    pub(crate) purge_receipts: String,
    pub(crate) specs: String,
    pub(crate) fts: String,
    pub(crate) streams: String,
    pub(crate) stream_heads: String,
    pub(crate) blobs: String,
    pub(crate) meta: String,
    /// Prefix used for table and index names.
    pub(crate) prefix: String,
}

impl Tables {
    /// Tables for a database whose names start with `prefix` (empty for a
    /// database that has its file to itself).
    pub(crate) fn new(prefix: &str) -> Self {
        let internal = |table: &str| format!("{prefix}_tsd_{table}");
        let plain = |table: &str| format!("{prefix}{table}");
        Self {
            docs: plain("docs"),
            admissions: internal("scope_admissions"),
            purge_receipts: internal("purge_receipts"),
            specs: internal("collections"),
            fts: plain("fts"),
            streams: plain("streams"),
            stream_heads: plain("stream_heads"),
            blobs: plain("blobs"),
            meta: plain("meta"),
            prefix: prefix.to_owned(),
        }
    }

    /// Every table with its `CREATE` statement, in creation order.
    fn declarations(&self) -> Vec<(&str, String)> {
        let mut all = self.document_declarations();
        all.extend(self.stream_declarations());
        all
    }

    /// The internal, document and search tables.
    fn document_declarations(&self) -> Vec<(&str, String)> {
        let (docs, admissions, purge_receipts, specs, fts) = (
            ident(&self.docs),
            ident(&self.admissions),
            ident(&self.purge_receipts),
            ident(&self.specs),
            ident(&self.fts),
        );
        vec![
            (
                &self.admissions,
                format!(
                    "CREATE TABLE IF NOT EXISTS {admissions} (
                 scope TEXT PRIMARY KEY,
                 generation INTEGER NOT NULL,
                 operation TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.purge_receipts,
                format!(
                    "CREATE TABLE IF NOT EXISTS {purge_receipts} (
                 scope TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 PRIMARY KEY (scope, operation)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.docs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {docs} (
                 scope TEXT NOT NULL,
                 coll TEXT NOT NULL,
                 id TEXT NOT NULL,
                 version INTEGER NOT NULL,
                 schema INTEGER NOT NULL DEFAULT 0,
                 created_ms INTEGER NOT NULL,
                 updated_ms INTEGER NOT NULL,
                 expires_ms INTEGER,
                 deleted INTEGER NOT NULL DEFAULT 0,
                 body TEXT NOT NULL,
                 PRIMARY KEY (scope, coll, id)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.specs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {specs} (
                 coll TEXT PRIMARY KEY,
                 spec TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.fts,
                format!(
                    "CREATE VIRTUAL TABLE IF NOT EXISTS {fts} USING fts5(
                 scope UNINDEXED, coll UNINDEXED, id UNINDEXED, body,
                 tokenize = 'unicode61'
             )"
                ),
            ),
        ]
    }

    /// The stream, blob and meta tables.
    fn stream_declarations(&self) -> Vec<(&str, String)> {
        let (streams, heads, blobs, meta) = (
            ident(&self.streams),
            ident(&self.stream_heads),
            ident(&self.blobs),
            ident(&self.meta),
        );
        vec![
            (
                &self.streams,
                format!(
                    "CREATE TABLE IF NOT EXISTS {streams} (
                 scope TEXT NOT NULL,
                 name TEXT NOT NULL,
                 seq INTEGER NOT NULL,
                 body TEXT NOT NULL,
                 PRIMARY KEY (scope, name, seq)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.stream_heads,
                format!(
                    "CREATE TABLE IF NOT EXISTS {heads} (
                 scope TEXT NOT NULL,
                 name TEXT NOT NULL,
                 base INTEGER NOT NULL,
                 len INTEGER NOT NULL,
                 sealed INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (scope, name)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.blobs,
                format!(
                    "CREATE TABLE IF NOT EXISTS {blobs} (
                 scope TEXT NOT NULL,
                 key TEXT NOT NULL,
                 content_type TEXT,
                 size INTEGER NOT NULL,
                 sha256 BLOB NOT NULL,
                 expires_ms INTEGER,
                 bytes BLOB NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID"
                ),
            ),
            (
                &self.meta,
                format!(
                    "CREATE TABLE IF NOT EXISTS {meta} (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             ) WITHOUT ROWID"
                ),
            ),
        ]
    }

    /// Create every table if missing, upgrading a v0.x file first.
    ///
    /// - A file with the v0.x `_tsd_docs` table and no layout-2 `meta` table
    ///   is upgraded in one transaction (see [`upgrade`]).
    /// - A file that is already layout 2 is left alone.
    /// - A layout-2 file that still has v0.x tables is corrupt (an upgrade
    ///   commits atomically, so it cannot be left half done) and is refused.
    pub(crate) fn ensure(&self, conn: &Connection) -> Result<()> {
        let has_v0 = table_exists(conn, &upgrade::V0::new(&self.prefix).docs)?;
        if has_v0 {
            if table_exists(conn, &self.meta)? && self.layout(conn)?.is_some() {
                return Err(StorageError::backend(
                    "corrupt sqlite storage: layout 2 file still has v0 tables",
                ));
            }
            upgrade::run(conn, self)?;
        } else {
            self.create(conn)?;
        }
        self.validate(conn)
    }

    /// Create the layout-2 tables, the expiry index and the layout marker.
    fn create(&self, conn: &Connection) -> Result<()> {
        let ddl: Vec<String> = self
            .declarations()
            .into_iter()
            .map(|(_, ddl)| ddl)
            .collect();
        conn.execute_batch(&ddl.join(";\n"))
            .map_err(during("create tables"))?;
        self.finish(conn)?;
        conn.execute(
            &format!(
                "INSERT OR IGNORE INTO {} (key, value) VALUES ('layout', ?1)",
                ident(&self.meta)
            ),
            [LAYOUT],
        )
        .map_err(during("record layout"))?;
        conn.execute_batch(&format!(
            "INSERT OR IGNORE INTO {} (scope, operation, generation) SELECT scope, operation, generation FROM {}",
            ident(&self.purge_receipts),
            ident(&self.admissions),
        ))
        .map_err(during("migrate purge receipts"))
    }

    /// Indexes that live beside the tables rather than in a collection spec.
    fn finish(&self, conn: &Connection) -> Result<()> {
        // Lets a lifetime write find lapsed blobs without reading every one.
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} (expires_ms) WHERE expires_ms IS NOT NULL",
            ident(&format!("{}_expiry", self.blobs)),
            ident(&self.blobs)
        ))
        .map_err(during("index blob expiry"))
    }

    /// The recorded layout, `None` when the file has no `layout` row.
    fn layout(&self, conn: &Connection) -> Result<Option<String>> {
        conn.query_row(
            &format!(
                "SELECT value FROM {} WHERE key = 'layout'",
                ident(&self.meta)
            ),
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(during("read layout"))
    }

    /// Create the declared indexes of `spec` over the document body.
    ///
    /// An index is a performance hint: one whose paths SQLite cannot express
    /// is skipped, and queries on it are evaluated in Rust either way.
    pub(crate) fn create_indexes(&self, conn: &Connection, spec: &CollectionSpec) -> Result<()> {
        for index in &spec.indexes {
            let Some(columns) = index
                .fields
                .iter()
                .map(|field| {
                    json_path(field).map(|path| format!("json_extract(body, {})", literal(&path)))
                })
                .collect::<Option<Vec<String>>>()
            else {
                continue;
            };
            let sql = format!(
                "CREATE INDEX IF NOT EXISTS {name} ON {docs} (scope, {columns}) WHERE coll = {coll}",
                name = ident(&format!(
                    "{}tsd_ix_{}_{}",
                    self.prefix, spec.name, index.name
                )),
                docs = ident(&self.docs),
                columns = columns.join(", "),
                coll = literal(&spec.name),
            );
            conn.execute_batch(&sql).map_err(during("create index"))?;
        }
        Ok(())
    }

    /// Inspect the exact driver layout without creating or migrating tables.
    pub(crate) fn validate(&self, conn: &Connection) -> Result<()> {
        let normalize = |sql: &str| {
            sql.to_ascii_uppercase()
                .replace("IF NOT EXISTS", "")
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .collect::<String>()
        };
        for (name, expected) in self.declarations() {
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
            if normalize(&stored) != normalize(&expected) {
                return Err(StorageError::invalid_input(
                    "existing driver table has incompatible schema",
                ));
            }
        }
        match self.layout(conn)?.as_deref() {
            Some(LAYOUT) => Ok(()),
            _ => Err(StorageError::invalid_input(
                "existing driver tables are not layout 2",
            )),
        }
    }
}

/// Whether a table (or virtual table) named `name` exists.
pub(crate) fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [name],
        |row| row.get(0),
    )
    .map_err(during("inspect tables"))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
