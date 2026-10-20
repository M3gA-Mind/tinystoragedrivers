//! The `doc_fields` side table: declared fields, projected out of the body.
//!
//! # Why a side table
//!
//! A body over 4 KiB is a compressed blob (see [`crate::sqlite::body`]), so SQL
//! cannot evaluate `json_extract` over it. The two ways to keep indexes and
//! pushdown working are:
//!
//! - **Generated columns.** They are computed from other columns of the same
//!   row by SQL expressions, and the only column that holds the document is
//!   the compressed one; no SQL function decompresses zstd. They would also
//!   have to be added with `ALTER TABLE` per declared index, on a table shared
//!   by every collection.
//! - **A side table** `doc_fields(scope, coll, id, path, kind, value)`. The
//!   driver extracts each declared field in Rust, before compressing, with the
//!   same [`value::lookup`] the filter uses, so the projection and the filter
//!   agree on every path (array indexes and awkward key names included). One
//!   generic table and two generic indexes serve every collection; declaring
//!   an index only changes which paths are projected. This is what the driver
//!   does.
//!
//! # What is stored
//!
//! For every projected path a document has, one row: `kind` is the JSON type
//! rank used by the sort order (0 null, 1 bool, 2 number, 3 string, 4 array,
//! 5 object) and `value` is the SQL value for a scalar (`NULL` for null,
//! arrays and objects; booleans are 0 and 1). A path the document lacks has no
//! row, which sorts like null. The projected paths of a collection are the
//! fields of its indexes and of its search declaration.
//!
//! Equality, membership and range filters, sorts and keyset cursors on a
//! projected path are answered from this table through
//! `(scope, coll, path, value, id)` (see `documents::order`); a filter or sort on any other path is
//! evaluated in Rust over the decoded rows. The SQL is a superset condition,
//! so [`Filter::matches_with`](tinystoragedrivers_types::Filter::matches_with)
//! still decides every row. Unique indexes are checked through the same
//! equality lookup. The projection is rewritten with every put, removed with
//! every delete, rebuilt for a collection when its projected paths change,
//! and rebuilt for the whole file when a layout-2 file written before the
//! side table is opened.

use std::collections::BTreeSet;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params};
use serde_json::Value;
use tinystoragedrivers_types::{CollectionSpec, Result, value};

use crate::sqlite::body;
use crate::sqlite::{SqliteTables, ident};
use tinystoragedrivers_connect::sqlite::error::during;

/// The paths of `spec` that are projected: index fields and search fields.
pub(crate) fn projected(spec: &CollectionSpec) -> BTreeSet<String> {
    spec.indexes
        .iter()
        .flat_map(|index| index.fields.iter())
        .chain(spec.search.iter().flat_map(|search| search.fields.iter()))
        .cloned()
        .collect()
}

/// The sort rank of a JSON value, the same one the cursors use.
pub(crate) fn rank(value: &Value) -> i64 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// The SQL value of a scalar, `NULL` for the rest.
pub(crate) fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Bool(flag) => SqlValue::Integer(i64::from(*flag)),
        Value::Number(number) => number.as_i64().map_or_else(
            || number.as_f64().map_or(SqlValue::Null, SqlValue::Real),
            SqlValue::Integer,
        ),
        Value::String(text) => SqlValue::Text(text.clone()),
        Value::Null | Value::Array(_) | Value::Object(_) => SqlValue::Null,
    }
}

/// Replace the projection of one document.
pub(crate) fn write(
    conn: &Connection,
    tables: &SqliteTables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    remove(conn, tables, scope, &spec.name, Some(id))?;
    insert(conn, tables, spec, scope, id, doc)
}

fn insert(
    conn: &Connection,
    tables: &SqliteTables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    let paths = projected(spec);
    if paths.is_empty() {
        return Ok(());
    }
    let mut statement = conn
        .prepare_cached(&format!(
            "INSERT INTO {} (scope, coll, id, path, kind, value) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            ident(tables.doc_fields())
        ))
        .map_err(during("project fields"))?;
    for path in &paths {
        if let Some(found) = value::lookup(doc, path) {
            statement
                .execute(params![
                    scope,
                    spec.name,
                    id,
                    path,
                    rank(found),
                    sql_value(found)
                ])
                .map_err(during("project fields"))?;
        }
    }
    Ok(())
}

/// Drop the projection of one document, or of the whole collection in `scope`.
pub(crate) fn remove(
    conn: &Connection,
    tables: &SqliteTables,
    scope: &str,
    collection: &str,
    id: Option<&str>,
) -> Result<()> {
    let table = ident(tables.doc_fields());
    match id {
        Some(id) => conn.execute(
            &format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2 AND id = ?3"),
            params![scope, collection, id],
        ),
        None => conn.execute(
            &format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2"),
            params![scope, collection],
        ),
    }
    .map_err(during("remove projected fields"))?;
    Ok(())
}

/// Rebuild the projection of every live document of the collection, in every
/// scope.
pub(crate) fn rebuild(
    conn: &Connection,
    tables: &SqliteTables,
    spec: &CollectionSpec,
) -> Result<()> {
    conn.execute(
        &format!("DELETE FROM {} WHERE coll = ?1", ident(tables.doc_fields())),
        [&spec.name],
    )
    .map_err(during("rebuild projected fields"))?;
    if projected(spec).is_empty() {
        return Ok(());
    }
    let mut statement = conn
        .prepare(&format!(
            "SELECT scope, id, body FROM {} WHERE coll = ?1 AND deleted = 0",
            ident(tables.docs())
        ))
        .map_err(during("rebuild projected fields"))?;
    let mut rows = statement
        .query([&spec.name])
        .map_err(during("rebuild projected fields"))?;
    while let Some(row) = rows.next().map_err(during("rebuild projected fields"))? {
        let scope: String = row.get(0).map_err(during("rebuild projected fields"))?;
        let id: String = row.get(1).map_err(during("rebuild projected fields"))?;
        let stored: SqlValue = row.get(2).map_err(during("rebuild projected fields"))?;
        insert(conn, tables, spec, &scope, &id, &body::decode(&stored)?)?;
    }
    Ok(())
}
