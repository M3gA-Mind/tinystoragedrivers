//! Synchronous document operations on one connection.
//!
//! Every function here runs inside the connection mutex and, for writes,
//! inside a transaction the caller opened, so a read-check-write sequence
//! (preconditions, unique indexes, claims) is atomic.

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, Row, params, params_from_iter};
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, CursorPosition, Filter, Meta, Precondition, Result, Sort, StorageError,
    Version, Versioned, WriteOptions, sort_documents_with_meta, validate_collection, validate_doc,
    validate_id, value,
};

use super::pushdown;
use crate::error::during;
use crate::sql::{Tables, ident, literal};

/// Who is asking and when.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Ctx<'a> {
    pub(crate) tables: &'a Tables,
    pub(crate) scope: &'a str,
    pub(crate) now_ms: u64,
}

/// The largest version SQLite's signed 64-bit column can hold.
const MAX_STORED_VERSION: u64 = i64::MAX as u64;

fn to_version(stored: i64) -> Version {
    Version(u64::try_from(stored).unwrap_or(0))
}

/// The collection's declaration, or a bare one when never declared.
pub(crate) fn load_spec(
    conn: &Connection,
    tables: &Tables,
    collection: &str,
) -> Result<CollectionSpec> {
    let sql = format!("SELECT spec FROM {} WHERE coll = ?1", ident(&tables.specs));
    let stored: Option<String> = conn
        .query_row(&sql, [collection], |row| row.get(0))
        .optional()
        .map_err(during("load collection"))?;
    match stored {
        Some(text) => Ok(serde_json::from_str(&text)?),
        None => Ok(CollectionSpec::new(collection)),
    }
}

/// Validate actual metadata presence without declaring schema.
pub(crate) fn validate_spec(
    conn: &Connection,
    tables: &Tables,
    required: &CollectionSpec,
) -> Result<()> {
    let sql = format!("SELECT spec FROM {} WHERE coll = ?1", ident(&tables.specs));
    let stored: Option<String> = conn
        .query_row(&sql, [&required.name], |row| row.get(0))
        .optional()
        .map_err(during("validate collection"))?;
    let stored =
        stored.ok_or_else(|| StorageError::not_found("collection has no persisted declaration"))?;
    let stored: CollectionSpec = serde_json::from_str(&stored)?;
    stored.validate()?;
    if stored.merge(required)? != stored {
        return Err(StorageError::invalid_input(
            "stored declaration lacks required features",
        ));
    }
    Ok(())
}

/// Merge `spec` into the stored declaration, create its indexes, and
/// rebuild the search index if the searchable fields changed.
pub(crate) fn declare(
    conn: &Connection,
    tables: &Tables,
    spec: &CollectionSpec,
    now_ms: u64,
) -> Result<()> {
    let existing = load_spec(conn, tables, &spec.name)?;
    let merged = existing.merge(spec)?;
    check_existing_unique(conn, tables, &existing, &merged, now_ms)?;
    let sql = format!(
        "INSERT INTO {} (coll, spec) VALUES (?1, ?2)
         ON CONFLICT (coll) DO UPDATE SET spec = excluded.spec",
        ident(&tables.specs)
    );
    conn.execute(&sql, params![merged.name, serde_json::to_string(&merged)?])
        .map_err(during("declare collection"))?;
    tables.create_indexes(conn, &merged)?;
    if existing.search != merged.search {
        reindex(conn, tables, &merged)?;
    }
    Ok(())
}

/// Refuse newly declared unique indexes that the stored live documents
/// already violate, in any scope.
fn check_existing_unique(
    conn: &Connection,
    tables: &Tables,
    existing: &CollectionSpec,
    merged: &CollectionSpec,
    now_ms: u64,
) -> Result<()> {
    let added: Vec<_> = merged
        .indexes
        .iter()
        .filter(|index| index.unique && !existing.indexes.contains(index))
        .collect();
    if added.is_empty() {
        return Ok(());
    }
    let sql = format!(
        "SELECT scope, body, expires_ms FROM {} WHERE coll = ?1 AND deleted = 0",
        ident(&tables.docs)
    );
    let mut statement = conn.prepare(&sql).map_err(during("check unique"))?;
    let rows: Vec<(String, String, Option<i64>)> = statement
        .query_map([&merged.name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(during("check unique"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("check unique"))?;
    let mut docs = Vec::with_capacity(rows.len());
    for (scope, doc, expires_ms) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        if !lapsed(merged, &doc, expires_ms, now_ms) {
            docs.push((scope, doc));
        }
    }
    for index in added {
        let mut keys: Vec<(String, Vec<Value>)> = docs
            .iter()
            .filter_map(|(scope, doc)| {
                let key = index
                    .fields
                    .iter()
                    .map(|field| value::lookup(doc, field).cloned())
                    .collect::<Option<Vec<Value>>>()?;
                Some((scope.clone(), key))
            })
            .collect();
        let order = |a: &(String, Vec<Value>), b: &(String, Vec<Value>)| {
            a.0.cmp(&b.0).then_with(|| {
                value::compare(&Value::Array(a.1.clone()), &Value::Array(b.1.clone()))
            })
        };
        keys.sort_by(order);
        if keys
            .windows(2)
            .any(|pair| order(&pair[0], &pair[1]).is_eq())
        {
            return Err(StorageError::already_exists(
                "stored documents already violate this unique index",
            ));
        }
    }
    Ok(())
}

/// Rebuild the full-text rows of every document in the collection.
fn reindex(conn: &Connection, tables: &Tables, spec: &CollectionSpec) -> Result<()> {
    let sql = format!("DELETE FROM {} WHERE coll = ?1", ident(&tables.fts));
    conn.execute(&sql, [&spec.name])
        .map_err(during("reindex"))?;
    let sql = format!(
        "SELECT scope, id, body FROM {} WHERE coll = ?1 AND deleted = 0",
        ident(&tables.docs)
    );
    let mut statement = conn.prepare(&sql).map_err(during("reindex"))?;
    let rows: Vec<(String, String, String)> = statement
        .query_map([&spec.name], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(during("reindex"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("reindex"))?;
    for (scope, id, doc) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        index_text(conn, tables, spec, &scope, &id, &doc)?;
    }
    Ok(())
}

/// The searchable text of `doc` under `spec`, if the collection is searchable.
fn search_body(spec: &CollectionSpec, doc: &Value) -> Option<String> {
    let search = spec.search.as_ref()?;
    let tokens: Vec<String> = search
        .fields
        .iter()
        .filter_map(|field| value::lookup(doc, field))
        .flat_map(value::tokens)
        .collect();
    Some(tokens.join(" "))
}

fn index_text(
    conn: &Connection,
    tables: &Tables,
    spec: &CollectionSpec,
    scope: &str,
    id: &str,
    doc: &Value,
) -> Result<()> {
    if let Some(body) = search_body(spec, doc) {
        let sql = format!(
            "INSERT INTO {} (scope, coll, id, body) VALUES (?1, ?2, ?3, ?4)",
            ident(&tables.fts)
        );
        conn.execute(&sql, params![scope, spec.name, id, body])
            .map_err(during("index text"))?;
    }
    Ok(())
}

fn unindex_text(conn: &Connection, ctx: Ctx<'_>, collection: &str, id: Option<&str>) -> Result<()> {
    let table = ident(&ctx.tables.fts);
    match id {
        Some(id) => conn.execute(
            &format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2 AND id = ?3"),
            params![ctx.scope, collection, id],
        ),
        None => conn.execute(
            &format!("DELETE FROM {table} WHERE scope = ?1 AND coll = ?2"),
            params![ctx.scope, collection],
        ),
    }
    .map_err(during("unindex text"))?;
    Ok(())
}

/// Whether `doc` has passed its collection's expiry time.
pub(crate) fn expired(spec: &CollectionSpec, doc: &Value, now_ms: u64) -> bool {
    spec.ttl_field
        .as_deref()
        .and_then(|field| value::lookup(doc, field))
        .and_then(Value::as_f64)
        .is_some_and(|expires| {
            // Epoch milliseconds fit an f64 exactly until the year 287396.
            #[allow(
                clippy::cast_precision_loss,
                reason = "epoch milliseconds are far below 2^53"
            )]
            let now = now_ms as f64;
            expires <= now
        })
}

/// Whether a document with `expires_ms` has lapsed: past its own expiry or
/// its collection's v1 ttl field.
pub(crate) fn lapsed(
    spec: &CollectionSpec,
    doc: &Value,
    expires_ms: Option<i64>,
    now_ms: u64,
) -> bool {
    let now = i64::try_from(now_ms).unwrap_or(i64::MAX);
    expires_ms.is_some_and(|expires| expires <= now) || expired(spec, doc, now_ms)
}

/// A stored row: live, expired or deleted.
struct Stored {
    version: Version,
    meta: Meta,
    deleted: bool,
    doc: Value,
}

impl Stored {
    /// Whether a reader sees this row.
    fn is_live(&self, spec: &CollectionSpec, now_ms: u64) -> bool {
        !self.deleted && !lapsed(spec, &self.doc, self.meta.expires_ms, now_ms)
    }
}

/// The metadata columns `schema, created_ms, updated_ms, expires_ms`, read
/// from `first` onward.
fn meta_at(row: &Row<'_>, first: usize) -> rusqlite::Result<Meta> {
    Ok(Meta {
        schema: u32::try_from(row.get::<_, i64>(first)?).unwrap_or(0),
        created_ms: row.get(first + 1)?,
        updated_ms: row.get(first + 2)?,
        expires_ms: row.get(first + 3)?,
    })
}

/// The stored row, live, expired or deleted.
fn stored(conn: &Connection, ctx: Ctx<'_>, collection: &str, id: &str) -> Result<Option<Stored>> {
    let sql = format!(
        "SELECT version, schema, created_ms, updated_ms, expires_ms, deleted, body FROM {}
         WHERE scope = ?1 AND coll = ?2 AND id = ?3",
        ident(&ctx.tables.docs)
    );
    let row: Option<(i64, Meta, bool, String)> = conn
        .query_row(&sql, params![ctx.scope, collection, id], |row| {
            Ok((row.get(0)?, meta_at(row, 1)?, row.get(5)?, row.get(6)?))
        })
        .optional()
        .map_err(during("get"))?;
    row.map(|(version, meta, deleted, body)| {
        Ok(Stored {
            version: to_version(version),
            meta,
            deleted,
            doc: serde_json::from_str(&body)?,
        })
    })
    .transpose()
}

/// Read one live document.
pub(crate) fn get(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
) -> Result<Option<Versioned<Value>>> {
    validate_collection(collection)?;
    validate_id(id)?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    Ok(stored(conn, ctx, collection, id)?
        .filter(|found| found.is_live(&spec, ctx.now_ms))
        .map(|found| Versioned {
            id: id.to_owned(),
            version: found.version,
            doc: found.doc,
            meta: found.meta,
        }))
}

/// Enforce a precondition against the live document, if any.
fn check(precondition: Precondition, current: Option<Version>) -> Result<()> {
    match (precondition, current) {
        (Precondition::None, _) | (Precondition::Absent, None) => Ok(()),
        (Precondition::Absent, Some(_)) => {
            Err(StorageError::conflict("the document already exists"))
        }
        (Precondition::Version(expected), Some(found)) if found == expected => Ok(()),
        (Precondition::Version(_), _) => Err(StorageError::conflict(
            "the document is not at the expected version",
        )),
    }
}

/// Live documents of the collection matching `filter`, in `sort` order.
pub(crate) fn matching(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
    sort: &[Sort],
) -> Result<Vec<Versioned<Value>>> {
    matching_after(conn, ctx, collection, filter, sort, None)
}

/// Live documents strictly after an optional cursor in `sort` order.
pub(crate) fn matching_after(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
    sort: &[Sort],
    after: Option<&CursorPosition>,
) -> Result<Vec<Versioned<Value>>> {
    validate_collection(collection)?;
    filter.validate()?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    let mut sql = format!(
        "SELECT id, version, schema, created_ms, updated_ms, expires_ms, body FROM {}
         WHERE scope = ? AND coll = {} AND deleted = 0 AND (expires_ms IS NULL OR expires_ms > ?)",
        ident(&ctx.tables.docs),
        literal(collection)
    );
    let mut params = vec![
        SqlValue::Text(ctx.scope.to_owned()),
        SqlValue::Integer(i64::try_from(ctx.now_ms).unwrap_or(i64::MAX)),
    ];
    if let Some(clause) = pushdown::clause(filter) {
        sql.push_str(" AND (");
        sql.push_str(&clause.sql);
        sql.push(')');
        params.extend(clause.params);
    }
    if let Some(clause) = after.and_then(|position| pushdown::after(position, sort)) {
        sql.push_str(" AND (");
        sql.push_str(&clause.sql);
        sql.push(')');
        params.extend(clause.params);
    }
    let mut statement = conn.prepare(&sql).map_err(during("query"))?;
    let rows: Vec<(String, i64, Meta, String)> = statement
        .query_map(params_from_iter(params), |row| {
            Ok((row.get(0)?, row.get(1)?, meta_at(row, 2)?, row.get(6)?))
        })
        .map_err(during("query"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("query"))?;
    let mut found = Vec::with_capacity(rows.len());
    for (id, version, meta, doc) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        if !expired(&spec, &doc, ctx.now_ms) && filter.matches_with(&id, &doc, &meta) {
            found.push(Versioned {
                id,
                version: to_version(version),
                doc,
                meta,
            });
        }
    }
    sort_documents_with_meta(&mut found, sort, |item| {
        (item.id.as_str(), &item.doc, &item.meta)
    });
    if let Some(after) = after {
        found.retain(|item| {
            CursorPosition::for_record(&item.id, &item.doc, &item.meta, sort).compare(after, sort)
                == std::cmp::Ordering::Greater
        });
    }
    Ok(found)
}

fn check_unique(
    conn: &Connection,
    ctx: Ctx<'_>,
    spec: &CollectionSpec,
    id: &str,
    doc: &Value,
) -> Result<()> {
    for index in spec.indexes.iter().filter(|index| index.unique) {
        let Some(key) = index
            .fields
            .iter()
            .map(|field| value::lookup(doc, field).cloned())
            .collect::<Option<Vec<Value>>>()
        else {
            continue;
        };
        let same = index
            .fields
            .iter()
            .zip(&key)
            .fold(Filter::All, |filter, (field, value)| {
                filter.and(Filter::eq(field.clone(), value.clone()))
            });
        let clash = matching(conn, ctx, &spec.name, &same, &[])?
            .into_iter()
            .any(|other| other.id != id);
        if clash {
            return Err(StorageError::already_exists(
                "a unique index already holds this value",
            ));
        }
    }
    Ok(())
}

/// Write one document under `precondition` and return its new version.
pub(crate) fn put(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    doc: &Value,
    precondition: Precondition,
) -> Result<Version> {
    put_with(
        conn,
        ctx,
        collection,
        id,
        doc,
        precondition,
        WriteOptions::default(),
    )
}

/// As [`put`], also recording the declared schema and expiry.
///
/// A live document keeps its `created_ms`; one that was never there, was
/// deleted or has expired is created now. `updated_ms` never runs backwards.
pub(crate) fn put_with(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    doc: &Value,
    precondition: Precondition,
    options: WriteOptions,
) -> Result<Version> {
    validate_collection(collection)?;
    validate_id(id)?;
    validate_doc(doc)?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    let previous = stored(conn, ctx, collection, id)?;
    let live = previous
        .as_ref()
        .filter(|found| found.is_live(&spec, ctx.now_ms));
    check(precondition, live.map(|found| found.version))?;
    check_unique(conn, ctx, &spec, id, doc)?;
    // Versions keep rising across expiry and deletion (a deleted row keeps its
    // version), so a stale CAS on a re-created document still fails.
    let version = match previous.as_ref().map(|found| found.version) {
        None => Version::FIRST,
        Some(last) => last
            .next()
            .filter(|next| next.0 <= MAX_STORED_VERSION)
            .ok_or_else(|| StorageError::backend("document version space is exhausted"))?,
    };
    let stored_version = i64::try_from(version.0)
        .map_err(|_| StorageError::backend("document version space is exhausted"))?;
    let now = i64::try_from(ctx.now_ms).unwrap_or(i64::MAX);
    // A migration may carry a record's original creation time over.
    let created_ms = options
        .created_ms
        .unwrap_or_else(|| live.map_or(now, |found| found.meta.created_ms));
    let updated_ms = live.map_or(now, |found| now.max(found.meta.updated_ms));
    let sql = format!(
        "INSERT INTO {} (scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9)
         ON CONFLICT (scope, coll, id) DO UPDATE SET
             version = excluded.version, schema = excluded.schema,
             created_ms = excluded.created_ms, updated_ms = excluded.updated_ms,
             expires_ms = excluded.expires_ms, deleted = 0, body = excluded.body",
        ident(&ctx.tables.docs)
    );
    conn.execute(
        &sql,
        params![
            ctx.scope,
            collection,
            id,
            stored_version,
            i64::from(options.schema),
            created_ms,
            updated_ms,
            options.expires_ms,
            serde_json::to_string(doc)?
        ],
    )
    .map_err(during("put"))?;
    unindex_text(conn, ctx, collection, Some(id))?;
    index_text(conn, ctx.tables, &spec, ctx.scope, id, doc)?;
    Ok(version)
}

/// Mark one document (or, with `id` unset, every document of the collection
/// in this scope) deleted. The row stays with its version and an empty body,
/// so the id never reuses a version.
fn bury(conn: &Connection, ctx: Ctx<'_>, collection: &str, id: Option<&str>) -> Result<()> {
    let sql = format!(
        "UPDATE {} SET deleted = 1, expires_ms = NULL, body = '{{}}'
         WHERE scope = ?1 AND coll = ?2{}",
        ident(&ctx.tables.docs),
        if id.is_some() { " AND id = ?3" } else { "" }
    );
    match id {
        Some(id) => conn.execute(&sql, params![ctx.scope, collection, id]),
        None => conn.execute(&sql, params![ctx.scope, collection]),
    }
    .map_err(during("delete"))?;
    Ok(())
}

/// Remove one document under `precondition`; report whether a live one went.
pub(crate) fn delete(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    id: &str,
    precondition: Precondition,
) -> Result<bool> {
    let current = get(conn, ctx, collection, id)?;
    check(precondition, current.as_ref().map(|found| found.version))?;
    bury(conn, ctx, collection, Some(id))?;
    unindex_text(conn, ctx, collection, Some(id))?;
    Ok(current.is_some())
}

/// Remove every live document matching `filter`.
pub(crate) fn delete_where(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    filter: &Filter,
) -> Result<u64> {
    let doomed = matching(conn, ctx, collection, filter, &[])?;
    for found in &doomed {
        delete(conn, ctx, collection, &found.id, Precondition::None)?;
    }
    Ok(doomed.len() as u64)
}

/// Remove every document of the collection in this scope.
pub(crate) fn drop_collection(conn: &Connection, ctx: Ctx<'_>, collection: &str) -> Result<()> {
    validate_collection(collection)?;
    bury(conn, ctx, collection, None)?;
    unindex_text(conn, ctx, collection, None)
}

/// List collections with records (including deleted ones) in one scope.
pub(crate) fn collections(conn: &Connection, ctx: Ctx<'_>, prefix: &str) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT coll FROM {} WHERE scope = ?1 ORDER BY coll",
        ident(&ctx.tables.docs),
    );
    let mut statement = conn.prepare(&sql).map_err(during("list collections"))?;
    let names: Vec<String> = statement
        .query_map([ctx.scope], |row| row.get(0))
        .map_err(during("list collections"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("list collections"))?;
    Ok(names
        .into_iter()
        .filter(|name| name.starts_with(prefix))
        .collect())
}

/// Full-text matches, best first.
pub(crate) fn search(
    conn: &Connection,
    ctx: Ctx<'_>,
    collection: &str,
    text: &str,
    limit: usize,
) -> Result<Vec<(String, f64)>> {
    validate_collection(collection)?;
    let spec = load_spec(conn, ctx.tables, collection)?;
    if spec.search.is_none() {
        return Err(StorageError::invalid_input(
            "this collection declares no search fields",
        ));
    }
    let wanted = value::tokens(&Value::String(text.to_owned()));
    if wanted.is_empty() || limit == 0 {
        return Ok(Vec::new());
    }
    let query = wanted
        .iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR ");
    let sql = format!(
        "SELECT f.id, bm25({fts}), d.body, d.expires_ms FROM {fts} AS f
         JOIN {docs} AS d ON d.scope = f.scope AND d.coll = f.coll AND d.id = f.id
         WHERE {fts} MATCH ?1 AND f.scope = ?2 AND f.coll = ?3 AND d.deleted = 0
         ORDER BY bm25({fts}), f.id",
        fts = ident(&ctx.tables.fts),
        docs = ident(&ctx.tables.docs),
    );
    let mut statement = conn.prepare(&sql).map_err(during("search"))?;
    let rows: Vec<(String, f64, String, Option<i64>)> = statement
        .query_map(params![query, ctx.scope, collection], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .map_err(during("search"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("search"))?;
    let mut hits = Vec::new();
    for (id, rank, doc, expires_ms) in rows {
        let doc: Value = serde_json::from_str(&doc)?;
        if !lapsed(&spec, &doc, expires_ms, ctx.now_ms) {
            // bm25 is lower-is-better; the port reports larger-is-better.
            hits.push((id, -rank));
        }
        if hits.len() == limit {
            break;
        }
    }
    Ok(hits)
}

#[cfg(test)]
#[path = "ops_tests.rs"]
mod tests;
