//! The in-place upgrade from the v0.x layout (`_tsd_docs` and friends) to
//! layout 2.
//!
//! The upgrade runs as one `BEGIN IMMEDIATE` transaction, so a crash leaves
//! either the whole v0.x file or the whole layout-2 file, and it is not
//! reversible. Inside it:
//!
//! 1. the layout-2 tables are created beside the v0.x ones;
//! 2. documents are copied with `schema = 0`; `created_ms` and `updated_ms`
//!    come from the body when it carries a recognised v1 field (see
//!    [`recognised_ms`]) and from the open time otherwise, and `expires_ms`
//!    from the collection's declared v1 `ttl` field when the body holds one
//!    (the body keeps the field, which stays honoured);
//! 3. tombstones are folded in as `deleted = 1` rows with the body `{}` and
//!    the version they remembered (a live row with a stale tombstone keeps
//!    its own, higher version);
//! 4. stream entries and heads, search rows and blobs are copied, blobs
//!    gaining `size` and `sha256`;
//! 5. the v0.x tables are dropped and the declared indexes are recreated over
//!    the new table;
//! 6. `meta.layout = 2` is recorded.
//!
//! `auto_vacuum` is not touched: `VACUUM` cannot run inside a transaction, so
//! a file that is still `NONE` converts at the first
//! [`maintain`](crate::maintenance::maintain), which already owns that
//! one-time rewrite. The freed v0.x pages are reclaimed by the same call.

use rusqlite::{Connection, params};
use serde_json::Value;
use tinystoragedrivers_core::{CollectionSpec, Result, value};

use super::{LAYOUT, Tables, ident};
use crate::blobs::digest;
use crate::error::during;

/// The table names of the v0.x layout.
pub(super) struct V0 {
    pub(super) docs: String,
    fts: String,
    specs: String,
    streams: String,
    stream_meta: String,
    blobs: String,
    tombstones: String,
}

impl V0 {
    pub(super) fn new(prefix: &str) -> Self {
        let name = |table: &str| format!("{prefix}_tsd_{table}");
        Self {
            docs: name("docs"),
            fts: name("fts"),
            specs: name("collections"),
            streams: name("streams"),
            stream_meta: name("stream_meta"),
            blobs: name("blobs"),
            tombstones: name("tombstones"),
        }
    }
}

/// Upgrade the v0.x tables of `tables`' database to layout 2, atomically.
pub(super) fn run(conn: &Connection, tables: &Tables) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")
        .map_err(during("begin upgrade"))?;
    match convert(conn, tables, system_now_ms()) {
        Ok(()) => conn
            .execute_batch("COMMIT")
            .map_err(during("commit upgrade")),
        Err(error) => {
            // The original error is the one worth reporting.
            let _ = conn.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn system_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    conn.query_row(
        "SELECT COUNT(*) > 0 FROM pragma_table_info(?1) WHERE name = ?2",
        [table, column],
        |row| row.get(0),
    )
    .map_err(during("inspect v0 table"))
}

fn convert(conn: &Connection, tables: &Tables, now_ms: i64) -> Result<()> {
    let old = V0::new(&tables.prefix);
    let ddl: Vec<String> = tables
        .declarations()
        .into_iter()
        .map(|(_, ddl)| ddl)
        .collect();
    conn.execute_batch(&ddl.join(";\n"))
        .map_err(during("create layout 2 tables"))?;

    copy_docs(conn, tables, &old, now_ms)?;
    let (docs, old_tombstones) = (ident(&tables.docs), ident(&old.tombstones));
    conn.execute(
        &format!(
            "INSERT OR IGNORE INTO {docs}
                 (scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
             SELECT scope, coll, id, version, 0, ?1, ?1, NULL, 1, '{{}}' FROM {old_tombstones}"
        ),
        [now_ms],
    )
    .map_err(during("fold tombstones"))?;

    conn.execute_batch(&format!(
        "INSERT INTO {fts} (scope, coll, id, body) SELECT scope, coll, id, body FROM {old_fts};
         INSERT INTO {streams} (scope, name, seq, body) SELECT scope, stream, seq, value FROM {old_streams}",
        fts = ident(&tables.fts),
        old_fts = ident(&old.fts),
        streams = ident(&tables.streams),
        old_streams = ident(&old.streams),
    ))
    .map_err(during("copy search rows and streams"))?;
    let sealed = if has_column(conn, &old.stream_meta, "sealed")? {
        "sealed"
    } else {
        "0"
    };
    conn.execute_batch(&format!(
        "INSERT INTO {heads} (scope, name, base, len, sealed)
         SELECT scope, stream, base, len, {sealed} FROM {old_meta}",
        heads = ident(&tables.stream_heads),
        old_meta = ident(&old.stream_meta),
    ))
    .map_err(during("copy stream heads"))?;
    copy_blobs(conn, tables, &old)?;

    let specs = load_specs(conn, &old)?;
    for name in [
        &old.docs,
        &old.tombstones,
        &old.fts,
        &old.streams,
        &old.stream_meta,
        &old.blobs,
    ] {
        conn.execute_batch(&format!("DROP TABLE {}", ident(name)))
            .map_err(during("drop v0 table"))?;
    }
    for spec in &specs {
        tables.create_indexes(conn, spec)?;
    }
    tables.finish(conn)?;
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO {} (key, value) VALUES ('layout', ?1)",
            ident(&tables.meta)
        ),
        [LAYOUT],
    )
    .map_err(during("record layout"))?;
    conn.execute_batch(&format!(
        "INSERT OR IGNORE INTO {} (scope, operation, generation) SELECT scope, operation, generation FROM {}",
        ident(&tables.purge_receipts),
        ident(&tables.admissions),
    ))
    .map_err(during("migrate purge receipts"))
}

fn load_specs(conn: &Connection, old: &V0) -> Result<Vec<CollectionSpec>> {
    let mut statement = conn
        .prepare(&format!("SELECT spec FROM {}", ident(&old.specs)))
        .map_err(during("read collections"))?;
    let texts: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .map_err(during("read collections"))?
        .collect::<rusqlite::Result<_>>()
        .map_err(during("read collections"))?;
    texts
        .iter()
        .map(|text| Ok(serde_json::from_str(text)?))
        .collect()
}

fn copy_docs(conn: &Connection, tables: &Tables, old: &V0, now_ms: i64) -> Result<()> {
    let specs = load_specs(conn, old)?;
    let ttl = |coll: &str| {
        specs
            .iter()
            .find(|spec| spec.name == coll)
            .and_then(|spec| spec.ttl_field.as_deref())
    };
    let mut select = conn
        .prepare(&format!(
            "SELECT scope, coll, id, version, doc FROM {}",
            ident(&old.docs)
        ))
        .map_err(during("read v0 documents"))?;
    let mut insert = conn
        .prepare(&format!(
            "INSERT INTO {}
                 (scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
             VALUES (?1, ?2, ?3, ?4, 0, ?5, ?6, ?7, 0, ?8)",
            ident(&tables.docs)
        ))
        .map_err(during("copy documents"))?;
    let mut rows = select.query([]).map_err(during("read v0 documents"))?;
    while let Some(row) = rows.next().map_err(during("read v0 documents"))? {
        let coll: String = row.get(1).map_err(during("read v0 documents"))?;
        let text: String = row.get(4).map_err(during("read v0 documents"))?;
        let doc: Value = serde_json::from_str(&text)?;
        let created = recognised_ms(&doc, &["created_ms", "created_at", "createdAt"]);
        let updated = recognised_ms(&doc, &["updated_ms", "updated_at", "updatedAt"]);
        let created_ms = created.unwrap_or(now_ms);
        let updated_ms = updated.unwrap_or(created_ms).max(created_ms);
        let expires_ms = ttl(&coll)
            .and_then(|field| value::lookup(&doc, field))
            .and_then(Value::as_i64);
        insert
            .execute(params![
                row.get::<_, String>(0)
                    .map_err(during("read v0 documents"))?,
                coll,
                row.get::<_, String>(2)
                    .map_err(during("read v0 documents"))?,
                row.get::<_, i64>(3).map_err(during("read v0 documents"))?,
                created_ms,
                updated_ms,
                expires_ms,
                text,
            ])
            .map_err(during("copy documents"))?;
    }
    Ok(())
}

fn copy_blobs(conn: &Connection, tables: &Tables, old: &V0) -> Result<()> {
    let expiry = if has_column(conn, &old.blobs, "expires_at_ms")? {
        "expires_at_ms"
    } else {
        "NULL"
    };
    let mut select = conn
        .prepare(&format!(
            "SELECT scope, key, content_type, bytes, {expiry} FROM {}",
            ident(&old.blobs)
        ))
        .map_err(during("read v0 blobs"))?;
    let mut insert = conn
        .prepare(&format!(
            "INSERT INTO {} (scope, key, content_type, size, sha256, expires_ms, bytes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            ident(&tables.blobs)
        ))
        .map_err(during("copy blobs"))?;
    let mut rows = select.query([]).map_err(during("read v0 blobs"))?;
    while let Some(row) = rows.next().map_err(during("read v0 blobs"))? {
        let bytes: Vec<u8> = row.get(3).map_err(during("read v0 blobs"))?;
        insert
            .execute(params![
                row.get::<_, String>(0).map_err(during("read v0 blobs"))?,
                row.get::<_, String>(1).map_err(during("read v0 blobs"))?,
                row.get::<_, Option<String>>(2)
                    .map_err(during("read v0 blobs"))?,
                i64::try_from(bytes.len()).unwrap_or(i64::MAX),
                digest(&bytes),
                row.get::<_, Option<i64>>(4)
                    .map_err(during("read v0 blobs"))?,
                bytes,
            ])
            .map_err(during("copy blobs"))?;
    }
    Ok(())
}

/// The first of `fields` that holds an instant, as epoch milliseconds: a
/// number is taken as milliseconds, a string as RFC 3339 text.
fn recognised_ms(doc: &Value, fields: &[&str]) -> Option<i64> {
    fields.iter().find_map(|field| match doc.get(field)? {
        Value::Number(number) => number.as_i64(),
        Value::String(text) => parse_rfc3339_ms(text),
        _ => None,
    })
}

/// Epoch milliseconds of an RFC 3339 timestamp (`2024-05-01T12:30:00.5+02:00`).
fn parse_rfc3339_ms(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    let digits = |from: usize, len: usize| -> Option<i64> {
        let part = bytes.get(from..from + len)?;
        part.iter()
            .all(u8::is_ascii_digit)
            .then(|| part.iter().fold(0, |n, d| n * 10 + i64::from(d - b'0')))
    };
    let (year, month, day) = (digits(0, 4)?, digits(5, 2)?, digits(8, 2)?);
    let (hour, minute, second) = (digits(11, 2)?, digits(14, 2)?, digits(17, 2)?);
    if bytes.get(4) != Some(&b'-')
        || bytes.get(7) != Some(&b'-')
        || !matches!(bytes.get(10), Some(b'T' | b't' | b' '))
        || bytes.get(13) != Some(&b':')
        || bytes.get(16) != Some(&b':')
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut at = 19;
    let mut millis = 0;
    if bytes.get(at) == Some(&b'.') {
        let start = at + 1;
        let end = start
            + bytes[start..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count();
        if end == start {
            return None;
        }
        for (place, digit) in bytes[start..end].iter().take(3).enumerate() {
            millis += i64::from(digit - b'0') * 10_i64.pow(2 - u32::try_from(place).ok()?);
        }
        at = end;
    }
    let offset_minutes = match bytes.get(at)? {
        b'Z' | b'z' if at + 1 == bytes.len() => 0,
        sign @ (b'+' | b'-') if at + 6 == bytes.len() && bytes.get(at + 3) == Some(&b':') => {
            let minutes = digits(at + 1, 2)? * 60 + digits(at + 4, 2)?;
            if *sign == b'-' { -minutes } else { minutes }
        }
        _ => return None,
    };
    // Days since 1970-01-01 of a proleptic Gregorian date.
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    let day_of_year = (153 * m + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let days = era * 146_097 + day_of_era - 719_468;
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset_minutes * 60;
    Some(seconds * 1000 + millis)
}

#[cfg(test)]
#[path = "upgrade_tests.rs"]
mod tests;
