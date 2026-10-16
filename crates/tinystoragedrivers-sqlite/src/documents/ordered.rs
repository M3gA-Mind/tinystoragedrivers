//! A limited query answered by SQL in the port's order.
//!
//! When every sort key is SQL-visible ([`order`]) and the page is limited, the
//! database does the sorting and the limiting; Rust then reads only the rows
//! it was handed. The SQL conditions are supersets, so every row is still
//! decoded and judged by [`Filter::matches_with`]; a row it rejects leaves the
//! page short, and the query continues from the last row it saw, the way a
//! cursor would, until the page is full or the rows run out.

use std::cmp::Ordering;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, params_from_iter};
use serde_json::Value;
use tinystoragedrivers_core::{CollectionSpec, CursorPosition, Filter, Result, Sort, Versioned};

use super::ops::{Ctx, stored};
use super::order::{self, FIRST_STRUCTURAL};
use super::pushdown::{self, Projection};
use crate::error::during;
use crate::sql::{ident, literal};

/// The first rows after `after` that match `filter`, in `sort` order, at most
/// `limit` of them; `None` when SQL cannot answer faithfully and the caller
/// must sort in Rust.
pub(super) fn matching_ordered(
    conn: &Connection,
    ctx: Ctx<'_>,
    spec: &CollectionSpec,
    filter: &Filter,
    sort: &[Sort],
    after: Option<&CursorPosition>,
    limit: usize,
) -> Result<Option<Vec<Versioned<Value>>>> {
    let collection = spec.name.as_str();
    let projection = Projection::new(ctx, spec, collection);
    let Some(order) = order::order(sort, &projection) else {
        return Ok(None);
    };
    for key in sort {
        if projection.has_rounded_numbers(conn, &key.field)? {
            return Ok(None);
        }
    }
    let ranks = order.structural_ranks();
    let selected: Vec<String> = ranks.iter().map(|rank| format!(", {rank}")).collect();
    let selected = selected.concat();
    let narrowing = pushdown::clause(filter, &projection);

    let mut found = Vec::new();
    let mut seek = after.cloned();
    let mut batch = limit.min(MAX_SQL_LIMIT);
    loop {
        let mut sql = format!(
            "SELECT id{selected} FROM {docs} {joins} WHERE scope = ? AND coll = {coll} AND deleted = 0 \
             AND (expires_ms IS NULL OR expires_ms > ?)",
            docs = ident(&ctx.tables.docs),
            joins = order.joins,
            coll = literal(collection),
        );
        let mut params = vec![
            SqlValue::Text(ctx.scope.to_owned()),
            SqlValue::Integer(i64::try_from(ctx.now_ms).unwrap_or(i64::MAX)),
        ];
        if let Some(clause) = &narrowing {
            and(&mut sql, &clause.sql);
            params.extend(clause.params.iter().cloned());
        }
        if let Some(position) = &seek {
            let Some(clause) = order::after(position, &order) else {
                return Ok(None);
            };
            and(&mut sql, &clause.sql);
            params.extend(clause.params);
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&order.terms);
        sql.push_str(" LIMIT ");
        sql.push_str(&batch.to_string());
        let mut statement = conn.prepare(&sql).map_err(during("query"))?;
        let rows: Vec<(String, bool)> = statement
            .query_map(params_from_iter(params), |row| {
                let mut structural = false;
                for index in 0..ranks.len() {
                    structural |= row.get::<_, i64>(index + 1)? >= FIRST_STRUCTURAL;
                }
                Ok((row.get(0)?, structural))
            })
            .map_err(during("query"))?
            .collect::<rusqlite::Result<_>>()
            .map_err(during("query"))?;
        if rows.iter().any(|(_, structural)| *structural) {
            return Ok(None);
        }
        let returned = rows.len();
        for (id, _) in rows {
            let Some(row) = stored(conn, ctx, collection, &id)? else {
                continue;
            };
            seek = Some(CursorPosition::for_record(&id, &row.doc, &row.meta, sort));
            let past = after.is_none_or(|after| {
                seek.as_ref()
                    .is_some_and(|here| here.compare(after, sort) == Ordering::Greater)
            });
            if past
                && row.is_live(spec, ctx.now_ms)
                && filter.matches_with(&id, &row.doc, &row.meta)
            {
                found.push(Versioned {
                    id,
                    version: row.version,
                    doc: row.doc,
                    meta: row.meta,
                });
                if found.len() == limit {
                    return Ok(Some(found));
                }
            }
        }
        if returned < batch {
            return Ok(Some(found));
        }
        batch = (limit - found.len())
            .max(MIN_CONTINUATION)
            .min(MAX_SQL_LIMIT);
    }
}

/// The largest `LIMIT` SQLite takes: a signed 64-bit integer. A larger caller
/// limit (`usize::MAX` means "all") is clamped, since no table holds more rows.
const MAX_SQL_LIMIT: usize = i64::MAX as usize;

/// Rows fetched when a page comes up short and the query continues.
const MIN_CONTINUATION: usize = 16;

fn and(sql: &mut String, condition: &str) {
    sql.push_str(" AND (");
    sql.push_str(condition);
    sql.push(')');
}
