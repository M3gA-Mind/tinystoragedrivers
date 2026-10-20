//! Translating the part of a [`Filter`] SQLite can evaluate exactly enough.
//!
//! The driver always re-checks every row with [`Filter::matches`], so the SQL
//! produced here only has to be a *superset* condition: it may let through rows
//! the filter rejects, never the reverse. That keeps it to clauses whose SQL
//! meaning is unambiguous: equality and membership on strings and booleans, and
//! on the document id. Numbers are left to the Rust check, because SQLite
//! turns very large integers into lossy floats.
//!
//! Body fields are answered from the `doc_fields` side table
//! ([`crate::sqlite::fields`]), because a large body is compressed and SQL cannot read
//! it. Only the projected paths of the collection (its index and search
//! fields) are pushed down; a term on any other path contributes nothing and
//! the Rust check decides.

use std::collections::BTreeSet;

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;
use tinystoragedrivers_types::{
    CREATED_FIELD, CollectionSpec, EXPIRES_FIELD, Filter, ID_FIELD, Result, SCHEMA_FIELD,
    UPDATED_FIELD,
};

use super::ops::Ctx;
use crate::sqlite::fields;
use crate::sqlite::{ident, literal};
use tinystoragedrivers_connect::sqlite::error::during;

/// Which body fields of one collection in one scope SQL can see, and where.
#[derive(Debug, Clone)]
pub(crate) struct Projection {
    pub(super) paths: BTreeSet<String>,
    fields_table: String,
    docs_table: String,
    scope: String,
    coll: String,
}

impl Projection {
    pub(crate) fn new(ctx: Ctx<'_>, spec: &CollectionSpec, collection: &str) -> Self {
        Self {
            paths: fields::projected(spec),
            fields_table: ident(ctx.tables.doc_fields()),
            docs_table: ident(ctx.tables.docs()),
            scope: literal(ctx.scope),
            coll: literal(collection),
        }
    }

    /// The `doc_fields` rows of `field` in this collection and scope, as a
    /// `FROM … WHERE` over alias `f` that still needs a closing condition.
    pub(super) fn rows(&self, field: &str) -> Option<String> {
        self.paths.contains(field).then(|| {
            format!(
                "FROM {} AS f WHERE f.scope = {} AND f.coll = {} AND f.path = {}",
                self.fields_table,
                self.scope,
                self.coll,
                literal(field)
            )
        })
    }

    /// A condition on the document id: `id IN (…)` over the projected rows of
    /// `field` whose value satisfies `test` (for example `= ?`).
    fn ids_where(&self, field: &str, test: &str) -> Option<String> {
        Some(format!(
            "id IN (SELECT f.id {} AND f.value {test})",
            self.rows(field)?
        ))
    }

    /// A `LEFT JOIN` that exposes the projected value of `field` as
    /// `s{index}.k` (kind) and `s{index}.v` (value) on each document row; a
    /// document without the path joins to nothing.
    pub(super) fn join(&self, index: usize, field: &str) -> Option<String> {
        Some(format!(
            "LEFT JOIN (SELECT f.id AS sid, f.kind AS k, f.value AS v {}) AS s{index} ON s{index}.sid = {}.id",
            self.rows(field)?,
            self.docs_table
        ))
    }

    /// Whether `field` holds a number SQLite may have rounded: a `u64` above
    /// `i64::MAX` is stored as a float, and two such values can tie in SQL
    /// where the port tells them apart. A sort over the path then stays in
    /// Rust. The range is the one a `u64` rounds into; text sorts above every
    /// number, so the upper bound also keeps strings out of the index scan.
    pub(super) fn has_rounded_numbers(&self, conn: &Connection, field: &str) -> Result<bool> {
        let Some(rows) = self.rows(field) else {
            return Ok(false);
        };
        conn.query_row(
            &format!(
                "SELECT 1 {rows} AND f.value >= 9223372036854775808.0 AND f.value <= 18446744073709551616.0 LIMIT 1"
            ),
            [],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(during("query"))
    }
}

/// SQL text and its bound parameters.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct Clause {
    pub(crate) sql: String,
    pub(crate) params: Vec<SqlValue>,
}

/// The SQL form of a value an equality can be pushed down for.
fn pushable(value: &Value) -> Option<SqlValue> {
    match value {
        Value::String(text) => Some(SqlValue::Text(text.clone())),
        Value::Bool(flag) => Some(SqlValue::Integer(i64::from(*flag))),
        _ => None,
    }
}

/// The column that holds a reserved metadata path (`_created_ms` and the
/// rest), `None` for any other path.
pub(super) fn meta_column(field: &str) -> Option<&'static str> {
    match field {
        SCHEMA_FIELD => Some("schema"),
        CREATED_FIELD => Some("created_ms"),
        UPDATED_FIELD => Some("updated_ms"),
        EXPIRES_FIELD => Some("expires_ms"),
        _ => None,
    }
}

/// A whole-number bound on a metadata column. The columns are `INTEGER`, so
/// comparing them with an integer is exact; any other number is left to Rust.
fn integer(value: &Value) -> Option<SqlValue> {
    value.as_i64().map(SqlValue::Integer)
}

/// The condition "`field` satisfies `test`" (for example `= ?`), or `None`
/// when SQL cannot see the field.
fn on_field(field: &str, test: &str, projection: &Projection) -> Option<String> {
    if field == ID_FIELD {
        Some(format!("id {test}"))
    } else {
        projection.ids_where(field, test)
    }
}

/// The `kind` rank and SQL value of a range bound, and whether the comparison
/// must be inclusive to stay a superset. Only numbers, strings and booleans
/// are pushed: SQLite orders each of them as the port does (numbers exactly,
/// text bytewise under the default `BINARY` collation, `false` before `true`),
/// and the `kind` test makes a bound of another JSON type match nothing, as in
/// the filter. A number the column cannot hold exactly (a `u64` above
/// `i64::MAX` is stored as a rounded float) is compared inclusively, because
/// rounding is monotone and so can only make a strict comparison tie.
fn range_bound(value: &Value) -> Option<(i64, SqlValue, bool)> {
    let lossy = match value {
        Value::Number(number) => {
            number.as_i64().is_none() && number.as_f64().is_some_and(|n| n.abs() >= LOSSY_FROM)
        }
        Value::String(_) | Value::Bool(_) => false,
        _ => return None,
    };
    Some((fields::rank(value), fields::sql_value(value), lossy))
}

/// Numbers from here up may be stored rounded (2^62).
const LOSSY_FROM: f64 = 4.611_686_018_427_388e18;

/// A superset condition for a range over a projected field or the id.
fn range(
    field: &str,
    bounds: [(&str, &Option<Value>); 4],
    projection: &Projection,
) -> Option<Clause> {
    let mut terms = Vec::new();
    let mut params = Vec::new();
    for (operator, bound) in bounds {
        let Some(bound) = bound else { continue };
        if field == ID_FIELD {
            if let Value::String(text) = bound {
                terms.push(format!("id {operator} ?"));
                params.push(SqlValue::Text(text.clone()));
            }
            continue;
        }
        let Some((kind, sql, lossy)) = range_bound(bound) else {
            continue;
        };
        let operator = match (lossy, operator) {
            (true, ">") => ">=",
            (true, "<") => "<=",
            _ => operator,
        };
        terms.push(format!("f.kind = {kind} AND f.value {operator} ?"));
        params.push(sql);
    }
    if terms.is_empty() {
        return None;
    }
    let sql = if field == ID_FIELD {
        terms.join(" AND ")
    } else {
        format!(
            "id IN (SELECT f.id {} AND {})",
            projection.rows(field)?,
            terms.join(" AND ")
        )
    };
    Some(Clause { sql, params })
}

/// A superset condition for `filter`, or `None` when nothing can be pushed.
pub(crate) fn clause(filter: &Filter, projection: &Projection) -> Option<Clause> {
    match filter {
        Filter::Eq { field, value } if meta_column(field).is_some() => Some(Clause {
            sql: format!("{} = ?", meta_column(field)?),
            params: vec![integer(value)?],
        }),
        Filter::In { field, values } if meta_column(field).is_some() => {
            let bound: Vec<SqlValue> = values.iter().map(integer).collect::<Option<_>>()?;
            if bound.is_empty() {
                return None;
            }
            Some(Clause {
                sql: format!(
                    "{} IN ({})",
                    meta_column(field)?,
                    vec!["?"; bound.len()].join(", ")
                ),
                params: bound,
            })
        }
        Filter::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } if meta_column(field).is_some() => {
            let column = meta_column(field)?;
            let mut terms = Vec::new();
            let mut params = Vec::new();
            for (operator, bound) in [(">", gt), (">=", gte), ("<", lt), ("<=", lte)] {
                if let Some(bound) = bound {
                    terms.push(format!("{column} {operator} ?"));
                    params.push(integer(bound)?);
                }
            }
            (!terms.is_empty()).then(|| Clause {
                sql: terms.join(" AND "),
                params,
            })
        }
        Filter::Range {
            field,
            gt,
            gte,
            lt,
            lte,
        } => range(
            field,
            [(">", gt), (">=", gte), ("<", lt), ("<=", lte)],
            projection,
        ),
        Filter::Eq { field, value } => {
            let bound = pushable(value)?;
            if field == ID_FIELD && !value.is_string() {
                return None;
            }
            Some(Clause {
                sql: on_field(field, "= ?", projection)?,
                params: vec![bound],
            })
        }
        Filter::In { field, values } => {
            let bound: Vec<SqlValue> = values.iter().map(pushable).collect::<Option<_>>()?;
            if bound.is_empty() || (field == ID_FIELD && !values.iter().all(Value::is_string)) {
                return None;
            }
            let marks = vec!["?"; bound.len()].join(", ");
            Some(Clause {
                sql: on_field(field, &format!("IN ({marks})"), projection)?,
                params: bound,
            })
        }
        Filter::And { filters } => {
            let parts: Vec<Clause> = filters
                .iter()
                .filter_map(|part| clause(part, projection))
                .collect();
            if parts.is_empty() {
                return None;
            }
            let sql = parts
                .iter()
                .map(|part| format!("({})", part.sql))
                .collect::<Vec<_>>()
                .join(" AND ");
            Some(Clause {
                sql,
                params: parts.into_iter().flat_map(|part| part.params).collect(),
            })
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod tests;
