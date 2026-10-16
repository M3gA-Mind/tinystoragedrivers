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
//! ([`crate::fields`]), because a large body is compressed and SQL cannot read
//! it. Only the projected paths of the collection (its index and search
//! fields) are pushed down; a term on any other path contributes nothing and
//! the Rust check decides.

use std::collections::BTreeSet;

use rusqlite::types::Value as SqlValue;
use serde_json::Value;
use tinystoragedrivers_core::{
    CREATED_FIELD, CollectionSpec, CursorPosition, Direction, EXPIRES_FIELD, Filter, ID_FIELD,
    SCHEMA_FIELD, Sort, UPDATED_FIELD,
};

use super::ops::Ctx;
use crate::fields;
use crate::sql::{ident, literal};

/// Which body fields of one collection in one scope SQL can see, and where.
#[derive(Debug, Clone)]
pub(crate) struct Projection {
    paths: BTreeSet<String>,
    fields_table: String,
    docs_table: String,
    scope: String,
    coll: String,
}

impl Projection {
    pub(crate) fn new(ctx: Ctx<'_>, spec: &CollectionSpec, collection: &str) -> Self {
        Self {
            paths: fields::projected(spec),
            fields_table: ident(&ctx.tables.fields),
            docs_table: ident(&ctx.tables.docs),
            scope: literal(ctx.scope),
            coll: literal(collection),
        }
    }

    /// The `doc_fields` rows of `field` in this collection and scope, as a
    /// `FROM … WHERE` over alias `f` that still needs a closing condition.
    fn rows(&self, field: &str) -> Option<String> {
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

    /// The `kind` or `value` of `field` on the current row of `docs`.
    fn of_row(&self, field: &str, column: &str) -> Option<String> {
        Some(format!(
            "(SELECT f.{column} {} AND f.id = {}.id)",
            self.rows(field)?,
            self.docs_table
        ))
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
fn meta_column(field: &str) -> Option<&'static str> {
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

/// A SQL superset predicate that seeks strictly after a keyset position.
///
/// Numeric comparisons may include a value that rounds to the same SQLite
/// number as the cursor; the caller rechecks the exact JSON order in Rust.
/// Arrays and objects are left to the Rust fallback because SQLite's JSON
/// comparison does not implement the port's structural ordering.
pub(crate) fn after(
    position: &CursorPosition,
    sorts: &[Sort],
    projection: &Projection,
) -> Option<Clause> {
    let mut alternatives = Vec::with_capacity(sorts.len() + 1);
    for current in 0..sorts.len() {
        let mut terms = Vec::with_capacity(current + 1);
        let mut params = Vec::new();
        for index in 0..current {
            let key = sorts.get(index)?;
            let value = position.values.get(index)?;
            if matches!(value, Value::Array(_) | Value::Object(_)) {
                return None;
            }
            let (rank_expression, expression) = expressions(&key.field, projection)?;
            terms.push(format!("({rank_expression} = ? AND {expression} IS ?)"));
            params.push(SqlValue::Integer(rank(value)));
            params.push(sql_value(value)?);
        }
        let key = sorts.get(current)?;
        let value = position.values.get(current)?;
        if matches!(value, Value::Array(_) | Value::Object(_)) {
            return None;
        }
        let (rank_expression, value_expression) = expressions(&key.field, projection)?;
        let cursor_rank = rank(value);
        let direction = match key.direction {
            Direction::Asc => ">",
            Direction::Desc => "<",
        };
        let advance = if cursor_rank == 0 {
            format!("{rank_expression} {direction} ?")
        } else {
            let inclusive = match key.direction {
                Direction::Asc => ">=",
                Direction::Desc => "<=",
            };
            format!(
                "({rank_expression} {direction} ? OR ({rank_expression} = ? AND {value_expression} {inclusive} ?))"
            )
        };
        terms.push(advance);
        params.push(SqlValue::Integer(cursor_rank));
        if cursor_rank != 0 {
            params.push(SqlValue::Integer(cursor_rank));
            params.push(sql_value(value)?);
        }
        alternatives.push((format!("({})", terms.join(" AND ")), params));
    }

    let mut terms = Vec::with_capacity(sorts.len() + 1);
    let mut params = Vec::new();
    for (index, key) in sorts.iter().enumerate() {
        let value = position.values.get(index)?;
        if matches!(value, Value::Array(_) | Value::Object(_)) {
            return None;
        }
        let (rank_expression, value_expression) = expressions(&key.field, projection)?;
        terms.push(format!(
            "({rank_expression} = ? AND {value_expression} IS ?)",
        ));
        params.push(SqlValue::Integer(rank(value)));
        params.push(sql_value(value)?);
    }
    terms.push("id > ?".to_owned());
    params.push(SqlValue::Text(position.id.clone()));
    alternatives.push((format!("({})", terms.join(" AND ")), params));

    Some(Clause {
        sql: alternatives
            .iter()
            .map(|(sql, _)| sql.as_str())
            .collect::<Vec<_>>()
            .join(" OR "),
        params: alternatives
            .into_iter()
            .flat_map(|(_, params)| params)
            .collect(),
    })
}

fn expressions(field: &str, projection: &Projection) -> Option<(String, String)> {
    if field == ID_FIELD {
        return Some(("3".to_owned(), "id".to_owned()));
    }
    if let Some(column) = meta_column(field) {
        // Metadata is a number when present; only `expires_ms` can be absent.
        let rank = if field == EXPIRES_FIELD {
            "CASE WHEN expires_ms IS NULL THEN 0 ELSE 2 END".to_owned()
        } else {
            "2".to_owned()
        };
        return Some((rank, column.to_owned()));
    }
    // A path with no row has no value and ranks with null.
    let rank = format!("COALESCE({}, 0)", projection.of_row(field, "kind")?);
    Some((rank, projection.of_row(field, "value")?))
}

fn sql_value(value: &Value) -> Option<SqlValue> {
    (!matches!(value, Value::Array(_) | Value::Object(_))).then(|| fields::sql_value(value))
}

fn rank(value: &Value) -> i64 {
    fields::rank(value)
}

#[cfg(test)]
#[path = "pushdown_tests.rs"]
mod tests;
