//! Model-free conditions checked atomically with storage mutations.

use crate::{Filter, Result, Scope, StorageError, validate_collection, validate_id};
use serde_json::Value;

/// Maximum number of independent guard documents in one conjunction.
pub const MAX_WRITE_CONDITION_CLAUSES: usize = 8;

/// A conjunction of host-supplied predicates over scope-qualified documents.
///
/// Primitive drivers evaluate every clause in the same atomic transaction as
/// each participating mutation. The type has no lease, epoch, renewal, or
/// ownership policy; those remain host concerns.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteCondition {
    clauses: Vec<WriteConditionClause>,
}

/// One validated guard document predicate within a [`WriteCondition`].
#[derive(Debug, Clone, PartialEq)]
pub struct WriteConditionClause {
    scope: Scope,
    collection: String,
    id: String,
    filter: Filter,
    valid_until_field: Option<String>,
}

impl WriteCondition {
    /// Create a condition bound to one document address and predicate.
    ///
    /// # Errors
    /// Returns `InvalidInput` when the guard address or filter is invalid.
    pub fn new(
        scope: Scope,
        collection: impl Into<String>,
        id: impl Into<String>,
        filter: Filter,
    ) -> Result<Self> {
        let collection = collection.into();
        let id = id.into();
        validate_collection(&collection)?;
        validate_id(&id)?;
        filter.validate()?;
        Ok(Self {
            clauses: vec![WriteConditionClause {
                scope,
                collection,
                id,
                filter,
                valid_until_field: None,
            }],
        })
    }

    /// Conjoin another independently scoped guard predicate.
    ///
    /// Every clause remains separate so each backend can validate and lock
    /// every guard within the same transaction as the target mutation.
    ///
    /// # Errors
    /// Returns `InvalidInput` if the resulting conjunction exceeds
    /// [`MAX_WRITE_CONDITION_CLAUSES`].
    pub fn and(mut self, other: Self) -> Result<Self> {
        if self.clauses.len() + other.clauses.len() > MAX_WRITE_CONDITION_CLAUSES {
            return Err(StorageError::invalid_input(format!(
                "a write condition may contain at most {MAX_WRITE_CONDITION_CLAUSES} clauses"
            )));
        }
        self.clauses.extend(other.clauses);
        Ok(self)
    }

    /// Require an integer Unix-millisecond deadline in this guard body's
    /// field to be later than the driver's clock when each mutation is
    /// checked. Set deadlines on individual conditions before combining them.
    ///
    /// Missing, non-integer, or expired fields fail closed. A condition with
    /// multiple clauses rejects this call so it cannot accidentally impose
    /// one clause's field path on every guard.
    ///
    /// # Errors
    /// Returns `InvalidInput` when `field` is invalid or the condition already
    /// contains multiple clauses.
    pub fn with_valid_until_field(mut self, field: impl Into<String>) -> Result<Self> {
        if self.clauses.len() != 1 {
            return Err(StorageError::invalid_input(
                "set each write-condition deadline before combining conditions",
            ));
        }
        let field = field.into();
        Filter::exists(&field, true).validate()?;
        self.clauses[0].valid_until_field = Some(field);
        Ok(self)
    }

    /// Validated guard clauses evaluated by the native mutation adapter.
    #[must_use]
    pub fn clauses(&self) -> &[WriteConditionClause] {
        &self.clauses
    }
}

impl WriteConditionClause {
    /// Scope containing the guard document.
    #[must_use]
    pub fn scope(&self) -> &Scope {
        &self.scope
    }

    /// Collection containing the guard document.
    #[must_use]
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Id of the guard document.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Predicate that must match the current guard document.
    #[must_use]
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    /// Body field containing this condition's per-mutation deadline, if set.
    #[must_use]
    pub fn valid_until_field(&self) -> Option<&str> {
        self.valid_until_field.as_deref()
    }

    /// Evaluate this predicate and optional deadline at a driver's mutation
    /// time. The caller must obtain `now_ms` inside the atomic write boundary.
    #[must_use]
    pub fn matches_at(&self, id: &str, doc: &Value, meta: &crate::Meta, now_ms: i64) -> bool {
        if !self.filter.matches_with(id, doc, meta) {
            return false;
        }
        self.valid_until_field.as_deref().is_none_or(|field| {
            crate::value::lookup(doc, field)
                .and_then(|value| {
                    value
                        .as_i64()
                        .map(i128::from)
                        .or_else(|| value.as_u64().map(i128::from))
                })
                .is_some_and(|deadline| deadline > i128::from(now_ms))
        })
    }
}

#[cfg(test)]
#[path = "condition_tests.rs"]
mod tests;
