//! Model-free conditions checked atomically with storage mutations.

use crate::{Filter, Result, Scope, validate_collection, validate_id};
use serde_json::Value;

/// A host-supplied condition over one scope-qualified document.
///
/// Primitive drivers evaluate `filter` against the current guard document in
/// the same atomic transaction as each participating mutation. The type has
/// no lease, epoch, renewal, or ownership policy; those remain host concerns.
#[derive(Debug, Clone, PartialEq)]
pub struct WriteCondition {
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
            scope,
            collection,
            id,
            filter,
            valid_until_field: None,
        })
    }

    /// Require an integer Unix-millisecond deadline in the guard body to be
    /// later than the driver's clock when each mutation is checked.
    ///
    /// This keeps the guard document readable after its deadline, so a host
    /// can retain persistent fencing history while a stale handle is refused.
    /// Missing, non-integer, or expired fields fail closed.
    ///
    /// # Errors
    /// Returns `InvalidInput` when `field` is not a valid document field path.
    pub fn with_valid_until_field(mut self, field: impl Into<String>) -> Result<Self> {
        let field = field.into();
        Filter::exists(&field, true).validate()?;
        self.valid_until_field = Some(field);
        Ok(self)
    }

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

    /// Evaluate the predicate and optional deadline at a driver's mutation
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
