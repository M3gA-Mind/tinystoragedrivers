//! Model-free conditions checked atomically with storage mutations.

use crate::{Filter, Result, Scope, validate_collection, validate_id};

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
        })
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
}

#[cfg(test)]
#[path = "condition_tests.rs"]
mod tests;
