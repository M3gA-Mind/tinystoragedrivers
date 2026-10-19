//! Versioned JSON document port.

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tinystoragedrivers_types::{
    Capabilities, Capability, CollectionSpec, Filter, Page, Precondition, Query, ReadCheck, Result,
    SearchHit, Sort, StorageError, Version, Versioned, WatchOptions, WriteOp, WriteOptions,
    WriteResult,
};

use crate::DocumentWatch;

/// Versioned JSON documents in named collections, bound to one scope.
#[async_trait]
pub trait DocumentStore: Send + Sync + std::fmt::Debug {
    /// Optional abilities provided by this driver.
    fn capabilities(&self) -> Capabilities;

    /// Subscribe to invalidation hints for matching documents.
    ///
    /// # Errors
    /// Returns `Unsupported(Watch)` by default, or a cursor, query, or backend
    /// error.
    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        let _ = (collection, filter, options);
        Err(unsupported(
            Capability::Watch,
            "native document watch is unsupported",
        ))
    }

    /// Ensure that a collection exists with the requested constraints.
    ///
    /// # Errors
    /// Returns an error for an invalid declaration or a backend failure.
    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()>;

    /// Validate a persisted collection declaration without changing it.
    ///
    /// # Errors
    /// Returns `Unsupported(CollectionValidation)` by default, or a declaration
    /// or backend error.
    async fn validate_collection_spec(&self, required: &CollectionSpec) -> Result<()> {
        let _ = required;
        Err(unsupported(
            Capability::CollectionValidation,
            "collection validation is unsupported",
        ))
    }

    /// Read one document, returning absent for an expired record.
    ///
    /// # Errors
    /// Returns an error for invalid names or a backend failure.
    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>>;

    /// Write a JSON object and return its new version.
    ///
    /// # Errors
    /// Returns an error for invalid input, a failed precondition, or a backend
    /// failure.
    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
    ) -> Result<Version>;

    /// Write a document with optional metadata, if supported.
    ///
    /// # Errors
    /// Returns `Unsupported(RecordMetadata)` for non-default metadata when not
    /// implemented, or an error from [`Self::put`].
    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> Result<Version> {
        if !options.is_default() {
            return Err(unsupported(
                Capability::RecordMetadata,
                "document metadata is unsupported",
            ));
        }
        self.put(collection, id, value, precondition).await
    }

    /// Delete one document using an optimistic precondition.
    ///
    /// # Errors
    /// Returns an error for invalid input, a failed precondition, or a backend
    /// failure.
    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool>;

    /// Read one page matching a query.
    ///
    /// # Errors
    /// Returns an error for an invalid query or a backend failure.
    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>>;

    /// Count records matching a filter.
    ///
    /// # Errors
    /// Returns an error for an invalid filter or a backend failure.
    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64>;

    /// Delete records matching a filter.
    ///
    /// # Errors
    /// Returns an error for an invalid filter or a backend failure.
    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64>;

    /// Atomically claim and patch the first matching document.
    ///
    /// # Errors
    /// Returns an error for an invalid filter or patch, or a backend failure.
    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>>;

    /// Apply a batch of writes atomically when supported.
    ///
    /// # Errors
    /// Returns `Unsupported(Transactions)` by default, or the first failed write
    /// error.
    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> Result<Vec<WriteResult>> {
        let _ = ops;
        Err(unsupported(
            Capability::Transactions,
            "atomic document batches are unsupported",
        ))
    }

    /// Validate dependencies and apply writes atomically when supported.
    ///
    /// # Errors
    /// Returns `Unsupported(ValidatedBatch)` by default, or a dependency,
    /// validation, or backend error.
    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        let _ = (checks, ops);
        Err(unsupported(
            Capability::ValidatedBatch,
            "validated document batches are unsupported",
        ))
    }

    /// Search configured full-text fields.
    ///
    /// # Errors
    /// Returns `Unsupported(FullText)` by default, or an invalid input or
    /// backend error.
    async fn search(&self, collection: &str, text: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let _ = (collection, text, limit);
        Err(unsupported(
            Capability::FullText,
            "full-text search is unsupported",
        ))
    }

    /// Remove a collection and its records.
    ///
    /// # Errors
    /// Returns an error for an invalid name or a backend failure.
    async fn drop_collection(&self, collection: &str) -> Result<()>;

    /// List names of collections with records under this scope.
    ///
    /// # Errors
    /// Returns `Unsupported(CollectionEnumeration)` by default or a backend
    /// failure.
    async fn collections(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        Err(unsupported(
            Capability::CollectionEnumeration,
            "collection enumeration is unsupported",
        ))
    }
}

/// Serde conveniences over [`DocumentStore`].
#[async_trait]
pub trait DocumentStoreExt: DocumentStore {
    /// Read a document and deserialize its body.
    ///
    /// # Errors
    /// Returns an error from [`DocumentStore::get`] or when deserialization
    /// fails.
    async fn get_as<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<Versioned<T>>> {
        self.get(collection, id).await?.map(decode).transpose()
    }

    /// Serialize and write a document.
    ///
    /// # Errors
    /// Returns an error when serialization or [`DocumentStore::put`] fails.
    async fn put_as<T: Serialize + Sync>(
        &self,
        collection: &str,
        id: &str,
        value: &T,
        precondition: Precondition,
    ) -> Result<Version> {
        let value = serde_json::to_value(value).map_err(|error| {
            StorageError::serialization(format!("document serialization failed: {error}"))
        })?;
        self.put(collection, id, value, precondition).await
    }

    /// Read one query page and deserialize each body.
    ///
    /// # Errors
    /// Returns an error from [`DocumentStore::query`] or when deserialization
    /// fails.
    async fn query_as<T: DeserializeOwned + Send>(
        &self,
        collection: &str,
        query: &Query,
    ) -> Result<Page<Versioned<T>>> {
        let page = self.query(collection, query).await?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            items.push(decode(item)?);
        }
        Ok(Page {
            items,
            next: page.next,
        })
    }

    /// Read every page matching a query.
    ///
    /// # Errors
    /// Returns an error from [`DocumentStore::query`].
    async fn query_all(&self, collection: &str, query: &Query) -> Result<Vec<Versioned<Value>>> {
        let mut query = query.clone();
        let mut all = Vec::new();
        let mut seen_cursors = std::collections::HashSet::new();
        loop {
            let page = self.query(collection, &query).await?;
            all.extend(page.items);
            let Some(next) = page.next else { break };
            if !seen_cursors.insert(next.0.clone()) {
                return Err(StorageError::backend("query paging did not advance"));
            }
            query.cursor = Some(next);
        }
        Ok(all)
    }
}

impl<T: DocumentStore + ?Sized> DocumentStoreExt for T {}

fn decode<T: DeserializeOwned>(value: Versioned<Value>) -> Result<Versioned<T>> {
    Ok(Versioned {
        id: value.id,
        version: value.version,
        doc: serde_json::from_value(value.doc).map_err(|error| {
            StorageError::serialization(format!("document deserialization failed: {error}"))
        })?,
        meta: value.meta,
    })
}

fn unsupported(capability: Capability, message: &'static str) -> StorageError {
    StorageError::unsupported(capability, message)
}

#[cfg(test)]
#[path = "document_tests.rs"]
mod tests;
