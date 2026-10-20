use std::{collections::BTreeMap, sync::Mutex};

use serde_json::Value;
use tinystoragedrivers_api::{DocumentStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, CollectionSpec, ErrorKind, Filter, Meta, Page, Precondition, Query, Result, Sort,
    StorageError, Version, Versioned,
};

#[derive(Debug, Default)]
pub(crate) struct MemoryDocuments(Mutex<BTreeMap<(String, String), Versioned<Value>>>);

fn unavailable() -> StorageError {
    StorageError::new(ErrorKind::Backend, "test document lock poisoned")
}

#[async_trait]
impl DocumentStore for MemoryDocuments {
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }

    async fn ensure_collection(&self, _: &CollectionSpec) -> Result<()> {
        Ok(())
    }

    async fn get(&self, collection: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| unavailable())?
            .get(&(collection.into(), id.into()))
            .cloned())
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: Precondition,
    ) -> Result<Version> {
        let mut docs = self.0.lock().map_err(|_| unavailable())?;
        let key = (collection.into(), id.into());
        let current = docs.get(&key);
        let allowed = match (precondition, current) {
            (Precondition::None, _) | (Precondition::Absent, None) => true,
            (Precondition::Version(expected), Some(found)) => expected == found.version,
            _ => false,
        };
        if !allowed {
            return Err(StorageError::conflict("test precondition failed"));
        }
        let version = current.map_or(Version::FIRST, |found| {
            found.version.next().unwrap_or(found.version)
        });
        docs.insert(
            key,
            Versioned {
                id: id.into(),
                version,
                doc: value,
                meta: Meta::unknown(),
            },
        );
        Ok(version)
    }

    async fn delete(&self, collection: &str, id: &str, precondition: Precondition) -> Result<bool> {
        let mut docs = self.0.lock().map_err(|_| unavailable())?;
        let key = (collection.into(), id.into());
        let Some(found) = docs.get(&key) else {
            return Ok(false);
        };
        if matches!(precondition, Precondition::Version(v) if v != found.version)
            || precondition == Precondition::Absent
        {
            return Err(StorageError::conflict("test precondition failed"));
        }
        docs.remove(&key);
        Ok(true)
    }

    async fn query(&self, collection: &str, query: &Query) -> Result<Page<Versioned<Value>>> {
        let docs = self.0.lock().map_err(|_| unavailable())?;
        let mut items: Vec<_> = docs
            .iter()
            .filter(|((name, _), value)| {
                name == collection && query.filter.matches(&value.id, &value.doc)
            })
            .map(|(_, value)| value.clone())
            .collect();
        tinystoragedrivers_types::sort_documents(&mut items, &query.sort, |v| (&v.id, &v.doc));
        if let Some(limit) = query.limit {
            items.truncate(limit);
        }
        Ok(Page { items, next: None })
    }

    async fn count(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let docs = self.0.lock().map_err(|_| unavailable())?;
        Ok(docs
            .iter()
            .filter(|((name, _), v)| name == collection && filter.matches(&v.id, &v.doc))
            .count() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let mut docs = self.0.lock().map_err(|_| unavailable())?;
        let before = docs.len();
        docs.retain(|(name, _), v| name != collection || !filter.matches(&v.id, &v.doc));
        Ok((before - docs.len()) as u64)
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        let mut docs = self.0.lock().map_err(|_| unavailable())?;
        let mut candidates: Vec<_> = docs
            .iter()
            .filter(|((name, _), value)| {
                name == collection && filter.matches(&value.id, &value.doc)
            })
            .map(|(_, value)| value.clone())
            .collect();
        tinystoragedrivers_types::sort_documents(&mut candidates, sort, |v| (&v.id, &v.doc));
        let Some(mut claimed) = candidates.into_iter().next() else {
            return Ok(None);
        };
        tinystoragedrivers_types::value::merge_patch(&mut claimed.doc, patch);
        claimed.version = claimed
            .version
            .next()
            .ok_or_else(|| StorageError::backend("test version overflow"))?;
        docs.insert((collection.into(), claimed.id.clone()), claimed.clone());
        Ok(Some(claimed))
    }
    async fn drop_collection(&self, collection: &str) -> Result<()> {
        self.delete_where(collection, &Filter::default())
            .await
            .map(|_| ())
    }
}
