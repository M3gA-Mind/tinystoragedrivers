use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use serde_json::Value;
use tinystoragedrivers_api::{DocumentStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, CollectionSpec, ErrorKind, Filter, Meta, Page, Precondition, Query, Result, Sort,
    StorageError, Version, Versioned,
};
use tokio::sync::Notify;

const GROUPS: &str = "tsd.queue_groups";
const MESSAGES: &str = "tsd.queue";

#[derive(Debug, Default)]
pub(crate) struct MemoryDocuments {
    docs: Mutex<BTreeMap<(String, String), Versioned<Value>>>,
    pause_next_group_get: Mutex<Option<Arc<GroupGetGate>>>,
    pause_next_message_put: Mutex<Option<Arc<GroupGetGate>>>,
}

#[derive(Debug, Default)]
pub(crate) struct GroupGetGate {
    entered: Notify,
    release: Notify,
}

#[allow(
    dead_code,
    reason = "the integration races consume gates not needed by unit tests"
)]
impl GroupGetGate {
    pub(crate) async fn wait_until_paused(&self) {
        self.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[allow(
    dead_code,
    reason = "the integration races consume controls not needed by unit tests"
)]
impl MemoryDocuments {
    pub(crate) fn pause_next_group_get(&self) -> Arc<GroupGetGate> {
        let gate = Arc::new(GroupGetGate::default());
        *self
            .pause_next_group_get
            .lock()
            .expect("test gate mutex is not poisoned") = Some(Arc::clone(&gate));
        gate
    }

    pub(crate) fn pause_next_message_put(&self) -> Arc<GroupGetGate> {
        let gate = Arc::new(GroupGetGate::default());
        *self
            .pause_next_message_put
            .lock()
            .expect("test gate mutex is not poisoned") = Some(Arc::clone(&gate));
        gate
    }

    pub(crate) fn set_group_until(&self, until_ms: i64) {
        let mut docs = self
            .docs
            .lock()
            .expect("test document mutex is not poisoned");
        let (_, found) = docs
            .iter_mut()
            .find(|((collection, _), _)| collection == GROUPS)
            .expect("a group lock exists");
        found.doc["until_ms"] = serde_json::json!(until_ms);
        found.version = found
            .version
            .next()
            .expect("test version does not overflow");
    }

    pub(crate) fn group_until(&self) -> i64 {
        let docs = self
            .docs
            .lock()
            .expect("test document mutex is not poisoned");
        docs.iter()
            .find(|((collection, _), _)| collection == GROUPS)
            .and_then(|(_, found)| found.doc["until_ms"].as_i64())
            .expect("a group lock with an expiry exists")
    }
}

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
        let gate = if collection == GROUPS {
            self.pause_next_group_get
                .lock()
                .map_err(|_| unavailable())?
                .take()
        } else {
            None
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(self
            .docs
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
        let gate = if collection == MESSAGES {
            self.pause_next_message_put
                .lock()
                .map_err(|_| unavailable())?
                .take()
        } else {
            None
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        let mut docs = self.docs.lock().map_err(|_| unavailable())?;
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
        let mut docs = self.docs.lock().map_err(|_| unavailable())?;
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
        let docs = self.docs.lock().map_err(|_| unavailable())?;
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
        let docs = self.docs.lock().map_err(|_| unavailable())?;
        Ok(docs
            .iter()
            .filter(|((name, _), v)| name == collection && filter.matches(&v.id, &v.doc))
            .count() as u64)
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> Result<u64> {
        let mut docs = self.docs.lock().map_err(|_| unavailable())?;
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
        let mut docs = self.docs.lock().map_err(|_| unavailable())?;
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
