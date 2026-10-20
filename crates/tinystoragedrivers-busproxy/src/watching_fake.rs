//! A memory store whose documents report a native watch fed by a channel, so a
//! test decides exactly which invalidations fire.
use std::sync::{Arc, Mutex};

use serde_json::Value;
use tinystoragedrivers_api::{DocumentStore, DocumentWatch, async_trait};
use tinystoragedrivers_types::{
    CollectionSpec, Filter, Page, Precondition, Query, Result, Sort, StorageError, Version,
    Versioned, WatchNotice, WatchOptions,
};
use tokio::sync::mpsc;

/// The senders of every watch a [`Docs`] opened, in order.
#[derive(Debug, Default)]
pub(crate) struct Source(pub Mutex<Vec<mpsc::UnboundedSender<Result<Option<WatchNotice>>>>>);

#[derive(Debug)]
pub(crate) struct Docs {
    pub inner: Arc<dyn DocumentStore>,
    pub source: Arc<Source>,
    pub fail_open: bool,
}

#[derive(Debug)]
struct Feed(mpsc::UnboundedReceiver<Result<Option<WatchNotice>>>);

#[async_trait]
impl DocumentWatch for Feed {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        self.0.recv().await.unwrap_or(Ok(None))
    }
}

#[async_trait]
impl DocumentStore for Docs {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner
            .capabilities()
            .with(tinystoragedrivers_types::Capability::Watch)
    }
    async fn watch(
        &self,
        _collection: &str,
        _filter: &Filter,
        _options: &WatchOptions,
    ) -> Result<Box<dyn DocumentWatch>> {
        if self.fail_open {
            return Err(StorageError::backend("private native watch failure"));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.source.0.lock().unwrap().push(tx);
        Ok(Box::new(Feed(rx)))
    }
    async fn ensure_collection(&self, spec: &CollectionSpec) -> Result<()> {
        self.inner.ensure_collection(spec).await
    }
    async fn validate_collection_spec(&self, spec: &CollectionSpec) -> Result<()> {
        self.inner.validate_collection_spec(spec).await
    }
    async fn get(&self, c: &str, id: &str) -> Result<Option<Versioned<Value>>> {
        self.inner.get(c, id).await
    }
    async fn put(&self, c: &str, id: &str, doc: Value, p: Precondition) -> Result<Version> {
        self.inner.put(c, id, doc, p).await
    }
    async fn delete(&self, c: &str, id: &str, p: Precondition) -> Result<bool> {
        self.inner.delete(c, id, p).await
    }
    async fn query(&self, c: &str, q: &Query) -> Result<Page<Versioned<Value>>> {
        self.inner.query(c, q).await
    }
    async fn count(&self, c: &str, f: &Filter) -> Result<u64> {
        self.inner.count(c, f).await
    }
    async fn delete_where(&self, c: &str, f: &Filter) -> Result<u64> {
        self.inner.delete_where(c, f).await
    }
    async fn claim(
        &self,
        c: &str,
        f: &Filter,
        s: &[Sort],
        p: &Value,
    ) -> Result<Option<Versioned<Value>>> {
        self.inner.claim(c, f, s, p).await
    }
    async fn drop_collection(&self, c: &str) -> Result<()> {
        self.inner.drop_collection(c).await
    }
}
