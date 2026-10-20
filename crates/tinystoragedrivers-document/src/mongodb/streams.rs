//! MongoDB-backed append-only streams in the existing segment collections.

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{FindOptions, IndexOptions};
use mongodb::{Collection, Database, IndexModel};
use serde_json::Value;
use tinystoragedrivers_api::{StreamStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, Capability, Result, Scope, SealOutcome, StorageError, StreamEntry, StreamInfo,
};

use tinystoragedrivers_connect::MongoConnection;
use super::documents::{bson_to_json, json_to_bson};
use crate::validate_stream;

const HEADS: &str = "_tsd_stream_heads";
const SEGMENTS: &str = "_tsd_streams";

#[derive(Debug, Clone)]
pub struct MongoStreams {
    database: Database,
    scope: Scope,
    clock: crate::Clock,
}

impl MongoStreams {
    /// Bind a stream adapter to a shared Mongo connection and opaque scope.
    pub fn new(connection: MongoConnection, scope: Scope) -> Self {
        Self { database: connection.database(), scope, clock: connection.clock() }
    }

    fn segments(&self) -> Collection<Document> { self.database.collection(SEGMENTS) }
    fn heads(&self) -> Collection<Document> { self.database.collection(HEADS) }
    fn head_filter(&self, stream: &str) -> Document { doc! {"_scope": self.scope.as_str(), "s": stream} }

    async fn generation(&self, stream: &str) -> Result<i64> {
        Ok(self.heads().find_one(self.head_filter(stream)).await.map_err(|error| mongo_error("read a stream head", error))?
            .and_then(|head| head.get_i64("g").ok()).unwrap_or(0))
    }

    async fn segment_rows(&self, stream: &str) -> Result<Vec<Document>> {
        let generation = self.generation(stream).await?;
        let options = FindOptions::builder().sort(doc! {"o": 1}).build();
        let mut cursor = self.segments().find(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation}).with_options(options).await
            .map_err(|error| mongo_error("read stream segments", error))?;
        let mut rows = Vec::new();
        while let Some(row) = cursor.try_next().await.map_err(|error| mongo_error("read stream segments", error))? { rows.push(row); }
        Ok(rows)
    }

    async fn extent(&self, stream: &str) -> Result<(u64, u64, Option<i64>)> {
        let rows = self.segment_rows(stream).await?;
        let mut base = 0;
        let mut len = 0;
        let mut last_append = None;
        for row in rows {
            let start = row.get_i64("o").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(0);
            let trimmed = row.get_i64("t").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(0);
            let values = row.get_array("vs").map_or(0, Vec::len) as u64;
            let end = row.get_i64("e").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(start);
            if end > len { len = end; }
            if values > 0 { base = start.saturating_add(trimmed); }
            last_append = row.get_i64("ms").ok().or(last_append);
        }
        Ok((base.min(len), len, last_append))
    }
}

#[async_trait]
impl StreamStore for MongoStreams {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none().with(Capability::Streams).with(Capability::ConditionalStreamAppend)
    }

    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.append_at(stream, None, values).await
    }

    async fn append_batch_if_offset(&self, stream: &str, expected_next_offset: u64, values: Vec<Value>) -> Result<u64> {
        self.append_at(stream, Some(expected_next_offset), values).await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let mut output = Vec::new();
        for row in self.segment_rows(stream).await? {
            let start = row.get_i64("o").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(0);
            let trimmed = row.get_i64("t").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(0);
            let Some(values) = row.get_array("vs").ok() else { continue };
            for (index, value) in values.iter().enumerate() {
                let offset = start.saturating_add(trimmed).saturating_add(index as u64);
                if offset >= from && output.len() < limit {
                    output.push(StreamEntry { offset, value: bson_to_json(value)? });
                }
            }
            if output.len() >= limit { break; }
        }
        Ok(output)
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        Ok(self.extent(stream).await?.1)
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        let generation = self.generation(stream).await?;
        for row in self.segment_rows(stream).await? {
            let start = row.get_i64("o").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(0);
            let end = row.get_i64("e").ok().and_then(|v| u64::try_from(v).ok()).unwrap_or(start);
            let id = row.get("_id").cloned().unwrap_or(Bson::Null);
            if end <= offset {
                self.segments().delete_one(doc! {"_id": id}).await.map_err(|error| mongo_error("truncate a stream", error))?;
            } else if start < offset {
                let trimmed = i64::try_from(offset - start).unwrap_or(i64::MAX);
                self.segments().update_one(doc! {"_id": id}, doc! {"$set": {"t": trimmed}}).await.map_err(|error| mongo_error("truncate a stream", error))?;
            }
        }
        Ok(self.extent(stream).await?.1)
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let filter = self.head_filter(stream);
        let previous = self.heads().find_one(filter.clone()).await.map_err(|error| mongo_error("read a stream head", error))?;
        let generation = previous.as_ref().and_then(|head| head.get_i64("g").ok()).unwrap_or(0);
        let exists = !self.segment_rows(stream).await?.is_empty();
        self.heads().replace_one(filter.clone(), doc! {"_id": {"_scope": self.scope.as_str(), "s": stream}, "_scope": self.scope.as_str(), "s": stream, "g": generation.saturating_add(1)}).upsert(true).await
            .map_err(|error| mongo_error("advance a stream generation", error))?;
        self.segments().delete_many(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation}).await
            .map_err(|error| mongo_error("delete stream segments", error))?;
        Ok(exists)
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let mut cursor = self.heads().find(doc! {"_scope": self.scope.as_str()}).await.map_err(|error| mongo_error("list stream heads", error))?;
        let mut names = Vec::new();
        while let Some(row) = cursor.try_next().await.map_err(|error| mongo_error("list stream heads", error))? {
            if let Ok(name) = row.get_str("s") && name.starts_with(prefix) { names.push(name.to_owned()); }
        }
        names.sort(); names.dedup(); Ok(names)
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        validate_stream(stream)?;
        let (base, len, last) = self.extent(stream).await?;
        if len == 0 && self.segment_rows(stream).await?.is_empty() { return Ok(None); }
        Ok(Some(StreamInfo::new(base, len, None, last)))
    }

    async fn seal(&self, _: &str) -> Result<SealOutcome> {
        Err(StorageError::unsupported(Capability::StreamSeal, "Mongo stream sealing requires the blob adapter"))
    }
}

impl MongoStreams {
    async fn append_at(&self, stream: &str, expected: Option<u64>, values: Vec<Value>) -> Result<u64> {
        validate_stream(stream)?;
        if values.is_empty() { return self.len(stream).await; }
        let bson_values = values.iter().map(json_to_bson).collect::<Result<Vec<_>>>()?;
        let (base, start, _) = self.extent(stream).await?;
        let _ = base;
        if let Some(expected) = expected && start != expected { return Err(StorageError::conflict("stream offset changed")); }
        let count = i64::try_from(bson_values.len()).map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
        let start_i = i64::try_from(start).map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
        let end_i = start_i.checked_add(count).ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
        let now = i64::try_from((self.clock)()).unwrap_or(i64::MAX);
        let generation = self.generation(stream).await?;
        let segment = doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation, "o": start_i, "n": count, "e": end_i, "t": 0_i64, "vs": bson_values, "ms": now};
        let indexes = self.segments().create_index(IndexModel::builder().keys(doc! {"_scope": 1, "s": 1, "g": 1, "o": 1}).options(IndexOptions::builder().unique(true).name("_tsd_stream_segment").build()).build()).await;
        indexes.map_err(|error| mongo_error("prepare stream segments", error))?;
        self.segments().insert_one(segment).await.map_err(|error| mongo_error("append stream entries", error))?;
        Ok(u64::try_from(end_i).unwrap_or(u64::MAX))
    }
}

fn mongo_error(operation: &'static str, error: mongodb::error::Error) -> StorageError {
    StorageError::unavailable(format!("MongoDB failed to {operation}")).with_source(error)
}
