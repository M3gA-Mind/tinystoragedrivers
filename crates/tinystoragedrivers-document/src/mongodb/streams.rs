//! MongoDB-backed append-only streams in the existing segment collections.

use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{FindOptions, IndexOptions};
use mongodb::{Client, Collection, Database, IndexModel};
use serde_json::Value;
use std::sync::Arc;
use tinystoragedrivers_api::{BlobStore, StreamStore, async_trait};
use tinystoragedrivers_types::{
    Capabilities, Capability, Result, Scope, SealOutcome, StorageError, StreamEntry, StreamInfo,
};

use super::documents::{bson_to_json, json_to_bson, mongo_error};
use crate::validate_stream;
use tinystoragedrivers_connect::MongoConnection;

const HEADS: &str = "_tsd_stream_heads";
const SEGMENTS: &str = "_tsd_streams";

#[derive(Clone)]
/// MongoDB append-only stream adapter bound to one shared connection and scope.
pub struct MongoStreams {
    client: Client,
    database: Database,
    scope: Scope,
    clock: crate::Clock,
    transactions: bool,
    blobs: Option<Arc<dyn BlobStore>>,
    #[cfg(test)]
    truncate_pause: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}

impl std::fmt::Debug for MongoStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoStreams")
            .field("database", &self.database.name())
            .field("scope", &self.scope)
            .field("transactions", &self.transactions)
            .field("has_blob_store", &self.blobs.is_some())
            .finish_non_exhaustive()
    }
}

impl MongoStreams {
    /// Bind a stream adapter to a shared Mongo connection and opaque scope.
    #[must_use]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "the constructor consumes a shared connection handle by contract"
    )]
    pub fn new(connection: MongoConnection, scope: Scope) -> Self {
        Self {
            client: connection.client(),
            database: connection.database(),
            scope,
            clock: connection.clock(),
            transactions: connection.transactions_available(),
            blobs: None,
            #[cfg(test)]
            truncate_pause: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn without_transactions_for_test(mut self) -> Self {
        self.transactions = false;
        self
    }

    #[cfg(test)]
    pub(crate) fn pause_truncate_for_test(
        mut self,
        started: Arc<tokio::sync::Notify>,
        resume: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.truncate_pause = Some((started, resume));
        self
    }

    #[cfg(test)]
    async fn pause_at_truncate_boundary(&self) {
        if let Some((started, resume)) = &self.truncate_pause {
            started.notify_one();
            resume.notified().await;
        }
    }

    /// Attach the scope-bound blob adapter used to store sealed stream logs.
    #[must_use]
    pub fn with_blob_store(mut self, blobs: Arc<dyn BlobStore>) -> Self {
        self.blobs = Some(blobs);
        self
    }

    fn segments(&self) -> Collection<Document> {
        self.database.collection(SEGMENTS)
    }
    fn heads(&self) -> Collection<Document> {
        self.database.collection(HEADS)
    }
    fn head_filter(&self, stream: &str) -> Document {
        doc! {"_scope": self.scope.as_str(), "s": stream}
    }

    fn generation_blob_key(stream: &str, generation: i64) -> Result<String> {
        Ok(format!("{}/g{generation}", crate::sealed_blob_key(stream)?))
    }

    fn blobs(&self) -> Result<&Arc<dyn BlobStore>> {
        self.blobs
            .as_ref()
            .filter(|blobs| blobs.supports_sealed_blobs())
            .ok_or_else(|| {
                StorageError::unsupported(
                    Capability::StreamSeal,
                    "Mongo stream sealing requires a shared blob adapter",
                )
            })
    }

    async fn sealed_log(&self, key: &str) -> Result<crate::stream::SealedLog> {
        let blob = self
            .blobs()?
            .get_sealed(key)
            .await?
            .ok_or_else(|| StorageError::backend("sealed stream has no blob"))?;
        crate::stream::SealedLog::decode(&blob.bytes)
    }

    fn marker_blob_key(marker: &Document, stream: &str, generation: i64) -> Result<String> {
        match marker.get_str("bk") {
            Ok(key) => Ok(key.to_owned()),
            Err(_) => Self::generation_blob_key(stream, generation),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keeps the Mongo transaction and its rollback path in one reviewable unit"
    )]
    async fn truncate_transactional(&self, stream: &str, offset: u64) -> Result<u64> {
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(mongo_error("start a stream truncation"))?;
        session
            .start_transaction()
            .await
            .map_err(mongo_error("start a stream truncation"))?;
        let result = async {
            let head = self
                .heads()
                .find_one(self.head_filter(stream))
                .session(&mut session)
                .await
                .map_err(mongo_error("read a stream head"))?;
            let generation = head
                .as_ref()
                .and_then(|value| value.get_i64("g").ok())
                .unwrap_or(0);
            let mut cursor = self
                .segments()
                .find(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation})
                .with_options(FindOptions::builder().sort(doc! {"o": 1}).build())
                .session(&mut session)
                .await
                .map_err(mongo_error("read stream segments"))?;
            let mut rows = Vec::new();
            while let Some(row) = cursor.next(&mut session).await {
                rows.push(row.map_err(mongo_error("read stream segments"))?);
            }
            let Some(last) = rows.last() else {
                return Ok(0);
            };
            if last.get_bool("seal").unwrap_or(false) {
                return Err(StorageError::conflict("stream is sealed"));
            }
            let start_of = |row: &Document| {
                row.get_i64("o")
                    .ok()
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or(0)
            };
            let end_of = |row: &Document| {
                row.get_i64("e")
                    .ok()
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or_else(|| start_of(row))
            };
            let trimmed_of = |row: &Document| {
                row.get_i64("t")
                    .ok()
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or(0)
            };
            let base = rows
                .iter()
                .map(|row| start_of(row).saturating_add(trimmed_of(row)))
                .next()
                .unwrap_or_else(|| end_of(last));
            let len = end_of(last);
            let cut = offset.clamp(base, len);
            let removed = cut - base;
            if removed == 0 {
                return Ok(0);
            }
            self.heads()
                .update_one(
                    doc! {"_id": {"_scope": self.scope.as_str(), "s": stream}, "_scope": self.scope.as_str(), "s": stream, "g": generation},
                    doc! {"$inc": {"truncate_epoch": 1_i64}},
                )
                .session(&mut session)
                .await
                .map_err(|error| {
                    transaction_conflict_error("record a stream truncation", error)
                })?;
            let last_id = last.get("_id").cloned().unwrap_or(Bson::Null);
            for row in rows {
                let start = start_of(&row);
                let end = end_of(&row);
                let trimmed = trimmed_of(&row);
                let id = row.get("_id").cloned().unwrap_or(Bson::Null);
                if end <= cut && id != last_id {
                    self.segments()
                        .delete_one(doc! {"_id": id})
                        .session(&mut session)
                        .await
                        .map_err(|error| transaction_conflict_error("truncate a stream", error))?;
                } else if start.saturating_add(trimmed) < cut {
                    let next_trimmed = cut.min(end).saturating_sub(start);
                    let next_trimmed_i = i64::try_from(next_trimmed)
                        .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
                    let drop = usize::try_from(next_trimmed.saturating_sub(trimmed))
                        .unwrap_or(usize::MAX);
                    let values = row
                        .get_array("vs")
                        .map_err(|_| StorageError::serialization("malformed stream segment"))?;
                    let retained = values.iter().skip(drop).cloned().collect::<Vec<_>>();
                    self.segments()
                        .update_one(
                            doc! {"_id": id},
                            doc! {"$set": {"t": next_trimmed_i, "vs": retained}},
                        )
                        .session(&mut session)
                        .await
                        .map_err(|error| transaction_conflict_error("truncate a stream", error))?;
                }
            }
            Ok(removed)
        }
        .await;
        match result {
            Ok(removed) => {
                session.commit_transaction().await.map_err(|error| {
                    transaction_conflict_error("commit a stream truncation", error)
                })?;
                Ok(removed)
            }
            Err(error) => {
                let _ = session.abort_transaction().await;
                Err(error)
            }
        }
    }

    async fn publish_seal_marker(
        &self,
        stream: &str,
        generation: i64,
        truncate_epoch: i64,
        append_epoch: i64,
        marker: Document,
    ) -> Result<()> {
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(mongo_error("start a stream seal"))?;
        session
            .start_transaction()
            .await
            .map_err(mongo_error("start a stream seal"))?;
        let result = async {
            let mut head_filter = doc! {
                "_id": {"_scope": self.scope.as_str(), "s": stream},
                "_scope": self.scope.as_str(), "s": stream,
                "g": generation,
            };
            if truncate_epoch == 0 {
                head_filter.insert(
                    "$or",
                    vec![
                        doc! {"truncate_epoch": 0_i64},
                        doc! {"truncate_epoch": {"$exists": false}},
                    ],
                );
            } else {
                head_filter.insert("truncate_epoch", truncate_epoch);
            }
            if append_epoch == 0 {
                head_filter.insert(
                    "$and",
                    vec![doc! {"$or": [
                        {"append_epoch": 0_i64},
                        {"append_epoch": {"$exists": false}},
                    ]}],
                );
            } else {
                head_filter.insert("append_epoch", append_epoch);
            }
            let held = self
                .heads()
                .update_one(head_filter, doc! {"$inc": {"seal_epoch": 1_i64}})
                .session(&mut session)
                .await
                .map_err(|error| transaction_conflict_error("hold a stream seal", error))?;
            if held.matched_count != 1 {
                return Err(StorageError::conflict("stream changed while sealing"));
            }
            self.segments()
                .insert_one(marker)
                .session(&mut session)
                .await
                .map_err(|error| {
                    if error.contains_label("TransientTransactionError")
                        || error.to_string().contains("E11000")
                    {
                        StorageError::conflict("stream changed while sealing").with_source(error)
                    } else {
                        mongo_error("mark a sealed stream")(error)
                    }
                })?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => session
                .commit_transaction()
                .await
                .map_err(|error| transaction_conflict_error("commit a stream seal", error)),
            Err(error) => {
                let _ = session.abort_transaction().await;
                Err(error)
            }
        }
    }

    async fn generation(&self, stream: &str) -> Result<i64> {
        Ok(self
            .heads()
            .find_one(self.head_filter(stream))
            .await
            .map_err(mongo_error("read a stream head"))?
            .and_then(|head| head.get_i64("g").ok())
            .unwrap_or(0))
    }

    async fn advance_generation(&self, stream: &str, generation: i64) -> Result<()> {
        let mut filter = self.head_filter(stream);
        if generation == 0 {
            filter.insert(
                "$or",
                vec![doc! {"g": 0_i64}, doc! {"g": {"$exists": false}}],
            );
        } else {
            filter.insert("g", generation);
        }
        let result = self
            .heads()
            .update_one(filter, doc! {"$inc": {"g": 1_i64}})
            .await
            .map_err(mongo_error("advance a stream generation"))?;
        if result.matched_count != 1 {
            return Err(StorageError::conflict("stream generation changed"));
        }
        Ok(())
    }

    async fn segment_rows(&self, stream: &str) -> Result<Vec<Document>> {
        let generation = self.generation(stream).await?;
        self.segment_rows_in_generation(stream, generation).await
    }

    async fn segment_rows_in_generation(
        &self,
        stream: &str,
        generation: i64,
    ) -> Result<Vec<Document>> {
        let options = FindOptions::builder().sort(doc! {"o": 1}).build();
        let mut cursor = self
            .segments()
            .find(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation})
            .with_options(options)
            .await
            .map_err(mongo_error("read stream segments"))?;
        let mut rows = Vec::new();
        while let Some(row) = cursor
            .try_next()
            .await
            .map_err(mongo_error("read stream segments"))?
        {
            rows.push(row);
        }
        Ok(rows)
    }

    async fn extent(&self, stream: &str) -> Result<(u64, u64, Option<i64>)> {
        let rows = self.segment_rows(stream).await?;
        let mut base = None;
        let mut len = 0;
        let mut last_append = None;
        for row in rows {
            let start = row
                .get_i64("o")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0);
            let trimmed = row
                .get_i64("t")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0);
            let end = row
                .get_i64("e")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(start);
            if end > len {
                len = end;
            }
            if base.is_none() {
                base = Some(start.saturating_add(trimmed));
            }
            last_append = row.get_i64("ms").ok().or(last_append);
        }
        Ok((base.unwrap_or(len).min(len), len, last_append))
    }
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "the stream trait implementation keeps each persisted transition explicit"
)]
impl StreamStore for MongoStreams {
    fn capabilities(&self) -> Capabilities {
        let capabilities = Capabilities::none().with(Capability::Streams);
        let capabilities = if self.transactions {
            capabilities.with(Capability::ConditionalStreamAppend)
        } else {
            capabilities
        };
        if self
            .blobs
            .as_ref()
            .is_some_and(|blobs| blobs.supports_sealed_blobs())
        {
            capabilities.with(Capability::StreamSeal)
        } else {
            capabilities
        }
    }

    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        self.append_at(stream, None, values).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        if !self.transactions {
            return Err(StorageError::unsupported(
                Capability::ConditionalStreamAppend,
                "conditional MongoDB stream append requires transactions",
            ));
        }
        self.append_at(stream, Some(expected_next_offset), values)
            .await
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        validate_stream(stream)?;
        let generation = self.generation(stream).await?;
        let rows = self.segment_rows_in_generation(stream, generation).await?;
        if let Some(marker) = rows
            .last()
            .filter(|row| row.get_bool("seal").unwrap_or(false))
        {
            let key = Self::marker_blob_key(marker, stream, generation)?;
            return Ok(self.sealed_log(&key).await?.window(from, limit));
        }
        let mut output = Vec::new();
        for row in rows {
            let start = row
                .get_i64("o")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0);
            let trimmed = row
                .get_i64("t")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0);
            let Some(values) = row.get_array("vs").ok() else {
                continue;
            };
            for (index, value) in values.iter().enumerate() {
                let offset = start.saturating_add(trimmed).saturating_add(index as u64);
                if offset >= from && output.len() < limit {
                    output.push(StreamEntry {
                        offset,
                        value: bson_to_json(value)?,
                    });
                }
            }
            if output.len() >= limit {
                break;
            }
        }
        Ok(output)
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        validate_stream(stream)?;
        Ok(self.extent(stream).await?.1)
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        validate_stream(stream)?;
        if self.transactions {
            return self.truncate_transactional(stream, offset).await;
        }
        let rows = self.segment_rows(stream).await?;
        let Some(last) = rows.last() else {
            return Ok(0);
        };
        if last.get_bool("seal").unwrap_or(false) {
            return Err(StorageError::conflict("stream is sealed"));
        }
        let (base, len, _) = self.extent(stream).await?;
        let cut = offset.clamp(base, len);
        let removed = cut - base;
        if removed == 0 {
            return Ok(0);
        }
        if self
            .segment_rows(stream)
            .await?
            .last()
            .is_some_and(|row| row.get_bool("seal").unwrap_or(false))
        {
            return Err(StorageError::conflict("stream was sealed while truncating"));
        }
        let last_id = last.get("_id").cloned().unwrap_or(Bson::Null);
        for row in rows {
            let start = row
                .get_i64("o")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0);
            let end = row
                .get_i64("e")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(start);
            let id = row.get("_id").cloned().unwrap_or(Bson::Null);
            let trimmed = row
                .get_i64("t")
                .ok()
                .and_then(|v| u64::try_from(v).ok())
                .unwrap_or(0);
            if end <= cut && id != last_id {
                self.segments()
                    .delete_one(doc! {"_id": id})
                    .await
                    .map_err(mongo_error("truncate a stream"))?;
            } else if start.saturating_add(trimmed) < cut {
                let next_trimmed = cut.min(end).saturating_sub(start);
                let next_trimmed_i = i64::try_from(next_trimmed)
                    .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
                let drop =
                    usize::try_from(next_trimmed.saturating_sub(trimmed)).unwrap_or(usize::MAX);
                let values = row
                    .get_array("vs")
                    .map_err(|_| StorageError::serialization("malformed stream segment"))?;
                let retained = values.iter().skip(drop).cloned().collect::<Vec<_>>();
                self.segments()
                    .update_one(
                        doc! {"_id": id},
                        doc! {"$set": {"t": next_trimmed_i, "vs": retained}},
                    )
                    .await
                    .map_err(mongo_error("truncate a stream"))?;
            }
        }
        // Match the legacy standalone protocol: publish the epoch only after
        // the segment edits are complete. A seal that snapshots before this
        // point either sees the edited rows or is rejected by the epoch check.
        #[cfg(test)]
        self.pause_at_truncate_boundary().await;
        self.heads()
            .update_one(
                doc! {"_id": {"_scope": self.scope.as_str(), "s": stream}, "_scope": self.scope.as_str(), "s": stream},
                doc! {"$inc": {"truncate_epoch": 1_i64}, "$max": {"g": 0_i64}},
            )
            .upsert(true)
            .await
            .map_err(mongo_error("record a stream truncation"))?;
        if self
            .segment_rows(stream)
            .await?
            .last()
            .is_some_and(|row| row.get_bool("seal").unwrap_or(false))
        {
            return Err(StorageError::conflict("stream was sealed while truncating"));
        }
        Ok(removed)
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        validate_stream(stream)?;
        let generation = self.generation(stream).await?;
        let exists = !self.segment_rows(stream).await?.is_empty();
        if !exists {
            return Ok(false);
        }
        self.advance_generation(stream, generation).await?;
        self.segments()
            .delete_many(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation})
            .await
            .map_err(mongo_error("delete stream segments"))?;
        if let Some(blobs) = self
            .blobs
            .as_ref()
            .filter(|blobs| blobs.supports_sealed_blobs())
        {
            let canonical = Self::generation_blob_key(stream, generation)?;
            blobs.delete_sealed(&canonical).await?;
            let attempt_prefix = format!("{canonical}/");
            for meta in blobs.list_sealed(&attempt_prefix).await? {
                blobs.delete_sealed(&meta.key).await?;
            }
        }
        Ok(exists)
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        let mut cursor = self
            .heads()
            .find(doc! {"_scope": self.scope.as_str()})
            .await
            .map_err(mongo_error("list stream heads"))?;
        let mut names = Vec::new();
        while let Some(row) = cursor
            .try_next()
            .await
            .map_err(mongo_error("list stream heads"))?
        {
            if let Ok(name) = row.get_str("s")
                && name.starts_with(prefix)
            {
                names.push(name.to_owned());
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        validate_stream(stream)?;
        let rows = self.segment_rows(stream).await?;
        if let Some(marker) = rows
            .last()
            .filter(|row| row.get_bool("seal").unwrap_or(false))
        {
            let len = marker
                .get_i64("e")
                .ok()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0);
            let base = marker
                .get_i64("sb")
                .ok()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(len);
            return Ok(Some(StreamInfo::new(
                base,
                len,
                marker.get_i64("ms").ok(),
                marker.get_i64("la").ok(),
            )));
        }
        let (base, len, last) = self.extent(stream).await?;
        if len == 0 && rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(StreamInfo::new(base, len, None, last)))
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        validate_stream(stream)?;
        let blobs = self.blobs()?;
        let generation = self.generation(stream).await?;
        // Take the truncation boundary before collecting the segment snapshot.
        // A truncate after this point advances the epoch and invalidates the
        // marker before it can become the authoritative representation.
        let epoch = self
            .heads()
            .find_one(self.head_filter(stream))
            .await
            .map_err(mongo_error("read a stream truncation epoch"))?
            .and_then(|head| head.get_i64("truncate_epoch").ok())
            .unwrap_or(0);
        let append_epoch = self
            .heads()
            .find_one(self.head_filter(stream))
            .await
            .map_err(mongo_error("read a stream append epoch"))?
            .and_then(|head| head.get_i64("append_epoch").ok())
            .unwrap_or(0);
        let rows = self.segment_rows_in_generation(stream, generation).await?;
        let last = rows
            .last()
            .ok_or_else(|| StorageError::not_found("stream does not exist"))?;
        if last.get_bool("seal").unwrap_or(false) {
            let key = Self::marker_blob_key(last, stream, generation)?;
            let log = self.sealed_log(&key).await?;
            let blob = blobs
                .head_sealed(&key)
                .await?
                .ok_or_else(|| StorageError::backend("sealed stream has no blob"))?;
            self.segments().delete_many(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation, "seal": {"$ne": true}}).await.map_err(mongo_error("finish stream sealing"))?;
            return Ok(SealOutcome::new(log.base(), log.len(), blob.len));
        }
        let len = last
            .get_i64("e")
            .ok()
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(0);
        let last_append = rows.iter().rev().find_map(|row| row.get_i64("ms").ok());
        let mut entries = Vec::new();
        for row in &rows {
            let start = row
                .get_i64("o")
                .ok()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0);
            let trimmed = row
                .get_i64("t")
                .ok()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or(0);
            if let Ok(values) = row.get_array("vs") {
                for (index, value) in values.iter().enumerate() {
                    entries.push((start + trimmed + index as u64, bson_to_json(value)?));
                }
            }
        }
        let base = entries.first().map_or(len, |(offset, _)| *offset);
        if entries
            .iter()
            .enumerate()
            .any(|(index, (offset, _))| *offset != base + index as u64)
            || base + entries.len() as u64 != len
        {
            return Err(StorageError::backend(
                "stream segments do not tile the stream; refusing to seal",
            ));
        }
        let log = crate::stream::SealedLog::new(
            base,
            entries.into_iter().map(|(_, value)| value).collect(),
        )?;
        let key = Self::generation_blob_key(stream, generation)?;
        let len_i = i64::try_from(len)
            .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
        let base_i = i64::try_from(base)
            .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
        let unique_key = format!("{key}/{}", mongodb::bson::oid::ObjectId::new().to_hex());
        let encoded = log.encode()?;
        blobs
            .put_sealed(&unique_key, encoded.clone(), Some("application/zstd"))
            .await?;
        let mut marker = doc! {
            "_scope": self.scope.as_str(), "s": stream, "g": generation,
            "o": len_i, "n": 0_i64, "e": len_i, "t": 0_i64,
            "vs": [], "seal": true, "sb": base_i,
            "ms": i64::try_from((self.clock)()).unwrap_or(i64::MAX),
            "bk": unique_key,
        };
        if let Some(last_append) = last_append {
            marker.insert("la", last_append);
        }
        if self.transactions {
            self.publish_seal_marker(stream, generation, epoch, append_epoch, marker)
                .await?;
        } else {
            if let Err(error) = self.segments().insert_one(marker).await {
                if error.to_string().contains("E11000") {
                    return Err(StorageError::conflict("stream changed while sealing"));
                }
                return Err(mongo_error("mark a sealed stream")(error));
            }
            let latest_epoch = self
                .heads()
                .find_one(self.head_filter(stream))
                .await
                .map_err(mongo_error("verify a stream truncation epoch"))?
                .and_then(|head| head.get_i64("truncate_epoch").ok())
                .unwrap_or(0);
            let latest_append_epoch = self
                .heads()
                .find_one(self.head_filter(stream))
                .await
                .map_err(mongo_error("verify a stream append epoch"))?
                .and_then(|head| head.get_i64("append_epoch").ok())
                .unwrap_or(0);
            if latest_epoch != epoch || latest_append_epoch != append_epoch {
                self.segments().delete_one(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation, "seal": true}).await.map_err(mongo_error("roll back stream sealing"))?;
                return Err(StorageError::conflict("stream was truncated while sealing"));
            }
        }
        self.segments().delete_many(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation, "seal": {"$ne": true}}).await.map_err(mongo_error("finish stream sealing"))?;
        Ok(SealOutcome::new(base, len, encoded.len() as u64))
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        validate_stream(stream)?;
        let generation = self.generation(stream).await?;
        let Some(marker) = self.segment_rows(stream).await?.last().cloned() else {
            return Ok(false);
        };
        if !marker.get_bool("seal").unwrap_or(false) || marker.get_i64("ms").ok() != Some(sealed_ms)
        {
            return Ok(false);
        }
        self.advance_generation(stream, generation).await?;
        self.segments()
            .delete_many(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation})
            .await
            .map_err(mongo_error("delete a sealed stream"))?;
        let blob_key = Self::marker_blob_key(&marker, stream, generation)?;
        self.blobs()?.delete_sealed(&blob_key).await?;
        Ok(true)
    }
}

impl MongoStreams {
    #[allow(
        clippy::too_many_lines,
        reason = "keeps the retry and segment-claim sequence together for review"
    )]
    async fn append_at(
        &self,
        stream: &str,
        expected: Option<u64>,
        values: Vec<Value>,
    ) -> Result<u64> {
        validate_stream(stream)?;
        if self.transactions {
            return self.append_transactional(stream, expected, values).await;
        }
        if values.is_empty() {
            return self.len(stream).await;
        }
        let bson_values = values
            .iter()
            .map(json_to_bson)
            .collect::<Result<Vec<_>>>()?;
        let count = i64::try_from(bson_values.len())
            .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
        let indexes = self
            .segments()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"_scope": 1, "s": 1, "g": 1, "o": 1})
                    .options(
                        IndexOptions::builder()
                            .unique(true)
                            .name("_tsd_stream_segment".to_owned())
                            .build(),
                    )
                    .build(),
            )
            .await;
        indexes.map_err(mongo_error("prepare stream segments"))?;
        for _ in 0..128 {
            self.heads()
                .update_one(
                    doc! {"_id": {"_scope": self.scope.as_str(), "s": stream}, "_scope": self.scope.as_str(), "s": stream},
                    doc! {"$max": {"g": 0_i64}, "$inc": {"append_epoch": 1_i64}, "$setOnInsert": {"truncate_epoch": 0_i64}},
                )
                .upsert(true)
                .await
                .map_err(mongo_error("prepare a stream head"))?;
            let (_, start, _) = self.extent(stream).await?;
            if expected.is_some_and(|offset| offset != start) {
                return Err(StorageError::conflict("stream offset changed"));
            }
            let generation = self.generation(stream).await?;
            if self
                .segment_rows(stream)
                .await?
                .last()
                .is_some_and(|row| row.get_bool("seal").unwrap_or(false))
            {
                return Err(StorageError::conflict("stream is sealed"));
            }
            let start_i = i64::try_from(start)
                .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
            let end_i = start_i
                .checked_add(count)
                .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
            let now = i64::try_from((self.clock)()).unwrap_or(i64::MAX);
            let segment = doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation, "o": start_i, "n": count, "e": end_i, "t": 0_i64, "vs": bson_values.clone(), "ms": now};
            match self.segments().insert_one(segment).await {
                Ok(_) => return Ok(u64::try_from(start_i).unwrap_or(u64::MAX)),
                Err(error) if error.to_string().contains("E11000") && expected.is_none() => {}
                Err(error) if error.to_string().contains("E11000") => {
                    return Err(StorageError::conflict("stream offset changed"));
                }
                Err(error) => return Err(mongo_error("append stream entries")(error)),
            }
        }
        Err(StorageError::unavailable(
            "stream kept growing concurrently; retry",
        ))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "keeps the transaction boundary and write-conflict behavior together"
    )]
    async fn append_transactional(
        &self,
        stream: &str,
        expected: Option<u64>,
        values: Vec<Value>,
    ) -> Result<u64> {
        let bson_values = values
            .iter()
            .map(json_to_bson)
            .collect::<Result<Vec<_>>>()?;
        let count = i64::try_from(bson_values.len())
            .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
        self.segments()
            .create_index(
                IndexModel::builder()
                    .keys(doc! {"_scope": 1, "s": 1, "g": 1, "o": 1})
                    .options(
                        IndexOptions::builder()
                            .unique(true)
                            .name("_tsd_stream_segment".to_owned())
                            .build(),
                    )
                    .build(),
            )
            .await
            .map_err(mongo_error("prepare stream segments"))?;

        for _ in 0..128 {
            let mut session = self
                .client
                .start_session()
                .await
                .map_err(mongo_error("start a stream append"))?;
            session
                .start_transaction()
                .await
                .map_err(mongo_error("start a stream append"))?;
            let result = async {
                let head_id = doc! {"_scope": self.scope.as_str(), "s": stream};
                self.heads()
                    .update_one(
                        self.head_filter(stream),
                        doc! {"$max": {"g": 0_i64}, "$setOnInsert": {"_id": head_id.clone(), "truncate_epoch": 0_i64}},
                    )
                    .upsert(true)
                    .session(&mut session)
                    .await
                    .map_err(mongo_error("prepare a stream head"))?;
                let head = self
                    .heads()
                    .find_one(self.head_filter(stream))
                    .session(&mut session)
                    .await
                    .map_err(mongo_error("read a stream head"))?;
                let generation = head
                    .as_ref()
                    .and_then(|value| value.get_i64("g").ok())
                    .unwrap_or(0);
                let options = FindOptions::builder().sort(doc! {"o": 1}).build();
                let mut cursor = self
                    .segments()
                    .find(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation})
                    .with_options(options)
                    .session(&mut session)
                    .await
                    .map_err(mongo_error("read stream segments"))?;
                let mut rows = Vec::new();
                while let Some(row) = cursor.next(&mut session).await {
                    rows.push(row.map_err(mongo_error("read stream segments"))?);
                }
                if rows.last().is_some_and(|row| row.get_bool("seal").unwrap_or(false)) {
                    return Err(StorageError::conflict("stream is sealed"));
                }
                let start = rows
                    .last()
                    .and_then(|row| row.get_i64("e").ok())
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or(0);
                if expected.is_some_and(|offset| offset != start) {
                    return Err(StorageError::conflict("stream offset changed"));
                }
                if bson_values.is_empty() {
                    return Ok(start);
                }
                let append_claim = self
                    .heads()
                    .update_one(
                        doc! {"_id": head_id, "_scope": self.scope.as_str(), "s": stream, "g": generation},
                        doc! {"$inc": {"append_epoch": 1_i64}},
                    )
                    .session(&mut session)
                    .await
                    .map_err(mongo_error("claim a stream append"))?;
                if append_claim.matched_count != 1 {
                    return Err(StorageError::conflict("stream generation changed"));
                }
                let start_i = i64::try_from(start)
                    .map_err(|_| StorageError::backend("stream offset space is exhausted"))?;
                let end_i = start_i
                    .checked_add(count)
                    .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
                let now = i64::try_from((self.clock)()).unwrap_or(i64::MAX);
                self.segments()
                    .insert_one(doc! {"_scope": self.scope.as_str(), "s": stream, "g": generation, "o": start_i, "n": count, "e": end_i, "t": 0_i64, "vs": bson_values.clone(), "ms": now})
                    .session(&mut session)
                    .await
                    .map_err(|error| {
                        if error.to_string().contains("E11000") {
                            StorageError::conflict("stream offset changed")
                        } else {
                            mongo_error("append stream entries")(error)
                        }
                    })?;
                Ok(start)
            }
            .await;
            match result {
                Ok(offset) => match session.commit_transaction().await {
                    Ok(()) => return Ok(offset),
                    Err(error) if error.to_string().contains("TransientTransactionError") => {}
                    Err(error) => return Err(mongo_error("commit a stream append")(error)),
                },
                Err(error) => {
                    let retry = std::error::Error::source(&error)
                        .and_then(|source| source.downcast_ref::<mongodb::error::Error>())
                        .is_some_and(|source| source.contains_label("TransientTransactionError"));
                    let _ = session.abort_transaction().await;
                    if !retry {
                        return Err(error);
                    }
                }
            }
        }
        Err(StorageError::unavailable(
            "stream kept changing concurrently; retry",
        ))
    }
}

fn transaction_conflict_error(
    operation: &'static str,
    error: mongodb::error::Error,
) -> StorageError {
    if error.contains_label("TransientTransactionError") {
        StorageError::conflict("stream changed while sealing").with_source(error)
    } else {
        mongo_error(operation)(error)
    }
}

#[cfg(test)]
#[path = "streams_tests.rs"]
mod tests;
