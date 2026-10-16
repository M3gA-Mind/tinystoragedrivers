//! Stream storage on MongoDB. Replica-set writes use native scope-admission
//! transactions; standalone writes retain the segment uniqueness protocol.
//!
//! A stream is a run of *segments* in `<prefix>_tsd_streams`, one per append:
//! `{_scope, s, o, n, e, t, vs}` covers offsets `o..e` (`e = o + n`), of which
//! the first `t` were truncated away and `vs` holds the rest.
//!
//! **Density.** An append reads the stream's end `L` (the last segment's `e`)
//! and inserts one segment at `o = L`. The unique `(_scope, s, o)` index lets
//! exactly one appender claim each `L`; the others re-read and retry. Because
//! a segment is one document, it lands whole or not at all, so the segments
//! always tile `0..len` with no gap, even across a crash, and a batch's
//! records are always contiguous. The cost is that one batch must fit in a
//! single 16 MiB BSON document.
//!
//! **Truncation** deletes whole segments below the cut, trims the one the cut
//! falls inside, and never deletes the last segment (it is trimmed to empty
//! instead), so `len` survives. Each step is idempotent and monotonic, so an
//! interrupted truncation is finished by the next one.
//!
//! **Deletion** is coordinated with appends through a *generation*. Each
//! stream has a header in `<prefix>_tsd_stream_heads` holding its generation
//! `g` (0 when absent), every segment records the generation it was appended
//! in, and every read considers only the current generation's segments.
//! `delete_stream` advances the generation first and then removes the older
//! segments. An append that read the old generation before the delete lands
//! in the old generation, which no reader looks at: it is ordered before the
//! delete, never interleaved with the new stream, so the new stream still
//! starts at offset 0 with no gap. The next delete removes such leftovers.

use crate::fence::Fencing;

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::{Bson, Document, doc};
use mongodb::options::{FindOptions, IndexOptions};
use mongodb::{ClientSession, IndexModel};
use serde_json::Value;
use tinystoragedrivers_core::{
    Capability, Result, Scope, SealOutcome, StorageError, StreamEntry, StreamInfo, StreamStore,
    validate_stream,
};

mod seal;

use crate::backend::Shared;
use crate::blobs::MongoBlobs;
use crate::convert::{from_bson, to_bson};
use crate::errors;
use crate::fence::in_write;
use crate::naming::{
    SCOPE, STREAM_END_INDEX, STREAM_HEADS, STREAM_SEGMENT_INDEX, STREAMS, prefix_regex,
};
use crate::scoped::ScopedCollection;

/// How many times an append retries after another appender took its offset.
const APPEND_ATTEMPTS: usize = 128;

/// One stored segment.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Segment {
    pub(crate) id: Bson,
    /// First offset covered.
    pub(crate) start: u64,
    /// One past the last offset covered.
    pub(crate) end: u64,
    /// How many leading offsets were truncated away.
    pub(crate) trimmed: u64,
    /// The retained records, for offsets `start + trimmed..end`.
    pub(crate) values: Vec<Bson>,
    /// For the seal marker, the first retained offset of the sealed stream.
    /// A marker is the stream's last segment and holds no entries.
    pub(crate) sealed_base: Option<u64>,
    /// When the segment was appended (`ms`), or for the seal marker when the
    /// stream was sealed. Absent on segments written before times existed.
    pub(crate) at_ms: Option<i64>,
    /// On a seal marker, when the last entry was appended (`la`): the entry
    /// segments that held it are gone once sealing finishes.
    pub(crate) last_append_ms: Option<i64>,
}

fn offset(doc: &Document, field: &str) -> Result<u64> {
    doc.get_i64(field)
        .ok()
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| StorageError::serialization("malformed stream segment"))
}

fn stored_offset(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| StorageError::backend("stream offset space is exhausted"))
}

impl Segment {
    /// Decode a stored segment.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Serialization`](tinystoragedrivers_core::ErrorKind::Serialization)
    /// for a malformed one.
    pub(crate) fn decode(doc: &Document) -> Result<Self> {
        let segment = Self {
            id: doc.get("_id").cloned().unwrap_or(Bson::Null),
            start: offset(doc, "o")?,
            end: offset(doc, "e")?,
            trimmed: offset(doc, "t")?,
            values: doc
                .get_array("vs")
                .map_err(|_| StorageError::serialization("malformed stream segment"))?
                .clone(),
            sealed_base: if doc.get_bool("seal").unwrap_or(false) {
                Some(offset(doc, "sb")?)
            } else {
                None
            },
            at_ms: doc.get_i64("ms").ok(),
            last_append_ms: doc.get_i64("la").ok(),
        };
        let covered = segment
            .start
            .checked_add(segment.trimmed)
            .and_then(|first| first.checked_add(segment.values.len() as u64));
        if covered != Some(segment.end) {
            return Err(StorageError::serialization("malformed stream segment"));
        }
        Ok(segment)
    }

    /// The retained entries at or after `from`.
    pub(crate) fn entries_from(&self, from: u64) -> impl Iterator<Item = (u64, &Bson)> {
        let first = self.start + self.trimmed;
        self.values
            .iter()
            .enumerate()
            .map(move |(index, value)| (first + index as u64, value))
            .filter(move |(offset, _)| *offset >= from)
    }
}

/// The segment document for `values` appended at `start` in `generation`.
///
/// # Errors
///
/// A value that does not convert to BSON, or an exhausted offset space.
pub(crate) fn new_segment(
    stream: &str,
    generation: i64,
    start: u64,
    values: &[Value],
    now_ms: i64,
) -> Result<Document> {
    let values: Vec<Bson> = values.iter().map(to_bson).collect::<Result<_>>()?;
    let count = values.len() as u64;
    let end = start
        .checked_add(count)
        .ok_or_else(|| StorageError::backend("stream offset space is exhausted"))?;
    Ok(doc! {
        "s": stream,
        "g": generation,
        "o": stored_offset(start)?,
        "n": stored_offset(count)?,
        "e": stored_offset(end)?,
        "t": 0_i64,
        "vs": values,
        "ms": now_ms,
    })
}

pub(crate) fn segment_models() -> Vec<IndexModel> {
    let named = |name: &str, unique: bool| {
        let mut options = IndexOptions::builder().name(name.to_owned()).build();
        options.unique = Some(unique);
        options
    };
    vec![
        IndexModel::builder()
            .keys(doc! {SCOPE: 1, "s": 1, "g": 1, "o": 1})
            .options(named(STREAM_SEGMENT_INDEX, true))
            .build(),
        IndexModel::builder()
            .keys(doc! {SCOPE: 1, "s": 1, "g": 1, "e": 1})
            .options(named(STREAM_END_INDEX, false))
            .build(),
    ]
}

/// The `_id` of a stream header: unique per scope and stream.
pub(crate) fn head_id(scope: &Scope, stream: &str) -> Document {
    doc! {"s": scope.as_str(), "n": stream}
}

/// MongoDB streams bound to one scope.
#[derive(Debug)]
pub(crate) struct MongoStreams {
    shared: Arc<Shared>,
    scope: Scope,
    generation: tinystoragedrivers_core::ScopeGeneration,
    /// Every write runs in a transaction that holds this fence, when set.
    fence: Option<Arc<Fencing>>,
}

impl MongoStreams {
    pub(crate) fn new(shared: Arc<Shared>, scope: Scope, fence: Option<Arc<Fencing>>) -> Self {
        Self {
            shared,
            scope,
            generation: tinystoragedrivers_core::ScopeGeneration::default(),
            fence,
        }
    }
    pub(crate) fn with_generation(
        mut self,
        generation: tinystoragedrivers_core::ScopeGeneration,
    ) -> Self {
        self.generation = generation;
        self
    }

    /// The injected clock's reading, as the signed value BSON stores.
    fn now_ms(&self) -> i64 {
        i64::try_from((self.shared.clock)()).unwrap_or(i64::MAX)
    }

    async fn hold_admission(&self, session: &mut ClientSession) -> Result<()> {
        self.shared
            .hold_validation(Some(&self.scope), &mut *session)
            .await?;
        crate::purge::check(&self.shared, &self.scope, self.generation, Some(session)).await
    }

    async fn segments(&self) -> Result<ScopedCollection> {
        if self.shared.existing {
            self.shared.require_collection(STREAM_HEADS).await?;
        }
        self.shared.prepare(STREAMS, segment_models()).await?;
        Ok(self.shared.scoped(STREAMS, &self.scope))
    }

    /// Blobs of this scope, unfenced: GridFS cannot join a fenced transaction,
    /// and a sealed stream's blob is written and removed around the fenced
    /// document writes, never inside one.
    fn blobs(&self) -> MongoBlobs {
        MongoBlobs::new(Arc::clone(&self.shared), self.scope.clone(), false)
            .with_generation(self.generation)
            .with_sealed_access()
    }

    fn heads(&self) -> ScopedCollection {
        self.shared.scoped(STREAM_HEADS, &self.scope)
    }

    /// The current generation of `stream`.
    async fn generation(&self, stream: &str, session: Option<&mut ClientSession>) -> Result<i64> {
        let head = self
            .heads()
            .find_one(doc! {"_id": head_id(&self.scope, stream)}, session)
            .await
            .map_err(errors::failed("read a stream header"))?;
        Ok(head.map_or(0, |head| head.get_i64("g").unwrap_or(0)))
    }

    /// The segment of `stream` in `generation` first in `order` (`1` first,
    /// `-1` last).
    async fn edge(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
        order: i32,
        session: Option<&mut ClientSession>,
    ) -> Result<Option<Segment>> {
        let found = handle
            .find_in(
                doc! {"s": stream, "g": generation},
                FindOptions::builder()
                    .sort(doc! {"o": order})
                    .limit(1)
                    .build(),
                session,
            )
            .await
            .map_err(errors::failed("read a stream"))?;
        found.first().map(Segment::decode).transpose()
    }

    /// One append attempt at the stream's current end. `None` when another
    /// appender took that offset first.
    async fn append_once(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        values: &[Value],
        expected: Option<u64>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<Option<u64>> {
        if expected.is_some() {
            // Snapshot reads alone do not conflict with deletion. Holding the
            // generation header by writing it makes deletion/takeover abort
            // this attempt rather than publish into an obsolete generation.
            self.heads()
                .update_one(
                    doc! {"_id": head_id(&self.scope, stream), "n": stream},
                    doc! {"$inc": {"conditional_append_epoch": 1_i64}},
                    true,
                    session.as_deref_mut(),
                )
                .await
                .map_err(|error| {
                    if errors::duplicate_index(&error).is_some() {
                        StorageError::unavailable("stream header changed concurrently; retry")
                    } else {
                        errors::map(error, "hold a stream generation")
                    }
                })?;
        }
        let generation = self.generation(stream, session.as_deref_mut()).await?;
        let last = self
            .edge(handle, stream, generation, -1, session.as_deref_mut())
            .await?;
        if last.as_ref().is_some_and(|last| last.sealed_base.is_some()) {
            return Err(StorageError::conflict("stream is sealed"));
        }
        let end = last.map_or(0, |last| last.end);
        if expected.is_some_and(|offset| offset != end) {
            return Err(StorageError::conflict("stream next offset changed"));
        }
        if values.is_empty() {
            return Ok(Some(end));
        }
        match handle
            .insert_one(
                new_segment(stream, generation, end, values, self.now_ms())?,
                session,
            )
            .await
        {
            Ok(()) => Ok(Some(end)),
            Err(error)
                if errors::duplicate_index(&error).as_deref() == Some(STREAM_SEGMENT_INDEX) =>
            {
                Ok(None)
            }
            Err(error) => Err(errors::map(error, "append to a stream")),
        }
    }

    async fn truncate_in(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        offset: u64,
        mut session: Option<&mut ClientSession>,
    ) -> Result<u64> {
        let generation = self.generation(stream, session.as_deref_mut()).await?;
        let Some(last) = self
            .edge(handle, stream, generation, -1, session.as_deref_mut())
            .await?
        else {
            return Ok(0);
        };
        if last.sealed_base.is_some() {
            return Err(StorageError::conflict("stream is sealed"));
        }
        let affected = handle
            .find_in(
                doc! {"s": stream, "g": generation, "o": {"$lt": stored_offset(offset).unwrap_or(i64::MAX)}},
                FindOptions::builder().sort(doc! {"o": 1}).build(),
                session.as_deref_mut(),
            )
            .await
            .map_err(errors::failed("read a stream"))?;
        let segments: Vec<Segment> = affected
            .iter()
            .map(Segment::decode)
            .collect::<Result<_>>()?;
        // The first retained offset: past every fully trimmed segment.
        let base = segments
            .iter()
            .find(|segment| segment.start + segment.trimmed < segment.end)
            .map_or_else(
                || {
                    segments
                        .last()
                        .map_or(last.end, |segment| segment.end.min(last.end))
                },
                |segment| segment.start + segment.trimmed,
            );
        let cut = offset.clamp(base, last.end);
        for segment in &segments {
            let keep_from = cut.clamp(segment.start, segment.end) - segment.start;
            if segment.end <= cut && segment.start != last.start {
                handle
                    .delete_many(doc! {"_id": segment.id.clone()}, session.as_deref_mut())
                    .await
                    .map_err(errors::failed("truncate a stream"))?;
            } else if keep_from > segment.trimmed {
                let drop = usize::try_from(keep_from - segment.trimmed).unwrap_or(usize::MAX);
                let rest: Vec<Bson> = segment.values.iter().skip(drop).cloned().collect();
                handle
                    .update_one(
                        doc! {"_id": segment.id.clone(), "t": stored_offset(segment.trimmed)?},
                        doc! {"$set": {"t": stored_offset(keep_from)?, "vs": rest}},
                        false,
                        session.as_deref_mut(),
                    )
                    .await
                    .map_err(errors::failed("truncate a stream"))?;
            }
        }
        Ok(cut - base)
    }

    async fn delete_in(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        mut session: Option<&mut ClientSession>,
    ) -> Result<bool> {
        let current = self.generation(stream, session.as_deref_mut()).await?;
        // Advance first: from here on every reader ignores the old segments,
        // and an append still holding the old generation lands among them.
        self.heads()
            .update_one(
                doc! {"_id": head_id(&self.scope, stream), "n": stream},
                doc! {"$inc": {"g": 1_i64}},
                true,
                session.as_deref_mut(),
            )
            .await
            .map_err(errors::failed("delete a stream"))?;
        let removed = handle
            .delete_many(doc! {"s": stream, "g": {"$lte": current}}, session)
            .await
            .map_err(errors::failed("delete a stream"))?;
        Ok(removed > 0)
    }
}

#[async_trait]
impl StreamStore for MongoStreams {
    async fn append(&self, stream: &str, value: Value) -> Result<u64> {
        self.append_batch(stream, vec![value]).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> Result<u64> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            let handle = self.segments().await?;
            if self.shared.physical_transactions {
                self.shared.prepare_validation(Some(&self.scope)).await?;
                return in_write!(&self.shared, self.fence.as_deref(), session => {
                    self.hold_admission(&mut *session).await?;
                    self.append_once(&handle, stream, &values, None, Some(session)).await
                        .and_then(|offset| offset.ok_or_else(|| StorageError::unavailable("stream grew concurrently; retry")))
                });
            }
            for _ in 0..APPEND_ATTEMPTS {
                if let Some(end) = self
                    .append_once(&handle, stream, &values, None, None)
                    .await?
                {
                    return Ok(end);
                }
            }
            Err(StorageError::unavailable(
                "stream kept growing concurrently; retry",
            ))
        })
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> Result<u64> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            if !self.shared.transactions {
                return Err(StorageError::unsupported(
                    Capability::ConditionalStreamAppend,
                    "conditional stream append requires MongoDB transactions",
                ));
            }
            let handle = self.segments().await?;
            self.shared.prepare_validation(Some(&self.scope)).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_admission(&mut *session).await?;
                self.append_once(&handle, stream, &values, Some(expected_next_offset), Some(session)).await
                    .and_then(|offset| offset.ok_or_else(|| StorageError::unavailable("stream grew concurrently; retry")))
            })
        })
    }

    async fn read_window(&self, stream: &str, from: u64, limit: usize) -> Result<Vec<StreamEntry>> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            if limit == 0 {
                if self.shared.existing {
                    self.segments().await?;
                }
                return Ok(Vec::new());
            }
            let handle = self.segments().await?;
            let generation = self.generation(stream, None).await?;
            let mut cursor = handle
            .cursor(
                doc! {"s": stream, "g": generation, "e": {"$gt": stored_offset(from).unwrap_or(i64::MAX)}},
                FindOptions::builder().sort(doc! {"o": 1}).build(),
            )
            .await
            .map_err(errors::failed("read a stream"))?;
            let mut out = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(errors::failed("read a stream"))?
            {
                let segment = Segment::decode(&doc)?;
                if segment.sealed_base.is_some() {
                    // The marker is the last segment: the blob holds it all.
                    return self.sealed_window(stream, generation, from, limit).await;
                }
                for (offset, value) in segment.entries_from(from) {
                    out.push(StreamEntry {
                        offset,
                        value: from_bson(value)?,
                    });
                    if out.len() == limit {
                        return Ok(out);
                    }
                }
            }
            Ok(out)
        })
    }

    async fn len(&self, stream: &str) -> Result<u64> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            let handle = self.segments().await?;
            let generation = self.generation(stream, None).await?;
            Ok(self
                .edge(&handle, stream, generation, -1, None)
                .await?
                .map_or(0, |last| last.end))
        })
    }

    async fn info(&self, stream: &str) -> Result<Option<StreamInfo>> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            let handle = self.segments().await?;
            let generation = self.generation(stream, None).await?;
            let Some(last) = self.edge(&handle, stream, generation, -1, None).await? else {
                return Ok(None);
            };
            if let Some(base) = last.sealed_base {
                return Ok(Some(StreamInfo::new(
                    base,
                    last.end,
                    last.at_ms,
                    last.last_append_ms,
                )));
            }
            let first = self
                .edge(&handle, stream, generation, 1, None)
                .await?
                .unwrap_or_else(|| last.clone());
            Ok(Some(StreamInfo::new(
                first.start + first.trimmed,
                last.end,
                None,
                last.at_ms,
            )))
        })
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> Result<u64> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            let handle = self.segments().await?;
            if !self.shared.physical_transactions {
                return self.truncate_in(&handle, stream, offset, None).await;
            }
            self.shared.prepare_validation(Some(&self.scope)).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_admission(&mut *session).await?;
                self.truncate_in(&handle, stream, offset, Some(session)).await
            })
        })
    }

    async fn delete_stream(&self, stream: &str) -> Result<bool> {
        crate::purge::admitted!(self, {
            validate_stream(stream)?;
            let handle = self.segments().await?;
            let removed = if self.shared.physical_transactions {
                self.shared.prepare_validation(Some(&self.scope)).await?;
                in_write!(&self.shared, self.fence.as_deref(), session => {
                    self.hold_admission(&mut *session).await?;
                    self.delete_in(&handle, stream, Some(session)).await
                })?
            } else {
                self.delete_in(&handle, stream, None).await?
            };
            // Even when the stream is gone, clear the blobs of the generations
            // it deleted, including one a crashed or stalled seal left.
            self.purge_stale_sealed_blobs(stream).await?;
            Ok(removed)
        })
    }

    async fn seal(&self, stream: &str) -> Result<SealOutcome> {
        crate::purge::admitted!(self, self.seal_stream(stream).await)
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> Result<bool> {
        crate::purge::admitted!(
            self,
            self.delete_sealed_stream_impl(stream, sealed_ms).await
        )
    }

    async fn streams(&self, prefix: &str) -> Result<Vec<String>> {
        crate::purge::admitted!(self, {
            let handle = self.segments().await?;
            let present = handle
                .aggregate(
                    doc! {"s": prefix_regex(prefix)},
                    vec![doc! {"$group": {"_id": {"s": "$s", "g": "$g"}}}],
                )
                .await
                .map_err(errors::failed("list streams"))?;
            let heads = self
                .heads()
                .find(doc! {"n": prefix_regex(prefix)}, FindOptions::default())
                .await
                .map_err(errors::failed("list streams"))?;
            let generations: std::collections::HashMap<&str, i64> = heads
                .iter()
                .filter_map(|head| Some((head.get_str("n").ok()?, head.get_i64("g").ok()?)))
                .collect();
            let mut names: Vec<String> = present
                .iter()
                .filter_map(|group| {
                    let key = group.get_document("_id").ok()?;
                    let name = key.get_str("s").ok()?;
                    let generation = key.get_i64("g").unwrap_or(0);
                    (name.starts_with(prefix)
                        && generations.get(name).copied().unwrap_or(0) == generation)
                        .then(|| name.to_owned())
                })
                .collect();
            names.sort();
            names.dedup();
            Ok(names)
        })
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
