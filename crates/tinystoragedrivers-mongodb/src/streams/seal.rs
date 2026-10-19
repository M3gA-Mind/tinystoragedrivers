//! Sealing a MongoDB stream: the three steps and the marker that records them.
//!
//! A sealed stream keeps its entries as one compressed blob (GridFS, under
//! [`sealed_blob_key`] plus `/g<generation>`, so a blob belongs to one
//! incarnation of the stream). The steps run in the order that keeps a crash between
//! any two recoverable:
//!
//! 1. **Write the blob.** The stream's own segments are still the stream, so
//!    readers see nothing different.
//! 2. **Insert the seal marker**, provided no truncation completed since the
//!    snapshot (below), as an empty segment `{o: len, e: len, seal:
//!    true, sb: base}` at the stream's end. The unique `(_scope, s, g, o)`
//!    index that serializes appenders serializes the marker too, so an
//!    append racing the seal either lands first (the marker insert is refused
//!    and the seal reports a conflict) or loses to the marker and is refused
//!    as an append to a sealed stream. Since the marker is the last segment,
//!    every operation that already finds the stream's end also finds that it
//!    is sealed; there is no separate flag to keep in step. Reads of a sealed
//!    stream decode the blob.
//!
//!    **Truncation.** A truncation edits or deletes earlier segments, so it
//!    never collides with the marker's key. It counts itself on the stream head
//!    (`truncate_epoch`) instead, and the snapshot records that count. With
//!    transactions the marker's transaction holds the head at the recorded
//!    count (a write that conflicts with a concurrent truncation's), so a
//!    truncation completed after the snapshot refuses the marker. Without
//!    them the marker is checked against the count right after the insert and
//!    removed, with a `Conflict`, when it moved; a truncation that finishes
//!    after the marker finds the stream sealed and is refused too. Either way
//!    the blob never outlives an acknowledged truncation.
//! 3. **Delete the entry segments** the blob replaces, keeping the marker.
//!
//! Sealing a stream that is already sealed only repeats step 3, so a seal
//! interrupted after step 2 is finished by sealing again.

use tinystoragedrivers_core::{
    Capability, ErrorKind, Result, SealOutcome, SealedLog, StorageError, StreamEntry,
    sealed_blob_key, validate_stream,
};

use super::{MongoStreams, Segment, head_id, stored_offset};
use crate::convert::from_bson;
use crate::errors;
use crate::fence::in_write;
use crate::naming::STREAM_SEGMENT_INDEX;
use crate::scoped::ScopedCollection;
use futures_util::TryStreamExt;
use mongodb::ClientSession;
use mongodb::bson::doc;
use mongodb::options::FindOptions;

/// How many snapshots a seal takes while appends keep landing between its
/// snapshot and its marker, before it reports the conflict.
const SEAL_ATTEMPTS: usize = 8;

/// The sealed blob's `content_type`.
const CONTENT_TYPE: &str = "application/zstd";

/// What a seal's snapshot was taken from, carried to its marker.
#[derive(Debug, Clone, Copy)]
pub(super) struct SealBasis {
    /// The `at_ms` of the last entry segment, kept on the marker (`la`).
    pub(super) last_append_ms: Option<i64>,
    /// The head's `truncate_epoch` before the snapshot was read.
    pub(super) truncations: i64,
}

fn missing_blob() -> StorageError {
    StorageError::backend("sealed stream has no blob")
}

impl MongoStreams {
    /// The entries of sealed stream `stream`, decoded from its blob.
    pub(super) async fn sealed_log(&self, stream: &str, generation: i64) -> Result<SealedLog> {
        use tinystoragedrivers_core::BlobStore;
        let blob = self
            .blobs()
            .get(&Self::generation_blob_key(stream, generation)?)
            .await?
            .ok_or_else(missing_blob)?;
        SealedLog::decode(&blob.bytes)
    }

    /// The window `[from, from + limit)` of sealed stream `stream`.
    pub(super) async fn sealed_window(
        &self,
        stream: &str,
        generation: i64,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StreamEntry>> {
        Ok(self
            .sealed_log(stream, generation)
            .await?
            .window(from, limit))
    }

    /// The blob holding sealed `stream` of `generation`.
    ///
    /// The key carries the generation so a seal that stalled across a delete
    /// and recreate writes (and later removes) only its own incarnation's
    /// blob, never the one the recreated stream is sealed with:
    /// `<sealed_blob_key>/g<generation>`. A stream name's segment never holds
    /// a raw `/`, so no other stream's key can collide with it.
    fn generation_blob_key(stream: &str, generation: i64) -> Result<String> {
        Ok(format!("{}/g{generation}", sealed_blob_key(stream)?))
    }

    /// Remove the blobs of `stream` from generations before the current one:
    /// the blobs of deleted incarnations, and any a stalled seal published
    /// after the delete.
    pub(super) async fn purge_stale_sealed_blobs(&self, stream: &str) -> Result<()> {
        use tinystoragedrivers_core::BlobStore;
        if self.shared.existing {
            // A stream here cannot be sealed, and a delete must not create
            // the GridFS collections.
            return Ok(());
        }
        let current = self.generation(stream, None).await?;
        let prefix = format!("{}/g", sealed_blob_key(stream)?);
        let blobs = self.blobs();
        for meta in blobs.list(&prefix).await? {
            let stale = meta
                .key
                .strip_prefix(&prefix)
                .and_then(|rest| rest.parse::<i64>().ok())
                .is_some_and(|generation| generation < current);
            if stale {
                blobs.delete(&meta.key).await?;
            }
        }
        Ok(())
    }

    /// Seal `stream`; see [`StreamStore::seal`](tinystoragedrivers_core::StreamStore::seal).
    pub(super) async fn seal_stream(&self, stream: &str) -> Result<SealOutcome> {
        validate_stream(stream)?;
        if self.shared.existing {
            return Err(StorageError::unsupported(
                Capability::ExistingSchema,
                "sealing writes a GridFS blob, which may initialize schema",
            ));
        }
        if self.fence.is_some() && self.shared.physical_transactions {
            // A stale holder is refused before anything is read or written,
            // whether or not the stream exists.
            self.shared.prepare_validation(Some(&self.scope)).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(&mut *session).await
            })?;
        }
        let handle = self.segments().await?;
        // An append that lands between the snapshot and the marker makes the
        // marker insert (or the check) report a conflict: the snapshot is stale,
        // not the seal wrong, so take a fresh one. The append is never lost
        // (the marker is refused) and never silently outside the blob.
        for attempt in 1..=SEAL_ATTEMPTS {
            match self.seal_attempt(&handle, stream).await {
                Err(error) if error.kind() == ErrorKind::Conflict && attempt < SEAL_ATTEMPTS => {}
                done => return done,
            }
        }
        unreachable!("the last attempt returns")
    }

    /// One snapshot, publish and mark of `stream`.
    async fn seal_attempt(&self, handle: &ScopedCollection, stream: &str) -> Result<SealOutcome> {
        let generation = self.generation(stream, None).await?;
        let last = self
            .edge(handle, stream, generation, -1, None)
            .await?
            .ok_or_else(|| StorageError::not_found("stream does not exist"))?;
        if let Some(base) = last.sealed_base {
            self.seal_drop_segments(handle, stream, generation).await?;
            return self
                .sealed_outcome(stream, generation, base, last.end)
                .await;
        }
        let (log, basis) = self.seal_collect(handle, stream, generation).await?;
        let outcome = self.seal_publish(stream, generation, &log).await?;
        let marked = self
            .seal_mark(handle, stream, generation, outcome, basis)
            .await;
        if let Err(error) = marked {
            // A stream deleted meanwhile leaves this seal's blob under a
            // retired generation: remove it.
            self.purge_stale_sealed_blobs(stream).await?;
            return Err(error);
        }
        self.seal_drop_segments(handle, stream, generation).await?;
        Ok(outcome)
    }

    /// Step 1's output: write `log` as the blob of `stream` in `generation`.
    async fn seal_publish(
        &self,
        stream: &str,
        generation: i64,
        log: &SealedLog,
    ) -> Result<SealOutcome> {
        use tinystoragedrivers_core::BlobStore;
        let bytes = log.encode()?;
        let outcome = SealOutcome::new(log.base(), log.len(), bytes.len() as u64);
        self.blobs()
            .put(
                &Self::generation_blob_key(stream, generation)?,
                bytes,
                Some(CONTENT_TYPE),
            )
            .await?;
        Ok(outcome)
    }

    /// Step 1's input: every retained entry, checked to be dense.
    async fn seal_collect(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
    ) -> Result<(SealedLog, SealBasis)> {
        // Before the first read: a truncation that completes after this
        // point moves the count past the one the marker is conditioned on.
        let truncations = self.truncate_epoch(stream, None).await?;
        let mut cursor = handle
            .cursor(
                doc! {"s": stream, "g": generation},
                FindOptions::builder().sort(doc! {"o": 1}).build(),
            )
            .await
            .map_err(errors::failed("read a stream"))?;
        let mut entries = Vec::new();
        let mut end = 0;
        let mut last_append_ms = None;
        while let Some(found) = cursor
            .try_next()
            .await
            .map_err(errors::failed("read a stream"))?
        {
            let segment = Segment::decode(&found)?;
            if segment.sealed_base.is_some() {
                return Err(StorageError::conflict("stream was sealed concurrently"));
            }
            end = segment.end;
            last_append_ms = segment.at_ms;
            for (offset, value) in segment.entries_from(0) {
                entries.push((offset, from_bson(value)?));
            }
        }
        let base = entries.first().map_or(end, |(offset, _)| *offset);
        let dense = entries
            .iter()
            .zip(base..)
            .all(|((offset, _), expected)| *offset == expected)
            && base + entries.len() as u64 == end;
        if !dense {
            return Err(StorageError::backend(
                "stream segments do not tile the stream; refusing to seal",
            ));
        }
        Ok((
            SealedLog::new(base, entries.into_iter().map(|(_, value)| value).collect())?,
            SealBasis {
                last_append_ms,
                truncations,
            },
        ))
    }

    /// Step 2: insert the marker at the stream's end, provided the stream is
    /// still the one the blob was written from: no append took the end, and no
    /// truncation completed since the snapshot (`basis`).
    async fn seal_mark(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
        written: SealOutcome,
        basis: SealBasis,
    ) -> Result<()> {
        let mut marker = doc! {
            "s": stream,
            "g": generation,
            "o": stored_offset(written.len)?,
            "n": 0_i64,
            "e": stored_offset(written.len)?,
            "t": 0_i64,
            "vs": [],
            "seal": true,
            "sb": stored_offset(written.base)?,
            "ms": self.now_ms(),
        };
        if let Some(last) = basis.last_append_ms {
            marker.insert("la", last);
        }
        if self.shared.physical_transactions {
            // A fenced handle must hold its fence for the one write that makes
            // the stream sealed, so a stale holder is refused here.
            self.shared.prepare_validation(Some(&self.scope)).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(&mut *session).await?;
                self.hold_truncations(stream, basis.truncations, &mut *session).await?;
                match handle.insert_one(marker.clone(), Some(session)).await {
                    Ok(()) => Ok(()),
                    Err(error) if errors::duplicate_index(&error).as_deref() == Some(STREAM_SEGMENT_INDEX) => {
                        Err(StorageError::conflict("stream changed while sealing"))
                    }
                    Err(error) => Err(errors::map(error, "seal a stream")),
                }
            })?;
            return self.seal_generation_unchanged(stream, generation).await;
        }
        let inserted = match handle.insert_one(marker, None).await {
            Ok(()) => true,
            Err(error)
                if errors::duplicate_index(&error).as_deref() == Some(STREAM_SEGMENT_INDEX) =>
            {
                // Either an appender took the offset, or another sealer of
                // the same entries placed the identical marker.
                let last = self.edge(handle, stream, generation, -1, None).await?;
                let same = last.is_some_and(|last| {
                    last.sealed_base == Some(written.base) && last.end == written.len
                });
                if !same {
                    return Err(StorageError::conflict("stream changed while sealing"));
                }
                false
            }
            Err(error) => return Err(errors::map(error, "seal a stream")),
        };
        // The insert cannot be conditioned on the head here, so check it
        // after: a truncation that completed since the snapshot leaves the
        // marker publishing entries it cut, and the marker is taken back
        // before the entry segments are dropped (a sealer that did not insert
        // refuses too, so none of them drops what the owner may take back).
        if self.truncate_epoch(stream, None).await? != basis.truncations {
            if inserted {
                handle
                    .delete_many(
                        doc! {"s": stream, "g": generation, "o": stored_offset(written.len)?, "seal": true},
                        None,
                    )
                    .await
                    .map_err(errors::failed("seal a stream"))?;
            }
            return Err(StorageError::conflict("stream was truncated while sealing"));
        }
        self.seal_generation_unchanged(stream, generation).await
    }

    /// Hold the head at the snapshot's truncation count for the rest of the
    /// transaction: the write conflicts with a concurrent truncation's, and
    /// the filter refuses one that already completed.
    async fn hold_truncations(
        &self,
        stream: &str,
        expected: i64,
        session: &mut ClientSession,
    ) -> Result<()> {
        let unchanged = if expected == 0 {
            doc! {"$not": {"$gt": 0_i64}}
        } else {
            doc! {"$eq": expected}
        };
        let held = self
            .heads()
            .update_one(
                doc! {"_id": head_id(&self.scope, stream), "n": stream, "truncate_epoch": unchanged},
                doc! {"$inc": {"seal_epoch": 1_i64}, "$max": {"g": 0_i64}},
                expected == 0,
                Some(session),
            )
            .await;
        match held {
            Ok(result) if result.matched_count > 0 || result.upserted_id.is_some() => Ok(()),
            Ok(_) => Err(StorageError::conflict("stream was truncated while sealing")),
            // The head exists at another count: an upsert met its own key.
            Err(error) if errors::duplicate_index(&error).is_some() => {
                Err(StorageError::conflict("stream was truncated while sealing"))
            }
            Err(error) => Err(errors::map(error, "seal a stream")),
        }
    }

    /// Delete `stream` only while it is the incarnation sealed at `sealed_ms`;
    /// see [`StreamStore::delete_sealed_stream`](tinystoragedrivers_core::StreamStore::delete_sealed_stream).
    ///
    /// The incarnation is its generation: the seal marker is read from the
    /// current generation, and the generation is then advanced with a
    /// compare-and-swap on the value read (inside the write transaction when
    /// the deployment has them). A stream deleted and recreated in between
    /// has a newer generation, so the swap matches nothing and the new
    /// incarnation stays.
    pub(super) async fn delete_sealed_stream_impl(
        &self,
        stream: &str,
        sealed_ms: i64,
    ) -> Result<bool> {
        validate_stream(stream)?;
        let handle = self.segments().await?;
        let deleted = if self.shared.physical_transactions {
            self.shared.prepare_validation(Some(&self.scope)).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_validation(&mut *session).await?;
                self.delete_sealed_in(&handle, stream, sealed_ms, Some(session)).await
            })?
        } else {
            self.delete_sealed_in(&handle, stream, sealed_ms, None)
                .await?
        };
        if deleted {
            self.purge_stale_sealed_blobs(stream).await?;
        }
        Ok(deleted)
    }

    async fn delete_sealed_in(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        sealed_ms: i64,
        mut session: Option<&mut ClientSession>,
    ) -> Result<bool> {
        let current = self.generation(stream, session.as_deref_mut()).await?;
        let Some(last) = self
            .edge(handle, stream, current, -1, session.as_deref_mut())
            .await?
        else {
            return Ok(false);
        };
        if last.sealed_base.is_none() || last.at_ms != Some(sealed_ms) {
            return Ok(false);
        }
        // Advance only from the generation the marker was read in. A head
        // that exists holds a positive generation, so only a missing one
        // (generation 0) is created by the upsert.
        let advanced = self
            .heads()
            .update_one(
                doc! {"_id": head_id(&self.scope, stream), "n": stream, "g": current},
                doc! {"$inc": {"g": 1_i64}},
                current == 0,
                session.as_deref_mut(),
            )
            .await;
        match advanced {
            Ok(result) if result.matched_count > 0 || result.upserted_id.is_some() => {}
            Ok(_) => return Ok(false),
            Err(error) if errors::duplicate_index(&error).is_some() => return Ok(false),
            Err(error) => return Err(errors::map(error, "delete a sealed stream")),
        }
        handle
            .delete_many(doc! {"s": stream, "g": {"$lte": current}}, session)
            .await
            .map_err(errors::failed("delete a sealed stream"))?;
        Ok(true)
    }

    /// Refuse to report a seal of a stream that was deleted meanwhile: the
    /// marker then sits in a retired generation no reader looks at.
    async fn seal_generation_unchanged(&self, stream: &str, generation: i64) -> Result<()> {
        if self.generation(stream, None).await? != generation {
            return Err(StorageError::conflict("stream was deleted while sealing"));
        }
        Ok(())
    }

    /// Step 3: delete the entry segments, keeping the marker. Idempotent.
    async fn seal_drop_segments(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
    ) -> Result<()> {
        handle
            .delete_many(
                doc! {"s": stream, "g": generation, "seal": {"$ne": true}},
                None,
            )
            .await
            .map_err(errors::failed("seal a stream"))?;
        Ok(())
    }

    /// The outcome of an already sealed stream, sized from its blob.
    async fn sealed_outcome(
        &self,
        stream: &str,
        generation: i64,
        base: u64,
        len: u64,
    ) -> Result<SealOutcome> {
        use tinystoragedrivers_core::BlobStore;
        let meta = self
            .blobs()
            .head(&Self::generation_blob_key(stream, generation)?)
            .await?
            .ok_or_else(missing_blob)?;
        Ok(SealOutcome::new(base, len, meta.len))
    }
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
