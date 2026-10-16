//! Sealing a MongoDB stream: the three steps and the marker that records them.
//!
//! A sealed stream keeps its entries as one compressed blob (GridFS, under
//! [`sealed_blob_key`]). The steps run in the order that keeps a crash between
//! any two recoverable:
//!
//! 1. **Write the blob.** The stream's own segments are still the stream, so
//!    readers see nothing different.
//! 2. **Insert the seal marker**: an empty segment `{o: len, e: len, seal:
//!    true, sb: base}` at the stream's end. The unique `(_scope, s, g, o)`
//!    index that serializes appenders serializes the marker too, so an
//!    append racing the seal either lands first (the marker insert is refused
//!    and the seal reports a conflict) or loses to the marker and is refused
//!    as an append to a sealed stream. Since the marker is the last segment,
//!    every operation that already finds the stream's end also finds that it
//!    is sealed; there is no separate flag to keep in step. Reads of a sealed
//!    stream decode the blob.
//! 3. **Delete the entry segments** the blob replaces, keeping the marker.
//!
//! Sealing a stream that is already sealed only repeats step 3, so a seal
//! interrupted after step 2 is finished by sealing again.

use tinystoragedrivers_core::{
    Capability, Result, SealOutcome, SealedLog, StorageError, StreamEntry, sealed_blob_key,
    validate_stream,
};

use super::{MongoStreams, Segment, stored_offset};
use crate::convert::from_bson;
use crate::errors;
use crate::fence::in_write;
use crate::naming::STREAM_SEGMENT_INDEX;
use crate::scoped::ScopedCollection;
use futures_util::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::FindOptions;

/// The sealed blob's `content_type`.
const CONTENT_TYPE: &str = "application/zstd";

fn missing_blob() -> StorageError {
    StorageError::backend("sealed stream has no blob")
}

impl MongoStreams {
    /// The entries of sealed stream `stream`, decoded from its blob.
    pub(super) async fn sealed_log(&self, stream: &str) -> Result<SealedLog> {
        use tinystoragedrivers_core::BlobStore;
        let blob = self
            .blobs()
            .get(&sealed_blob_key(stream)?)
            .await?
            .ok_or_else(missing_blob)?;
        SealedLog::decode(&blob.bytes)
    }

    /// The window `[from, from + limit)` of sealed stream `stream`.
    pub(super) async fn sealed_window(
        &self,
        stream: &str,
        from: u64,
        limit: usize,
    ) -> Result<Vec<StreamEntry>> {
        Ok(self.sealed_log(stream).await?.window(from, limit))
    }

    /// Remove the sealed blob of `stream`, if there is one. Called after a
    /// delete, so a name that is reused starts with no stale blob.
    pub(super) async fn delete_sealed_blob(&self, stream: &str) -> Result<()> {
        use tinystoragedrivers_core::BlobStore;
        if self.shared.existing {
            // A stream here cannot be sealed, and a delete must not create
            // the GridFS collections.
            return Ok(());
        }
        self.blobs().delete(&sealed_blob_key(stream)?).await?;
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
                self.hold_admission(&mut *session).await
            })?;
        }
        let handle = self.segments().await?;
        let generation = self.generation(stream, None).await?;
        let last = self
            .edge(&handle, stream, generation, -1, None)
            .await?
            .ok_or_else(|| StorageError::not_found("stream does not exist"))?;
        if let Some(base) = last.sealed_base {
            self.seal_drop_segments(&handle, stream, generation).await?;
            return self.sealed_outcome(stream, base, last.end).await;
        }
        let (log, last_append_ms) = self.seal_collect(&handle, stream, generation).await?;
        let bytes = log.encode()?;
        let outcome = SealOutcome::new(log.base(), log.len(), bytes.len() as u64);
        {
            use tinystoragedrivers_core::BlobStore;
            self.blobs()
                .put(&sealed_blob_key(stream)?, bytes, Some(CONTENT_TYPE))
                .await?;
        }
        self.seal_mark(&handle, stream, generation, outcome, last_append_ms)
            .await?;
        self.seal_drop_segments(&handle, stream, generation).await?;
        Ok(outcome)
    }

    /// Step 1's input: every retained entry, checked to be dense.
    async fn seal_collect(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
    ) -> Result<(SealedLog, Option<i64>)> {
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
            SealedLog::new(base, entries.into_iter().map(|(_, value)| value).collect()),
            last_append_ms,
        ))
    }

    /// Step 2: insert the marker at the stream's end, provided the stream is
    /// still the one the blob was written from.
    async fn seal_mark(
        &self,
        handle: &ScopedCollection,
        stream: &str,
        generation: i64,
        written: SealOutcome,
        last_append_ms: Option<i64>,
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
        if let Some(last) = last_append_ms {
            marker.insert("la", last);
        }
        if self.shared.physical_transactions {
            // A fenced handle must hold its fence for the one write that makes
            // the stream sealed, so a stale holder is refused here.
            self.shared.prepare_validation(Some(&self.scope)).await?;
            in_write!(&self.shared, self.fence.as_deref(), session => {
                self.hold_admission(&mut *session).await?;
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
        match handle.insert_one(marker, None).await {
            Ok(()) => {}
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
            }
            Err(error) => return Err(errors::map(error, "seal a stream")),
        }
        self.seal_generation_unchanged(stream, generation).await
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
    async fn sealed_outcome(&self, stream: &str, base: u64, len: u64) -> Result<SealOutcome> {
        use tinystoragedrivers_core::BlobStore;
        let meta = self
            .blobs()
            .head(&sealed_blob_key(stream)?)
            .await?
            .ok_or_else(missing_blob)?;
        Ok(SealOutcome::new(base, len, meta.len))
    }
}
