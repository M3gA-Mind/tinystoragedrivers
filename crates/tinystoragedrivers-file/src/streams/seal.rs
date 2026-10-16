//! Sealing a file stream: the three steps and the blob they share.
//!
//! In order, so that a crash between any two is recoverable:
//!
//! 1. [`FileStreams::seal_write_blob`] writes the compressed entries as the
//!    stream's blob. The meta file is untouched, so the data file is still the
//!    stream.
//! 2. [`FileStreams::seal_mark`] records `sealed_len` in the meta file,
//!    provided the stream is still what the blob was written from. From here
//!    on reads decode the blob.
//! 3. [`FileStreams::seal_drop_data`] removes the data file the blob replaces.
//!
//! [`FileStreams::seal_stream`] runs them in order, and on a stream that is
//! already sealed only finishes step 3, so sealing again completes what a
//! crash interrupted. Production sealing runs all three under one hold of the
//! database lock; tests run them one at a time.

use tinystoragedrivers_core::{
    Result, Scope, SealOutcome, SealedLog, StorageError, sealed_blob_key,
};

use super::{FileStreams, Paths, read_lines, read_meta, stream_len};
use crate::blobs::FileBlobs;
use crate::fsio::{remove_optional, write_json};
use crate::storage::Db;

/// The blob's `content_type`.
const CONTENT_TYPE: &str = "application/zstd";

fn missing_blob() -> StorageError {
    StorageError::backend("sealed stream has no blob")
}

/// The entries of a sealed stream, decoded from its blob.
pub(super) fn read_sealed(db: &Db, scope: &Scope, stream: &str) -> Result<SealedLog> {
    let bytes =
        FileBlobs::get_locked(db, scope, &sealed_blob_key(stream)?)?.ok_or_else(missing_blob)?;
    SealedLog::decode(&bytes)
}

/// Remove the sealed blob, if there is one.
pub(super) fn delete_blob(db: &Db, scope: &Scope, stream: &str) -> Result<()> {
    FileBlobs::delete_locked(db, scope, &sealed_blob_key(stream)?)?;
    Ok(())
}

/// Step 1.
fn write_blob(db: &Db, scope: &Scope, paths: &Paths, name: &str) -> Result<SealOutcome> {
    let meta =
        read_meta(paths, name)?.ok_or_else(|| StorageError::not_found("stream does not exist"))?;
    if meta.sealed_len.is_some() {
        return Err(StorageError::conflict("stream is already sealed"));
    }
    let len = stream_len(paths, &meta)?;
    let lines = read_lines(paths, meta.base, usize::MAX)?;
    let dense = lines.len() as u64 == len - meta.base
        && lines
            .iter()
            .zip(meta.base..)
            .all(|(entry, expected)| entry.offset == expected);
    if !dense {
        return Err(StorageError::backend(
            "stream file does not match its meta; refusing to seal",
        ));
    }
    let log = SealedLog::new(
        meta.base,
        lines.into_iter().map(|entry| entry.value).collect(),
    );
    let bytes = log.encode()?;
    FileBlobs::put_locked(db, scope, &sealed_blob_key(name)?, &bytes, CONTENT_TYPE)?;
    Ok(SealOutcome::new(log.base(), log.len(), bytes.len() as u64))
}

/// Step 2.
fn mark(db: &Db, scope: &Scope, paths: &Paths, name: &str, written: SealOutcome) -> Result<()> {
    let mut meta = read_meta(paths, name)?
        .ok_or_else(|| StorageError::conflict("stream was deleted while sealing"))?;
    if meta.base != written.base || stream_len(paths, &meta)? != written.len {
        return Err(StorageError::conflict("stream changed while sealing"));
    }
    let blob_len = FileBlobs::len_locked(db, scope, &sealed_blob_key(name)?)?;
    if blob_len != Some(written.blob_bytes) {
        return Err(StorageError::conflict("sealed blob changed while sealing"));
    }
    meta.sealed_len = Some(written.len);
    meta.sealed_ms = Some(i64::try_from(db.now()).unwrap_or(i64::MAX));
    write_json(&paths.meta, &meta)
}

/// Step 3: remove the data file of a sealed stream. Idempotent, and a no-op
/// for a stream that is not sealed.
fn drop_data(paths: &Paths, name: &str) -> Result<()> {
    if read_meta(paths, name)?.is_some_and(|meta| meta.sealed_len.is_some()) {
        remove_optional(&paths.data)?;
    }
    Ok(())
}

impl FileStreams {
    /// Seal `stream`; see [`StreamStore::seal`](tinystoragedrivers_core::StreamStore::seal).
    pub(super) async fn seal_stream(&self, stream: &str) -> Result<SealOutcome> {
        let name = stream.to_owned();
        self.write_db(stream, move |db, scope, paths| {
            if let Some(meta) = read_meta(paths, &name)?
                && let Some(len) = meta.sealed_len
            {
                let bytes = FileBlobs::len_locked(db, scope, &sealed_blob_key(&name)?)?
                    .ok_or_else(missing_blob)?;
                drop_data(paths, &name)?;
                return Ok(SealOutcome::new(meta.base, len, bytes));
            }
            let written = write_blob(db, scope, paths, &name)?;
            mark(db, scope, paths, &name, written)?;
            drop_data(paths, &name)?;
            Ok(written)
        })
        .await
    }

    /// Step 1 on its own, for tests that stop the protocol after it.
    #[cfg(test)]
    pub(super) async fn seal_write_blob(&self, stream: &str) -> Result<SealOutcome> {
        let name = stream.to_owned();
        self.write_db(stream, move |db, scope, paths| {
            write_blob(db, scope, paths, &name)
        })
        .await
    }

    /// Step 2 on its own, for tests that stop the protocol after it.
    #[cfg(test)]
    pub(super) async fn seal_mark(&self, stream: &str, written: SealOutcome) -> Result<()> {
        let name = stream.to_owned();
        self.write_db(stream, move |db, scope, paths| {
            mark(db, scope, paths, &name, written)
        })
        .await
    }
}
