//! Sealing a file stream into its legacy sealed-blob path.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tinystoragedrivers_types::{Result, Scope, SealOutcome, StorageError};

use super::{FileStreams, Paths, read_lines, read_meta, stream_len};
use crate::file::encode::file_stem;
use crate::file::fsio::{read_json, read_optional, remove_optional, write_atomic, write_json};
use crate::sealed_blob_key;
use crate::stream::SealedLog;

const CONTENT_TYPE: &str = "application/zstd";
const META_SUFFIX: &str = ".meta.json";

#[derive(Debug, Serialize, Deserialize)]
struct BlobSidecar {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at_ms: Option<u64>,
}

fn blob_dir(root: &Path, scope: &Scope) -> PathBuf {
    crate::file::scope_dir(root, scope.as_str()).join("blobs")
}

fn blob_paths(root: &Path, scope: &Scope, key: &str) -> (PathBuf, PathBuf) {
    let dir = blob_dir(root, scope);
    let stem = file_stem(key);
    (dir.join(&stem), dir.join(format!("{stem}{META_SUFFIX}")))
}

fn read_blob(root: &Path, scope: &Scope, key: &str) -> Result<Option<Vec<u8>>> {
    let (data, meta) = blob_paths(root, scope, key);
    match read_json::<BlobSidecar>(&meta)? {
        Some(sidecar) if sidecar.key == key && sidecar.expires_at_ms.is_none() => {
            read_optional(&data)
        }
        Some(sidecar) if sidecar.key == key => Ok(None),
        Some(_) => Err(StorageError::backend(
            "file storage name hash collision; refusing to overwrite another entry",
        )),
        None => Ok(None),
    }
}

fn write_blob(root: &Path, scope: &Scope, paths: &Paths, name: &str) -> Result<SealOutcome> {
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
    )?;
    let bytes = log.encode()?;
    let key = sealed_blob_key(name)?;
    let (data, sidecar) = blob_paths(root, scope, &key);
    write_atomic(&data, &bytes)?;
    write_json(
        &sidecar,
        &BlobSidecar {
            key,
            content_type: Some(CONTENT_TYPE.to_owned()),
            expires_at_ms: None,
        },
    )?;
    Ok(SealOutcome::new(log.base(), log.len(), bytes.len() as u64))
}

fn mark(
    root: &Path,
    scope: &Scope,
    paths: &Paths,
    name: &str,
    written: SealOutcome,
    now_ms: i64,
) -> Result<()> {
    let mut meta = read_meta(paths, name)?
        .ok_or_else(|| StorageError::conflict("stream was deleted while sealing"))?;
    if meta.base != written.base || stream_len(paths, &meta)? != written.len {
        return Err(StorageError::conflict("stream changed while sealing"));
    }
    let actual =
        read_blob(root, scope, &sealed_blob_key(name)?)?.map_or(0, |bytes| bytes.len() as u64);
    if actual != written.blob_bytes {
        return Err(StorageError::conflict("sealed blob changed while sealing"));
    }
    meta.sealed_len = Some(written.len);
    meta.sealed_ms = Some(now_ms);
    write_json(&paths.meta, &meta)
}

impl FileStreams {
    pub(super) async fn seal_stream(&self, stream: &str) -> Result<SealOutcome> {
        let name = stream.to_owned();
        let now_ms = i64::try_from((self.connection.clock())()).unwrap_or(i64::MAX);
        self.write_db(stream, move |root, scope, paths| {
            if let Some(meta) = read_meta(paths, &name)?
                && let Some(len) = meta.sealed_len
            {
                let bytes = read_blob(root, scope, &sealed_blob_key(&name)?)?
                    .ok_or_else(|| StorageError::backend("sealed stream has no blob"))?;
                remove_optional(&paths.data)?;
                return Ok(SealOutcome::new(meta.base, len, bytes.len() as u64));
            }
            let written = write_blob(root, scope, paths, &name)?;
            mark(root, scope, paths, &name, written, now_ms)?;
            remove_optional(&paths.data)?;
            Ok(written)
        })
        .await
    }
}

pub(super) fn read_sealed(root: &Path, scope: &Scope, stream: &str) -> Result<SealedLog> {
    let bytes = read_blob(root, scope, &sealed_blob_key(stream)?)?
        .ok_or_else(|| StorageError::backend("sealed stream has no blob"))?;
    SealedLog::decode(&bytes)
}

pub(super) fn delete_blob(root: &Path, scope: &Scope, stream: &str) -> Result<()> {
    let (data, sidecar) = blob_paths(root, scope, &sealed_blob_key(stream)?);
    remove_optional(&sidecar)?;
    remove_optional(&data)?;
    Ok(())
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
