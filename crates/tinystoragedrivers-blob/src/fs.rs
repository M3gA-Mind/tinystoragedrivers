//! Plain-file blob engine preserving the v1 data and sidecar layout.

use std::{
    fmt::Write as FmtWrite,
    fs,
    io::Write,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tinystoragedrivers_api::BlobStore;
use tinystoragedrivers_connect::FileConnection;
use tinystoragedrivers_types::{
    Blob, BlobListEntry, BlobListPage, BlobMeta, Capabilities, Capability,
    MAX_BLOB_LIST_PAGE_ITEMS, Result, Scope, StorageError, is_reserved_blob_key,
};

use crate::key::{MAX_KEY_LEN, clamp_range, validate_key, validate_prefix, validate_sealed_key};

const META_SUFFIX: &str = ".meta.json";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sidecar {
    key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    written_at_ms: Option<u64>,
}

struct StoreOptions {
    content_type: Option<String>,
    ttl: Option<Duration>,
    allow_sealed: bool,
}

/// A scope-bound file blob engine using raw bytes and JSON sidecars.
#[derive(Debug, Clone)]
pub struct FileBlobStore {
    connection: FileConnection,
    scope: Scope,
}

impl FileBlobStore {
    /// Opens the blob directory rooted at `root` for one scope.
    ///
    /// # Errors
    /// Returns a backend error when the directory cannot be created.
    pub fn open(root: impl AsRef<Path>, scope: Scope) -> Result<Self> {
        Ok(Self::from_connection(FileConnection::open(root)?, scope))
    }

    /// Opens the blob directory with an injected Unix-millisecond clock.
    ///
    /// # Errors
    /// Returns a backend error when the directory cannot be created.
    pub fn open_with_clock(
        root: impl AsRef<Path>,
        scope: Scope,
        clock: fn() -> u64,
    ) -> Result<Self> {
        Ok(Self::from_connection(
            FileConnection::open_with_clock(root, Arc::new(clock))?,
            scope,
        ))
    }

    /// Bind a shared file connection to one scope.
    #[must_use]
    pub fn from_connection(connection: FileConnection, scope: Scope) -> Self {
        Self { connection, scope }
    }

    fn base(root: &Path, scope: &Scope) -> PathBuf {
        scope_dir(root, scope).join("blobs")
    }

    fn paths(root: &Path, scope: &Scope, key: &str) -> (PathBuf, PathBuf) {
        let stem = file_stem(key);
        let base = Self::base(root, scope);
        (base.join(&stem), base.join(format!("{stem}{META_SUFFIX}")))
    }

    fn sidecar(path: &Path, key: &str) -> Result<Option<Sidecar>> {
        let Some(sidecar) = read_sidecar(path)? else {
            return Ok(None);
        };
        if sidecar.key != key {
            return Err(StorageError::backend("file storage name hash collision"));
        }
        Ok(Some(sidecar))
    }

    fn read_meta(
        data: &Path,
        path: &Path,
        key: &str,
        now: u64,
        include_expired: bool,
    ) -> Result<Option<(BlobMeta, Sidecar)>> {
        let Some(sidecar) = Self::sidecar(path, key)? else {
            return Ok(None);
        };
        if !include_expired && sidecar.expires_at_ms.is_some_and(|expires| expires <= now) {
            return Ok(None);
        }
        let metadata = match fs::symlink_metadata(data) {
            Ok(metadata) if metadata.file_type().is_file() => metadata,
            Ok(_) => return Err(StorageError::backend("blob path is not a regular file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(error)),
        };
        let meta = BlobMeta {
            key: key.to_owned(),
            len: metadata.len(),
            content_type: sidecar.content_type.clone(),
        };
        Ok(Some((meta, sidecar)))
    }
}

#[async_trait]
impl BlobStore for FileBlobStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Blobs)
            .with(Capability::BlobTtl)
            .with(Capability::BlobListing)
    }
    fn max_key_len(&self) -> usize {
        MAX_KEY_LEN
    }
    fn supports_sealed_blobs(&self) -> bool {
        true
    }

    async fn put(&self, key: &str, bytes: Vec<u8>, content_type: Option<&str>) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, false).await
    }
    async fn put_expiring(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Duration,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, Some(ttl), false).await
    }
    async fn get(&self, key: &str) -> Result<Option<Blob>> {
        self.read_key(key, false).await
    }
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        validate_key(key)?;
        clamp_range(0, range.start, range.end)?;
        let Some(blob) = self.get(key).await? else {
            return Ok(None);
        };
        Ok(Some(
            blob.bytes[clamp_range(blob.bytes.len(), range.start, range.end)?].to_vec(),
        ))
    }
    async fn head(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.head_key(key, false).await
    }
    async fn delete(&self, key: &str) -> Result<bool> {
        self.delete_key(key, false).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_prefix(prefix, None)?;
        let prefix = prefix.to_owned();
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                let mut items = Vec::new();
                for (key, data, metadata) in Self::scan(root, &scope)? {
                    if key.starts_with(&prefix)
                        && !is_reserved_blob_key(&key)
                        && let Some((meta, _)) =
                            Self::read_meta(&data, &metadata, &key, clock(), false)?
                    {
                        items.push(meta);
                    }
                }
                items.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
                Ok(items)
            })
            .await
    }
    async fn list_page(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<BlobListPage> {
        if limit == 0 || limit > MAX_BLOB_LIST_PAGE_ITEMS {
            return Err(StorageError::invalid_input(
                "invalid blob listing page size",
            ));
        }
        validate_prefix(prefix, after)?;
        let prefix = prefix.to_owned();
        let after = after.map(str::to_owned);
        let scope = self.scope.clone();
        self.connection
            .run(move |root| {
                let mut entries: Vec<_> = Self::scan(root, &scope)?
                    .into_iter()
                    .filter(|(key, _, _)| {
                        key.starts_with(&prefix)
                            && !is_reserved_blob_key(key)
                            && after
                                .as_deref()
                                .is_none_or(|cursor| key.as_bytes() > cursor.as_bytes())
                    })
                    .collect();
                entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                let more = entries.len() > limit;
                entries.truncate(limit);
                let mut items = Vec::with_capacity(entries.len());
                for (key, data, path) in entries {
                    if let Some((meta, sidecar)) = Self::read_meta(&data, &path, &key, 0, true)? {
                        items.push(BlobListEntry {
                            meta,
                            written_at_ms: sidecar.written_at_ms,
                        });
                    }
                }
                let next_after = if more {
                    items.last().map(|entry| entry.meta.key.clone())
                } else {
                    None
                };
                Ok(BlobListPage { items, next_after })
            })
            .await
    }

    async fn put_sealed(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<BlobMeta> {
        self.store(key, bytes, content_type, None, true).await
    }
    async fn get_sealed(&self, key: &str) -> Result<Option<Blob>> {
        self.read_key(key, true).await
    }
    async fn head_sealed(&self, key: &str) -> Result<Option<BlobMeta>> {
        self.head_key(key, true).await
    }
    async fn delete_sealed(&self, key: &str) -> Result<bool> {
        self.delete_key(key, true).await
    }
    async fn list_sealed(&self, prefix: &str) -> Result<Vec<BlobMeta>> {
        validate_sealed_key(prefix)?;
        let prefix = prefix.to_owned();
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                let mut items = Vec::new();
                for (key, data, metadata) in Self::scan(root, &scope)? {
                    if is_reserved_blob_key(&key)
                        && key.starts_with(&prefix)
                        && let Some((meta, _)) =
                            Self::read_meta(&data, &metadata, &key, clock(), false)?
                    {
                        items.push(meta);
                    }
                }
                items.sort_by(|a, b| a.key.as_bytes().cmp(b.key.as_bytes()));
                Ok(items)
            })
            .await
    }

    async fn sweep_expired(&self) -> Result<u64> {
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                let now = clock();
                let mut swept = 0;
                for (key, data, metadata) in Self::scan(root, &scope)? {
                    if Self::sidecar(&metadata, &key)?
                        .and_then(|sidecar| sidecar.expires_at_ms)
                        .is_some_and(|expires| expires <= now)
                    {
                        remove_if_exists(&data)?;
                        remove_if_exists(&metadata)?;
                        swept += 1;
                    }
                }
                Ok(swept)
            })
            .await
    }
}

impl FileBlobStore {
    async fn read_key(&self, key: &str, allow_sealed: bool) -> Result<Option<Blob>> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let key = key.to_owned();
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                let (data, metadata) = Self::paths(root, &scope, &key);
                let Some((meta, _)) = Self::read_meta(&data, &metadata, &key, clock(), false)?
                else {
                    return Ok(None);
                };
                Ok(Some(Blob {
                    meta,
                    bytes: read_regular(&data)?,
                }))
            })
            .await
    }

    async fn head_key(&self, key: &str, allow_sealed: bool) -> Result<Option<BlobMeta>> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let key = key.to_owned();
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                let (data, metadata) = Self::paths(root, &scope, &key);
                Ok(Self::read_meta(&data, &metadata, &key, clock(), false)?.map(|(meta, _)| meta))
            })
            .await
    }

    async fn delete_key(&self, key: &str, allow_sealed: bool) -> Result<bool> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let key = key.to_owned();
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                let (data, metadata) = Self::paths(root, &scope, &key);
                let existed = Self::read_meta(&data, &metadata, &key, clock(), false)?.is_some();
                remove_if_exists(&metadata)?;
                remove_if_exists(&data)?;
                Ok(existed)
            })
            .await
    }

    async fn store(
        &self,
        key: &str,
        bytes: Vec<u8>,
        content_type: Option<&str>,
        ttl: Option<Duration>,
        allow_sealed: bool,
    ) -> Result<BlobMeta> {
        if allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let key = key.to_owned();
        let content_type = content_type.map(str::to_owned);
        let scope = self.scope.clone();
        let clock = self.connection.clock();
        self.connection
            .run(move |root| {
                Self::store_at(
                    root,
                    &scope,
                    clock(),
                    &key,
                    &bytes,
                    StoreOptions {
                        content_type,
                        ttl,
                        allow_sealed,
                    },
                )
            })
            .await
    }

    fn store_at(
        root: &Path,
        scope: &Scope,
        now: u64,
        key: &str,
        bytes: &[u8],
        options: StoreOptions,
    ) -> Result<BlobMeta> {
        if options.allow_sealed {
            validate_sealed_key(key)?;
        } else {
            validate_key(key)?;
        }
        let base = Self::base(root, scope);
        let (data, metadata) = Self::paths(root, scope, key);
        fs::create_dir_all(&base).map_err(io_error)?;
        if Self::sidecar(&metadata, key)?.is_none() && metadata.exists() {
            return Err(StorageError::backend("file storage name hash collision"));
        }
        write_atomic(&data, bytes)?;
        let sidecar = Sidecar {
            key: key.to_owned(),
            content_type: options.content_type,
            expires_at_ms: options
                .ttl
                .map(|ttl| now.saturating_add(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))),
            written_at_ms: Some(now),
        };
        let json = serde_json::to_vec(&sidecar).map_err(StorageError::from)?;
        write_atomic(&metadata, &json)?;
        Ok(BlobMeta {
            key: key.to_owned(),
            len: bytes.len() as u64,
            content_type: sidecar.content_type,
        })
    }

    fn scan(root: &Path, scope: &Scope) -> Result<Vec<(String, PathBuf, PathBuf)>> {
        let directory = Self::base(root, scope);
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(io_error(error)),
        };
        let mut found = Vec::new();
        for entry in entries {
            let entry = entry.map_err(io_error)?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json")
                || !path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(META_SUFFIX))
            {
                continue;
            }
            if let Some(sidecar) = read_sidecar(&path)? {
                found.push((
                    sidecar.key.clone(),
                    directory.join(file_stem(&sidecar.key)),
                    path,
                ));
            }
        }
        Ok(found)
    }
}

fn read_sidecar(path: &Path) -> Result<Option<Sidecar>> {
    if let Ok(metadata) = fs::symlink_metadata(path)
        && !metadata.file_type().is_file()
    {
        return Err(StorageError::backend(
            "blob metadata path is not a regular file",
        ));
    }
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(StorageError::from),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(error)),
    }
}

fn read_regular(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.file_type().is_file() {
        return Err(StorageError::backend("blob path is not a regular file"));
    }
    fs::read(path).map_err(io_error)
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::invalid_input("blob path has no parent"))?;
    fs::create_dir_all(parent).map_err(io_error)?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| StorageError::invalid_input("invalid blob path"))?;
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = parent.join(format!(".{name}.tmp-{}-{sequence}", std::process::id()));
    let mut file = fs::File::create(&temporary).map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(&temporary, path).map_err(io_error)
}

fn io_error(error: std::io::Error) -> StorageError {
    StorageError::backend(error.to_string()).with_source(error)
}

fn scope_dir(root: &Path, scope: &Scope) -> PathBuf {
    root.join("scopes").join(dir_components(scope.as_str()))
}

fn encode(name: &str) -> String {
    let mut result = String::new();
    for byte in name.bytes() {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-') {
            result.push(char::from(byte));
        } else {
            let _ = write!(result, "%{byte:02X}");
        }
    }
    if is_reserved(&result) {
        let mut chars = result.chars();
        if let Some(first) = chars.next() {
            return format!("%{:02X}{}", first as u32, chars.as_str());
        }
    }
    result
}

fn is_reserved(encoded: &str) -> bool {
    const RESERVED: [&str; 4] = ["con", "prn", "aux", "nul"];
    RESERVED.contains(&encoded)
        || (encoded.len() == 4
            && (encoded.starts_with("com") || encoded.starts_with("lpt"))
            && encoded.as_bytes()[3].is_ascii_digit())
}

fn dir_components(name: &str) -> PathBuf {
    let encoded = encode(name);
    let mut path = PathBuf::new();
    for (index, chunk) in encoded.as_bytes().chunks(200).enumerate() {
        let chunk = std::str::from_utf8(chunk).unwrap_or("");
        path.push(if index == 0 {
            chunk.to_owned()
        } else {
            format!("+{chunk}")
        });
    }
    path
}

fn file_stem(name: &str) -> String {
    let encoded = encode(name);
    if encoded.len() <= 200 {
        return encoded;
    }
    let hash = name.bytes().fold(
        0x6c62_272e_07bb_0142_62b8_2175_6295_c58d_u128,
        |hash, byte| {
            (hash ^ u128::from(byte)).wrapping_mul(0x0000_0000_0100_0000_0000_0000_0000_013b)
        },
    );
    format!("~{hash:032x}")
}

#[cfg(test)]
#[path = "fs_tests.rs"]
mod tests;
