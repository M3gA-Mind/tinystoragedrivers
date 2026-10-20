//! Plain-file KV engine with durable atomic value replacement.

use std::collections::BTreeMap;
#[cfg(unix)]
use std::fs::File;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_connect::FileConnection;
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Meta, Page, Precondition, Result, Scope, StorageError,
    Version, Versioned,
};

use crate::Clock;
use crate::common::{
    check_precondition, cursor_key, duration_millis, make_cursor, next_version, validate_key,
    validate_limit, validate_prefix,
};

const COMPONENT_CHUNK: usize = 160;
const DIRECTORY: &str = "_tsd_kv";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    key: String,
    version: Version,
    #[serde(default)]
    expires_ms: Option<u64>,
    deleted: bool,
    value: Value,
}

/// File-backed KV store bound to one scope directory.
#[derive(Clone)]
pub struct FileKv {
    connection: FileConnection,
    scope: String,
    directory: PathBuf,
    clock: Clock,
}

impl std::fmt::Debug for FileKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileKv")
            .field("scope", &self.scope)
            .field("directory", &self.directory)
            .finish_non_exhaustive()
    }
}

impl FileKv {
    /// Open the KV store for `scope` under `root`, creating its directory.
    ///
    /// # Errors
    /// Returns an error if the root cannot be created or resolved.
    pub fn open(root: impl AsRef<Path>, scope: &Scope) -> Result<Self> {
        Self::new(FileConnection::open(root)?, scope.clone())
    }

    /// Open the store with an injected clock for deterministic expiry tests.
    ///
    /// # Errors
    /// Returns an error if the root cannot be created or resolved.
    pub fn open_with_clock(root: impl AsRef<Path>, scope: &Scope, clock: Clock) -> Result<Self> {
        Self::new(FileConnection::open_with_clock(root, clock)?, scope.clone())
    }

    /// Bind the store to one scope under a shared local storage connection.
    ///
    /// # Errors
    /// Returns an error if its reserved scope directory cannot be created.
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(connection: FileConnection, scope: Scope) -> Result<Self> {
        let scope_name = scope.as_str().to_owned();
        let directory_scope = encode_components(&scope_name);
        let directory = connection.run_now(move |root| {
            let directory = root.join("scopes").join(directory_scope).join(DIRECTORY);
            fs::create_dir_all(&directory).map_err(io_error("create its scope directory"))?;
            Ok(directory)
        })?;
        let clock = connection.clock();
        Ok(Self {
            connection,
            scope: scope_name,
            directory,
            clock,
        })
    }

    fn path(&self, key: &str) -> PathBuf {
        self.directory
            .join(encode_components(key))
            .join("value.json")
    }

    fn read(&self, key: &str) -> Result<Option<Record>> {
        let path = self.path(key);
        match read_optional(&path)? {
            Some(bytes) => {
                let record: Record = serde_json::from_slice(&bytes)?;
                if record.key == key {
                    Ok(Some(record))
                } else {
                    Err(StorageError::backend(
                        "file KV key path did not match its stored key",
                    ))
                }
            }
            None => Ok(None),
        }
    }

    fn write(&self, record: &Record) -> Result<()> {
        write_atomic(&self.path(&record.key), &serde_json::to_vec(record)?)
    }

    fn live(record: &Record, now: u64) -> Option<&Value> {
        (!record.deleted && record.expires_ms.is_none_or(|expires_ms| expires_ms > now))
            .then_some(&record.value)
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Self) -> Result<T> + Send + 'static,
    {
        let store = self.clone();
        self.connection.run(move |_| operation(&store)).await
    }

    fn save(
        &self,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<u64>,
        now: u64,
    ) -> Result<Version> {
        let old = self.read(key)?;
        check_precondition(
            expected,
            old.as_ref()
                .and_then(|record| Self::live(record, now).map(|_| record.version)),
        )?;
        let version = next_version(old.map(|record| record.version))?;
        self.write(&Record {
            key: key.to_owned(),
            version,
            expires_ms,
            deleted: false,
            value,
        })?;
        Ok(version)
    }

    fn list_records(&self) -> Result<Vec<Record>> {
        let mut paths = Vec::new();
        collect_record_paths(&self.directory, &mut paths)?;
        let mut records = Vec::with_capacity(paths.len());
        for path in paths {
            if let Some(bytes) = read_optional(&path)? {
                records.push(serde_json::from_slice(&bytes)?);
            }
        }
        Ok(records)
    }
}

#[async_trait]
impl KvStore for FileKv {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Kv)
            .with(Capability::Ttl)
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        validate_key(key)?;
        let key = key.to_owned();
        self.run(move |store| {
            let now = store.now();
            Ok(store.read(&key)?.and_then(|record| {
                Self::live(&record, now).map(|value| Versioned {
                    id: key,
                    version: record.version,
                    doc: value.clone(),
                    meta: Meta::unknown(),
                })
            }))
        })
        .await
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        validate_key(key)?;
        let key = key.to_owned();
        self.run(move |store| {
            let now = store.now();
            store.save(&key, value, Precondition::None, None, now)
        })
        .await
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        validate_key(key)?;
        let key = key.to_owned();
        self.run(move |store| {
            let now = store.now();
            store.save(&key, value, expected, None, now)
        })
        .await
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        validate_key(key)?;
        let key = key.to_owned();
        self.run(move |store| {
            let now = store.now();
            let old = store.read(&key)?;
            let current = old
                .as_ref()
                .and_then(|record| Self::live(record, now).map(|_| record.version));
            check_precondition(expected, current)?;
            let Some(current) = current else {
                return Ok(false);
            };
            store.write(&Record {
                key,
                version: next_version(Some(current))?,
                expires_ms: None,
                deleted: true,
                value: Value::Null,
            })?;
            Ok(true)
        })
        .await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        let after = cursor
            .as_ref()
            .map(|cursor| cursor_key(cursor, prefix))
            .transpose()?;
        let prefix = prefix.to_owned();
        self.run(move |store| {
            let now = store.now();
            let mut found: BTreeMap<String, Record> = store
                .list_records()?
                .into_iter()
                .filter(|record| {
                    record.key.starts_with(&prefix)
                        && after.as_ref().is_none_or(|after| record.key > *after)
                        && Self::live(record, now).is_some()
                })
                .map(|record| (record.key.clone(), record))
                .collect();
            let has_more = found.len() > limit;
            let keys: Vec<_> = found.keys().take(limit).cloned().collect();
            if has_more {
                found.retain(|key, _| keys.binary_search(key).is_ok());
            }
            let items: Vec<_> = found
                .into_values()
                .filter_map(|record| {
                    Self::live(&record, now).map(|value| Versioned {
                        id: record.key.clone(),
                        version: record.version,
                        doc: value.clone(),
                        meta: Meta::unknown(),
                    })
                })
                .collect();
            let next = if has_more {
                items
                    .last()
                    .map(|item| make_cursor(&item.id, &prefix))
                    .transpose()?
            } else {
                None
            };
            Ok(Page { items, next })
        })
        .await
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        validate_key(key)?;
        let key = key.to_owned();
        let ttl_ms = duration_millis(ttl);
        self.run(move |store| {
            let now = store.now();
            store.save(
                &key,
                value,
                Precondition::None,
                Some(now.saturating_add(ttl_ms)),
                now,
            )
        })
        .await
    }
}

fn encode_components(value: &str) -> PathBuf {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
    }
    let mut path = PathBuf::new();
    let mut rest = encoded.as_str();
    while !rest.is_empty() {
        let mut end = rest.len().min(COMPONENT_CHUNK);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        path.push(&rest[..end]);
        rest = &rest[end..];
    }
    path
}

fn collect_record_paths(directory: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("list its scope directory")(error)),
    };
    for entry in entries {
        let entry = entry.map_err(io_error("list its scope directory"))?;
        let kind = entry
            .file_type()
            .map_err(io_error("inspect a directory entry"))?;
        if kind.is_dir() {
            collect_record_paths(&entry.path(), paths)?;
        } else if kind.is_file() && entry.file_name() == "value.json" {
            paths.push(entry.path());
        }
    }
    Ok(())
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    let mut file = match no_follow(OpenOptions::new().read(true)).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("read a value")(error)),
    };
    if !file
        .metadata()
        .map_err(io_error("inspect a value"))?
        .file_type()
        .is_file()
    {
        return Err(StorageError::backend("file KV value is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(io_error("read a value"))?;
    Ok(Some(bytes))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::backend("file KV value path has no parent"))?;
    fs::create_dir_all(parent).map_err(io_error("create a key directory"))?;
    let temp = parent.join(format!(".tmp-{}-{}", std::process::id(), thread_sequence()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(io_error("create a temporary value"))?;
        file.write_all(bytes).map_err(io_error("write a value"))?;
        file.sync_all().map_err(io_error("sync a value"))?;
        drop(file);
        fs::rename(&temp, path).map_err(io_error("replace a value"))?;
        #[cfg(unix)]
        File::open(parent)
            .and_then(|handle| handle.sync_all())
            .map_err(io_error("sync a key directory"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

fn thread_sequence() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn no_follow(options: &mut OpenOptions) -> &mut OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000)
    }
    #[cfg(not(any(unix, windows)))]
    {
        options
    }
}

fn io_error(action: &'static str) -> impl FnOnce(std::io::Error) -> StorageError {
    move |error| StorageError::backend(format!("file KV could not {action}")).with_source(error)
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
