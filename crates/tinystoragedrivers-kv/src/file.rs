//! Plain-file KV engine with durable atomic value replacement.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Meta, Page, Precondition, Result, StorageError, Version,
    Versioned, Scope,
};

use crate::common::{
    check_precondition, cursor_key, make_cursor, next_version, validate_key, validate_limit,
    validate_prefix,
};
use crate::Clock;

const COMPONENT_CHUNK: usize = 160;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    key: String,
    version: Version,
    #[serde(default)]
    expires_ms: Option<u64>,
    #[serde(default)]
    value: Option<Value>,
}

type DirectoryLocks = HashMap<PathBuf, Weak<Mutex<()>>>;

fn directory_locks() -> &'static Mutex<DirectoryLocks> {
    static LOCKS: OnceLock<Mutex<DirectoryLocks>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// File-backed KV store bound to one scope directory.
#[derive(Clone)]
pub struct FileKv {
    directory: PathBuf,
    lock: Arc<Mutex<()>>,
    clock: Clock,
}

impl std::fmt::Debug for FileKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileKv").field("directory", &self.directory).finish()
    }
}

impl FileKv {
    /// Open the KV store for `scope` under `root`, creating its directory.
    ///
    /// # Errors
    /// Returns an error if the root cannot be created or resolved.
    pub fn open(root: impl AsRef<Path>, scope: &Scope) -> Result<Self> {
        Self::open_with_clock(root, scope, Arc::new(system_clock))
    }

    /// Open the store with an injected clock for deterministic expiry tests.
    ///
    /// # Errors
    /// Returns an error if the root cannot be created or resolved.
    pub fn open_with_clock(
        root: impl AsRef<Path>,
        scope: &Scope,
        clock: Clock,
    ) -> Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(io_error("create its directory"))?;
        let root = root
            .canonicalize()
            .map_err(io_error("resolve its directory"))?;
        let directory = root
            .join("scopes")
            .join(encode_components(scope.as_str()))
            .join("kv");
        fs::create_dir_all(&directory).map_err(io_error("create its scope directory"))?;
        let mut locks = directory_locks()
            .lock()
            .map_err(|_| StorageError::backend("file KV lock registry was poisoned"))?;
        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = locks
            .get(&directory)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(directory.clone(), Arc::downgrade(&lock));
                lock
            });
        Ok(Self { directory, lock, clock })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.lock
            .lock()
            .map_err(|_| StorageError::backend("file KV lock was poisoned"))
    }

    fn path(&self, key: &str) -> PathBuf {
        self.directory
            .join(encode_components(key))
            .join("value.json")
    }

    fn read(&self, key: &str) -> Result<Option<Record>> {
        let path = self.path(key);
        match read_optional(&path)? {
            Some(record) if record.key == key => Ok(Some(record)),
            Some(_) => Err(StorageError::backend(
                "file KV key path did not match its stored key",
            )),
            None => Ok(None),
        }
    }

    fn write(&self, record: &Record) -> Result<()> {
        write_atomic(&self.path(&record.key), &serde_json::to_vec(record)?)
    }

    fn live<'a>(record: &'a Record, now: u64) -> Option<&'a Value> {
        record.value.as_ref().filter(|_| {
            record
                .expires_ms
                .is_none_or(|expires_ms| expires_ms > now)
        })
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    fn save(
        &self,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<u64>,
        now: u64,
    ) -> Result<Version> {
        let _guard = self.lock()?;
        let old = self.read(key)?;
        check_precondition(
            expected,
            old.as_ref().and_then(|record| {
                Self::live(record, now).map(|_| record.version)
            }),
        )?;
        let version = next_version(old.map(|record| record.version))?;
        self.write(&Record {
            key: key.to_owned(),
            version,
            expires_ms,
            value: Some(value),
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
        Capabilities::none().with(Capability::Kv).with(Capability::Ttl)
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        validate_key(key)?;
        let _guard = self.lock()?;
        let now = self.now();
        Ok(self.read(key)?.and_then(|record| {
            Self::live(&record, now).map(|value| Versioned {
                id: key.to_owned(),
                version: record.version,
                doc: value.clone(),
                meta: Meta::unknown(),
            })
        }))
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        validate_key(key)?;
        let now = self.now();
        self.save(key, value, Precondition::None, None, now)
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        validate_key(key)?;
        let now = self.now();
        self.save(key, value, expected, None, now)
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        validate_key(key)?;
        let _guard = self.lock()?;
        let now = self.now();
        let old = self.read(key)?;
        let current = old.as_ref().and_then(|record| {
            Self::live(record, now).map(|_| record.version)
        });
        check_precondition(expected, current)?;
        let Some(current) = current else {
            return Ok(false);
        };
        self.write(&Record {
            key: key.to_owned(),
            version: next_version(Some(current))?,
            expires_ms: None,
            value: None,
        })?;
        Ok(true)
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        let after = cursor.as_ref().map(|cursor| cursor_key(cursor, prefix)).transpose()?;
        let _guard = self.lock()?;
        let now = self.now();
        let mut found: BTreeMap<String, Record> = self
            .list_records()?
            .into_iter()
            .filter(|record| {
                record.key.starts_with(prefix)
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
                    id: record.key,
                    version: record.version,
                    doc: value.clone(),
                    meta: Meta::unknown(),
                })
            })
            .collect();
        let next = if has_more {
            items.last().map(|item| make_cursor(&item.id, prefix)).transpose()?
        } else {
            None
        };
        Ok(Page { items, next })
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        validate_key(key)?;
        let now = self.now();
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        self.save(
            key,
            value,
            Precondition::None,
            Some(now.saturating_add(ttl_ms)),
            now,
        )
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
        file.write_all(bytes)
            .map_err(io_error("write a value"))?;
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

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
