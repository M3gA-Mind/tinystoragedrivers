//! Portable process-exclusive leases for hosts without a shared backend.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use fs2::FileExt;
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{LeaseRecord, StorageError, Version};

use crate::{LeaseError, LeaseGrant, LeaseStore};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct HeldLock {
    file: File,
    version: Version,
}

/// Local lease store backed by portable advisory file locks.
///
/// The lock is held for the lifetime of a grant, so it excludes other
/// processes using the same directory even if the lease TTL expires. This is
/// intentional: a local store cannot atomically fence writes performed by an
/// arbitrary backend. `driver_fence` therefore returns `None`.
#[derive(Debug)]
pub struct LocalLeases {
    root: PathBuf,
    holder: String,
    ttl_ms: i64,
    held: Mutex<HashMap<String, HeldLock>>,
}

impl LocalLeases {
    /// Open a local lease store rooted at `root` for `holder`.
    ///
    /// `ttl_ms` is the validity window reported to callers; the OS lock also
    /// prevents takeover while this process still holds the grant.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an empty holder or non-positive TTL, or a
    /// backend error when the directory cannot be created.
    pub fn new(
        root: impl AsRef<Path>,
        holder: impl Into<String>,
        ttl_ms: i64,
    ) -> Result<Self, StorageError> {
        if ttl_ms <= 0 {
            return Err(StorageError::invalid_input("lease ttl must be positive"));
        }
        let holder = holder.into();
        if holder.is_empty() {
            return Err(StorageError::invalid_input(
                "lease holder must not be empty",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_io)?;
        Ok(Self {
            root,
            holder,
            ttl_ms,
            held: Mutex::new(HashMap::new()),
        })
    }

    fn directory(&self, key: &str) -> Result<PathBuf, StorageError> {
        if key.is_empty() {
            return Err(StorageError::invalid_input("lease key must not be empty"));
        }
        let digest = Sha256::digest(key.as_bytes());
        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
        }
        Ok(self.root.join(hex))
    }

    fn read_record(dir: &Path) -> Result<Option<LeaseRecord>, StorageError> {
        let path = dir.join("lease.json");
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage_io(error)),
        };
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(storage_io)?;
        serde_json::from_str(&contents)
            .map(Some)
            .map_err(StorageError::from)
    }

    fn write_record(dir: &Path, record: &LeaseRecord) -> Result<(), StorageError> {
        let destination = dir.join("lease.json");
        for _ in 0..16 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let temporary = dir.join(format!(".lease-{}-{sequence}.tmp", std::process::id()));
            let opened = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary);
            let mut file = match opened {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(storage_io(error)),
            };
            let result = (|| {
                serde_json::to_writer(&mut file, record).map_err(StorageError::from)?;
                file.flush().map_err(storage_io)?;
                file.sync_all().map_err(storage_io)?;
                drop(file);
                fs::rename(&temporary, &destination).map_err(storage_io)
            })();
            if result.is_err() {
                let _ = fs::remove_file(temporary);
            }
            return result;
        }
        Err(StorageError::backend(
            "could not allocate a unique local lease temporary file",
        ))
    }

    fn current_grant(&self, grant: &LeaseGrant, now_ms: u64) -> Result<LeaseRecord, LeaseError> {
        if grant.holder != self.holder {
            return Err(LeaseError::Lost);
        }
        let now = i64::try_from(now_ms).map_err(|_| LeaseError::Lost)?;
        let held = self.held.lock().map_err(|_| LeaseError::Lost)?;
        if held
            .get(&grant.key)
            .is_none_or(|entry| entry.version != grant.version)
        {
            return Err(LeaseError::Lost);
        }
        let record = Self::read_record(&self.directory(&grant.key)?)?.ok_or(LeaseError::Lost)?;
        if record.holder != grant.holder
            || record.epoch != grant.epoch
            || record.released
            || record.expires_ms <= now
        {
            return Err(LeaseError::Lost);
        }
        Ok(record)
    }
}

#[async_trait::async_trait]
impl LeaseStore for LocalLeases {
    async fn acquire(&self, key: &str, now_ms: u64) -> Result<LeaseGrant, LeaseError> {
        let now = i64::try_from(now_ms).map_err(|_| LeaseError::Lost)?;
        let expires_ms = now.checked_add(self.ttl_ms).ok_or(LeaseError::Lost)?;
        let dir = self.directory(key)?;
        fs::create_dir_all(&dir).map_err(storage_io)?;
        {
            let mut held = self.held.lock().map_err(|_| LeaseError::Lost)?;
            if let Some(entry) = held.get_mut(key) {
                let mut record = Self::read_record(&dir)?.ok_or(LeaseError::Lost)?;
                if record.holder == self.holder && !record.released && now < record.expires_ms {
                    let version = entry.version.next().ok_or_else(|| {
                        StorageError::backend("lease grant version space exhausted")
                    })?;
                    record.expires_ms = expires_ms;
                    Self::write_record(&dir, &record)?;
                    entry.version = version;
                    return Ok(LeaseGrant {
                        key: key.to_owned(),
                        holder: record.holder,
                        epoch: record.epoch,
                        version,
                        expires_ms: record.expires_ms,
                        previous_unclean: false,
                    });
                }
                return Err(LeaseError::Held(record));
            }
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("lease.lock"))
            .map_err(storage_io)?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                let found = Self::read_record(&dir)?.unwrap_or(LeaseRecord {
                    holder: "unknown".into(),
                    endpoint: None,
                    epoch: 0,
                    expires_ms: expires_ms.max(now),
                    released: false,
                });
                return Err(LeaseError::Held(found));
            }
            Err(error) => return Err(LeaseError::Storage(storage_io(error))),
        }
        let mut held = self.held.lock().map_err(|_| LeaseError::Lost)?;
        if held.contains_key(key) {
            return Err(LeaseError::Lost);
        }
        let previous = Self::read_record(&dir)?;
        let epoch = previous.as_ref().map_or(Ok(1), |record| {
            record
                .epoch
                .checked_add(1)
                .ok_or_else(|| StorageError::backend("lease epoch overflow"))
        })?;
        let previous_unclean = previous.is_some_and(|record| !record.released);
        let record = LeaseRecord {
            holder: self.holder.clone(),
            endpoint: None,
            epoch,
            expires_ms,
            released: false,
        };
        Self::write_record(&dir, &record)?;
        held.insert(
            key.to_owned(),
            HeldLock {
                file: lock,
                version: Version::FIRST,
            },
        );
        Ok(LeaseGrant {
            key: key.to_owned(),
            holder: self.holder.clone(),
            epoch,
            version: Version(1),
            expires_ms,
            previous_unclean,
        })
    }

    async fn renew(&self, grant: &LeaseGrant, now_ms: u64) -> Result<LeaseGrant, LeaseError> {
        let mut record = self.current_grant(grant, now_ms)?;
        let now = i64::try_from(now_ms).map_err(|_| LeaseError::Lost)?;
        record.expires_ms = now.checked_add(self.ttl_ms).ok_or(LeaseError::Lost)?;
        Self::write_record(&self.directory(&grant.key)?, &record)?;
        let mut renewed = grant.clone();
        renewed.expires_ms = record.expires_ms;
        renewed.version = grant
            .version
            .next()
            .ok_or_else(|| StorageError::backend("lease grant version space exhausted"))?;
        self.held
            .lock()
            .map_err(|_| LeaseError::Lost)?
            .get_mut(&grant.key)
            .ok_or(LeaseError::Lost)?
            .version = renewed.version;
        Ok(renewed)
    }

    async fn release(&self, grant: LeaseGrant) -> Result<(), LeaseError> {
        if grant.holder != self.holder {
            return Err(LeaseError::Lost);
        }
        let mut held = self.held.lock().map_err(|_| LeaseError::Lost)?;
        if held
            .get(&grant.key)
            .is_none_or(|entry| entry.version != grant.version)
        {
            return Err(LeaseError::Lost);
        }
        let mut record =
            Self::read_record(&self.directory(&grant.key)?)?.ok_or(LeaseError::Lost)?;
        if record.holder != grant.holder || record.epoch != grant.epoch || record.released {
            return Err(LeaseError::Lost);
        }
        record.released = true;
        record.expires_ms = record.expires_ms.min(system_now_ms()?);
        Self::write_record(&self.directory(&grant.key)?, &record)?;
        let lock = held.remove(&grant.key).ok_or(LeaseError::Lost)?;
        FileExt::unlock(&lock.file).map_err(storage_io)?;
        Ok(())
    }

    async fn inspect(&self, key: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        Ok(Self::read_record(&self.directory(key)?)?)
    }
}

fn storage_io(error: std::io::Error) -> StorageError {
    StorageError::backend("local lease file operation failed").with_source(error)
}

fn system_now_ms() -> Result<i64, StorageError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            StorageError::backend("system clock is before Unix epoch").with_source(error)
        })?;
    i64::try_from(duration.as_millis())
        .map_err(|_| StorageError::backend("system clock exceeds lease timestamp range"))
}
