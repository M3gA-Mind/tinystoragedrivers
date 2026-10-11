//! [`FileStorage`], the backend, and the per-database state every port handle
//! shares.
//!
//! # Layout
//!
//! ```text
//! <root>/
//! ├── _meta/collections/<collection>.json     collection declarations
//! ├── scopes/<scope>/
//! │   ├── docs/<collection>/<id>.json         {"id", "version", "doc"}
//! │   ├── streams/<name>.jsonl                {"offset", "value"} per line
//! │   ├── streams/<name>.meta.json            {"name", "base"}
//! │   ├── blobs/<key>                         raw bytes
//! │   └── blobs/<key>.meta.json               {"key", "content_type"}
//! └── databases/<name>/                       the same layout per named database
//! ```
//!
//! Names are encoded by the `encode` module. Scopes live under `scopes/` and
//! named databases under `databases/`, so neither can shadow the other or the
//! `_meta` directory.
//!
//! # Concurrency
//!
//! Every directory has one native mutex shared by independently reopened
//! handles. Cross-database fenced operations take the target and guard locks
//! in canonical-path order, so `claim` and guard validation are atomic while
//! unrelated directories retain independent IO. The file
//! IO runs on tokio's blocking pool when a runtime is present. Several
//! *processes* writing the same directory are not coordinated: each file
//! replacement is atomic, but a compare-and-swap, claim or fenced write can
//! race another process.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use tinystoragedrivers_core::{
    Capabilities, Capability, Clock, Fence, GuardDatabase, Result, Scope, ScopedStorage,
    StorageBackend, StorageError, validate_database,
};

use crate::blobs::FileBlobs;
use crate::documents::FileDocuments;
use crate::encode::{dir_components, file_stem};
use crate::fsio::io_error;
use crate::streams::FileStreams;

/// What this driver provides: expiry, full-text search and fencing. Not
/// transactions, because a batch spanning several files cannot be made
/// crash-atomic with renames alone. Fencing holds only among the handles of
/// one process, like every other guarantee here: the fence is checked under
/// the process-wide native gate.
pub(crate) const CAPABILITIES: Capabilities = Capabilities::none()
    .with(Capability::Ttl)
    .with(Capability::FullText)
    .with(Capability::Fencing)
    .with(Capability::CrossDatabaseFencing)
    .with(Capability::ConditionalStreamAppend)
    .with(Capability::CollectionEnumeration);

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// One native lock per canonical database directory in this process.
fn lock_for(dir: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(locks.entry(dir.to_path_buf()).or_default())
}

/// A validated guard address bound by the host when creating a handle.
#[derive(Debug)]
pub(crate) struct Fencing {
    pub(crate) fence: Fence,
    pub(crate) db: Arc<Db>,
}

/// One database directory and the lock guarding it.
pub(crate) struct Db {
    dir: PathBuf,
    lock: Arc<Mutex<()>>,
    clock: Clock,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl Db {
    fn new(dir: PathBuf, clock: Clock) -> Self {
        let lock = lock_for(&dir);
        Self { dir, lock, clock }
    }

    /// Milliseconds since the Unix epoch, from the injected clock.
    pub(crate) fn now(&self) -> u64 {
        (self.clock)()
    }

    /// Where a collection's declaration is kept.
    pub(crate) fn specs_dir(&self) -> PathBuf {
        self.dir.join("_meta").join("collections")
    }

    /// The directory holding every scope's data.
    pub(crate) fn scopes_dir(&self) -> PathBuf {
        self.dir.join("scopes")
    }

    /// The directory holding one scope's data.
    pub(crate) fn scope_dir(&self, scope: &Scope) -> PathBuf {
        self.scopes_dir().join(dir_components(scope.as_str()))
    }

    /// Hold the database lock. It guards no in-memory state (everything is on
    /// disk and every file replacement is atomic), so a poisoned lock is
    /// simply taken over.
    fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run `work` under the database lock, on the blocking pool when called
    /// from inside a tokio runtime, inline otherwise.
    pub(crate) async fn run<T, F>(self: &Arc<Self>, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Db) -> Result<T> + Send + 'static,
    {
        self.run_with_guard(None, work).await
    }

    /// Hold target and optional guard directories in canonical order for the
    /// entire native operation; aliases share a lock and are acquired once.
    pub(crate) async fn run_with_guard<T, F>(
        self: &Arc<Self>,
        guard: Option<Arc<Db>>,
        work: F,
    ) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Db) -> Result<T> + Send + 'static,
    {
        let db = Arc::clone(self);
        let job = move || {
            if let Some(guard) = guard
                .as_ref()
                .filter(|guard| !Arc::ptr_eq(&db.lock, &guard.lock))
            {
                let (first, second) = if db.dir < guard.dir {
                    (db.as_ref(), guard.as_ref())
                } else {
                    (guard.as_ref(), db.as_ref())
                };
                let _first = first.lock();
                let _second = second.lock();
                return work(&db);
            }
            let _guard = db.lock();
            work(&db)
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.spawn_blocking(job).await.map_err(|error| {
                StorageError::backend("file storage task did not complete").with_source(error)
            })?,
            Err(_) => job(),
        }
    }
}

/// A [`StorageBackend`] that keeps everything as plain files under one
/// directory: JSON per document, JSONL per stream, raw bytes per blob.
///
/// ```
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(async {
/// use serde_json::json;
/// use tinystoragedrivers_core::{Precondition, Scope, StorageBackend};
/// use tinystoragedrivers_file::FileStorage;
///
/// let dir = tempfile::tempdir().unwrap();
/// let storage = FileStorage::open(dir.path())?;
/// let local = storage.for_scope(&Scope::local())?;
/// local.documents().put("notes", "n1", json!({"text": "hi"}), Precondition::Absent).await?;
///
/// let reopened = FileStorage::open(dir.path())?.for_scope(&Scope::local())?;
/// assert!(reopened.documents().get("notes", "n1").await?.is_some());
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// # }).unwrap();
/// ```
#[derive(Clone)]
pub struct FileStorage {
    root: PathBuf,
    db: Arc<Db>,
}

impl fmt::Debug for FileStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileStorage")
            .field("dir", &self.db.dir)
            .finish_non_exhaustive()
    }
}

impl FileStorage {
    /// Open (creating if needed) a file store rooted at `root`, reading the
    /// system clock for document expiry.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Backend`](tinystoragedrivers_core::ErrorKind::Backend)
    /// when the directory cannot be created or resolved.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_clock(root, Arc::new(system_clock))
    }

    /// [`FileStorage::open`], reading time from `clock` (Unix epoch
    /// milliseconds) so tests can drive expiry deterministically.
    ///
    /// # Errors
    ///
    /// As [`FileStorage::open`].
    pub fn open_with_clock(root: impl Into<PathBuf>, clock: Clock) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(io_error("create the storage directory"))?;
        // Resolve symlinks and relative paths so two handles on one directory
        // share a lock however they spelled it.
        let root = root
            .canonicalize()
            .map_err(io_error("resolve the storage directory"))?;
        let db = Arc::new(Db::new(root.clone(), clock));
        Ok(Self { root, db })
    }

    fn handles(&self, scope: &Scope, fence: Option<&Arc<Fencing>>) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(FileDocuments::new(
                Arc::clone(&self.db),
                scope.clone(),
                fence.cloned(),
            )),
            Arc::new(FileStreams::new(
                Arc::clone(&self.db),
                scope.clone(),
                fence.cloned(),
            )),
            Arc::new(FileBlobs::new(
                Arc::clone(&self.db),
                scope.clone(),
                fence.cloned(),
            )),
        )
    }

    /// The directory this store keeps its root database in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.db.dir
    }
}

impl StorageBackend for FileStorage {
    fn driver(&self) -> &'static str {
        "file"
    }

    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                db: Arc::clone(&self.db),
            })),
        ))
    }

    fn for_scope_fenced_in_database(
        &self,
        scope: &Scope,
        fence: &Fence,
        guard_database: &GuardDatabase,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        guard_database.validate()?;
        let db = match guard_database {
            GuardDatabase::Current => Arc::clone(&self.db),
            GuardDatabase::Default => {
                Arc::new(Db::new(self.root.clone(), Arc::clone(&self.db.clock)))
            }
            GuardDatabase::Named(name) => self.named(name)?,
        };
        Ok(self.handles(
            scope,
            Some(&Arc::new(Fencing {
                fence: fence.clone(),
                db,
            })),
        ))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        Ok(Arc::new(Self {
            root: self.root.clone(),
            db: self.named(name)?,
        }))
    }
}

impl FileStorage {
    fn named(&self, name: &str) -> Result<Arc<Db>> {
        validate_database(name)?;
        let dir = self.root.join("databases").join(file_stem(name));
        std::fs::create_dir_all(&dir).map_err(io_error("create the database directory"))?;
        let dir = dir
            .canonicalize()
            .map_err(io_error("resolve the database directory"))?;
        Ok(Arc::new(Db::new(dir, Arc::clone(&self.db.clock))))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
