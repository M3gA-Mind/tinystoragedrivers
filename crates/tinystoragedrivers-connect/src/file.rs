//! Shared canonical directory handle for local primitive engines.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use tinystoragedrivers_types::{Result, StorageError};

use crate::Clock;

/// A canonical local storage root shared by every file-backed primitive.
#[derive(Clone)]
pub struct FileConnection {
    root: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
    clock: Clock,
}

impl std::fmt::Debug for FileConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileConnection")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl FileConnection {
    /// Open or create a shared filesystem storage root.
    ///
    /// # Errors
    /// Returns a backend error when the root cannot be created or canonicalized.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_clock(root, Arc::new(system_clock))
    }

    /// Open a shared root with an injected clock for primitive timestamps.
    ///
    /// # Errors
    /// Returns a backend error when the root cannot be created or canonicalized.
    pub fn open_with_clock(root: impl AsRef<Path>, clock: Clock) -> Result<Self> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(|error| {
            StorageError::backend("create the file storage root").with_source(error)
        })?;
        let root = std::fs::canonicalize(root).map_err(|error| {
            StorageError::backend("canonicalize the file storage root").with_source(error)
        })?;
        let lock = lock_for(&root);
        Ok(Self {
            root: Arc::new(root),
            lock,
            clock,
        })
    }

    /// Return the canonical storage root shared by local primitive adapters.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the clock shared by local primitive adapters.
    #[must_use]
    pub fn clock(&self) -> Clock {
        Arc::clone(&self.clock)
    }

    /// Run blocking filesystem work under the root's process-wide lock.
    ///
    /// Work uses the supplied canonical root to derive paths using the
    /// primitive's established on-disk naming rules.
    ///
    /// # Errors
    /// Returns the closure's storage error, or an unavailable error if the
    /// blocking task could not be joined.
    pub async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Path) -> Result<T> + Send + 'static,
    {
        let root = Arc::clone(&self.root);
        let lock = Arc::clone(&self.lock);
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::spawn_blocking(move || {
                let _guard = lock.lock().unwrap_or_else(PoisonError::into_inner);
                operation(&root)
            })
            .await
            .map_err(|error| StorageError::unavailable("file storage task failed").with_source(error))?
        } else {
            self.run_now(operation)
        }
    }

    /// Run filesystem work synchronously under the root's process-wide lock.
    ///
    /// # Errors
    /// Returns the closure's storage error.
    pub fn run_now<T>(&self, operation: impl FnOnce(&Path) -> Result<T>) -> Result<T> {
        let _guard = self.lock();
        operation(&self.root)
    }

    fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn lock_for(root: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Arc::clone(locks.entry(root.to_path_buf()).or_default())
}

fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
