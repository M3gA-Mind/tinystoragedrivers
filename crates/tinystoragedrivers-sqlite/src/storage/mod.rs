//! The SQLite [`StorageBackend`]: file layout and handle construction.
//!
//! - **Directory mode** (any path not ending in `.db` or `.sqlite`): the root
//!   database is `<dir>/storage.db`, and `database(name)` is `<dir>/<name>.db`.
//!   This is the desktop layout: each owner keeps its own file, the way
//!   OpenHuman already splits `approval.db`, `flows.db` and `sessions.db`.
//! - **File mode** (a `.db` or `.sqlite` path): everything lives in that one
//!   file, and `database(name)` prefixes its tables with `<name>__`.
//!
//! Any scope may be used; rows carry their scope.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tinystoragedrivers_core::{
    Capabilities, Fence, GuardDatabase, Result, Scope, ScopedStorage, StorageBackend,
    validate_database,
};

use crate::blobs::SqliteBlobs;
use crate::connection::Db;
use crate::documents::{SqliteDocuments, capabilities};
use crate::fence::Fencing;
use crate::native::SqliteNative;
use crate::sql::Tables;
use crate::streams::SqliteStreams;

/// Milliseconds since the Unix epoch, as the driver sees them.
pub use tinystoragedrivers_core::Clock;

/// File name of the root database in directory mode.
pub const ROOT_FILE: &str = "storage.db";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Layout {
    Directory(PathBuf),
    File(PathBuf),
}

/// A SQLite-backed [`StorageBackend`].
///
/// ```
/// use tinystoragedrivers_core::{Scope, StorageBackend};
/// use tinystoragedrivers_sqlite::SqliteStorage;
///
/// let dir = tempfile::tempdir().unwrap();
/// let storage = SqliteStorage::open(dir.path())?;
/// let approvals = storage.database("approvals")?;
/// assert!(dir.path().join("approvals.db").exists());
/// assert_eq!(approvals.for_scope(&Scope::local())?.driver(), "sqlite");
/// # Ok::<(), tinystoragedrivers_core::StorageError>(())
/// ```
#[derive(Clone)]
pub struct SqliteStorage {
    layout: Layout,
    db: Arc<Db>,
    tables: Arc<Tables>,
    clock: Clock,
    existing: bool,
}

impl fmt::Debug for SqliteStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteStorage")
            .field("path", &self.db.path())
            .field("prefix", &self.tables.prefix)
            .finish_non_exhaustive()
    }
}

fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Whether `path` names a single database file rather than a directory.
fn is_file_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("db") || ext.eq_ignore_ascii_case("sqlite"))
}

impl SqliteStorage {
    /// Open storage at `path`, choosing directory or file mode from its
    /// extension, and create the driver's tables.
    ///
    /// # Errors
    ///
    /// When the directory or file cannot be created or opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let layout = if is_file_path(path) {
            Layout::File(path.to_path_buf())
        } else {
            Layout::Directory(path.to_path_buf())
        };
        let file = match &layout {
            Layout::Directory(dir) => dir.join(ROOT_FILE),
            Layout::File(file) => file.clone(),
        };
        Self::assemble(layout, &file, "", Arc::new(system_clock))
    }

    fn assemble(layout: Layout, file: &Path, prefix: &str, clock: Clock) -> Result<Self> {
        let db = Db::open(file)?;
        // Keep the opened namespace independent of later cwd changes. Resolve
        // the directory itself, since storage.db may be a symlink elsewhere.
        let layout = match layout {
            Layout::Directory(dir) => {
                let dir = if dir.as_os_str().is_empty() {
                    Path::new(".")
                } else {
                    dir.as_path()
                };
                Layout::Directory(dir.canonicalize().map_err(|error| {
                    tinystoragedrivers_core::StorageError::backend(
                        "cannot resolve the sqlite layout directory",
                    )
                    .with_source(error)
                })?)
            }
            Layout::File(_) => Layout::File(db.path().to_path_buf()),
        };
        let tables = Arc::new(Tables::new(prefix));
        let create = Arc::clone(&tables);
        db.run_now(move |conn| create.ensure(conn))?;
        Ok(Self {
            layout,
            db,
            tables,
            clock,
            existing: false,
        })
    }

    fn assemble_existing(layout: Layout, file: &Path, prefix: &str, clock: Clock) -> Result<Self> {
        let db = Db::open_existing(file)?;
        let tables = Arc::new(Tables::new(prefix));
        let inspect = Arc::clone(&tables);
        db.run_now(move |conn| inspect.validate(conn))?;
        Ok(Self {
            layout,
            db,
            tables,
            clock,
            existing: true,
        })
    }

    fn select(&self, file: &Path, prefix: &str) -> Result<Self> {
        if self.existing {
            Self::assemble_existing(self.layout.clone(), file, prefix, Arc::clone(&self.clock))
        } else {
            Self::assemble(self.layout.clone(), file, prefix, Arc::clone(&self.clock))
        }
    }

    /// Read document expiry time from `clock` instead of the system clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Raw access to this database's file, for owners that keep their own
    /// tables beside (or instead of) the generic ones.
    #[must_use]
    pub fn native(&self) -> SqliteNative {
        SqliteNative::new(Arc::clone(&self.db))
    }

    fn handles(&self, scope: &Scope, fencing: Option<&Arc<Fencing>>) -> ScopedStorage {
        ScopedStorage::new(
            scope.clone(),
            self.driver(),
            Arc::new(SqliteDocuments::new(
                Arc::clone(&self.db),
                Arc::clone(&self.tables),
                scope.clone(),
                Arc::clone(&self.clock),
                fencing.cloned(),
                self.existing,
            )),
            Arc::new(SqliteStreams::new(
                Arc::clone(&self.db),
                Arc::clone(&self.tables),
                scope.clone(),
                fencing.cloned(),
            )),
            Arc::new(SqliteBlobs::new(
                Arc::clone(&self.db),
                Arc::clone(&self.tables),
                scope.clone(),
                fencing.cloned(),
            )),
        )
    }

    /// The database file this storage reads and writes.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.db.path()
    }
}

#[tinystoragedrivers_core::async_trait]
impl StorageBackend for SqliteStorage {
    fn driver(&self) -> &'static str {
        "sqlite"
    }

    fn capabilities(&self) -> Capabilities {
        capabilities()
    }

    async fn prepare_schema(&self) -> Result<()> {
        if self.existing {
            return Err(tinystoragedrivers_core::StorageError::unsupported(
                tinystoragedrivers_core::Capability::ExistingSchema,
                "existing-schema handles cannot prepare internal schema",
            ));
        }
        let tables = Arc::clone(&self.tables);
        self.db.run(move |conn| tables.ensure(conn)).await
    }

    fn existing_schema(&self) -> Result<Arc<dyn StorageBackend>> {
        let inspect = Arc::clone(&self.tables);
        self.db.run_now(move |conn| inspect.validate(conn))?;
        let mut storage = self.clone();
        storage.existing = true;
        Ok(Arc::new(storage))
    }

    fn for_scope(&self, scope: &Scope) -> Result<ScopedStorage> {
        Ok(self.handles(scope, None))
    }

    fn for_scope_fenced(&self, scope: &Scope, fence: &Fence) -> Result<ScopedStorage> {
        fence.validate()?;
        let fencing = Arc::new(Fencing::new(fence.clone(), Arc::clone(&self.clock)));
        Ok(self.handles(scope, Some(&fencing)))
    }

    fn for_scope_fenced_in_database(
        &self,
        scope: &Scope,
        fence: &Fence,
        locator: &GuardDatabase,
    ) -> Result<ScopedStorage> {
        fence.validate()?;
        locator.validate()?;
        if *locator == GuardDatabase::Current {
            return self.for_scope_fenced(scope, fence);
        }
        let (file, prefix) = match (&self.layout, locator) {
            (Layout::Directory(dir), GuardDatabase::Default) => {
                (dir.join(ROOT_FILE), String::new())
            }
            (Layout::Directory(dir), GuardDatabase::Named(name)) => {
                (dir.join(format!("{name}.db")), String::new())
            }
            (Layout::File(file), GuardDatabase::Default) => (file.clone(), String::new()),
            (Layout::File(file), GuardDatabase::Named(name)) => (file.clone(), format!("{name}__")),
            (_, GuardDatabase::Current) => unreachable!(),
        };
        let guard = self.select(&file, &prefix)?;
        let attached = if guard.db.path() == self.db.path() {
            None
        } else {
            Some(guard.db.path().to_path_buf())
        };
        let fencing = Arc::new(Fencing::in_database(
            fence.clone(),
            Arc::clone(&self.clock),
            attached,
            guard.tables,
        ));
        Ok(self.handles(scope, Some(&fencing)))
    }

    fn database(&self, name: &str) -> Result<Arc<dyn StorageBackend>> {
        validate_database(name)?;
        let storage = match &self.layout {
            Layout::Directory(dir) => self.select(&dir.join(format!("{name}.db")), "")?,
            Layout::File(file) => self.select(file, &format!("{name}__"))?,
        };
        Ok(Arc::new(storage))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "schema_tests.rs"]
mod schema_tests;
