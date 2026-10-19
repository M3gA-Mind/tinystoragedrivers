//! Shared SQLite database connection and generic storage schema.

mod connection;
pub mod error;
mod schema;

use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tinystoragedrivers_types::Result;
pub use rusqlite;
pub use mongodb;

use connection::Db;
pub use schema::SqliteTables;

use crate::Clock;

/// One SQLite connection shared by all primitive engines using a database file.
#[derive(Clone)]
pub struct SqliteConnection {
    db: Arc<Db>,
    tables: Arc<SqliteTables>,
    clock: Clock,
}

impl std::fmt::Debug for SqliteConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteConnection")
            .field("path", &self.db.path())
            .finish_non_exhaustive()
    }
}

impl SqliteConnection {
    /// Open a storage path and create or validate the shared generic schema.
    /// Paths ending in `.db` or `.sqlite` name a database file; other paths
    /// name a directory containing `storage.db`.
    ///
    /// # Errors
    /// Returns a path, SQLite, or incompatible-schema error.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_clock(path, Arc::new(system_clock))
    }

    /// Open storage while using `clock` for document and blob expiries and
    /// stream timestamps.
    ///
    /// # Errors
    /// Returns a path, SQLite, or incompatible-schema error.
    pub fn open_with_clock(path: impl AsRef<Path>, clock: Clock) -> Result<Self> {
        let path = path.as_ref();
        let file = if is_file_path(path) {
            path.to_path_buf()
        } else {
            path.join("storage.db")
        };
        let db = Db::open(&file)?;
        let tables = Arc::new(SqliteTables::new());
        let tables_for_ensure = Arc::clone(&tables);
        db.run_now(move |conn| tables_for_ensure.ensure(conn))?;
        Ok(Self { db, tables, clock })
    }

    /// Return the canonical path of the shared database file.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.db.path()
    }

    /// Return the injected clock shared by all local primitive adapters.
    #[must_use]
    pub fn clock(&self) -> Clock {
        Arc::clone(&self.clock)
    }

    /// Return names for the generic tables shared by local primitive engines.
    #[must_use]
    pub fn tables(&self) -> &SqliteTables {
        &self.tables
    }

    /// Run a database operation on Tokio's blocking pool when a runtime is
    /// active, or inline otherwise.
    ///
    /// # Errors
    /// Returns a SQLite or task-join error produced by `operation`.
    pub async fn run<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        self.db.run(operation).await
    }

    /// Run a database operation synchronously while holding the shared
    /// connection lock.
    ///
    /// # Errors
    /// Returns a SQLite error produced by `operation` or a poisoned-lock
    /// error.
    pub fn run_now<T>(&self, operation: impl FnOnce(&mut Connection) -> Result<T>) -> Result<T> {
        self.db.run_now(operation)
    }
}

fn system_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}

fn is_file_path(path: &Path) -> bool {
    path.extension().and_then(|ext| ext.to_str()).is_some_and(|ext| {
        ext.eq_ignore_ascii_case("db") || ext.eq_ignore_ascii_case("sqlite")
    })
}
