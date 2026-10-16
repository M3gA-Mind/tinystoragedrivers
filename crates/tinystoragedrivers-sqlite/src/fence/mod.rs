//! Fenced writes on SQLite.
//!
//! A fenced handle reads its [`Fence`]'s guard document inside the same
//! immediate transaction as the write. An immediate transaction holds the
//! database's write lock from its first statement, so no other writer (in
//! this process or another) can move the guard between the check and the
//! write: a takeover either commits before the transaction begins, and the
//! write is refused, or waits until it has committed.
//!
//! Guards in another directory-mode database are attached to the target
//! connection before the immediate transaction begins. Only the target is
//! mutated, and the guard remains locked through commit.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::during;
use crate::sql::ident;
use rusqlite::{Connection, OptionalExtension, params};
use tinystoragedrivers_core::{CollectionSpec, Fence, Result, Version, Versioned};

use crate::documents::ops::{self, Ctx};
use crate::sql::Tables;
use crate::storage::Clock;

/// A fence and the clock that decides whether its guard has expired.
pub(crate) struct Fencing {
    fence: Fence,
    clock: Clock,
    database: Option<(PathBuf, Arc<Tables>)>,
}

impl fmt::Debug for Fencing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fencing")
            .field("fence", &self.fence)
            .finish_non_exhaustive()
    }
}

impl Fencing {
    pub(crate) fn guard_scope(&self) -> &tinystoragedrivers_core::Scope {
        self.fence.scope()
    }
    pub(crate) fn new(fence: Fence, clock: Clock) -> Self {
        Self {
            fence,
            clock,
            database: None,
        }
    }

    pub(crate) fn in_database(
        fence: Fence,
        clock: Clock,
        file: Option<PathBuf>,
        tables: Arc<Tables>,
    ) -> Self {
        Self {
            fence,
            clock,
            database: Some((file.unwrap_or_default(), tables)),
        }
    }

    /// Attach the guard before BEGIN IMMEDIATE: SQLite locks every attached
    /// database at transaction start, excluding a concurrent guard takeover.
    pub(crate) fn prepare(&self, conn: &Connection) -> Result<()> {
        detach(conn)?;
        let Some((file, _)) = &self.database else {
            return Ok(());
        };
        if file.as_os_str().is_empty() {
            return Ok(());
        }
        // SQLite accepts filename bytes as TEXT on Unix, just as its native
        // open path does. A lossy string could attach a different database.
        #[cfg(unix)]
        let filename = {
            use std::os::unix::ffi::OsStrExt;
            rusqlite::types::ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Text(
                file.as_os_str().as_bytes(),
            ))
        };
        #[cfg(not(unix))]
        let filename = file.to_str().ok_or_else(|| {
            tinystoragedrivers_core::StorageError::invalid_input(
                "guard database path must be valid Unicode on this platform",
            )
        })?;
        conn.execute("ATTACH DATABASE ?1 AS tsd_fence_guard", [filename])
            .map_err(during("attach guard database"))?;
        Ok(())
    }

    /// Refuse the write unless the fence holds. Call inside the write's
    /// immediate transaction.
    ///
    /// # Errors
    ///
    /// [`ErrorKind::Fenced`](tinystoragedrivers_core::ErrorKind::Fenced), or
    /// a failure reading the guard.
    pub(crate) fn check(&self, conn: &Connection, tables: &Tables) -> Result<()> {
        let Some((file, guard_tables)) = &self.database else {
            let ctx = Ctx {
                tables,
                scope: self.fence.scope().as_str(),
                now_ms: (self.clock)(),
            };
            return self
                .fence
                .check(ops::get(conn, ctx, self.fence.collection(), self.fence.id())?.as_ref());
        };
        let table = |name: &str| {
            if file.as_os_str().is_empty() {
                ident(name)
            } else {
                format!("tsd_fence_guard.{}", ident(name))
            }
        };
        let spec: Option<String> = conn
            .query_row(
                &format!(
                    "SELECT spec FROM {} WHERE coll = ?1",
                    table(&guard_tables.specs)
                ),
                [self.fence.collection()],
                |row| row.get(0),
            )
            .optional()
            .map_err(during("load guard collection"))?;
        let spec: CollectionSpec = match spec {
            Some(text) => serde_json::from_str(&text)?,
            None => CollectionSpec::new(self.fence.collection()),
        };
        let stored: Option<(String, i64, String)> = conn
            .query_row(
                &format!(
                    "SELECT id, version, doc FROM {} WHERE scope = ?1 AND coll = ?2 AND id = ?3",
                    table(&guard_tables.docs)
                ),
                params![
                    self.fence.scope().as_str(),
                    self.fence.collection(),
                    self.fence.id()
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(during("read guard document"))?;
        let guard = stored
            .map(|(id, version, text)| -> Result<_> {
                Ok(Versioned {
                    id,
                    version: Version(u64::try_from(version).unwrap_or(0)),
                    doc: serde_json::from_str(&text)?,
                    meta: tinystoragedrivers_core::Meta::unknown(),
                })
            })
            .transpose()?
            .filter(|found| !ops::expired(&spec, &found.doc, (self.clock)()));
        self.fence.check(guard.as_ref())
    }
}

fn detach(conn: &Connection) -> Result<()> {
    let attached: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_database_list WHERE name = 'tsd_fence_guard')",
            [],
            |row| row.get(0),
        )
        .map_err(during("inspect guard attachment"))?;
    if attached {
        conn.execute_batch("DETACH DATABASE tsd_fence_guard")
            .map_err(during("detach guard database"))?;
    }
    Ok(())
}

/// Execute a native fenced write, restoring the connection's attachments on
/// success and on refusal so later raw or unfenced transactions are unaffected.
pub(crate) fn write<T>(
    conn: &mut Connection,
    fencing: Option<&Fencing>,
    tables: &Tables,
    work: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    if let Some(fencing) = fencing {
        fencing.prepare(conn)?;
    } else {
        detach(conn)?;
    }
    let outcome = (|| {
        let txn = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(during("begin"))?;
        guard(fencing, &txn, tables)?;
        let out = work(&txn)?;
        txn.commit().map_err(during("commit"))?;
        Ok(out)
    })();
    // A cleanup error after a successful commit must not report an append as
    // failed after changing persistent state. The next write retries detach
    // before it starts a transaction; a retained attachment remains harmless.
    let _ = detach(conn);
    outcome
}

/// [`Fencing::check`] when a handle is fenced; nothing otherwise.
///
/// # Errors
///
/// As [`Fencing::check`].
pub(crate) fn guard(fencing: Option<&Fencing>, conn: &Connection, tables: &Tables) -> Result<()> {
    fencing.map_or(Ok(()), |fencing| fencing.check(conn, tables))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
