//! Space reclamation and WAL checkpoints.
//!
//! # Auto-vacuum
//!
//! SQLite only returns freed pages to the filesystem when the database was
//! created with `auto_vacuum` on; the setting is fixed once the first table
//! exists. [`Db::open`](crate::connection::Db::open) therefore sets
//! `auto_vacuum=INCREMENTAL` before it does anything else to a new file, and
//! [`maintain`] hands freed pages back with `PRAGMA incremental_vacuum(N)`.
//!
//! A file created before this driver set the pragma has `auto_vacuum=NONE`.
//! The first [`maintain`] call switches it to `INCREMENTAL` with a one-time
//! `VACUUM`, the only way SQLite applies the change. **That rewrites the whole
//! file**: it needs free disk space about the size of the database, holds the
//! write lock for the duration, and ignores the budget (other than a zero
//! budget, which does nothing). Schedule the first call for an idle moment.
//! Every later call is incremental and bounded by the budget.
//!
//! # WAL
//!
//! The database runs in WAL mode, so freed pages leave the file only when a
//! checkpoint copies the log back. [`maintain`] ends with a passive
//! checkpoint for that; [`checkpoint`] exposes the modes to hosts.

use rusqlite::Connection;
use tinystoragedrivers_core::{
    Checkpoint, MaintenanceBudget, MaintenanceReport, Result, StorageError,
};

use crate::error::during;

fn read(conn: &Connection, pragma: &'static str) -> Result<u64> {
    let value: i64 = conn
        .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
        .map_err(during("read pragma"))?;
    Ok(u64::try_from(value).unwrap_or(0))
}

/// Release up to `pages` free pages. SQLite frees one page per step of the
/// statement, so the rows have to be drained; `execute_batch` stops early.
fn free_pages(conn: &Connection, pages: u64) -> Result<()> {
    let mut statement = conn
        .prepare(&format!("PRAGMA incremental_vacuum({pages})"))
        .map_err(during("incremental vacuum"))?;
    let mut rows = statement.query([]).map_err(during("incremental vacuum"))?;
    while rows.next().map_err(during("incremental vacuum"))?.is_some() {}
    Ok(())
}

/// Return freed pages to the filesystem, within `budget`.
pub(crate) fn maintain(conn: &Connection, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
    if budget.max_reclaim_bytes == Some(0) {
        return Ok(MaintenanceReport::default());
    }
    let page_size = read(conn, "page_size")?.max(1);
    let pages_before = read(conn, "page_count")?;
    // 0 is `NONE`; 1 (`FULL`) and 2 (`INCREMENTAL`) already return space.
    if read(conn, "auto_vacuum")? == 0 {
        conn.pragma_update(None, "auto_vacuum", "INCREMENTAL")
            .map_err(during("enable incremental auto-vacuum"))?;
        conn.execute_batch("VACUUM").map_err(during("vacuum"))?;
    } else {
        let free = read(conn, "freelist_count")?;
        let pages = budget
            .max_reclaim_bytes
            .map_or(free, |bytes| free.min(bytes / page_size));
        if pages > 0 {
            free_pages(conn, pages)?;
        }
    }
    checkpoint(conn, Checkpoint::Passive)?;
    let pages_after = read(conn, "page_count")?;
    Ok(MaintenanceReport::new(
        pages_before
            .saturating_sub(pages_after)
            .saturating_mul(page_size),
    ))
}

/// Flush the write-ahead log in `mode`.
pub(crate) fn checkpoint(conn: &Connection, mode: Checkpoint) -> Result<()> {
    let (pragma, wait) = match mode {
        Checkpoint::Passive => ("PRAGMA wal_checkpoint(PASSIVE)", false),
        Checkpoint::Truncate => ("PRAGMA wal_checkpoint(TRUNCATE)", true),
        _ => return Err(StorageError::invalid_input("unknown checkpoint mode")),
    };
    // The row is (busy, log frames, frames checkpointed); `busy` is 1 when a
    // reader or writer kept the checkpoint from finishing.
    let busy: i64 = conn
        .query_row(pragma, [], |row| row.get(0))
        .map_err(during("checkpoint"))?;
    if wait && busy != 0 {
        return Err(StorageError::unavailable(
            "sqlite could not truncate the write-ahead log while it was in use",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
