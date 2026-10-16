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
//! A file created before this driver set the pragma (every v0.x file, which
//! the layout-2 upgrade carries over unchanged) has `auto_vacuum=NONE`.
//! SQLite applies the change only through a full `VACUUM`, which cannot run
//! inside the upgrade transaction, so the first [`maintain`] that can afford
//! it does the one-time conversion. **That rewrites the whole file**: it needs
//! free disk space about the size of the database and holds the write lock for
//! the duration, so it is never run under a bound that is smaller than the
//! database. With a smaller bound (but not zero) the call reclaims nothing,
//! reports `0` and leaves the conversion pending; pass
//! `MaintenanceBudget::unlimited()`, or a budget of at least the database
//! size, from an idle moment to run it. Every later call is incremental and
//! bounded by the budget.
//!
//! # WAL
//!
//! The database runs in WAL mode, so freed pages leave the file only when a
//! checkpoint copies the log back. [`maintain`] ends with a passive
//! checkpoint for that; [`checkpoint`] exposes the modes to hosts. A passive
//! checkpoint does not wait for readers, so the report counts only the bytes
//! the file actually lost.

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

/// The length of the main database file, when the connection has one.
fn file_len(conn: &Connection) -> Option<u64> {
    let path = conn.path().filter(|path| !path.is_empty())?;
    std::fs::metadata(path).ok().map(|meta| meta.len())
}

/// Whether `budget` is large enough to pay for rewriting a database of
/// `bytes`: the one-time `VACUUM` can reclaim no more than the file holds.
fn covers(budget: MaintenanceBudget, bytes: u64) -> bool {
    budget.max_reclaim_bytes.is_none_or(|limit| limit >= bytes)
}

/// Return freed pages to the filesystem, within `budget`.
///
/// `reclaimed_bytes` is how much the database *file* shrank, measured around a
/// checkpoint, so pages that are free in the log but not yet given back (a
/// reader kept the checkpoint from finishing) are not counted. They show up in
/// the call that finally completes the checkpoint.
pub(crate) fn maintain(conn: &Connection, budget: MaintenanceBudget) -> Result<MaintenanceReport> {
    if budget.max_reclaim_bytes == Some(0) {
        return Ok(MaintenanceReport::default());
    }
    let page_size = read(conn, "page_size")?.max(1);
    // Finishing an earlier checkpoint can shrink the file, and catching the
    // file up with the log can grow it. The largest length seen before the
    // work starts is what this call is measured against, so the first
    // counts as reclaimed here and the second does not.
    let len_pending = file_len(conn);
    checkpoint(conn, Checkpoint::Passive)?;
    let pages_before = read(conn, "page_count")?;
    let len_before = len_pending.max(file_len(conn));
    // 0 is `NONE`; 1 (`FULL`) and 2 (`INCREMENTAL`) already return space.
    if read(conn, "auto_vacuum")? == 0 {
        if !covers(budget, pages_before.saturating_mul(page_size)) {
            // Conversion pending: report nothing reclaimed; a larger budget
            // (or an unlimited one) runs it.
            return Ok(MaintenanceReport::default());
        }
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
    let reclaimed = match (len_before, file_len(conn)) {
        (Some(before), Some(after)) => before.saturating_sub(after),
        _ => pages_before
            .saturating_sub(read(conn, "page_count")?)
            .saturating_mul(page_size),
    };
    Ok(MaintenanceReport::new(reclaimed))
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
