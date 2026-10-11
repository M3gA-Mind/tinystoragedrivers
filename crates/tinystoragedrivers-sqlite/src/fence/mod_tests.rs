//! Unit tests for the SQLite fence check.

use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, Filter, Precondition, Scope};

use super::*;

fn conn() -> (Connection, Tables) {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("");
    tables.ensure(&conn).unwrap();
    (conn, tables)
}

fn fencing(epoch: u64) -> Fencing {
    Fencing::new(
        Fence::epoch(
            Scope::new("cluster").unwrap(),
            "leases",
            "l1",
            "epoch",
            epoch,
        ),
        Arc::new(|| 1_000),
    )
}

#[test]
fn an_unfenced_handle_always_passes() {
    let (conn, tables) = conn();
    guard(None, &conn, &tables).unwrap();
}

#[test]
fn checks_the_guard_in_its_own_scope() {
    let (conn, tables) = conn();
    let fenced = fencing(7);
    assert_eq!(
        guard(Some(&fenced), &conn, &tables).unwrap_err().kind(),
        ErrorKind::Fenced
    );
    let ctx = Ctx {
        tables: &tables,
        scope: "cluster",
        now_ms: 1_000,
    };
    ops::put(
        &conn,
        ctx,
        "leases",
        "l1",
        &json!({"epoch": 7}),
        Precondition::None,
    )
    .unwrap();
    guard(Some(&fenced), &conn, &tables).unwrap();
    assert_eq!(
        fencing(6).check(&conn, &tables).unwrap_err().kind(),
        ErrorKind::Fenced
    );
    let rendered = format!("{fenced:?}");
    assert!(rendered.contains("leases"), "{rendered}");
}

#[test]
fn an_invalid_guard_address_is_invalid_input() {
    let (conn, tables) = conn();
    let fenced = Fencing::new(
        Fence::new(Scope::local(), "leases", "", Filter::All),
        Arc::new(|| 0),
    );
    assert_eq!(
        fenced.check(&conn, &tables).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn attached_guard_is_locked_through_the_target_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let target_path = dir.path().join("target.db");
    let guard_path = dir.path().join("guard.db");
    let mut target = Connection::open(&target_path).unwrap();
    let guard = Connection::open(&guard_path).unwrap();
    target.pragma_update(None, "journal_mode", "WAL").unwrap();
    guard.pragma_update(None, "journal_mode", "WAL").unwrap();
    let tables = Arc::new(Tables::new(""));
    tables.ensure(&guard).unwrap();
    tables.ensure(&target).unwrap();
    let ctx = Ctx {
        tables: &tables,
        scope: "cluster",
        now_ms: 1000,
    };
    ops::put(
        &guard,
        ctx,
        "leases",
        "l1",
        &json!({"epoch": 7}),
        Precondition::None,
    )
    .unwrap();
    let fence = Fencing::in_database(
        fencing(7).fence,
        Arc::new(|| 1000),
        Some(guard_path.clone()),
        Arc::clone(&tables),
    );
    fence.prepare(&target).unwrap();
    let txn = target
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    fence.check(&txn, &tables).unwrap();
    // A physically independent connection cannot take over while the target
    // transaction still holds the guard database's native write lock.
    let mut takeover = Connection::open(&guard_path).unwrap();
    takeover.busy_timeout(std::time::Duration::ZERO).unwrap();
    let error = takeover
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap_err();
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(code, _) if code.code == rusqlite::ErrorCode::DatabaseBusy)
    );
    txn.commit().unwrap();
    let txn = takeover
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    ops::put(
        &txn,
        ctx,
        "leases",
        "l1",
        &json!({"epoch": 8}),
        Precondition::None,
    )
    .unwrap();
    txn.commit().unwrap();
    let txn = target
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    assert_eq!(
        fence.check(&txn, &tables).unwrap_err().kind(),
        ErrorKind::Fenced
    );
}

#[test]
fn cross_database_write_cleans_up_attachments_on_success_and_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let guard_path = dir.path().join("guard.db");
    let guard = Connection::open(&guard_path).unwrap();
    let tables = Arc::new(Tables::new(""));
    tables.ensure(&guard).unwrap();
    let ctx = Ctx {
        tables: &tables,
        scope: "cluster",
        now_ms: 1000,
    };
    ops::put(
        &guard,
        ctx,
        "leases",
        "l1",
        &json!({"epoch": 7}),
        Precondition::None,
    )
    .unwrap();
    let mut target = Connection::open(dir.path().join("target.db")).unwrap();
    tables.ensure(&target).unwrap();
    let fence = Fencing::in_database(
        fencing(7).fence,
        Arc::new(|| 1000),
        Some(guard_path),
        Arc::clone(&tables),
    );
    assert_eq!(
        write(&mut target, Some(&fence), &tables, |_| Ok(42)).unwrap(),
        42
    );
    let attached = |conn: &Connection| {
        conn.query_row(
            "SELECT COUNT(*) FROM pragma_database_list WHERE name = 'tsd_fence_guard'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
    };
    assert_eq!(attached(&target), 0);
    ops::put(
        &guard,
        ctx,
        "leases",
        "l1",
        &json!({"epoch": 8}),
        Precondition::None,
    )
    .unwrap();
    assert_eq!(
        write(&mut target, Some(&fence), &tables, |_| Ok(42))
            .unwrap_err()
            .kind(),
        ErrorKind::Fenced
    );
    assert_eq!(attached(&target), 0);
}
