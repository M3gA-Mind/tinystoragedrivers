//! Cursor fingerprints and handle rendering.

use tinystoragedrivers_core::{ErrorKind, Filter, Sort};

use super::*;

#[test]
fn fingerprints_are_stable_and_query_specific() {
    let scope = Scope::local();
    let a = fingerprint("c", &scope, &Query::all()).unwrap();
    assert_eq!(
        a,
        fingerprint("c", &scope, &Query::all().limit(5)).unwrap(),
        "limit does not matter"
    );
    assert_ne!(a, fingerprint("d", &scope, &Query::all()).unwrap());
    assert_ne!(
        a,
        fingerprint("c", &scope, &Query::filter(Filter::eq("x", 1))).unwrap()
    );
    assert_ne!(
        a,
        fingerprint("c", &scope, &Query::all().sort(Sort::asc("x"))).unwrap()
    );
    assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
}

#[test]
fn rejects_foreign_cursors() {
    let scope = Scope::local();
    let query = Query::all().after(Cursor("mem:keyset1:0000000000000000:{}".into()));
    assert_eq!(
        parse_cursor("c", &scope, &query).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(parse_cursor("c", &scope, &Query::all()).unwrap(), None);
}

#[test]
fn handles_render_their_scope_and_file() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(&dir.path().join("d.db")).unwrap();
    let docs = SqliteDocuments::new(
        db,
        Arc::new(Tables::new()),
        Scope::local(),
        Arc::new(|| 0),
        None,
        false,
    );
    let shown = format!("{docs:?}");
    assert!(shown.contains("local") && shown.contains("d.db"), "{shown}");
}

#[tokio::test]
async fn scope_generation_and_payload_share_one_read_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("snapshot.db");
    let db = Db::open(&path).unwrap();
    let tables = Arc::new(Tables::new());
    db.run_now(|conn| tables.ensure(conn)).unwrap();
    let scope = Scope::new("snapshot_reader").unwrap();
    let docs = SqliteDocuments::new(db, tables, scope, Arc::new(|| 0), None, false);
    docs.put(
        "records",
        "one",
        serde_json::json!({"source":"old"}),
        Precondition::None,
    )
    .await
    .unwrap();
    let found = docs.with(Access::Read, move |conn, ctx| {
        // A separate WAL connection commits reset/recreation after the
        // handle's generation check, before its payload SELECT.
        let writer = Connection::open(path).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        writer.execute(&format!("UPDATE {} SET body=?1 WHERE scope=?2", crate::sql::ident(&ctx.tables.docs)),
            rusqlite::params![r#"{"source":"new"}"#, ctx.scope]).unwrap();
        writer.execute(&format!("INSERT INTO {} (scope,generation,operation) VALUES (?1,1,'reset_snapshot')", crate::sql::ident(&ctx.tables.admissions)),
            [ctx.scope]).unwrap();
        writer.execute_batch("COMMIT").unwrap();
        ops::get(conn, ctx, "records", "one")
    }).await.unwrap().unwrap();
    assert_eq!(found.doc, serde_json::json!({"source":"old"}));
    assert_eq!(
        docs.get("records", "one").await.unwrap_err().kind(),
        ErrorKind::ScopeGenerationRejected
    );
}
