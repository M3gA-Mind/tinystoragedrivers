//! A file written by tinystoragedrivers v0.11.0 upgrades in place on open.
//!
//! The fixture is built here with the v0.11.0 table declarations copied
//! verbatim (`git show v0.11.0:crates/tinystoragedrivers-sqlite/src/sql/mod.rs`)
//! and rows for every table, so no binary file is checked in.

#![allow(
    clippy::unwrap_used,
    reason = "test helpers report a failed call by panicking"
)]

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    CollectionSpec, ErrorKind, Filter, IndexSpec, Precondition, Query, Scope, ScopedStorage,
    StorageBackend, Version,
};
use tinystoragedrivers_sqlite::SqliteStorage;

/// The v0.11.0 declarations of the tables.
fn v0_ddl() -> &'static str {
    "CREATE TABLE IF NOT EXISTS \"_tsd_scope_admissions\" (
             scope TEXT PRIMARY KEY,
             generation INTEGER NOT NULL,
             operation TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS \"_tsd_purge_receipts\" (
             scope TEXT NOT NULL,
             operation TEXT NOT NULL,
             generation INTEGER NOT NULL,
             PRIMARY KEY (scope, operation)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS \"_tsd_docs\" (
             scope TEXT NOT NULL,
             coll TEXT NOT NULL,
             id TEXT NOT NULL,
             version INTEGER NOT NULL,
             doc TEXT NOT NULL,
             PRIMARY KEY (scope, coll, id)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS \"_tsd_collections\" (
             coll TEXT PRIMARY KEY,
             spec TEXT NOT NULL
         ) WITHOUT ROWID;
         CREATE VIRTUAL TABLE IF NOT EXISTS \"_tsd_fts\" USING fts5(
             scope UNINDEXED, coll UNINDEXED, id UNINDEXED, body,
             tokenize = 'unicode61'
         );
         CREATE TABLE IF NOT EXISTS \"_tsd_streams\" (
             scope TEXT NOT NULL,
             stream TEXT NOT NULL,
             seq INTEGER NOT NULL,
             value TEXT NOT NULL,
             PRIMARY KEY (scope, stream, seq)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS \"_tsd_stream_meta\" (
             scope TEXT NOT NULL,
             stream TEXT NOT NULL,
             base INTEGER NOT NULL,
             len INTEGER NOT NULL,
             PRIMARY KEY (scope, stream)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS \"_tsd_blobs\" (
             scope TEXT NOT NULL,
             key TEXT NOT NULL,
             content_type TEXT,
             bytes BLOB NOT NULL,
             expires_at_ms INTEGER,
             PRIMARY KEY (scope, key)
         ) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS \"_tsd_tombstones\" (
             scope TEXT NOT NULL,
             coll TEXT NOT NULL,
             id TEXT NOT NULL,
             version INTEGER NOT NULL,
             PRIMARY KEY (scope, coll, id)
         ) WITHOUT ROWID;"
}

const LOCAL: &str = "local";
const BOB: &str = "u:bob";
const FAR_FUTURE_MS: i64 = 4_102_444_800_000;

/// Populate a v0.11.0 file: two scopes, three collections, a tombstone, a
/// three-entry stream, two blobs and a searchable collection.
fn write_v0_file(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(v0_ddl()).unwrap();
    let notes = CollectionSpec::new("notes")
        .index(IndexSpec::new("by_title", ["title"]))
        .searchable(["title"]);
    let sessions = CollectionSpec::new("sessions").ttl("expires");
    for spec in [&notes, &sessions, &CollectionSpec::new("plain")] {
        conn.execute(
            "INSERT INTO _tsd_collections (coll, spec) VALUES (?1, ?2)",
            params![spec.name, serde_json::to_string(spec).unwrap()],
        )
        .unwrap();
    }
    // The index the v0.11.0 driver built when `notes` was declared.
    conn.execute_batch(
        "CREATE INDEX \"tsd_ix_notes_by_title\" ON \"_tsd_docs\"
             (scope, json_extract(doc, '$.\"title\"')) WHERE coll = 'notes'",
    )
    .unwrap();
    let docs = [
        (
            LOCAL,
            "notes",
            "n1",
            2,
            json!({"title": "alpha", "created_at": "2024-05-01T12:30:00Z"}),
        ),
        (
            LOCAL,
            "notes",
            "n2",
            1,
            json!({"title": "beta bravo", "created_ms": 1_700_000_000_000_i64}),
        ),
        (
            BOB,
            "notes",
            "n1",
            1,
            json!({"title": "alpha", "createdAt": "2024-05-01T14:30:00.250+02:00"}),
        ),
        (
            LOCAL,
            "sessions",
            "live",
            1,
            json!({"expires": FAR_FUTURE_MS}),
        ),
        (LOCAL, "sessions", "dead", 1, json!({"expires": 1})),
        (LOCAL, "plain", "p1", 3, json!({"n": 1})),
    ];
    for (scope, coll, id, version, doc) in docs {
        conn.execute(
            "INSERT INTO _tsd_docs (scope, coll, id, version, doc) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![scope, coll, id, version, doc.to_string()],
        )
        .unwrap();
        if coll == "notes" {
            conn.execute(
                "INSERT INTO _tsd_fts (scope, coll, id, body) VALUES (?1, ?2, ?3, ?4)",
                params![scope, coll, id, doc["title"].as_str().unwrap()],
            )
            .unwrap();
        }
    }
    // `gone` was deleted at version 4; `p1` carries a stale tombstone from an
    // earlier deletion that its re-creation (version 3) superseded.
    for (id, version) in [("gone", 4), ("p1", 2)] {
        conn.execute(
            "INSERT INTO _tsd_tombstones (scope, coll, id, version) VALUES ('local', 'plain', ?1, ?2)",
            params![id, version],
        )
        .unwrap();
    }
    for seq in 0..3 {
        conn.execute(
            "INSERT INTO _tsd_streams (scope, stream, seq, value) VALUES ('local', 'journal', ?1, ?2)",
            params![seq, json!({"n": seq}).to_string()],
        )
        .unwrap();
    }
    conn.execute_batch(&format!(
        "INSERT INTO _tsd_stream_meta (scope, stream, base, len) VALUES ('local', 'journal', 0, 3);
         INSERT INTO _tsd_blobs (scope, key, content_type, bytes, expires_at_ms)
             VALUES ('local', 'files/a', 'text/plain', x'68656c6c6f', NULL);
         INSERT INTO _tsd_blobs (scope, key, content_type, bytes, expires_at_ms)
             VALUES ('local', 'files/b', NULL, x'0001', {FAR_FUTURE_MS});"
    ))
    .unwrap();
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

async fn ids(scoped: &ScopedStorage, coll: &str, filter: Filter) -> Vec<String> {
    let page = scoped
        .documents()
        .query(coll, &Query::filter(filter))
        .await
        .unwrap();
    let mut ids: Vec<String> = page.items.into_iter().map(|item| item.id).collect();
    ids.sort();
    ids
}

/// Everything the fixture holds must read back through the ports.
async fn assert_upgraded(storage: &dyn StorageBackend, opened_after_ms: i64) {
    let local = storage.for_scope(&Scope::new(LOCAL).unwrap()).unwrap();
    let docs = local.documents();

    let n1 = docs.get("notes", "n1").await.unwrap().unwrap();
    assert_eq!(
        (n1.version, n1.doc.clone()),
        (
            Version(2),
            json!({"title": "alpha", "created_at": "2024-05-01T12:30:00Z"})
        )
    );
    assert_eq!(n1.meta.schema, 0);
    assert_eq!(n1.meta.created_ms, 1_714_566_600_000, "RFC 3339 created_at");
    assert_eq!(n1.meta.updated_ms, n1.meta.created_ms);
    assert_eq!(
        docs.get("notes", "n2")
            .await
            .unwrap()
            .unwrap()
            .meta
            .created_ms,
        1_700_000_000_000
    );
    let plain = docs.get("plain", "p1").await.unwrap().unwrap();
    assert_eq!(
        plain.version,
        Version(3),
        "a live row outranks its stale tombstone"
    );
    assert!(
        plain.meta.created_ms >= opened_after_ms,
        "no recognised field: the open time"
    );
    assert_eq!(
        ids(&local, "notes", Filter::eq("title", "alpha")).await,
        ["n1"]
    );
    assert_eq!(docs.search("notes", "bravo", 10).await.unwrap().len(), 1);

    let live = docs.get("sessions", "live").await.unwrap().unwrap();
    assert_eq!(
        live.meta.expires_ms,
        Some(FAR_FUTURE_MS),
        "the ttl field moved to expires_ms"
    );
    assert!(
        docs.get("sessions", "dead").await.unwrap().is_none(),
        "expired stays hidden"
    );
    assert_eq!(ids(&local, "sessions", Filter::All).await, ["live"]);

    assert!(
        docs.get("plain", "gone").await.unwrap().is_none(),
        "a tombstone reads as absent"
    );
    assert_eq!(ids(&local, "plain", Filter::All).await, ["p1"]);
    let reborn = docs
        .put("plain", "gone", json!({}), Precondition::Absent)
        .await
        .unwrap();
    assert_eq!(reborn, Version(5), "versions continue across the deletion");

    let bob = storage.for_scope(&Scope::new(BOB).unwrap()).unwrap();
    let bobs = bob.documents().get("notes", "n1").await.unwrap().unwrap();
    assert_eq!(
        bobs.meta.created_ms, 1_714_566_600_250,
        "offset and fraction"
    );
    assert_eq!(
        bob.documents()
            .search("notes", "alpha", 10)
            .await
            .unwrap()
            .len(),
        1
    );

    let streams = local.streams();
    assert_eq!(streams.len("journal").await.unwrap(), 3);
    let entries = streams.read_window("journal", 0, 10).await.unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| (entry.offset, entry.value.clone()))
            .collect::<Vec<_>>(),
        (0..3)
            .map(|n| (n, json!({"n": n})))
            .collect::<Vec<(u64, Value)>>()
    );
    assert_eq!(streams.append("journal", json!({"n": 3})).await.unwrap(), 3);

    let file = local.blobs().get("files/a").await.unwrap().unwrap();
    assert_eq!(
        (file.bytes, file.meta.content_type.as_deref()),
        (b"hello".to_vec(), Some("text/plain"))
    );
    assert_eq!(
        local.blobs().get("files/b").await.unwrap().unwrap().bytes,
        [0, 1]
    );
}

fn raw(path: &Path) -> Connection {
    Connection::open(path).unwrap()
}

#[tokio::test]
async fn a_v0_11_file_upgrades_in_place_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.db");
    write_v0_file(&path);
    let before = now_ms();

    let storage = SqliteStorage::open(&path).unwrap();
    let conn = raw(&path);
    let (deleted, body, version): (i64, String, i64) = conn
        .query_row(
            "SELECT deleted, body, version FROM docs WHERE id = 'gone'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (deleted, body.as_str(), version),
        (1, "{}", 4),
        "a tombstone becomes a deleted row"
    );
    assert_upgraded(&storage, before).await;

    let layout: String = conn
        .query_row("SELECT value FROM meta WHERE key = 'layout'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(layout, "2");
    let leftovers: i64 = conn
        .query_row("SELECT count(*) FROM sqlite_schema WHERE name LIKE '%\\_tsd\\_docs' ESCAPE '\\' OR name IN ('_tsd_tombstones', '_tsd_streams', '_tsd_stream_meta', '_tsd_blobs', '_tsd_fts')", [], |r| r.get(0))
        .unwrap();
    assert_eq!(leftovers, 0, "the v0 tables are gone");
    let (size, sha): (i64, Vec<u8>) = conn
        .query_row(
            "SELECT size, sha256 FROM blobs WHERE key = 'files/a'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((size, sha.len()), (5, 32));
    let body_indexes: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND name LIKE 'tsd\\_ix\\_%' ESCAPE '\\'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(body_indexes, 0, "the v0 expression index is not recreated");
    let titles: i64 = conn
        .query_row(
            "SELECT count(*) FROM doc_fields WHERE coll = 'notes' AND path = 'title' AND kind = 3",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(titles >= 2, "declared fields are projected: {titles}");
    let lookup: i64 = conn
        .query_row("SELECT count(*) FROM sqlite_schema WHERE type = 'index' AND name = 'doc_fields_lookup'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(lookup, 1);
}

#[tokio::test]
async fn the_upgrade_is_idempotent_and_survives_reopening() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.db");
    write_v0_file(&path);
    let before = now_ms();
    drop(SqliteStorage::open(&path).unwrap());
    let created: i64 = raw(&path)
        .query_row("SELECT created_ms FROM docs WHERE id = 'p1'", [], |r| {
            r.get(0)
        })
        .unwrap();

    let again = SqliteStorage::open(&path).unwrap();
    let p1 = again
        .for_scope(&Scope::local())
        .unwrap()
        .documents()
        .get("plain", "p1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p1.meta.created_ms, created, "a second open changes nothing");
    assert!(created >= before);
    drop(again);
}

#[tokio::test]
async fn a_layout_two_file_with_v0_leftovers_is_refused_as_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.db");
    drop(SqliteStorage::open(&path).unwrap());
    raw(&path)
        .execute_batch("CREATE TABLE _tsd_docs (scope TEXT)")
        .unwrap();
    let error = SqliteStorage::open(&path).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(error.to_string().contains("corrupt"), "{error}");
}

#[tokio::test]
async fn a_failed_upgrade_rolls_back_to_the_v0_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.db");
    write_v0_file(&path);
    // A body that is not JSON cannot be copied.
    raw(&path)
        .execute("UPDATE _tsd_docs SET doc = 'not json' WHERE id = 'p1'", [])
        .unwrap();
    assert!(SqliteStorage::open(&path).is_err());
    let conn = raw(&path);
    let v0: i64 = conn
        .query_row("SELECT count(*) FROM _tsd_docs", [], |r| r.get(0))
        .unwrap();
    let v2: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name IN ('docs', 'meta')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        (v0, v2),
        (6, 0),
        "nothing of the upgrade survives a failure"
    );
}

#[tokio::test]
async fn the_upgrade_writes_large_bodies_compressed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.db");
    write_v0_file(&path);
    let big = json!({"title": "big", "pad": "lorem ipsum ".repeat(2000)});
    Connection::open(&path)
        .unwrap()
        .execute(
            "INSERT INTO _tsd_docs (scope, coll, id, version, doc) VALUES (?1, 'notes', 'big', 1, ?2)",
            params![LOCAL, big.to_string()],
        )
        .unwrap();
    let storage = SqliteStorage::open(&path).unwrap();
    let (kind, bytes): (String, i64) = raw(&path)
        .query_row(
            "SELECT typeof(body), length(body) FROM docs WHERE id = 'big'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(kind, "blob");
    assert!(bytes < 1000, "{bytes}");
    let docs = storage.for_scope(&Scope::local()).unwrap();
    assert_eq!(
        docs.documents()
            .get("notes", "big")
            .await
            .unwrap()
            .unwrap()
            .doc,
        big
    );
}
