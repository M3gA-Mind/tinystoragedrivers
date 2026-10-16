//! Bodies over 4 KiB are stored zstd-compressed, and nothing else notices:
//! queries, sorts, cursors, unique indexes, claims, deletes and search answer
//! exactly as the memory driver does, over a mix of small and large documents.

#![allow(
    clippy::unwrap_used,
    clippy::too_many_lines,
    clippy::print_stderr,
    reason = "test helpers report a failed call by panicking"
)]

use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    Checkpoint, CollectionSpec, DocumentStore, ErrorKind, Filter, IndexSpec, MemoryStorage,
    Precondition, Query, Scope, Sort, StorageBackend,
};
use tinystoragedrivers_sqlite::SqliteStorage;

/// Serialized JSON longer than this is stored compressed.
const THRESHOLD: usize = 4096;

fn big_text(seed: usize) -> String {
    format!("record {seed} filler text, highly repetitive. ").repeat(500)
}

fn raw_body(file: &Path, id: &str) -> (String, Vec<u8>) {
    let conn = Connection::open(file).unwrap();
    conn.query_row(
        "SELECT typeof(body), CAST(body AS BLOB) FROM docs WHERE id = ?1",
        [id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

#[tokio::test]
async fn a_large_body_round_trips_and_is_stored_as_a_compressed_blob() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("all.db");
    let storage = SqliteStorage::open(&file).unwrap();
    let docs = storage.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let big = json!({"pad": big_text(1), "n": 1});
    assert!(serde_json::to_string(&big).unwrap().len() > 20_000);
    docs.put("things", "big", big.clone(), Precondition::None)
        .await
        .unwrap();
    docs.put("things", "small", json!({"n": 2}), Precondition::None)
        .await
        .unwrap();

    assert_eq!(docs.get("things", "big").await.unwrap().unwrap().doc, big);
    let (kind, bytes) = raw_body(&file, "big");
    assert_eq!(kind, "blob");
    assert_eq!(bytes[0], 1, "marker byte");
    assert_eq!(&bytes[1..5], &[0x28, 0xB5, 0x2F, 0xFD], "zstd frame magic");
    assert!(bytes.len() < 2000, "{} bytes", bytes.len());
    let (kind, bytes) = raw_body(&file, "small");
    assert_eq!(kind, "text");
    assert_eq!(bytes, br#"{"n":2}"#);
}

#[tokio::test]
async fn the_threshold_is_four_kib_of_serialized_json() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("all.db");
    let storage = SqliteStorage::open(&file).unwrap();
    let docs = storage.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let overhead = serde_json::to_string(&json!({"p": ""})).unwrap().len();
    for (id, len) in [
        ("at", THRESHOLD - overhead),
        ("over", THRESHOLD - overhead + 1),
    ] {
        let doc = json!({"p": "a".repeat(len)});
        docs.put("things", id, doc.clone(), Precondition::None)
            .await
            .unwrap();
        assert_eq!(docs.get("things", id).await.unwrap().unwrap().doc, doc);
    }
    assert_eq!(raw_body(&file, "at").0, "text");
    assert_eq!(raw_body(&file, "over").0, "blob");
}

fn spec() -> CollectionSpec {
    CollectionSpec::new("jobs")
        .index(IndexSpec::new("by_state", ["state"]))
        .index(IndexSpec::new("by_rank", ["rank"]))
        .index(IndexSpec::new("by_key", ["key"]).unique())
        .index(IndexSpec::new("by_owner", ["owner.name"]))
        .searchable(["title"])
}

fn job(n: usize) -> Value {
    let state = ["queued", "running", "done"][n % 3];
    let mut doc = json!({
        "key": format!("k{n}"),
        "state": state,
        "n": n % 11,
        "done": n % 3 == 2,
        "title": if n.is_multiple_of(2) { "alpha report" } else { "beta summary" },
        "owner": {"name": format!("o{}", n % 4)},
    });
    if !n.is_multiple_of(5) {
        doc["rank"] = json!(n % 7);
    }
    if n.is_multiple_of(3) {
        // Large: well over the compression threshold.
        doc["pad"] = json!(big_text(n));
    }
    doc
}

/// Memory driver and SQLite, driven identically.
struct Pair {
    memory: Arc<dyn DocumentStore>,
    sqlite: Arc<dyn DocumentStore>,
    _storages: (MemoryStorage, SqliteStorage),
    _dir: tempfile::TempDir,
}

async fn pair() -> Pair {
    let dir = tempfile::tempdir().unwrap();
    let sqlite_storage = SqliteStorage::open(dir.path().join("all.db")).unwrap();
    let memory_storage = MemoryStorage::new();
    let sqlite = sqlite_storage.for_scope(&Scope::local()).unwrap();
    let memory = memory_storage.for_scope(&Scope::local()).unwrap();
    let pair = Pair {
        memory: Arc::clone(memory.documents()),
        sqlite: Arc::clone(sqlite.documents()),
        _storages: (memory_storage, sqlite_storage),
        _dir: dir,
    };
    for store in [&pair.memory, &pair.sqlite] {
        store.ensure_collection(&spec()).await.unwrap();
        for n in 0..60 {
            store
                .put("jobs", &format!("j{n:02}"), job(n), Precondition::None)
                .await
                .unwrap();
        }
    }
    pair
}

/// Id, version and body of every page item, following cursors.
async fn pages(store: &dyn DocumentStore, query: Query) -> Vec<(String, u64, Value)> {
    let mut out = Vec::new();
    let mut query = query;
    loop {
        let page = store.query("jobs", &query).await.unwrap();
        out.extend(
            page.items
                .into_iter()
                .map(|item| (item.id, item.version.0, item.doc)),
        );
        match page.next {
            Some(cursor) => query = query.after(cursor),
            None => return out,
        }
    }
}

async fn same_query(pair: &Pair, query: Query) -> usize {
    let memory = pages(pair.memory.as_ref(), query.clone()).await;
    let sqlite = pages(pair.sqlite.as_ref(), query.clone()).await;
    assert_eq!(sqlite, memory, "{query:?}");
    sqlite.len()
}

#[tokio::test]
async fn queries_over_mixed_sizes_match_the_memory_driver() {
    let pair = pair().await;
    let owner = |name: &str| Filter::eq("owner.name", name);
    let filters = vec![
        Filter::All,
        Filter::eq("state", "queued"),
        Filter::one_of("state", ["queued", "done"]),
        Filter::eq("done", true),
        owner("o1"),
        Filter::eq("state", "running").and(owner("o2")),
        Filter::eq("key", "k33"),
        Filter::eq("rank", 3),
        Filter::gte("rank", 2).and(Filter::lt("rank", 5)),
        Filter::eq("n", 4),
        Filter::eq("state", "done").and(Filter::gt("n", 5)),
        Filter::eq("state", "queued").or(Filter::eq("state", "done")),
        Filter::eq("rank", Value::Null),
        Filter::exists("rank", false),
        Filter::eq("state", "nonexistent"),
    ];
    let mut nonempty = 0;
    for filter in filters {
        let sorts: [Vec<Sort>; 5] = [
            vec![],
            vec![Sort::asc("rank")],
            vec![Sort::desc("rank"), Sort::asc("state")],
            vec![Sort::asc("n"), Sort::desc("id")],
            vec![Sort::desc("owner.name"), Sort::asc("key")],
        ];
        for sort in sorts {
            for limit in [None, Some(7)] {
                let mut query = Query::filter(filter.clone());
                for order in &sort {
                    query = query.sort(order.clone());
                }
                if let Some(limit) = limit {
                    query = query.limit(limit);
                }
                nonempty += usize::from(same_query(&pair, query).await > 0);
            }
        }
        assert_eq!(
            pair.sqlite.count("jobs", &filter).await.unwrap(),
            pair.memory.count("jobs", &filter).await.unwrap(),
            "{filter:?}"
        );
    }
    assert!(nonempty > 100);
}

#[tokio::test]
async fn search_claim_unique_and_deletes_match_the_memory_driver() {
    let pair = pair().await;
    for store in [&pair.memory, &pair.sqlite] {
        // Unique index over a large document.
        let clash = store
            .put("jobs", "dup", job(30), Precondition::None)
            .await
            .unwrap_err();
        assert_eq!(clash.kind(), ErrorKind::AlreadyExists);
        // Rewriting a document with its own key is fine, and moves its state.
        let mut moved = job(33);
        moved["state"] = json!("running");
        store
            .put("jobs", "j33", moved, Precondition::None)
            .await
            .unwrap();
    }
    same_query(&pair, Query::filter(Filter::eq("state", "running"))).await;
    same_query(&pair, Query::filter(Filter::eq("state", "queued"))).await;

    let mut ids = Vec::new();
    for store in [&pair.memory, &pair.sqlite] {
        let mut hits: Vec<String> = store
            .search("jobs", "alpha", 100)
            .await
            .unwrap()
            .into_iter()
            .map(|hit| hit.id)
            .collect();
        hits.sort();
        ids.push(hits);
    }
    assert_eq!(ids[0], ids[1]);
    assert_ne!(ids[0].len(), 0);

    let mut claimed = Vec::new();
    for store in [&pair.memory, &pair.sqlite] {
        let found = store
            .claim(
                "jobs",
                &Filter::eq("state", "queued"),
                &[Sort::desc("rank"), Sort::asc("id")],
                &json!({"state": "claimed"}),
            )
            .await
            .unwrap()
            .unwrap();
        claimed.push((found.id, found.version.0, found.doc));
    }
    assert_eq!(claimed[0], claimed[1]);
    same_query(&pair, Query::filter(Filter::eq("state", "claimed"))).await;
    same_query(&pair, Query::filter(Filter::eq("state", "queued"))).await;

    let removed = (
        pair.memory
            .delete_where("jobs", &Filter::eq("state", "done"))
            .await
            .unwrap(),
        pair.sqlite
            .delete_where("jobs", &Filter::eq("state", "done"))
            .await
            .unwrap(),
    );
    assert_eq!(removed.0, removed.1);
    assert!(removed.0 > 0);
    for store in [&pair.memory, &pair.sqlite] {
        assert!(
            store
                .delete("jobs", "j00", Precondition::None)
                .await
                .unwrap()
        );
    }
    same_query(&pair, Query::all()).await;
    same_query(&pair, Query::filter(Filter::eq("state", "done"))).await;
    // The deleted key is free again.
    for store in [&pair.memory, &pair.sqlite] {
        store
            .put("jobs", "again", job(0), Precondition::Absent)
            .await
            .unwrap();
    }
    same_query(&pair, Query::filter(Filter::eq("key", "k0"))).await;
}

#[tokio::test]
async fn compressible_documents_take_a_fraction_of_their_raw_size() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("all.db");
    let storage = SqliteStorage::open(&file).unwrap();
    let docs = storage.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let mut raw = 0_u64;
    for n in 0..1000 {
        let doc = json!({"n": n, "pad": big_text(n)});
        raw += serde_json::to_string(&doc).unwrap().len() as u64;
        docs.put("things", &format!("d{n}"), doc, Precondition::None)
            .await
            .unwrap();
    }
    storage.checkpoint(Checkpoint::Truncate).await.unwrap();
    let stored = std::fs::metadata(&file).unwrap().len();
    eprintln!("raw {raw} bytes, stored {stored} bytes");
    assert!(stored * 10 < raw, "raw {raw}, stored {stored}");
}

/// A layout-2 file written before compression: bodies are plain text, indexes
/// are expressions over `body`, and there is no side table.
#[tokio::test]
async fn a_layout_two_file_without_the_side_table_is_upgraded_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("all.db");
    drop(SqliteStorage::open(&file).unwrap());
    {
        let conn = Connection::open(&file).unwrap();
        conn.execute_batch("DROP TABLE doc_fields").unwrap();
        let spec = CollectionSpec::new("jobs").index(IndexSpec::new("by_state", ["state"]));
        conn.execute(
            "INSERT INTO _tsd_collections (coll, spec) VALUES ('jobs', ?1)",
            [serde_json::to_string(&spec).unwrap()],
        )
        .unwrap();
        conn.execute_batch(
            "CREATE INDEX tsd_ix_jobs_by_state ON docs (scope, json_extract(body, '$.\"state\"'))
                 WHERE coll = 'jobs'",
        )
        .unwrap();
        for (id, state) in [("a", "queued"), ("b", "done"), ("c", "queued")] {
            let body = json!({"state": state, "pad": big_text(1)}).to_string();
            conn.execute(
                "INSERT INTO docs (scope, coll, id, version, schema, created_ms, updated_ms, expires_ms, deleted, body)
                 VALUES ('local', 'jobs', ?1, 1, 0, 5, 5, NULL, 0, ?2)",
                rusqlite::params![id, body],
            )
            .unwrap();
        }
    }
    let storage = SqliteStorage::open(&file).unwrap();
    let docs = storage.for_scope(&Scope::local()).unwrap();
    let docs = docs.documents();
    let queued = docs
        .query("jobs", &Query::filter(Filter::eq("state", "queued")))
        .await
        .unwrap();
    let ids: Vec<_> = queued.items.iter().map(|item| item.id.as_str()).collect();
    assert_eq!(ids, ["a", "c"]);
    assert_eq!(queued.items[0].doc["pad"], big_text(1));
    // Rewrites compress, and the index keeps following them.
    docs.put(
        "jobs",
        "a",
        json!({"state": "done", "pad": big_text(2)}),
        Precondition::None,
    )
    .await
    .unwrap();
    assert_eq!(raw_body(&file, "a").0, "blob");
    let done = docs
        .query("jobs", &Query::filter(Filter::eq("state", "done")))
        .await
        .unwrap();
    assert_eq!(done.items.len(), 2);
    let conn = Connection::open(&file).unwrap();
    let stale: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'tsd_ix_jobs_by_state'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stale, 0, "the body-expression index is dropped");
}
