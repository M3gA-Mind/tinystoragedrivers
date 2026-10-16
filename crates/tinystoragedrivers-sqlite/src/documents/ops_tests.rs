//! Document operations directly on a connection.

use serde_json::json;
use tinystoragedrivers_core::{ErrorKind, Filter, IndexSpec, Sort};

use super::*;

fn setup() -> (Connection, Tables) {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("");
    tables.ensure(&conn).unwrap();
    (conn, tables)
}

fn ctx(tables: &Tables) -> Ctx<'_> {
    Ctx {
        tables,
        scope: "local",
        now_ms: 0,
    }
}

#[test]
fn an_exhausted_version_fails_the_write() {
    let (conn, tables) = setup();
    put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    conn.execute("UPDATE docs SET version = ?1", [i64::MAX])
        .unwrap();
    let error = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(to_version(-3), Version(0));
}

#[test]
fn declaring_search_later_indexes_existing_documents() {
    let (conn, tables) = setup();
    put(
        &conn,
        ctx(&tables),
        "notes",
        "a",
        &json!({"t": "hello world"}),
        Precondition::None,
    )
    .unwrap();
    declare(
        &conn,
        &tables,
        &CollectionSpec::new("notes").searchable(["t"]),
        0,
    )
    .unwrap();
    let hits = search(&conn, ctx(&tables), "notes", "hello", 5).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(
        search(&conn, ctx(&tables), "notes", "hello", 0)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        search(&conn, ctx(&tables), "notes", "!!", 5).unwrap().len(),
        0
    );
    declare(
        &conn,
        &tables,
        &CollectionSpec::new("notes").index(IndexSpec::new("by_t", ["t"])),
        0,
    )
    .unwrap();
}

#[test]
fn search_limits_and_scores_are_ordered() {
    let (conn, tables) = setup();
    declare(
        &conn,
        &tables,
        &CollectionSpec::new("n").searchable(["t"]),
        0,
    )
    .unwrap();
    for (id, text) in [("a", "cat"), ("b", "cat cat dog"), ("c", "cat")] {
        put(
            &conn,
            ctx(&tables),
            "n",
            id,
            &json!({"t": text}),
            Precondition::None,
        )
        .unwrap();
    }
    let hits = search(&conn, ctx(&tables), "n", "cat dog", 2).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0].0, "b",
        "the document matching both tokens ranks first"
    );
    assert!(hits[0].1 >= hits[1].1);
}

#[test]
fn corrupt_stored_json_is_a_serialization_error() {
    let (conn, tables) = setup();
    put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    conn.execute("UPDATE docs SET body = 'not json'", [])
        .unwrap();
    let error = get(&conn, ctx(&tables), "c", "a").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Serialization);
    conn.execute(
        "INSERT INTO _tsd_collections (coll, spec) VALUES ('bad', '{')",
        [],
    )
    .unwrap();
    assert_eq!(
        load_spec(&conn, &tables, "bad").unwrap_err().kind(),
        ErrorKind::Serialization
    );
}

#[test]
fn missing_tables_are_backend_errors() {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("absent");
    let error = get(&conn, ctx(&tables), "c", "a").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
}

#[test]
fn versions_survive_deletion_and_drops() {
    let (conn, tables) = setup();
    let v1 = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    assert!(delete(&conn, ctx(&tables), "c", "a", Precondition::None).unwrap());
    let v2 = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::Absent,
    )
    .unwrap();
    assert!(v2 > v1);
    drop_collection(&conn, ctx(&tables), "c").unwrap();
    let v3 = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({}),
        Precondition::Absent,
    )
    .unwrap();
    assert!(v3 > v2);
    assert_eq!(
        stored(&conn, ctx(&tables), "c", "zz")
            .unwrap()
            .map(|row| row.version),
        None,
        "deleting an id that never existed leaves no row"
    );
}

#[test]
fn a_deleted_document_keeps_a_hidden_row_with_its_version() {
    let (conn, tables) = setup();
    let version = put(
        &conn,
        ctx(&tables),
        "c",
        "a",
        &json!({"n": 1}),
        Precondition::None,
    )
    .unwrap();
    assert!(delete(&conn, ctx(&tables), "c", "a", Precondition::None).unwrap());
    let row = stored(&conn, ctx(&tables), "c", "a").unwrap().unwrap();
    assert!(row.deleted);
    assert_eq!(row.version, version);
    assert_eq!(row.doc, json!({}), "a deleted row holds the empty body");
    assert_eq!(get(&conn, ctx(&tables), "c", "a").unwrap(), None);
    assert!(
        matching(&conn, ctx(&tables), "c", &Filter::All, &[])
            .unwrap()
            .is_empty(),
        "queries skip deleted rows"
    );
    assert_eq!(collections(&conn, ctx(&tables), "").unwrap(), ["c"]);
}

#[test]
fn metadata_is_stamped_and_an_expired_record_reads_as_absent() {
    let (conn, tables) = setup();
    let at = |now_ms| Ctx {
        now_ms,
        ..ctx(&tables)
    };
    let options = WriteOptions::new().schema(3).expires_ms(5_000);
    put_with(
        &conn,
        at(1_000),
        "c",
        "a",
        &json!({}),
        Precondition::None,
        options,
    )
    .unwrap();
    put_with(
        &conn,
        at(2_000),
        "c",
        "a",
        &json!({}),
        Precondition::None,
        options,
    )
    .unwrap();
    let meta = get(&conn, at(2_500), "c", "a").unwrap().unwrap().meta;
    assert_eq!(
        meta,
        Meta {
            schema: 3,
            created_ms: 1_000,
            updated_ms: 2_000,
            expires_ms: Some(5_000)
        }
    );
    assert_eq!(get(&conn, at(5_000), "c", "a").unwrap(), None);
    assert_eq!(
        matching(&conn, at(5_000), "c", &Filter::All, &[])
            .unwrap()
            .len(),
        0
    );
    // Re-creating over an expired record is a new creation.
    put(&conn, at(6_000), "c", "a", &json!({}), Precondition::Absent).unwrap();
    assert_eq!(
        get(&conn, at(6_000), "c", "a")
            .unwrap()
            .unwrap()
            .meta
            .created_ms,
        6_000
    );
}

#[test]
fn late_unique_indexes_are_checked_per_scope() {
    let (conn, tables) = setup();
    let other = Ctx {
        scope: "other",
        ..ctx(&tables)
    };
    put(
        &conn,
        ctx(&tables),
        "u",
        "a",
        &json!({"e": "x"}),
        Precondition::None,
    )
    .unwrap();
    put(
        &conn,
        other,
        "u",
        "b",
        &json!({"e": "x"}),
        Precondition::None,
    )
    .unwrap();
    put(
        &conn,
        ctx(&tables),
        "u",
        "c",
        &json!({}),
        Precondition::None,
    )
    .unwrap();
    let unique = CollectionSpec::new("u").index(IndexSpec::new("by_e", ["e"]).unique());
    declare(&conn, &tables, &unique, 0).unwrap();
    declare(&conn, &tables, &unique, 0).unwrap();
    put(
        &conn,
        ctx(&tables),
        "w",
        "a",
        &json!({"e": 1}),
        Precondition::None,
    )
    .unwrap();
    put(
        &conn,
        ctx(&tables),
        "w",
        "b",
        &json!({"e": 1.0}),
        Precondition::None,
    )
    .unwrap();
    let error = declare(
        &conn,
        &tables,
        &CollectionSpec::new("w").index(IndexSpec::new("by_e", ["e"]).unique()),
        0,
    )
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::AlreadyExists);
    assert_eq!(load_spec(&conn, &tables, "w").unwrap().indexes.len(), 0);
}

fn decoded() -> usize {
    crate::body::DECODED.with(std::cell::Cell::get)
}

fn thousands(conn: &Connection, tables: &Tables) {
    let spec = CollectionSpec::new("c").index(IndexSpec::new("by_n", ["n"]));
    declare(conn, tables, &spec, 0).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    for n in 0..5000_u32 {
        put(
            conn,
            ctx(tables),
            "c",
            &format!("d{n:05}"),
            &json!({"n": (n * 7919) % 5000, "pad": "x".repeat(8)}),
            Precondition::None,
        )
        .unwrap();
    }
    conn.execute_batch("COMMIT").unwrap();
}

#[test]
fn a_ranged_sorted_limited_query_decodes_only_the_page() {
    let (conn, tables) = setup();
    thousands(&conn, &tables);
    let filter = Filter::gte("n", 1000).and(Filter::lt("n", 4000));
    let sort = [Sort::desc("n")];
    let before = decoded();
    let page = matching_after(&conn, ctx(&tables), "c", &filter, &sort, None, Some(11)).unwrap();
    let read = decoded() - before;
    assert_eq!(page.len(), 11);
    assert_eq!(page.first().unwrap().doc["n"], 3999);
    assert!(read <= 11, "decoded {read} bodies for an 11-row page");

    // The same page, sorted in Rust over every candidate, reads them all.
    let before = decoded();
    let all = matching_after(&conn, ctx(&tables), "c", &filter, &sort, None, None).unwrap();
    assert_eq!(all.len(), 3000);
    assert!(decoded() - before >= 3000);
    assert_eq!(
        page.iter().map(|item| &item.id).collect::<Vec<_>>(),
        all.iter().take(11).map(|item| &item.id).collect::<Vec<_>>()
    );
}

#[test]
fn a_limited_page_continues_past_rows_the_recheck_rejects() {
    let (conn, tables) = setup();
    thousands(&conn, &tables);
    // `pad` is not projected, so SQL cannot narrow by it and rejects nothing
    // itself; the Rust check does. Every match is still found, in order.
    let filter = Filter::gte("n", 100).and(Filter::eq("n", 777).or(Filter::eq("n", 4000)));
    let page = matching_after(
        &conn,
        ctx(&tables),
        "c",
        &filter,
        &[Sort::asc("n")],
        None,
        Some(5),
    )
    .unwrap();
    let found: Vec<_> = page.iter().map(|item| item.doc["n"].clone()).collect();
    assert_eq!(found, [json!(777), json!(4000)]);
}
