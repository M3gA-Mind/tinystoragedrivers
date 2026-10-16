//! Quoting and schema creation.

use super::*;

#[test]
fn quotes_identifiers_and_literals() {
    assert_eq!(ident("a\"b"), "\"a\"\"b\"");
    assert_eq!(literal("it's"), "'it''s'");
}

#[test]
fn creates_prefixed_layout_two_tables_idempotently() {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("flows__");
    tables.ensure(&conn).unwrap();
    tables.ensure(&conn).unwrap();
    assert_eq!(tables.docs, "flows__docs");
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN (
                 'flows__docs', 'flows___tsd_collections', 'flows__fts', 'flows__streams',
                 'flows__stream_heads', 'flows__blobs', 'flows__meta', 'flows__doc_fields',
                 'flows___tsd_scope_admissions', 'flows___tsd_purge_receipts')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 10);
    assert_eq!(tables.layout(&conn).unwrap().as_deref(), Some("2"));
    assert_eq!(Tables::new("").prefix, "");
}

#[test]
fn a_foreign_table_in_the_way_is_refused() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE docs (id TEXT)").unwrap();
    assert!(Tables::new("").ensure(&conn).is_err());
}

#[test]
fn a_layout_two_file_with_v0_tables_is_corrupt() {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("");
    tables.ensure(&conn).unwrap();
    conn.execute_batch("CREATE TABLE _tsd_docs (scope TEXT)")
        .unwrap();
    let error = tables.ensure(&conn).unwrap_err();
    assert!(error.to_string().contains("corrupt"), "{error}");
}

#[test]
fn a_file_without_the_layout_marker_does_not_validate() {
    let conn = Connection::open_in_memory().unwrap();
    let tables = Tables::new("");
    tables.ensure(&conn).unwrap();
    conn.execute_batch(&format!("DELETE FROM {}", ident(&tables.meta)))
        .unwrap();
    assert!(tables.validate(&conn).is_err());
}
