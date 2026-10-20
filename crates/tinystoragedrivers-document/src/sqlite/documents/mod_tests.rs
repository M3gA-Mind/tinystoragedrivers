use super::*;
use tinystoragedrivers_types::{Cursor, ErrorKind};

#[test]
fn fingerprints_are_stable_and_query_specific() {
    let scope = Scope::new("local").unwrap();
    let baseline = fingerprint("c", &scope, &Query::all()).unwrap();
    assert_eq!(
        baseline,
        fingerprint("c", &scope, &Query::all().limit(5)).unwrap(),
        "limit does not change cursor ordering",
    );
    assert_ne!(baseline, fingerprint("d", &scope, &Query::all()).unwrap());
    assert_ne!(
        baseline,
        fingerprint("c", &scope, &Query::filter(Filter::eq("x", 1))).unwrap(),
    );
    assert_ne!(
        baseline,
        fingerprint("c", &scope, &Query::all().sort(Sort::asc("x"))).unwrap(),
    );
    assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
}

#[test]
fn rejects_foreign_cursors_and_accepts_no_cursor() {
    let scope = Scope::new("local").unwrap();
    let query = Query::all().after(Cursor("mem:keyset1:0000000000000000:{}".into()));
    assert_eq!(
        parse_cursor("c", &scope, &query).unwrap_err().kind(),
        ErrorKind::InvalidInput,
    );
    assert_eq!(parse_cursor("c", &scope, &Query::all()).unwrap(), None);
}

#[test]
fn debug_output_identifies_the_handle_scope_and_database_path() {
    let dir = tempfile::tempdir().unwrap();
    let connection =
        tinystoragedrivers_connect::sqlite::SqliteConnection::open(dir.path().join("documents.db"))
            .unwrap();
    let docs = SqliteDocuments::new(connection, Scope::new("local").unwrap());
    let shown = format!("{docs:?}");
    assert!(
        shown.contains("local") && shown.contains("documents.db"),
        "{shown}"
    );
}
