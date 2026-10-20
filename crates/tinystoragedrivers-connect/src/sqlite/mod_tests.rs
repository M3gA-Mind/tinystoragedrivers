use super::*;

#[test]
fn sqlite_filename_extensions_and_debug_are_stable() {
    let directory = tempfile::tempdir().unwrap();
    let db_path = directory.path().join("explicit.sqlite");
    let sqlite = SqliteConnection::open(&db_path).unwrap();
    assert_eq!(sqlite.path(), db_path.canonicalize().unwrap());
    assert!(format!("{sqlite:?}").contains("SqliteConnection"));

    let directory_store = SqliteConnection::open(directory.path().join("nested-store")).unwrap();
    assert_eq!(
        directory_store.path(),
        directory
            .path()
            .join("nested-store/storage.db")
            .canonicalize()
            .unwrap()
    );
    assert!(is_file_path(Path::new("database.db")));
    assert!(is_file_path(Path::new("database.SQLITE")));
    assert!(!is_file_path(Path::new("storage")));
}
