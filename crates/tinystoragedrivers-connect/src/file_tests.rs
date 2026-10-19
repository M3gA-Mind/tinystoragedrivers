use super::*;

#[test]
fn cloned_connections_use_the_same_canonical_root_and_clock() {
    let dir = tempfile::tempdir().unwrap();
    let expected = dir.path().canonicalize().unwrap();
    let connection = FileConnection::open_with_clock(dir.path(), Arc::new(|| 37)).unwrap();
    let clone = connection.clone();
    assert_eq!(connection.root(), expected);
    assert_eq!(clone.root(), expected);
    assert_eq!((connection.clock())(), 37);
    assert_eq!((clone.clock())(), 37);
}
