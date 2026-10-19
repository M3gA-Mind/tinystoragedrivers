//! Public API check for private atomic secret-file writes.

use tinystoragedrivers_secrets::write_atomic_private;

#[test]
fn exported_private_writer_preserves_owner_only_file_mode() {
    let dir = tempfile::tempdir().unwrap();
    // The public writer rejects symlinked ancestors; use the physical temp
    // directory so this fixture tests the writer rather than TMPDIR aliases.
    let root = dir.path().canonicalize().unwrap();
    let path = root.join("dev-keychain.json");

    write_atomic_private(&path, br#"{"token":"secret"}"#).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), br#"{"token":"secret"}"#);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
