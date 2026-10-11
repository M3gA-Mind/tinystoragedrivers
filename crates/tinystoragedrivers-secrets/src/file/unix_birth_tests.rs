//! Native file-birth regression with umask isolated in a disposable child.

use super::*;

use std::os::unix::fs::PermissionsExt;
use std::process::Command;

#[test]
fn reserved_empty_file_is_owner_only_at_birth_with_permissive_umask() {
    const CHILD: &str = "TSD_BIRTH_UMASK_CHILD";
    const TEST: &str =
        "file::tests::unix_birth::reserved_empty_file_is_owner_only_at_birth_with_permissive_umask";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "native umask child failed: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    // This test process runs exactly one test. Its umask cannot affect sibling
    // tests in the parent, and process exit restores the surrounding context.
    rustix::process::umask(rustix::fs::Mode::empty());
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let (path, file) = reserve_temp_file(&dir.path().join(SECRETS_FILE_NAME), &mut || 0).unwrap();
    let metadata = file.metadata().unwrap();
    assert!(metadata.is_file());
    assert_eq!(
        metadata.len(),
        0,
        "inspection must precede every byte write"
    );
    assert_eq!(
        metadata.permissions().mode() & 0o777,
        0o600,
        "reserved empty production file exposes access before chmod"
    );
    assert_eq!(fs::metadata(path).unwrap().len(), 0);
}

#[test]
fn a_symlinked_key_is_rejected_without_touching_its_target() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let outside = dir.path().join("outside-key");
    fs::write(&outside, crate::crypto::tests::FIXTURE_KEY_HEX).unwrap();
    let path = dir.path().join(KEY_FILE_NAME);
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    assert!(
        load_or_create_key_file(&path).is_err(),
        "a symlink must never become key authority"
    );
    assert_eq!(
        fs::read_to_string(outside).unwrap(),
        crate::crypto::tests::FIXTURE_KEY_HEX
    );
}

#[test]
fn a_hard_linked_key_is_rejected_without_replacing_either_link() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let original = dir.path().join("original-key");
    fs::write(&original, crate::crypto::tests::FIXTURE_KEY_HEX).unwrap();
    fs::set_permissions(&original, fs::Permissions::from_mode(0o600)).unwrap();
    let path = dir.path().join(KEY_FILE_NAME);
    fs::hard_link(&original, &path).unwrap();
    assert!(
        load_or_create_key_file(&path).is_err(),
        "aliased keys must not be admitted"
    );
    assert_eq!(
        fs::read_to_string(original).unwrap(),
        crate::crypto::tests::FIXTURE_KEY_HEX
    );
}

#[test]
fn a_broad_existing_key_is_rejected_without_repair_or_replacement() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let path = dir.path().join(KEY_FILE_NAME);
    fs::write(&path, crate::crypto::tests::FIXTURE_KEY_HEX).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        load_or_create_key_file(&path).is_err(),
        "exposed key must not be repaired and adopted"
    );
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
}

#[test]
fn a_foreign_mutable_namespace_never_mints_a_key() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o777)).unwrap();
    let path = dir.path().join(KEY_FILE_NAME);
    assert!(
        load_or_create_key_file(&path).is_err(),
        "foreign namespace writers can replace key authority"
    );
    assert!(
        !path.exists(),
        "unsafe namespace must not mint key material"
    );
}

#[tokio::test]
async fn a_symlinked_ciphertext_is_rejected_before_read_or_rewrite() {
    let dir = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let key = fixture_key();
    let outside = dir.path().join("outside-data");
    let blob = crypto::encrypt(&key, br#"{"fixture":"untouched"}"#).unwrap();
    fs::write(&outside, &blob).unwrap();
    let path = dir.path().join(SECRETS_FILE_NAME);
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    let store = EncryptedFileSecrets::at_path(&path, key);
    assert!(store.get("fixture").await.is_err());
    assert!(store.set("fixture", b"changed").await.is_err());
    assert_eq!(fs::read(outside).unwrap(), blob);
}
