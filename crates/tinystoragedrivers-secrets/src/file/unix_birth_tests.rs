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
    let dir = tempfile::tempdir().unwrap();
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
