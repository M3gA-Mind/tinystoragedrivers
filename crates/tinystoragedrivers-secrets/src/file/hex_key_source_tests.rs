//! Protected encoded-key source behavior.
use super::*;
use std::path::PathBuf;

fn directory() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
    let path = root.path().join("private");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
    }
    #[cfg(windows)]
    clack_private_fs::create_dir(&path).unwrap();
    (root, path)
}

fn inventory(path: &Path) -> Vec<std::ffi::OsString> {
    let mut names: Vec<_> = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    names
}

#[test]
fn admitted_missing_leaf_is_absence_without_mutation() {
    let (_root, dir) = directory();
    let before = inventory(&dir);
    let result = read_encoded(&dir.join(".secret_key")).unwrap();
    let after = inventory(&dir);
    assert_eq!(after, before, "read-only source created filesystem entries");
    assert!(result.is_none(), "missing source minted a replacement");
}

fn fixture(path: &Path, bytes: &[u8]) {
    use std::io::Write;
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap()
    };
    #[cfg(windows)]
    let mut file = clack_private_fs::create_new_file(path).unwrap();
    file.write_all(bytes).unwrap();
}

fn encoded() -> Zeroizing<Vec<u8>> {
    Zeroizing::new(b"Ab".repeat(32))
}

#[test]
fn legacy_master_loader_is_a_mutating_initializer() {
    let (_root, dir) = directory();
    super::super::load_or_create_key_file(&dir.join(".secret_key")).unwrap();
    assert_eq!(inventory(&dir), [".secret_key", ".secret_key.lock"]);
}

#[tokio::test]
async fn public_trait_preserves_original_encoding_without_sidecars() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    let mut bytes = Zeroizing::new("\u{2003}\t".as_bytes().to_vec());
    bytes.extend_from_slice(&encoded());
    bytes.extend_from_slice("\r\n\u{a0}".as_bytes());
    fixture(&path, &bytes);
    let before = inventory(&dir);
    let source: Box<dyn HexKeyFileSource> = Box::new(ProtectedHexKeyFile::new(&path).unwrap());
    let result = source.read_encoded().await.unwrap().unwrap();
    assert_eq!(result, bytes);
    assert_eq!(
        crypto::key_from_hex(std::str::from_utf8(&result).unwrap())
            .unwrap()
            .as_ref(),
        &[0xab; 32]
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes.as_slice());
    assert_eq!(inventory(&dir), before);
    assert!(!format!("{source:?}").contains("registered"));
    assert!(!format!("{source:?}").contains("AbAb"));
}

#[test]
fn exact_bound_is_accepted_and_bound_plus_one_is_rejected_without_repair() {
    for length in [MAX_HEX_KEY_FILE_BYTES, MAX_HEX_KEY_FILE_BYTES + 1] {
        let (_root, dir) = directory();
        let path = dir.join("registered");
        let mut bytes = Zeroizing::new(vec![b' '; length - 64]);
        bytes.extend_from_slice(&encoded());
        fixture(&path, &bytes);
        if length == MAX_HEX_KEY_FILE_BYTES {
            assert_eq!(read_encoded(&path).unwrap().unwrap(), bytes);
        } else {
            assert_eq!(
                read_encoded(&path).unwrap_err().kind(),
                ErrorKind::InvalidInput
            );
        }
        assert_eq!(std::fs::read(&path).unwrap(), bytes.as_slice());
        assert_eq!(inventory(&dir), ["registered"]);
    }
}

#[tokio::test]
async fn malformed_sources_fail_with_redacted_errors_and_remain_unchanged() {
    use std::error::Error;
    for (bytes, kind) in [
        (Zeroizing::new(vec![0xff; 64]), ErrorKind::InvalidInput),
        (Zeroizing::new(b"g".repeat(64)), ErrorKind::Crypto),
        (Zeroizing::new(vec![]), ErrorKind::Crypto),
        (Zeroizing::new(b"0".repeat(63)), ErrorKind::Crypto),
        (Zeroizing::new(b"0".repeat(65)), ErrorKind::Crypto),
        (Zeroizing::new(b"0".repeat(32)), ErrorKind::Crypto),
        (Zeroizing::new(vec![0; 64]), ErrorKind::Crypto),
        (Zeroizing::new(b"0 ".repeat(32)), ErrorKind::Crypto),
        (
            Zeroizing::new(
                "\u{feff}"
                    .as_bytes()
                    .iter()
                    .copied()
                    .chain(b"0".repeat(64))
                    .collect(),
            ),
            ErrorKind::Crypto,
        ),
    ] {
        let (_root, dir) = directory();
        let path = dir.join("private-synthetic-path");
        fixture(&path, &bytes);
        let error = ProtectedHexKeyFile::new(&path)
            .unwrap()
            .read_encoded()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert!(error.source().is_none());
        let diagnostics = format!("{error} {error:?}");
        assert!(!diagnostics.contains("private-synthetic-path"));
        assert!(!diagnostics.contains("0000000000000000"));
        assert_eq!(std::fs::read(&path).unwrap(), bytes.as_slice());
        assert_eq!(inventory(&dir), ["private-synthetic-path"]);
    }
}

#[tokio::test]
async fn missing_parent_is_error_and_is_never_created() {
    use std::error::Error;
    let (_root, dir) = directory();
    let path = dir.join("missing-parent").join("registered");
    let error = ProtectedHexKeyFile::new(&path)
        .unwrap()
        .read_encoded()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(error.source().is_none());
    assert!(!format!("{error:?}").contains("missing-parent"));
    assert!(inventory(&dir).is_empty());
}

#[test]
fn relative_ambiguous_null_and_leafless_bindings_are_rejected() {
    for path in [
        PathBuf::from("relative"),
        PathBuf::from("/"),
        PathBuf::from("/private/../registered"),
        PathBuf::from("/private/null\0"),
    ] {
        assert_eq!(
            ProtectedHexKeyFile::new(path).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
}

#[test]
fn nonregular_source_is_error_without_sidecars() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
    }
    #[cfg(windows)]
    clack_private_fs::create_dir(&path).unwrap();
    assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn hardlinked_source_is_error_without_mutation() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    std::fs::hard_link(&path, dir.join("alias")).unwrap();
    assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir), ["alias", "registered"]);
    assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
}

#[test]
fn source_growth_after_metadata_is_bounded() {
    use std::io::Write;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Metadata) {
            let mut writer = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writer
                .write_all(&vec![b' '; MAX_HEX_KEY_FILE_BYTES])
                .unwrap();
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::InvalidInput);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        MAX_HEX_KEY_FILE_BYTES as u64 + 64
    );
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn stream_limit_reads_only_bound_plus_one() {
    let mut reader = std::io::Cursor::new(vec![b' '; MAX_HEX_KEY_FILE_BYTES * 2]);
    assert_eq!(
        read_bounded(&mut reader).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(reader.position(), MAX_HEX_KEY_FILE_BYTES as u64 + 1);
}

#[test]
fn retained_source_byte_change_is_rejected() {
    use std::io::Write;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            let mut writer = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            writer.write_all(&b"42".repeat(32)).unwrap();
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(std::fs::read(&path).unwrap(), b"42".repeat(32));
    assert_eq!(inventory(&dir), ["registered"]);
}

#[test]
fn a_leaf_appearing_during_absence_verification_is_error() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Absence) {
            fixture(&path, &encoded());
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
}

#[test]
fn named_source_replacement_after_read_is_rejected() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let replacement = dir.join("replacement");
    fixture(&replacement, &encoded());
    let result = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            std::fs::rename(&replacement, &path).unwrap();
        }
    });
    assert_eq!(result.unwrap_err().kind(), ErrorKind::Backend);
    assert_eq!(inventory(&dir), ["registered"]);
}

#[cfg(unix)]
#[test]
fn unsafe_modes_and_symlink_sources_are_rejected_without_repair() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    for mode in [0o644, 0o400, 0o000] {
        let (_root, dir) = directory();
        let path = dir.join("registered");
        fixture(&path, &encoded());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            mode
        );
        assert_eq!(inventory(&dir), ["registered"]);
    }
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&dir.join("original"), &encoded());
    symlink(dir.join("original"), &path).unwrap();
    assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
    assert!(
        std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[cfg(unix)]
#[test]
fn unsafe_or_symlink_ancestor_cannot_be_confirmed_absence() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (_root, dir) = directory();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777)).unwrap();
    assert_eq!(
        read_encoded(&dir.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(inventory(&dir).is_empty());
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    let alias = dir.with_file_name("alias");
    symlink(&dir, &alias).unwrap();
    assert_eq!(
        read_encoded(&alias.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert!(inventory(&dir).is_empty());
}

#[cfg(unix)]
#[test]
fn source_protection_change_after_read_is_rejected_and_not_repaired() {
    use std::os::unix::fs::PermissionsExt;
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let error = read_checked(&path, &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Read) {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
}

#[cfg(unix)]
#[test]
fn replaced_namespace_cannot_be_confirmed_absence() {
    use std::os::unix::fs::DirBuilderExt;
    let (_root, dir) = directory();
    let moved = dir.with_file_name("moved");
    let error = read_checked(&dir.join("registered"), &mut |checkpoint| {
        if matches!(checkpoint, Checkpoint::Absence) {
            std::fs::rename(&dir, &moved).unwrap();
            std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
        }
    })
    .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(inventory(&dir).is_empty());
    assert!(inventory(&moved).is_empty());
}

#[test]
fn native_transient_errors_have_finite_content_free_categories() {
    use std::error::Error;
    for kind in [
        std::io::ErrorKind::WouldBlock,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::TimedOut,
    ] {
        let error = native_error(std::io::Error::new(kind, "synthetic-private-payload"));
        assert_eq!(error.kind(), ErrorKind::Unavailable);
        assert!(error.source().is_none());
        assert!(!format!("{error:?}").contains("synthetic-private-payload"));
    }
}

#[test]
fn all_hex_cases_return_original_bytes_and_construction_is_io_free() {
    let (_root, dir) = directory();
    let missing = dir.join("absent-parent").join("registered");
    let _source = ProtectedHexKeyFile::new(missing).unwrap();
    assert!(inventory(&dir).is_empty());
    for (index, pair) in [b"ab", b"AB", b"Ab"].iter().enumerate() {
        let path = dir.join(format!("case-{index}"));
        let bytes = Zeroizing::new(pair.repeat(32));
        fixture(&path, &bytes);
        assert_eq!(read_encoded(&path).unwrap().unwrap(), bytes);
    }
}

#[tokio::test]
async fn canceled_blocking_read_can_finish_without_filesystem_mutation() {
    let (_root, dir) = directory();
    let path = dir.join("registered");
    fixture(&path, &encoded());
    let before = inventory(&dir);
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        crate::task::run_blocking(move || {
            let mut entered = Some(entered_tx);
            let result = read_checked(&path, &mut |checkpoint| {
                if matches!(checkpoint, Checkpoint::Read) {
                    entered.take().unwrap().send(()).unwrap();
                    release_rx.recv().unwrap();
                }
            });
            finished_tx.send(()).unwrap();
            result
        })
        .await
    });
    entered_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    release_tx.send(()).unwrap();
    finished_rx.await.unwrap();
    assert_eq!(inventory(&dir), before);
    assert_eq!(
        std::fs::read(dir.join("registered")).unwrap(),
        encoded().as_slice()
    );
}

#[cfg(windows)]
fn native_descriptor(
    file: &File,
) -> windows_permissions::LocalBox<windows_permissions::SecurityDescriptor> {
    use windows_permissions::{
        constants::{SeObjectType, SecurityInformation},
        wrappers,
    };
    wrappers::GetSecurityInfo(
        file,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .unwrap()
}

#[cfg(windows)]
fn native_owner(file: &File) -> String {
    windows_permissions::wrappers::ConvertSidToStringSid(native_descriptor(file).owner().unwrap())
        .unwrap()
        .into_string()
        .unwrap()
}

#[cfg(windows)]
fn native_acl(path: &Path, sddl: &str) {
    use windows_permissions::{
        LocalBox, SecurityDescriptor,
        constants::{SeObjectType, SecurityInformation},
        wrappers,
    };
    let descriptor: LocalBox<SecurityDescriptor> = sddl.parse().unwrap();
    wrappers::SetNamedSecurityInfo(
        path.as_os_str(),
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        None,
        None,
        descriptor.dacl(),
        None,
    )
    .unwrap();
}

#[cfg(windows)]
#[test]
fn native_foreign_grant_and_read_denial_are_errors_without_acl_repair() {
    for denied in [false, true] {
        let (_root, dir) = directory();
        let path = dir.join("registered");
        fixture(&path, &encoded());
        let file = File::open(&path).unwrap();
        let user = native_owner(&file);
        let sddl = if denied {
            format!("D:P(D;;FR;;;{user})(A;;FA;;;{user})")
        } else {
            format!("D:P(A;;FA;;;{user})(A;;FR;;;WD)")
        };
        native_acl(&path, &sddl);
        let before = native_descriptor(&file)
            .as_sddl()
            .unwrap()
            .into_string()
            .unwrap();
        assert_eq!(read_encoded(&path).unwrap_err().kind(), ErrorKind::Backend);
        assert_eq!(
            native_descriptor(&file)
                .as_sddl()
                .unwrap()
                .into_string()
                .unwrap(),
            before
        );
        assert_eq!(inventory(&dir), ["registered"]);
        // Restore only the disposable fixture so its drop can remove it.
        native_acl(&path, &format!("D:P(A;;FA;;;{user})"));
        assert_eq!(std::fs::read(&path).unwrap(), encoded().as_slice());
    }
}

#[cfg(windows)]
#[test]
fn native_foreign_mutation_parent_cannot_be_confirmed_absence() {
    use std::os::windows::fs::OpenOptionsExt;
    let (_root, dir) = directory();
    // Open a directory using the native fixture-only backup-semantics flag.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x0200_0000)
        .open(&dir)
        .unwrap();
    let user = native_owner(&file);
    native_acl(&dir, &format!("D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;WD)"));
    let before = native_descriptor(&file)
        .as_sddl()
        .unwrap()
        .into_string()
        .unwrap();
    assert_eq!(
        read_encoded(&dir.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(
        native_descriptor(&file)
            .as_sddl()
            .unwrap()
            .into_string()
            .unwrap(),
        before
    );
    assert!(inventory(&dir).is_empty());
    native_acl(&dir, &format!("D:P(A;OICI;FA;;;{user})"));
}

#[cfg(windows)]
#[test]
fn native_junction_ancestor_is_rejected_without_source_mutation() {
    let (_root, dir) = directory();
    fixture(&dir.join("registered"), &encoded());
    let alias = dir.with_file_name("junction");
    let result = std::process::Command::new("cmd")
        .args(["/C", "mklink", "/J"])
        .arg(&alias)
        .arg(&dir)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "native disposable junction creation failed"
    );
    let before = inventory(&dir);
    assert_eq!(
        read_encoded(&alias.join("registered")).unwrap_err().kind(),
        ErrorKind::Backend
    );
    assert_eq!(inventory(&dir), before);
    assert_eq!(
        std::fs::read(dir.join("registered")).unwrap(),
        encoded().as_slice()
    );
    std::fs::remove_dir(alias).unwrap();
}

#[cfg(windows)]
#[test]
fn native_owner_only_inherited_dacl_is_admitted_without_repair() {
    use std::io::Write;
    use std::os::windows::fs::OpenOptionsExt;
    use windows_permissions::constants::AceFlags;
    let (_root, dir) = directory();
    let directory = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(0x0200_0000)
        .open(&dir)
        .unwrap();
    let user = native_owner(&directory);
    native_acl(&dir, &format!("D:P(A;OICI;FA;;;{user})"));
    let path = dir.join("registered");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    file.write_all(&encoded()).unwrap();
    let descriptor = native_descriptor(&file);
    let acl = descriptor.dacl().unwrap();
    assert_eq!(acl.len(), 1);
    assert!(
        acl.get_ace(0)
            .unwrap()
            .flags()
            .contains(AceFlags::Inherited)
    );
    let before = descriptor.as_sddl().unwrap().into_string().unwrap();
    assert_eq!(read_encoded(&path).unwrap().unwrap(), encoded());
    assert_eq!(
        native_descriptor(&file)
            .as_sddl()
            .unwrap()
            .into_string()
            .unwrap(),
        before
    );
    assert_eq!(inventory(&dir), ["registered"]);
}

#[cfg(windows)]
#[test]
fn native_unsupported_unc_namespace_is_error_without_network_discovery() {
    use std::error::Error;
    // The existing floor rejects the prefix before any native volume/file call.
    let source = ProtectedHexKeyFile::new(r"\\synthetic-invalid\share\registered").unwrap();
    let error = read_encoded(&source.path).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(error.source().is_none());
    assert!(!format!("{error:?}").contains("synthetic-invalid"));
}
