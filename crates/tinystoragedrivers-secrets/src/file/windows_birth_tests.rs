//! Native ACL inspection of production-created empty and published files.

use super::*;

use windows_permissions::{
    LocalBox, SecurityDescriptor, Sid,
    constants::{AccessRights, AceFlags, AceType, SeObjectType, SecurityInformation},
    wrappers,
};

fn current_user() -> String {
    let token = match winsafe::HTHREAD::GetCurrentThread()
        .OpenThreadToken(winsafe::co::TOKEN::QUERY, true)
    {
        Ok(token) => token,
        Err(error) if error == winsafe::co::ERROR::NO_TOKEN => {
            winsafe::HPROCESS::GetCurrentProcess()
                .OpenProcessToken(winsafe::co::TOKEN::QUERY)
                .unwrap()
        }
        Err(error) => panic!("cannot inspect effective fixture identity: {error:?}"),
    };
    let winsafe::TokenInfo::User(user) = token
        .GetTokenInformation(winsafe::co::TOKEN_INFORMATION_CLASS::User)
        .unwrap()
    else {
        panic!("native token did not return its user");
    };
    winsafe::ConvertSidToStringSid(user.User.Sid().unwrap()).unwrap()
}

fn sid_text(sid: &Sid) -> String {
    wrappers::ConvertSidToStringSid(sid)
        .unwrap()
        .into_string()
        .unwrap()
}

fn descriptor(file: &File) -> LocalBox<SecurityDescriptor> {
    wrappers::GetSecurityInfo(
        file,
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Owner | SecurityInformation::Dacl,
    )
    .unwrap()
}

fn broad_parent() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let user = current_user();
    // Only a disposable, still-empty fixture directory receives this broad
    // inheritable ACL. No production file is repaired after creation.
    let sd: LocalBox<SecurityDescriptor> = format!("D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;WD)")
        .parse()
        .unwrap();
    wrappers::SetNamedSecurityInfo(
        dir.path().as_os_str(),
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        None,
        None,
        sd.dacl(),
        None,
    )
    .unwrap();
    // Prove that the fixture actually grants inheritable Everyone access.
    let probe = dir.path().join("fixture-probe");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .unwrap();
    let observed = descriptor(&file);
    let acl = observed.dacl().unwrap();
    assert!(
        (0..acl.len()).any(|index| {
            let ace = acl.get_ace(index).unwrap();
            ace.ace_type() == AceType::ACCESS_ALLOWED_ACE_TYPE
                && ace.flags().contains(AceFlags::Inherited)
                && sid_text(ace.sid().unwrap()) == "S-1-1-0"
                && ace.mask().contains(AccessRights::FileAllAccess)
        }),
        "native broad-parent fixture did not inherit Everyone access"
    );
    drop(file);
    fs::remove_file(probe).unwrap();
    (dir, user)
}

fn restricted_handle(file: &File, user: &str, label: &str) -> bool {
    let sd = descriptor(file);
    let owner_matches = sd.owner().is_some_and(|sid| sid_text(sid) == user);
    let sddl = sd.as_sddl().unwrap().into_string().unwrap();
    let protected = sddl
        .split("D:")
        .nth(1)
        .and_then(|dacl| dacl.split('(').next())
        .is_some_and(|flags| flags.starts_with('P'));
    let acl = sd.dacl().unwrap();
    let owner_only = acl.len() == 1
        && (0..acl.len()).all(|index| {
            let ace = acl.get_ace(index).unwrap();
            ace.ace_type() == AceType::ACCESS_ALLOWED_ACE_TYPE
                && !ace.flags().contains(AceFlags::Inherited)
                && ace.mask().contains(AccessRights::FileAllAccess)
                && ace.sid().is_some_and(|sid| sid_text(sid) == user)
        });
    // Retain only fixed classifications, never paths, SIDs or plaintext.
    eprintln!(
        "fixture={label} owner_matches={owner_matches} protected={protected} owner_only={owner_only}"
    );
    owner_matches && protected && owner_only
}

#[test]
fn reserved_empty_file_has_protected_owner_dacl_before_bytes_under_broad_parent() {
    let (dir, user) = broad_parent();
    let (_path, file) = reserve_temp_file(&dir.path().join(SECRETS_FILE_NAME), &mut || 0).unwrap();
    assert!(file.metadata().unwrap().is_file());
    assert_eq!(
        file.metadata().unwrap().len(),
        0,
        "inspection must precede every byte write"
    );
    assert!(
        restricted_handle(&file, &user, "empty-reservation"),
        "empty production handle must have its effective owner and protected owner-only DACL before bytes"
    );
}

#[tokio::test]
async fn published_key_data_and_lock_have_protected_owner_dacl_in_admitted_namespace() {
    // Stage1 proved broad-parent inherited exposure. Full namespace admission
    // now refuses that namespace; positive final objects need an admitted one.
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("private");
    clack_private_fs::create_dir(&private).unwrap();
    let user = current_user();
    let store = EncryptedFileSecrets::with_key_file(&private).unwrap();
    store
        .set("fixture:value", b"disposable-fixture-value")
        .await
        .unwrap();
    assert_eq!(
        store
            .get("fixture:value")
            .await
            .unwrap()
            .unwrap()
            .as_slice(),
        b"disposable-fixture-value"
    );
    let key_path = private.join(KEY_FILE_NAME);
    let loaded = load_or_create_key_file(&key_path).unwrap();
    assert!(
        key_from_hex(&fs::read_to_string(&key_path).unwrap()).unwrap() == loaded,
        "public key loader must return the published fixture key"
    );
    let mut all_restricted = true;
    for (label, name) in [
        ("published-key", KEY_FILE_NAME),
        ("published-data", SECRETS_FILE_NAME),
        ("sidecar-lock", "secrets.enc.lock"),
        ("key-creation-lock", ".secret_key.lock"),
    ] {
        let file = File::open(private.join(name)).unwrap();
        all_restricted &= restricted_handle(&file, &user, label);
    }
    assert!(
        all_restricted,
        "published key, data and lock must all be restricted"
    );
}

#[test]
fn a_broad_parent_is_rejected_without_minting_a_key() {
    let (dir, _user) = broad_parent();
    assert!(EncryptedFileSecrets::with_key_file(dir.path()).is_err());
    assert!(!dir.path().join(KEY_FILE_NAME).exists());
    assert!(!dir.path().join(SECRETS_FILE_NAME).exists());
}

#[test]
fn an_existing_broad_key_is_rejected_without_repair_or_replacement() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("private");
    clack_private_fs::create_dir(&private).unwrap();
    let key = private.join(KEY_FILE_NAME);
    fs::write(&key, b"disposable-invalid-fixture-key").unwrap();
    let user = current_user();
    let sd: LocalBox<SecurityDescriptor> =
        format!("D:P(A;;FA;;;{user})(A;;FR;;;WD)").parse().unwrap();
    wrappers::SetNamedSecurityInfo(
        key.as_os_str(),
        SeObjectType::SE_FILE_OBJECT,
        SecurityInformation::Dacl | SecurityInformation::ProtectedDacl,
        None,
        None,
        sd.dacl(),
        None,
    )
    .unwrap();
    assert!(load_or_create_key_file(&key).is_err());
    assert_eq!(fs::read(&key).unwrap(), b"disposable-invalid-fixture-key");
    let file = File::open(&key).unwrap();
    assert_eq!(descriptor(&file).dacl().unwrap().len(), 2);
}

#[test]
fn a_hardlinked_key_is_rejected_without_minting_another_key() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("private");
    clack_private_fs::create_dir(&private).unwrap();
    let key = private.join(KEY_FILE_NAME);
    let expected = load_or_create_key_file(&key).unwrap();
    let alias = private.join("fixture-alias");
    fs::hard_link(&key, &alias).unwrap();
    let before = fs::read(&key).unwrap();
    assert!(load_or_create_key_file(&key).is_err());
    assert_eq!(fs::read(&key).unwrap(), before);
    assert_eq!(fs::read(&alias).unwrap(), before);
    fs::remove_file(alias).unwrap();
    assert_eq!(load_or_create_key_file(&key).unwrap(), expected);
}

#[test]
fn a_native_sharing_violation_retains_the_previous_publication() {
    use std::os::windows::fs::OpenOptionsExt;
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("private");
    clack_private_fs::create_dir(&private).unwrap();
    let path = private.join(SECRETS_FILE_NAME);
    write_atomic(&path, b"fixture-old").unwrap();
    // This actual retained handle permits read sharing but denies deletion.
    let held = OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(&path)
        .unwrap();
    let error = write_atomic(&path, b"fixture-new").unwrap_err();
    let native = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .unwrap();
    assert_eq!(native.raw_os_error(), Some(32));
    eprintln!("OBSERVED_NATIVE_SHARING_VIOLATION=32");
    assert_eq!(fs::read(&path).unwrap(), b"fixture-old");
    assert_eq!(fs::read_dir(&private).unwrap().count(), 1);
    drop(held);
    write_atomic(&path, b"fixture-new").unwrap();
    assert_eq!(fs::read(path).unwrap(), b"fixture-new");
}

#[test]
fn native_reserved_and_alternate_stream_names_never_mint_keys() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("private");
    clack_private_fs::create_dir(&private).unwrap();
    for name in [
        "CON",
        "NUL.txt",
        "COM1",
        "LPT9",
        "fixture:stream",
        "trailing.",
        "trailing ",
        "bad?name",
    ] {
        assert!(load_or_create_key_file(&private.join(name)).is_err());
    }
    // Validation may establish an empty private lock before rejecting a leaf.
    for entry in fs::read_dir(private).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_name().to_string_lossy().ends_with(".lock"));
        assert_eq!(entry.metadata().unwrap().len(), 0);
    }
}

#[test]
fn unsupported_namespace_prefixes_fail_before_remote_or_device_access() {
    for name in [
        r"\\fixture.invalid\share",
        r"\\?\C:\fixture",
        r"\\.\fixture",
        r"C:relative",
    ] {
        assert!(super::super::windows::Namespace::open(Path::new(name), false).is_err());
    }
}
