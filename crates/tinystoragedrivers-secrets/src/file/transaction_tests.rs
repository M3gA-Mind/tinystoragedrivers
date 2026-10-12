//! Guarded single-operation transaction regressions.
use super::*;
use crate::file::{SECRETS_FILE_NAME, lock_file};
use std::collections::BTreeMap;
use std::time::Duration;

struct Lease;
impl FileTransactionLease for Lease {}
struct Guard;
impl FileTransactionGuard for Guard {
    fn enter(&self, _: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>> {
        Ok(Box::new(Lease))
    }
}
struct Deny;
impl FileTransactionGuard for Deny {
    fn enter(&self, _: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>> {
        Err(StorageError::unavailable("operation denied"))
    }
}
fn budget() -> FileTransactionBudget {
    FileTransactionBudget {
        ciphertext_bytes: 4096,
        json_bytes: 2048,
        entries: 16,
        name_bytes: 256,
        value_bytes: 512,
        allocation_bytes: 100_000,
        output_bytes: 512,
        deadline: Instant::now() + Duration::from_secs(10),
    }
}
fn fixture() -> (
    super::super::tests::TempDir,
    FileTransactionResource,
    Zeroizing<[u8; KEY_LEN]>,
) {
    let dir = super::super::tests::TempDir::new("bounded");
    let resource = FileTransactionResource::open(dir.0.join(SECRETS_FILE_NAME)).unwrap();
    (dir, resource, Zeroizing::new([42; KEY_LEN]))
}
#[test]
fn denied_owner_never_publishes() {
    let (dir, mut resource, key) = fixture();
    assert!(
        resource
            .execute(
                &key,
                FileTransactionOperation::Set("a", b"value"),
                budget(),
                &Deny
            )
            .is_err()
    );
    assert!(!dir.0.join(SECRETS_FILE_NAME).exists());
}
#[test]
fn value_budget_rejects_before_publication() {
    let (dir, mut resource, key) = fixture();
    let mut limits = budget();
    limits.value_bytes = 2;
    assert!(
        resource
            .execute(
                &key,
                FileTransactionOperation::Set("a", b"value"),
                limits,
                &Guard
            )
            .is_err()
    );
    assert!(!dir.0.join(SECRETS_FILE_NAME).exists());
}

fn set(resource: &mut FileTransactionResource, key: &Zeroizing<[u8; KEY_LEN]>) {
    assert!(matches!(
        resource.execute(
            key,
            FileTransactionOperation::Set("a", b"value"),
            budget(),
            &Guard
        ),
        Ok(FileTransactionResult::Set)
    ));
}
fn present(result: Result<FileTransactionResult>, expected: &[u8]) {
    match result {
        Ok(FileTransactionResult::Get(Some(value))) => {
            assert!(secret_bytes_match(&value, expected));
        }
        _ => panic!("expected one confirmed secret"),
    }
}
#[test]
fn initial_publication_flushes_and_reads_back_before_acknowledgment() {
    let (_dir, mut resource, key) = fixture();
    let published = std::rc::Rc::new(std::cell::Cell::new(false));
    let read_back = std::rc::Rc::new(std::cell::Cell::new(false));
    let observed_publish = std::rc::Rc::clone(&published);
    let observed_read = std::rc::Rc::clone(&read_back);
    AFTER_PUBLISH.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            observed_publish.set(true);
            AFTER_READ.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || observed_read.set(true)));
            });
        }));
    });
    let result = resource.execute(
        &key,
        FileTransactionOperation::Set("a", b"value"),
        budget(),
        &Guard,
    );
    // Remove an unused hook if publication failed before exact readback.
    AFTER_READ.with(|slot| slot.borrow_mut().take());
    assert!(published.get(), "P01");
    assert!(matches!(result, Ok(FileTransactionResult::Set)), "P02");
    assert!(read_back.get(), "P03");
    assert!(!resource.fenced, "P04");
    present(
        resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
        b"value",
    );
    assert!(matches!(
        resource.execute(
            &key,
            FileTransactionOperation::Set("a", b"replacement"),
            budget(),
            &Guard
        ),
        Ok(FileTransactionResult::Set)
    ));
    present(
        resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
        b"replacement",
    );
    assert!(matches!(
        resource.execute(
            &key,
            FileTransactionOperation::Delete("a"),
            budget(),
            &Guard
        ),
        Ok(FileTransactionResult::Delete(true))
    ));
    assert!(matches!(
        resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
        Ok(FileTransactionResult::Get(None))
    ));
}
#[test]
fn get_set_delete_preserve_existing_raw_codec_and_readback() {
    let (dir, mut resource, key) = fixture();
    assert!(matches!(
        resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
        Ok(FileTransactionResult::Get(None))
    ));
    set(&mut resource, &key);
    present(
        resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
        b"value",
    );
    let blob = std::fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap();
    let json = crypto::decrypt(&key, &blob).unwrap();
    let ordinary: BTreeMap<String, String> = serde_json::from_slice(&json).unwrap();
    assert!(ordinary.get("a").is_some_and(|v| v.as_bytes() == b"value"));
    assert!(matches!(
        resource.execute(
            &key,
            FileTransactionOperation::Set("a", b"new"),
            budget(),
            &Guard
        ),
        Ok(FileTransactionResult::Set)
    ));
    present(
        resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
        b"new",
    );
    assert!(matches!(
        resource.execute(
            &key,
            FileTransactionOperation::Delete("a"),
            budget(),
            &Guard
        ),
        Ok(FileTransactionResult::Delete(true))
    ));
    assert!(matches!(
        resource.execute(
            &key,
            FileTransactionOperation::Delete("a"),
            budget(),
            &Guard
        ),
        Ok(FileTransactionResult::Delete(false))
    ));
    assert!(secret_bytes_match(
        &crypto::decrypt(&key, &std::fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap()).unwrap(),
        b"{}"
    ));
}
#[test]
fn every_later_owner_denial_prevents_acknowledgment() {
    struct DenyPhase(FileTransactionPhase);
    impl FileTransactionGuard for DenyPhase {
        fn enter(&self, p: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>> {
            if p == self.0 {
                Err(StorageError::unavailable("operation denied"))
            } else {
                Ok(Box::new(Lease))
            }
        }
    }
    for p in [
        FileTransactionPhase::Locked,
        FileTransactionPhase::Read,
        FileTransactionPhase::Stage,
        FileTransactionPhase::Publish,
        FileTransactionPhase::Output,
    ] {
        let (dir, mut resource, key) = fixture();
        assert!(
            resource
                .execute(
                    &key,
                    FileTransactionOperation::Set("a", b"value"),
                    budget(),
                    &DenyPhase(p)
                )
                .is_err()
        );
        if p == FileTransactionPhase::Output {
            assert!(dir.0.join(SECRETS_FILE_NAME).exists());
            assert!(
                resource
                    .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
                    .is_err()
            );
        } else {
            assert!(!dir.0.join(SECRETS_FILE_NAME).exists());
            assert!(
                std::fs::read_dir(&dir.0).unwrap().all(|e| !e
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".tmp"))
            );
            set(&mut resource, &key);
        }
    }
}
#[test]
fn budgets_names_utf8_and_overflow_fail_closed() {
    for mutate in [
        |b: &mut FileTransactionBudget| b.allocation_bytes = 0,
        |b: &mut FileTransactionBudget| b.name_bytes = 0,
        |b: &mut FileTransactionBudget| b.entries = 0,
        |b: &mut FileTransactionBudget| b.ciphertext_bytes = 0,
        |b: &mut FileTransactionBudget| b.json_bytes = 1,
        |b: &mut FileTransactionBudget| b.entries = usize::MAX,
    ] {
        let (dir, mut resource, key) = fixture();
        let mut b = budget();
        mutate(&mut b);
        assert!(
            resource
                .execute(
                    &key,
                    FileTransactionOperation::Set("a", b"value"),
                    b,
                    &Guard
                )
                .is_err()
        );
        assert!(!dir.0.join(SECRETS_FILE_NAME).exists());
    }
    let (_dir, mut resource, key) = fixture();
    for name in ["", "a\n", "a\0"] {
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Get(name), budget(), &Guard)
                .is_err()
        );
    }
    assert!(
        resource
            .execute(
                &key,
                FileTransactionOperation::Set("a", &[255]),
                budget(),
                &Guard
            )
            .is_err()
    );
    set(&mut resource, &key);
    let mut b = budget();
    b.output_bytes = 1;
    assert!(
        resource
            .execute(&key, FileTransactionOperation::Get("a"), b, &Guard)
            .is_err()
    );
    let mut b = budget();
    b.ciphertext_bytes = 1;
    assert!(
        resource
            .execute(&key, FileTransactionOperation::Delete("a"), b, &Guard)
            .is_err()
    );
    let mut b = budget();
    b.json_bytes = 1;
    assert!(
        resource
            .execute(&key, FileTransactionOperation::Get("a"), b, &Guard)
            .is_err()
    );
    let mut b = budget();
    b.value_bytes = 1;
    assert!(
        resource
            .execute(&key, FileTransactionOperation::Get("a"), b, &Guard)
            .is_err()
    );
}
#[test]
fn corrupt_wrong_key_and_empty_existing_files_are_distinct() {
    for contents in [b"".as_slice(), b"broken".as_slice()] {
        let (dir, _, key) = fixture();
        super::super::tests::fixture_write(dir.0.join(SECRETS_FILE_NAME), contents).unwrap();
        let mut resource = FileTransactionResource::open(dir.0.join(SECRETS_FILE_NAME)).unwrap();
        let result = resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard);
        if contents.is_empty() {
            assert!(matches!(result, Ok(FileTransactionResult::Get(None))));
        } else {
            assert!(result.is_err());
        }
        assert!(secret_bytes_match(
            &std::fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap(),
            contents
        ));
    }
    let (dir, mut resource, key) = fixture();
    set(&mut resource, &key);
    let before = std::fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap();
    assert!(
        resource
            .execute(
                &Zeroizing::new([7; KEY_LEN]),
                FileTransactionOperation::Delete("a"),
                budget(),
                &Guard
            )
            .is_err()
    );
    assert!(secret_bytes_match(
        &std::fs::read(dir.0.join(SECRETS_FILE_NAME)).unwrap(),
        &before
    ));
}
#[test]
fn replacement_of_data_lock_or_parent_invalidates_resource() {
    for suffix in ["", ".lock"] {
        let (dir, mut resource, key) = fixture();
        set(&mut resource, &key);
        let path = dir.0.join(format!("{SECRETS_FILE_NAME}{suffix}"));
        let alternate = dir.0.join("alternate");
        super::super::tests::fixture_write(&alternate, []).unwrap();
        std::fs::rename(&alternate, &path).unwrap();
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
                .is_err()
        );
    }
    let (dir, mut resource, key) = fixture();
    let moved = dir.0.with_extension("moved");
    #[cfg(unix)]
    {
        std::fs::rename(&dir.0, &moved).unwrap();
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
                .is_err()
        );
        std::fs::rename(&moved, &dir.0).unwrap();
    }
    #[cfg(windows)]
    {
        // Native directory pins deny delete sharing, so retargeting is blocked.
        assert!(std::fs::rename(&dir.0, &moved).is_err());
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
                .is_ok()
        );
    }
}
#[cfg(unix)]
#[test]
fn symlink_retargeting_and_permissions_fail_admission() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (dir, mut resource, key) = fixture();
    set(&mut resource, &key);
    let path = dir.0.join(SECRETS_FILE_NAME);
    std::fs::rename(&path, dir.0.join("original")).unwrap();
    symlink(dir.0.join("original"), &path).unwrap();
    assert!(
        resource
            .execute(
                &key,
                FileTransactionOperation::Delete("a"),
                budget(),
                &Guard
            )
            .is_err()
    );
    assert!(FileTransactionResource::open(&path).is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::rename(dir.0.join("original"), &path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(FileTransactionResource::open(&path).is_err());
}
#[test]
fn lock_wait_can_be_canceled_without_waiting_for_other_writer() {
    struct Cancel(std::cell::Cell<usize>);
    impl FileTransactionGuard for Cancel {
        fn enter(&self, _: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>> {
            self.0.set(self.0.get() + 1);
            if self.0.get() == 3 {
                Err(StorageError::unavailable("operation canceled"))
            } else {
                Ok(Box::new(Lease))
            }
        }
    }
    let (dir, mut resource, key) = fixture();
    let other = lock_file(&dir.0.join("secrets.enc.lock")).unwrap();
    let cancel = Cancel(std::cell::Cell::new(0));
    assert!(
        resource
            .execute(
                &key,
                FileTransactionOperation::Set("a", b"value"),
                budget(),
                &cancel
            )
            .is_err()
    );
    assert_eq!(cancel.0.get(), 3);
    drop(other);
    set(&mut resource, &key);
    let mut b = budget();
    b.deadline = Instant::now();
    assert!(
        resource
            .execute(&key, FileTransactionOperation::Get("a"), b, &Guard)
            .is_err()
    );
}
#[test]
fn string_codec_is_bounded_compatible_and_wipes_error_scratch() {
    let source = r#" {"escapes":"\"\\\/\b\f\n\r\t\u0041\u00e9\uD83D\uDE00", "\u00e9":"\u00e9", "raw-é":"é😀", "a":"first", "a":"duplicate", "b":""} "#.as_bytes();
    let parsed = decode(source, &budget()).unwrap();
    let ordinary: BTreeMap<String, String> = serde_json::from_slice(source).unwrap();
    assert_eq!(parsed.0.len(), 5);
    assert_eq!(parsed.0.len(), ordinary.len());
    assert!(
        parsed
            .0
            .iter()
            .all(|(n, v)| ordinary.get(n.as_str()).is_some_and(|s| s == v.as_str()))
    );
    for (name, expected) in [
        ("escapes", "\"\\/\u{8}\u{c}\n\r\tAé😀"),
        ("é", "é"),
        ("raw-é", "é😀"),
        ("a", "duplicate"),
        ("b", ""),
    ] {
        assert!(
            parsed
                .0
                .iter()
                .any(|(n, v)| n.as_str() == name && v.as_str() == expected)
        );
    }
    for source in [
        b"[]".as_slice(),
        b"{",
        br#"{"a":1}"#,
        br#"{"a":"\q"}"#,
        br#"{"a":"\uZZZZ"}"#,
        br#"{"a":"\uD800"}"#,
        br#"{"a":"\uD800\u0041"}"#,
        br#"{"a":"\uDC00"}"#,
        br#"{"a":"unterminated}"#,
        b"{\"a\":\"\xff\"}",
        b"{\"a\":\"\n\"}",
        br#"{"a":"x"} trailing"#,
        br#"{"a":"x",}"#,
    ] {
        assert!(decode(source, &budget()).is_err());
    }
    let mut b = budget();
    b.entries = 1;
    assert!(decode(br#"{"a":"1","b":"2"}"#, &b).is_err());
    let mut b = budget();
    b.name_bytes = 0;
    assert!(decode(br#"{"a":"1"}"#, &b).is_err());
    let mut b = budget();
    b.value_bytes = 1;
    assert!(decode(br#"{"a":"\u00e9"}"#, &b).is_err());
    assert!(decode(br#"{"a":"\u0"}"#, &budget()).is_err());
    assert!(decode(br#"{"a":"\"#, &budget()).is_err());
}

// Boolean-only assertions deliberately never format plaintext on failure.
fn secret_bytes_match(actual: &[u8], expected: &[u8]) -> bool {
    actual == expected
}

thread_local! {
    static AFTER_PUBLISH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
}
pub(super) fn after_publish() {
    let hook = AFTER_PUBLISH.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}
#[test]
fn exact_postwrite_readback_is_required_and_uncertainty_is_fenced() {
    for changed in [b"changed".as_slice(), &[0; 5000]] {
        let (dir, mut resource, key) = fixture();
        let path = dir.0.join(SECRETS_FILE_NAME);
        let data = changed.to_vec();
        AFTER_PUBLISH.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                // Retain inode/permissions; exact readback must still detect tampering.
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(path)
                    .unwrap();
                file.write_all(&data).unwrap();
            }));
        });
        assert!(
            resource
                .execute(
                    &key,
                    FileTransactionOperation::Set("a", b"value"),
                    budget(),
                    &Guard
                )
                .is_err()
        );
        assert!(resource.fenced);
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
                .is_err()
        );
    }
}
#[test]
fn actual_publish_and_readback_hold_the_owner_generation_gate() {
    struct Owner {
        gate: std::sync::Mutex<()>,
        active: std::cell::Cell<bool>,
        published: std::cell::Cell<bool>,
    }
    struct GateLease<'a> {
        owner: &'a Owner,
        _gate: std::sync::MutexGuard<'a, ()>,
    }
    impl FileTransactionLease for GateLease<'_> {}
    impl Drop for GateLease<'_> {
        fn drop(&mut self) {
            self.owner.active.set(false);
            self.owner.published.set(true);
        }
    }
    impl FileTransactionGuard for Owner {
        fn enter(&self, phase: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>> {
            if phase == FileTransactionPhase::Publish {
                self.active.set(true);
                Ok(Box::new(GateLease {
                    owner: self,
                    _gate: self.gate.lock().unwrap(),
                }))
            } else {
                if phase == FileTransactionPhase::Output {
                    assert!(self.published.get());
                    assert!(!self.active.get());
                }
                Ok(Box::new(Lease))
            }
        }
    }
    let (_dir, mut resource, key) = fixture();
    let owner = std::rc::Rc::new(Owner {
        gate: std::sync::Mutex::new(()),
        active: std::cell::Cell::new(false),
        published: std::cell::Cell::new(false),
    });
    let observed = std::rc::Rc::clone(&owner);
    AFTER_PUBLISH.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            assert!(observed.active.get());
            // This is the actual rename boundary, before the exact readback.
            assert!(observed.gate.try_lock().is_err());
        }));
    });
    assert!(matches!(
        resource.execute(
            &key,
            FileTransactionOperation::Set("a", b"value"),
            budget(),
            &*owner
        ),
        Ok(FileTransactionResult::Set)
    ));
    assert!(owner.gate.try_lock().is_ok());
}
#[test]
fn final_guard_detects_retargeting_at_the_actual_publish_phase() {
    struct Retarget {
        path: PathBuf,
    }
    impl FileTransactionGuard for Retarget {
        fn enter(&self, phase: FileTransactionPhase) -> Result<Box<dyn FileTransactionLease + '_>> {
            if phase == FileTransactionPhase::Publish {
                let alternate = self.path.with_file_name("replacement");
                super::super::tests::fixture_write(&alternate, []).unwrap();
                std::fs::rename(alternate, &self.path).unwrap();
            }
            Ok(Box::new(Lease))
        }
    }
    let (dir, mut resource, key) = fixture();
    let guard = Retarget {
        path: dir.0.join(SECRETS_FILE_NAME),
    };
    assert!(
        resource
            .execute(
                &key,
                FileTransactionOperation::Set("a", b"value"),
                budget(),
                &guard
            )
            .is_err()
    );
    assert!(secret_bytes_match(
        &std::fs::read(&guard.path).unwrap(),
        &[]
    ));
}
#[test]
fn bounded_read_and_writer_check_growth_and_capacity() {
    let (dir, _, _) = fixture();
    let path = dir.0.join("bounded");
    super::super::tests::fixture_write(&path, [1, 2, 3]).unwrap();
    assert!(read_bounded(File::open(&path).unwrap(), 2).is_err());
    // A stream with a zero advertised length still cannot exceed the read cap.
    #[cfg(unix)]
    {
        let stream = File::open("/dev/zero").unwrap();
        assert!(read_bounded(stream, 1).is_err());
    }
    let mut writer = BoundedWriter {
        bytes: Zeroizing::new(Vec::with_capacity(1)),
        maximum: 1,
    };
    assert!(writer.write_all(b"x").is_ok());
    assert!(writer.write_all(b"y").is_err());
    assert!(writer.flush().is_ok());
}
#[test]
fn redacted_debug_never_contains_request_resource_or_result() {
    let (_dir, resource, _) = fixture();
    assert_eq!(
        format!("{resource:?}"),
        "FileTransactionResource([redacted])"
    );
    assert_eq!(
        format!(
            "{:?}",
            FileTransactionOperation::Set("private-name", b"private-value")
        ),
        "FileTransactionOperation([redacted])"
    );
    assert_eq!(
        format!(
            "{:?}",
            FileTransactionResult::Get(Some(Zeroizing::new(b"private-value".to_vec())))
        ),
        "FileTransactionResult([redacted])"
    );
}

#[test]
fn publication_identity_absence_and_serialization_limits_fail_closed() {
    for replacement in [false, true] {
        let (dir, mut resource, key) = fixture();
        let path = dir.0.join(SECRETS_FILE_NAME);
        AFTER_PUBLISH.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                std::fs::remove_file(&path).unwrap();
                if replacement {
                    super::super::tests::fixture_write(&path, []).unwrap();
                }
            }));
        });
        assert!(
            resource
                .execute(
                    &key,
                    FileTransactionOperation::Set("a", b"value"),
                    budget(),
                    &Guard
                )
                .is_err()
        );
        assert!(resource.fenced);
    }
    for json_max in [0, 7] {
        let (dir, mut resource, key) = fixture();
        let mut b = budget();
        b.json_bytes = json_max;
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Set("a", b""), b, &Guard)
                .is_err()
        );
        assert!(!dir.0.join(SECRETS_FILE_NAME).exists());
    }
}
#[test]
fn staging_collisions_are_finite_and_swapped_staging_is_preserved() {
    let (dir, resource, _) = fixture();
    let name = dir
        .0
        .join(format!("secrets.enc.{}.0.tmp", std::process::id()));
    super::super::tests::fixture_write(&name, []).unwrap();
    assert!(
        Staged::with_sequence(
            &resource.lock.namespace,
            &resource.path,
            b"ciphertext",
            &mut || 0
        )
        .is_err()
    );
    assert!(name.exists());
    std::fs::remove_file(&name).unwrap();
    let staged = Staged::with_sequence(
        &resource.lock.namespace,
        &resource.path,
        b"ciphertext",
        &mut || 0,
    )
    .unwrap();
    std::fs::rename(&name, dir.0.join("retained")).unwrap();
    super::super::tests::fixture_write(&name, []).unwrap();
    assert!(staged.verify().is_err());
    drop(staged);
    assert!(name.exists());
}
#[test]
fn unsafe_missing_parent_lock_and_plaintext_json_never_admit() {
    let (dir, _, key) = fixture();
    assert!(FileTransactionResource::open(dir.0.join("missing").join(SECRETS_FILE_NAME)).is_err());
    assert!(FileTransactionResource::open(&dir.0).is_err());
    let path = dir.0.join("bad.enc");
    for json in [b"[]".as_slice(), b"{}", br#"{"a":42}"#, b"{\"a\":\"\xff\"}"] {
        let blob = crypto::encrypt(&key, json).unwrap();
        if path.exists() {
            std::fs::remove_file(&path).unwrap();
        }
        super::super::tests::fixture_write(&path, &blob).unwrap();
        let mut resource = FileTransactionResource::open(&path).unwrap();
        let result = resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard);
        if json == b"{}" {
            assert!(matches!(result, Ok(FileTransactionResult::Get(None))));
        } else {
            assert!(result.is_err());
        }
        assert!(secret_bytes_match(&std::fs::read(&path).unwrap(), &blob));
    }
}
#[cfg(unix)]
#[test]
fn native_directory_read_failure_is_sanitized() {
    let (dir, resource, _) = fixture();
    assert!(read_bounded(File::open(&dir.0).unwrap(), 10000).is_err());
    let path = dir.0.join(&resource.lock.name);
    let read_only = File::open(path).unwrap();
    assert!(acquire(&read_only, &budget(), &Guard).is_ok());
}

thread_local! {
    static BEFORE_READ_OPEN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
    static AFTER_READ_OPEN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
    static AFTER_READ: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
}
pub(super) fn before_read_open() {
    let hook = BEFORE_READ_OPEN.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}
pub(super) fn after_read_open() {
    let hook = AFTER_READ_OPEN.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}
pub(super) fn after_read() {
    let hook = AFTER_READ.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

// Move legitimate private fixtures only during the actual bounded open. The
// name is restored before any subsequent path verification; the selected file
// handle must itself match the resource's captured identity/presence.
fn retarget_read(path: PathBuf, temporary: Option<Vec<u8>>) {
    let retained = path.with_file_name("retained-read-fixture");
    let replacement = path.with_file_name("alternate-read-fixture");
    if let Some(bytes) = temporary.as_ref() {
        super::super::tests::fixture_write(&replacement, bytes).unwrap();
    }
    let originally_present = path.exists();
    let restore_path = path.clone();
    BEFORE_READ_OPEN.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            if originally_present {
                std::fs::rename(&path, &retained).unwrap();
            }
            if temporary.is_some() {
                std::fs::rename(&replacement, &path).unwrap();
            }
        }));
    });
    AFTER_READ_OPEN.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            if restore_path.exists() {
                std::fs::rename(
                    &restore_path,
                    restore_path.with_file_name("opened-read-fixture"),
                )
                .unwrap();
            }
            if originally_present {
                std::fs::rename(
                    restore_path.with_file_name("retained-read-fixture"),
                    &restore_path,
                )
                .unwrap();
            }
        }));
    });
}

#[test]
fn bounded_open_rejects_retargeted_present_data_for_every_operation() {
    for operation in [
        FileTransactionOperation::Get("a"),
        FileTransactionOperation::Set("a", b"new"),
        FileTransactionOperation::Delete("a"),
    ] {
        for temporary_present in [false, true] {
            let (dir, mut resource, key) = fixture();
            set(&mut resource, &key);
            let path = dir.0.join(SECRETS_FILE_NAME);
            let before = std::fs::read(&path).unwrap();
            let alternate =
                temporary_present.then(|| crypto::encrypt(&key, br#"{"a":"alternate"}"#).unwrap());
            retarget_read(path.clone(), alternate);
            assert!(resource.execute(&key, operation, budget(), &Guard).is_err());
            assert!(!resource.fenced);
            assert!(secret_bytes_match(&std::fs::read(&path).unwrap(), &before));
            present(
                resource.execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard),
                b"value",
            );
        }
    }
}

#[test]
fn bounded_open_rejects_temporary_presence_for_every_absent_operation() {
    for operation in [
        FileTransactionOperation::Get("a"),
        FileTransactionOperation::Set("a", b"new"),
        FileTransactionOperation::Delete("a"),
    ] {
        let (dir, mut resource, key) = fixture();
        let path = dir.0.join(SECRETS_FILE_NAME);
        retarget_read(
            path.clone(),
            Some(crypto::encrypt(&key, br#"{"a":"alternate"}"#).unwrap()),
        );
        assert!(resource.execute(&key, operation, budget(), &Guard).is_err());
        assert!(!path.exists());
        assert!(!resource.fenced);
    }
}

#[test]
fn published_readback_rejects_same_bytes_from_another_opened_object() {
    for operation in [
        FileTransactionOperation::Set("a", b"new"),
        FileTransactionOperation::Delete("a"),
    ] {
        let (dir, mut resource, key) = fixture();
        set(&mut resource, &key);
        let path = dir.0.join(SECRETS_FILE_NAME);
        AFTER_PUBLISH.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                let bytes = std::fs::read(&path).unwrap();
                retarget_read(path, Some(bytes));
            }));
        });
        assert!(resource.execute(&key, operation, budget(), &Guard).is_err());
        assert!(resource.fenced);
        assert!(
            resource
                .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
                .is_err()
        );
    }
}

#[cfg(unix)]
#[test]
fn bounded_open_rechecks_private_floor_on_the_actual_read_handle() {
    use std::os::unix::fs::PermissionsExt;
    let (dir, mut resource, key) = fixture();
    set(&mut resource, &key);
    let path = dir.0.join(SECRETS_FILE_NAME);
    let changed = path.clone();
    AFTER_READ_OPEN.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            std::fs::set_permissions(changed, std::fs::Permissions::from_mode(0o644)).unwrap();
        }));
    });
    let restored = path.clone();
    AFTER_READ.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            std::fs::set_permissions(restored, std::fs::Permissions::from_mode(0o600)).unwrap();
        }));
    });
    assert!(
        resource
            .execute(&key, FileTransactionOperation::Get("a"), budget(), &Guard)
            .is_err()
    );
    AFTER_READ.with(|slot| {
        slot.borrow_mut().take();
    });
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
