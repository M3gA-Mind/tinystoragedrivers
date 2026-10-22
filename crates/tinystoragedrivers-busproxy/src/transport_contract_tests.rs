use super::*;
use tinystoragedrivers_bus::{StorageFailure, StorageKind};

#[test]
fn each_storage_object_has_one_roundtrippable_fixed_name() {
    for object in [
        Object::Documents,
        Object::Streams,
        Object::Blobs,
        Object::Secrets,
        Object::Queues,
        Object::Kv,
    ] {
        assert_eq!(Object::parse(object.as_str()), Some(object));
    }
    assert_eq!(Object::parse("Documents/other"), None);
    assert_eq!(Object::parse(""), None);
}

#[test]
fn wire_error_display_is_stable_and_never_includes_storage_payloads() {
    let storage = WireError::Storage(StorageFailure {
        kind: StorageKind::Conflict,
    });
    assert_eq!(storage.to_string(), "storage failure: Conflict");
    let denied = WireError::NotAttested;
    assert_eq!(
        denied.to_string(),
        "confidential route requires an attested peer"
    );
    let transport = WireError::Transport("closed".into());
    assert_eq!(transport.to_string(), "transport failure: closed");
    assert!(!storage.to_string().contains("payload"));
}
