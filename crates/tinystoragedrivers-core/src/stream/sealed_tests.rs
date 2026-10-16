//! The sealed blob: its key, its encoding and windowed reads over it.

use serde_json::json;

use super::*;
use crate::ErrorKind;
use crate::blob::validate_blob_key;

#[test]
fn blob_key_is_reserved_prefix_plus_escaped_name() {
    assert_eq!(
        sealed_blob_key("journal").unwrap(),
        "_tsd/sealed/journal".to_owned()
    );
    assert_eq!(
        sealed_blob_key("run/1:a%b").unwrap(),
        "_tsd/sealed/run%2F1:a%25b".to_owned()
    );
}

#[test]
fn blob_key_is_always_a_valid_blob_key() {
    for name in [
        ".",
        "..",
        "a/../b",
        "/",
        "//",
        "~lead",
        "%2E",
        "x",
        &"a/".repeat(256),
        &"é".repeat(256),
    ] {
        let key = sealed_blob_key(name).unwrap();
        validate_blob_key(&key).unwrap_or_else(|error| panic!("{name:?} -> {key:?}: {error}"));
        assert!(key.starts_with(SEALED_BLOB_PREFIX));
    }
}

#[test]
fn blob_key_is_injective_over_awkward_names() {
    let names = [
        ".", "%2E", "..", "%2E%2E", "a/b", "a%2Fb", "~x", "%7Ex", "a", "A",
    ];
    let keys: std::collections::BTreeSet<_> = names
        .iter()
        .map(|name| sealed_blob_key(name).unwrap())
        .collect();
    assert_eq!(keys.len(), names.len());
}

#[test]
fn blob_key_hashes_names_that_outgrow_the_key_limit() {
    let long = "/".repeat(512);
    let key = sealed_blob_key(&long).unwrap();
    assert!(key.len() < 100, "{key}");
    assert_ne!(key, sealed_blob_key(&"/".repeat(511)).unwrap());
}

#[test]
fn blob_key_rejects_an_invalid_stream_name() {
    assert_eq!(
        sealed_blob_key("").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        sealed_blob_key("a\0b").unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn log_round_trips_through_its_encoding() {
    let values: Vec<_> = (0..50)
        .map(|i| json!({"i": i, "text": "x".repeat(40)}))
        .collect();
    let log = SealedLog::new(7, values.clone());
    let bytes = log.encode().unwrap();
    let decoded = SealedLog::decode(&bytes).unwrap();
    assert_eq!(decoded, log);
    assert_eq!(decoded.base(), 7);
    assert_eq!(decoded.len(), 57);
    assert!(bytes.len() < serde_json::to_vec(&values).unwrap().len());
}

#[test]
fn encoding_is_deterministic_and_is_zstd() {
    let log = SealedLog::new(0, vec![json!(1), json!("two")]);
    assert_eq!(log.encode().unwrap(), log.encode().unwrap());
    // The zstd frame magic number.
    assert_eq!(log.encode().unwrap()[..4], [0x28, 0xB5, 0x2F, 0xFD]);
}

#[test]
fn an_empty_log_keeps_its_base() {
    let log = SealedLog::new(9, Vec::new());
    let decoded = SealedLog::decode(&log.encode().unwrap()).unwrap();
    assert_eq!((decoded.base(), decoded.len()), (9, 9));
    assert_eq!(decoded.window(0, 10), Vec::<StreamEntry>::new());
}

#[test]
fn decoding_garbage_is_a_backend_error() {
    for bytes in [
        &b""[..],
        b"not zstd",
        &zstd::encode_all(&b"{}"[..], 0).unwrap(),
    ] {
        assert_eq!(
            SealedLog::decode(bytes).unwrap_err().kind(),
            ErrorKind::Backend
        );
    }
}

#[test]
fn decoding_refuses_an_unknown_format_version() {
    let bytes = zstd::encode_all(
        &serde_json::to_vec(&json!({"version": 99, "base": 0, "values": []})).unwrap()[..],
        0,
    )
    .unwrap();
    assert_eq!(
        SealedLog::decode(&bytes).unwrap_err().kind(),
        ErrorKind::Backend
    );
}

#[test]
fn window_pages_by_offset_and_skips_below_base() {
    let log = SealedLog::new(10, (0..5).map(|i| json!(i)).collect());
    let offsets =
        |from, limit| -> Vec<u64> { log.window(from, limit).iter().map(|e| e.offset).collect() };
    assert_eq!(offsets(0, 100), [10, 11, 12, 13, 14]);
    assert_eq!(offsets(12, 2), [12, 13]);
    assert_eq!(offsets(14, 100), [14]);
    assert_eq!(offsets(15, 100), Vec::<u64>::new());
    assert_eq!(offsets(10, 0), Vec::<u64>::new());
    assert_eq!(offsets(u64::MAX, 3), Vec::<u64>::new());
    assert_eq!(log.window(11, 1)[0].value, json!(1));
}
