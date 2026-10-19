//! Watch cursor values enforce size bounds and avoid leaking native tokens.
use std::time::Duration;

use super::*;

#[test]
fn cursors_redact_and_validate_persisted_bytes() {
    let cursor = WatchCursor::new(b"private-native-token".to_vec()).unwrap();
    assert_eq!(cursor.as_bytes(), b"private-native-token");
    assert_eq!(format!("{cursor:?}"), "WatchCursor([REDACTED])");
    assert_eq!(
        WatchCursor::new(vec![]).unwrap_err().kind(),
        crate::ErrorKind::InvalidInput
    );
    assert!(WatchCursor::new(vec![0; MAX_WATCH_CURSOR_BYTES + 1]).is_err());
    let copied: WatchCursor =
        serde_json::from_value(serde_json::to_value(&cursor).unwrap()).unwrap();
    assert_eq!(cursor, copied);
}

#[test]
fn options_bound_quiet_interval() {
    assert!(WatchOptions::default().validate().is_ok());
    for interval in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_secs(61),
    ] {
        assert!(
            WatchOptions {
                after: None,
                interval
            }
            .validate()
            .is_err()
        );
    }
}

#[test]
fn notices_debug_without_exposing_backend_source_payloads() {
    let notice = WatchNotice::ResetRequired {
        reason: WatchReset::ResumeLost,
        source: Some(
            crate::StorageError::backend("resume failed")
                .with_source(std::io::Error::other("private native response")),
        ),
    };
    let debug = format!("{notice:?}");
    assert!(debug.contains("ResumeLost"));
    assert!(!debug.contains("private native response"));
    let invalidated = WatchNotice::Invalidated {
        cursor: Some(WatchCursor::new(vec![1]).unwrap()),
    };
    assert!(format!("{invalidated:?}").contains("Invalidated"));
}
