//! Watch values validate persisted input and redact cursor contents.
use super::*;
use crate::{Capability, ErrorKind, Filter, MemoryStorage, Scope, StorageBackend};

#[test]
fn cursors_redact_and_validate_persisted_bytes() {
    let cursor = WatchCursor::new(b"private-native-token".to_vec()).unwrap();
    assert_eq!(cursor.as_bytes(), b"private-native-token");
    assert_eq!(format!("{cursor:?}"), "WatchCursor([REDACTED])");
    assert_eq!(
        WatchCursor::new(vec![]).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert!(WatchCursor::new(vec![0; MAX_WATCH_CURSOR_BYTES + 1]).is_err());
    let invalid: WatchCursor = serde_json::from_str("[]").unwrap();
    assert!(invalid.validate().is_err());
    let copied: WatchCursor =
        serde_json::from_value(serde_json::to_value(&cursor).unwrap()).unwrap();
    assert_eq!(cursor, copied);
}

#[test]
fn options_bound_quiet_interval_and_revalidate_serde_cursor() {
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
    for interval in [Duration::from_millis(1), Duration::from_secs(60)] {
        assert!(
            WatchOptions {
                after: None,
                interval
            }
            .validate()
            .is_ok()
        );
    }
    assert!(
        WatchOptions {
            after: Some(serde_json::from_str("[]").unwrap()),
            ..WatchOptions::default()
        }
        .validate()
        .is_err()
    );
}

#[tokio::test]
async fn optional_watch_refuses_without_native_capability() {
    let backend = MemoryStorage::new();
    assert!(!backend.capabilities().contains(Capability::Watch));
    let scoped = backend.for_scope(&Scope::local()).unwrap();
    assert!(
        !scoped
            .documents()
            .capabilities()
            .contains(Capability::Watch)
    );
    let error = scoped
        .documents()
        .watch("notes", &Filter::All, &WatchOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported(Capability::Watch));
}

#[test]
fn reset_debug_never_logs_native_error_payload() {
    let notice = WatchNotice::ResetRequired {
        reason: WatchReset::ResumeLost,
        source: Some(
            StorageError::backend("resume failed")
                .with_source(std::io::Error::other("private-native-response")),
        ),
    };
    assert!(!format!("{notice:?}").contains("private-native-response"));
}
