use super::*;

#[test]
fn native_watch_resets_are_redacted_to_stable_wire_categories() {
    assert_eq!(
        reset_of(WatchReset::ResumeLost),
        wire::WatchReset::CursorExpired
    );
    assert_eq!(reset_of(WatchReset::Invalidated), wire::WatchReset::Backend);
    assert_eq!(reset_of(WatchReset::Overflow), wire::WatchReset::Backend);
}

#[test]
fn notices_forward_only_cursors_or_reset_categories() {
    assert_eq!(
        notice_of(WatchNotice::Invalidated {
            cursor: Some(WatchCursor::new(vec![7, 9]).unwrap()),
        }),
        wire::WatchNotice::Invalidated {
            cursor: Some(vec![7, 9])
        }
    );
    assert_eq!(
        notice_of(WatchNotice::Invalidated { cursor: None }),
        wire::WatchNotice::Invalidated { cursor: None }
    );
    assert_eq!(
        notice_of(WatchNotice::ResetRequired {
            reason: WatchReset::Overflow,
            source: Some(tinystoragedrivers_types::StorageError::backend(
                "private detail"
            )),
        }),
        wire::WatchNotice::ResetRequired {
            reason: wire::WatchReset::Backend
        }
    );
}
