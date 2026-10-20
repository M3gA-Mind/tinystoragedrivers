use super::*;
use crate::{Admission, Loopback, StorageServer};
use std::sync::Arc;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::{WatchNotice as NativeNotice, WatchReset as NativeReset};

fn make_watch(id: &str, capacity: usize) -> (BusWatch, broadcast::Sender<wire::WatchChanged>) {
    let (signals, receiver) = broadcast::channel(capacity);
    let storage = memory_storage(&tinystoragedrivers_types::Scope::new("watch-tests").unwrap());
    let server = Arc::new(StorageServer::new(storage));
    let transport = Wire::new(Arc::new(Loopback::new(server, Admission::Private)));
    (BusWatch::new(transport, id.into(), receiver), signals)
}

#[tokio::test]
async fn foreign_watch_ids_are_ignored_and_invalid_cursors_are_dropped() {
    let (mut watch, signals) = make_watch("mine", 8);
    signals
        .send(wire::WatchChanged {
            watch_id: "other".into(),
            notice: wire::WatchNotice::Invalidated { cursor: None },
        })
        .unwrap();
    signals
        .send(wire::WatchChanged {
            watch_id: "mine".into(),
            notice: wire::WatchNotice::Invalidated {
                cursor: Some(Vec::new()),
            },
        })
        .unwrap();
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(NativeNotice::Invalidated { cursor: None })
    ));
}

#[tokio::test]
async fn terminal_resets_map_categories_and_finish_the_watch() {
    for (wire, native) in [
        (wire::WatchReset::CursorExpired, NativeReset::ResumeLost),
        (wire::WatchReset::Backend, NativeReset::Invalidated),
    ] {
        let (mut watch, signals) = make_watch("mine", 2);
        signals
            .send(wire::WatchChanged {
                watch_id: "mine".into(),
                notice: wire::WatchNotice::ResetRequired { reason: wire },
            })
            .unwrap();
        assert!(
            matches!(watch.next().await.unwrap(), Some(NativeNotice::ResetRequired { reason, source: None }) if reason == native)
        );
        assert!(watch.next().await.unwrap().is_none());
    }
}

#[tokio::test]
async fn a_lagged_receiver_requires_reset_and_closed_channels_end_cleanly() {
    let (mut watch, signals) = make_watch("mine", 2);
    for sequence in 0..5 {
        signals
            .send(wire::WatchChanged {
                watch_id: format!("foreign-{sequence}"),
                notice: wire::WatchNotice::Invalidated { cursor: None },
            })
            .unwrap();
    }
    assert!(matches!(
        watch.next().await.unwrap(),
        Some(NativeNotice::ResetRequired {
            reason: NativeReset::Overflow,
            source: None
        })
    ));
    assert!(watch.next().await.unwrap().is_none());

    let (mut closed, signals) = make_watch("mine", 1);
    drop(signals);
    assert!(closed.next().await.unwrap().is_none());
}

#[tokio::test]
async fn a_watch_close_is_best_effort_after_runtime_shutdown() {
    let (watch, _signals) = make_watch("mine", 1);
    assert!(format!("{watch:?}").contains("BusWatch"));
    std::thread::spawn(move || drop(watch)).join().unwrap();
}

#[tokio::test]
async fn a_watch_drop_attempts_close_while_a_runtime_is_available() {
    let (watch, _signals) = make_watch("open-watch", 1);
    drop(watch);
    tokio::task::yield_now().await;
}
