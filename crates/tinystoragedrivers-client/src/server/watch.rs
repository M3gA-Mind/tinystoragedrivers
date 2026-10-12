//! Host-owned watch subscriptions projected to payload-free signals.
use std::sync::atomic::Ordering;
use std::time::Duration;

use tinystoragedrivers_bus as wire;
use tinystoragedrivers_core::{WatchCursor, WatchNotice, WatchOptions, WatchReset};

use super::{MAX_WATCHES, Reply, StorageServer, encode, invalid, native};
use crate::convert::via;

fn reset_of(reason: WatchReset) -> wire::WatchReset {
    match reason {
        WatchReset::ResumeLost => wire::WatchReset::CursorExpired,
        WatchReset::GenerationChanged => wire::WatchReset::Fenced,
        _ => wire::WatchReset::Backend,
    }
}

fn notice_of(notice: WatchNotice) -> wire::WatchNotice {
    match notice {
        WatchNotice::Invalidated { cursor } => wire::WatchNotice::Invalidated {
            cursor: cursor.map(|cursor| cursor.as_bytes().to_vec()),
        },
        WatchNotice::ResetRequired { reason, .. } => wire::WatchNotice::ResetRequired {
            reason: reset_of(reason),
        },
    }
}

impl StorageServer {
    pub(super) async fn watch_open(&self, request: wire::WatchOpenRequest) -> Reply {
        let after = match request.after {
            Some(bytes) => Some(native(WatchCursor::new(bytes))?),
            None => None,
        };
        let options = WatchOptions {
            after,
            interval: Duration::from_millis(request.interval_ms),
        };
        native(options.validate())?;
        let filter = native(via(&request.filter))?;
        let capacity = self.watches.lock().map_or(0, |watches| watches.len());
        if capacity >= MAX_WATCHES {
            return Err(invalid());
        }
        let mut watch = native(
            self.storage
                .documents()
                .watch(&request.collection, &filter, &options)
                .await,
        )?;
        let watch_id = format!("w{}", self.next_watch.fetch_add(1, Ordering::Relaxed));
        let signals = self.signals.clone();
        let id = watch_id.clone();
        let task = tokio::spawn(async move {
            loop {
                let notice = match watch.next().await {
                    Ok(Some(notice)) => notice_of(notice),
                    Ok(None) => break,
                    Err(_) => wire::WatchNotice::ResetRequired {
                        reason: wire::WatchReset::Backend,
                    },
                };
                let terminal = matches!(notice, wire::WatchNotice::ResetRequired { .. });
                // A send error only means no client is listening right now.
                let _ = signals.send(wire::WatchChanged {
                    watch_id: id.clone(),
                    notice,
                });
                if terminal {
                    break;
                }
            }
        });
        if let Ok(mut watches) = self.watches.lock() {
            watches.insert(watch_id.clone(), task);
        } else {
            task.abort();
            return Err(wire::StorageFailure {
                kind: wire::StorageKind::Backend,
            });
        }
        encode(&wire::WatchOpenResponse { watch_id })
    }

    pub(super) fn watch_close(&self, request: &wire::WatchCloseRequest) -> Reply {
        let task = self
            .watches
            .lock()
            .ok()
            .and_then(|mut watches| watches.remove(&request.watch_id));
        match task {
            Some(task) => {
                task.abort();
                encode(&())
            }
            None => Err(wire::StorageFailure {
                kind: wire::StorageKind::NotFound,
            }),
        }
    }
}
