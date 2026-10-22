//! Host-owned watch subscriptions projected to payload-free signals.
use std::sync::atomic::Ordering;
use std::time::Duration;

use tinystoragedrivers_bus as wire;
use tinystoragedrivers_types::{WatchCursor, WatchNotice, WatchOptions, WatchReset};

use super::{MAX_WATCHES, Reply, StorageServer, encode, invalid, native};
use crate::convert::via;

fn reset_of(reason: WatchReset) -> wire::WatchReset {
    match reason {
        WatchReset::ResumeLost => wire::WatchReset::CursorExpired,
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

fn backend_failure() -> wire::StorageFailure {
    wire::StorageFailure {
        kind: wire::StorageKind::Backend,
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
        // Reserve the slot before the await so concurrent opens cannot all pass
        // the capacity check; release it if the native open fails.
        {
            let Ok(mut watches) = self.watches.lock() else {
                return Err(backend_failure());
            };
            if watches.used() >= MAX_WATCHES {
                return Err(invalid());
            }
            watches.reserved += 1;
        }
        let opened = self
            .storage
            .documents()
            .watch(&request.collection, &filter, &options)
            .await;
        let mut watch = match opened {
            Ok(watch) => watch,
            Err(error) => {
                self.release_reservation();
                return Err(crate::convert::failure_of(&error));
            }
        };
        let watch_id = format!("w{}", self.next_watch.fetch_add(1, Ordering::Relaxed));
        let signals = self.signals.clone();
        let id = watch_id.clone();
        let registry = self.watches.clone();
        let (registered_tx, registered_rx) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            // Wait until the handle is in the registry so a watch that ends
            // at once still finds its own entry to remove.
            let _ = registered_rx.await;
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
            // A finished watch frees its slot; the client's WatchClose is then
            // a harmless NotFound.
            if let Ok(mut watches) = registry.lock() {
                watches.tasks.remove(&id);
            }
        });
        if let Ok(mut watches) = self.watches.lock() {
            watches.reserved = watches.reserved.saturating_sub(1);
            watches.tasks.insert(watch_id.clone(), task);
        } else {
            task.abort();
            return Err(backend_failure());
        }
        let _ = registered_tx.send(());
        encode(&wire::WatchOpenResponse { watch_id })
    }

    fn release_reservation(&self) {
        if let Ok(mut watches) = self.watches.lock() {
            watches.reserved = watches.reserved.saturating_sub(1);
        }
    }

    pub(super) fn watch_close(&self, request: &wire::WatchCloseRequest) -> Reply {
        let task = self
            .watches
            .lock()
            .ok()
            .and_then(|mut watches| watches.tasks.remove(&request.watch_id));
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

#[cfg(test)]
#[path = "watch_tests.rs"]
mod tests;
