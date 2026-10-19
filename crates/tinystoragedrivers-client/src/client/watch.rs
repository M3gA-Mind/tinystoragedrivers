//! A watch: host-owned subscription, payload-free `WatchChanged` signals.
use tinystoragedrivers_bus as wire;
use tinystoragedrivers_core::{
    DocumentWatch, Result, WatchCursor, WatchNotice, WatchReset, async_trait,
};
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use super::Wire;
use crate::transport::Object;

/// A [`DocumentWatch`] fed by signals for one host-issued watch id.
pub(super) struct BusWatch {
    wire: Wire,
    watch_id: String,
    signals: broadcast::Receiver<wire::WatchChanged>,
    finished: bool,
}

impl std::fmt::Debug for BusWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusWatch").finish_non_exhaustive()
    }
}

impl BusWatch {
    pub(super) fn new(
        wire: Wire,
        watch_id: String,
        signals: broadcast::Receiver<wire::WatchChanged>,
    ) -> Self {
        Self {
            wire,
            watch_id,
            signals,
            finished: false,
        }
    }
}

fn reset_of(reason: wire::WatchReset) -> WatchReset {
    match reason {
        wire::WatchReset::CursorExpired => WatchReset::ResumeLost,
        wire::WatchReset::Backend => WatchReset::Invalidated,
    }
}

#[async_trait]
impl DocumentWatch for BusWatch {
    async fn next(&mut self) -> Result<Option<WatchNotice>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            match self.signals.recv().await {
                Ok(changed) if changed.watch_id == self.watch_id => {
                    return Ok(Some(match changed.notice {
                        wire::WatchNotice::Invalidated { cursor } => WatchNotice::Invalidated {
                            cursor: cursor.and_then(|bytes| WatchCursor::new(bytes).ok()),
                        },
                        wire::WatchNotice::ResetRequired { reason } => {
                            self.finished = true;
                            WatchNotice::ResetRequired {
                                reason: reset_of(reason),
                                source: None,
                            }
                        }
                    }));
                }
                Ok(_) => {}
                Err(RecvError::Lagged(_)) => {
                    self.finished = true;
                    return Ok(Some(WatchNotice::ResetRequired {
                        reason: WatchReset::Overflow,
                        source: None,
                    }));
                }
                Err(RecvError::Closed) => return Ok(None),
            }
        }
    }
}

impl Drop for BusWatch {
    fn drop(&mut self) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let wire = self.wire.clone();
        let request = wire::WatchCloseRequest {
            watch_id: std::mem::take(&mut self.watch_id),
        };
        runtime.spawn(async move {
            // Best effort: the host also drops watches when the binding ends.
            let _: Result<()> = wire
                .call(Object::Documents, "WatchClose", &request, None)
                .await;
        });
    }
}
