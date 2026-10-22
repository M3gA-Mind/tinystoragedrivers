//! Clock injection shared by local queue engines.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Injectable Unix epoch-millisecond clock for deterministic queue behavior.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub(crate) fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
