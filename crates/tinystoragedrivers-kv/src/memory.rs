//! In-memory KV engine with an injectable clock for deterministic TTL tests.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_api::KvStore;
use tinystoragedrivers_types::{
    Capabilities, Capability, Cursor, Meta, Page, Precondition, Result, StorageError, Version,
    Versioned,
};

use crate::common::{
    check_precondition, cursor_key, duration_millis, make_cursor, next_version, validate_key,
    validate_limit, validate_prefix,
};

use crate::Clock;

#[derive(Debug, Clone)]
struct Entry {
    version: Version,
    value: Option<Value>,
    expires_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<String, Entry>,
}

/// Cloneable in-memory key/value store bound to one scope.
#[derive(Clone)]
pub struct MemoryKv {
    scope: String,
    state: Arc<Mutex<State>>,
    clock: Clock,
}

impl fmt::Debug for MemoryKv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryKv")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl MemoryKv {
    /// Create an empty store reading the system clock.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(scope: tinystoragedrivers_types::Scope) -> Self {
        Self::with_clock(scope, Arc::new(system_clock))
    }

    /// Create an empty store reading epoch milliseconds from `clock`.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn with_clock(scope: tinystoragedrivers_types::Scope, clock: Clock) -> Self {
        Self {
            scope: scope.as_str().to_owned(),
            state: Arc::default(),
            clock,
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| StorageError::backend("memory KV lock was poisoned"))
    }

    fn live(entry: &Entry, now: u64) -> Option<&Value> {
        entry
            .value
            .as_ref()
            .filter(|_| entry.expires_ms.is_none_or(|expires_ms| expires_ms > now))
    }

    fn live_version(state: &State, key: &str, now: u64) -> Option<Version> {
        state
            .entries
            .get(key)
            .and_then(|entry| Self::live(entry, now).map(|_| entry.version))
    }

    fn put_locked(
        state: &mut State,
        key: &str,
        value: Value,
        expected: Precondition,
        expires_ms: Option<u64>,
        now: u64,
    ) -> Result<Version> {
        let current = Self::live_version(state, key, now);
        check_precondition(expected, current)?;
        let last = state.entries.get(key).map(|entry| entry.version);
        let version = next_version(last)?;
        state.entries.insert(
            key.to_owned(),
            Entry {
                version,
                value: Some(value),
                expires_ms,
            },
        );
        Ok(version)
    }
}

fn system_clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

fn expiry(now: u64, ttl: Duration) -> u64 {
    now.saturating_add(duration_millis(ttl))
}

#[async_trait]
impl KvStore for MemoryKv {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
            .with(Capability::Kv)
            .with(Capability::Ttl)
    }

    async fn get(&self, key: &str) -> Result<Option<Versioned<Value>>> {
        validate_key(key)?;
        let now = (self.clock)();
        let state = self.lock()?;
        Ok(state.entries.get(key).and_then(|entry| {
            Self::live(entry, now).map(|value| Versioned {
                id: key.to_owned(),
                version: entry.version,
                doc: value.clone(),
                meta: Meta::unknown(),
            })
        }))
    }

    async fn put(&self, key: &str, value: Value) -> Result<Version> {
        validate_key(key)?;
        let mut state = self.lock()?;
        Self::put_locked(
            &mut state,
            key,
            value,
            Precondition::None,
            None,
            (self.clock)(),
        )
    }

    async fn cas(&self, key: &str, value: Value, expected: Precondition) -> Result<Version> {
        validate_key(key)?;
        let mut state = self.lock()?;
        Self::put_locked(&mut state, key, value, expected, None, (self.clock)())
    }

    async fn delete(&self, key: &str, expected: Precondition) -> Result<bool> {
        validate_key(key)?;
        let now = (self.clock)();
        let mut state = self.lock()?;
        let current = Self::live_version(&state, key, now);
        check_precondition(expected, current)?;
        let Some(current) = current else {
            return Ok(false);
        };
        let version = next_version(Some(current))?;
        state.entries.insert(
            key.to_owned(),
            Entry {
                version,
                value: None,
                expires_ms: None,
            },
        );
        Ok(true)
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<Cursor>,
        limit: usize,
    ) -> Result<Page<Versioned<Value>>> {
        validate_prefix(prefix)?;
        validate_limit(limit)?;
        let after = cursor
            .as_ref()
            .map(|cursor| cursor_key(cursor, prefix))
            .transpose()?;
        let now = (self.clock)();
        let state = self.lock()?;
        let mut found: Vec<_> = state
            .entries
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .filter(|(key, _)| after.as_ref().is_none_or(|after| *key > after))
            .filter_map(|(key, entry)| {
                Self::live(entry, now).map(|value| Versioned {
                    id: key.clone(),
                    version: entry.version,
                    doc: value.clone(),
                    meta: Meta::unknown(),
                })
            })
            .take(limit + 1)
            .collect();
        let has_more = found.len() > limit;
        if has_more {
            found.pop();
        }
        let next = if has_more {
            found
                .last()
                .map(|item| make_cursor(&item.id, prefix))
                .transpose()?
        } else {
            None
        };
        Ok(Page { items: found, next })
    }

    async fn put_with_ttl(&self, key: &str, value: Value, ttl: Duration) -> Result<Version> {
        validate_key(key)?;
        let now = (self.clock)();
        let mut state = self.lock()?;
        Self::put_locked(
            &mut state,
            key,
            value,
            Precondition::None,
            Some(expiry(now, ttl)),
            now,
        )
    }
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
