//! Native in-memory implementation of the queue port.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use serde_json::Value;
use tinystoragedrivers_types::{
    DeadLetter, Delivery, Enqueued, NewMessage, QueueFeatures, QueueSpec, QueueStats, Receipt,
    Result, Retry, StorageError, validate_queue_name,
};

use crate::clock::{Clock, system_clock};
use crate::{QueueStore, validate_message, validate_spec};

#[derive(Debug, Clone)]
enum State {
    Ready {
        at: i64,
    },
    Leased {
        until: i64,
        token: String,
        consumer: String,
    },
    Dead {
        reason: String,
    },
    Done {
        expires: i64,
    },
}

#[derive(Debug, Clone)]
struct Message {
    id: String,
    queue: String,
    payload: Value,
    priority: u8,
    group: Option<String>,
    dedupe_key: Option<String>,
    enqueued_ms: i64,
    seq: u64,
    attempt: u32,
    state: State,
}

#[derive(Debug, Default)]
struct Data {
    messages: HashMap<String, Message>,
    specs: HashMap<String, QueueSpec>,
    groups: HashMap<(String, String), (String, i64)>,
    sequence: u64,
}

/// A queue kept entirely in process memory. Clones share its messages and
/// declarations; dropping the last handle discards them.
#[derive(Clone)]
pub struct MemoryQueue {
    data: Arc<Mutex<Data>>,
    clock: Clock,
}

impl std::fmt::Debug for MemoryQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryQueue").finish_non_exhaustive()
    }
}

impl Default for MemoryQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryQueue {
    /// Create an empty queue using the system clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(system_clock))
    }

    /// Create an empty queue using `clock`.
    #[must_use]
    pub fn with_clock(clock: Clock) -> Self {
        Self {
            data: Arc::new(Mutex::new(Data::default())),
            clock,
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Data>> {
        self.data
            .lock()
            .map_err(|_| StorageError::backend("memory queue lock poisoned"))
    }

    fn now(&self) -> Result<i64> {
        i64::try_from((self.clock)()).map_err(|_| StorageError::invalid_input("clock out of range"))
    }

    fn after(now: i64, ms: u64) -> Result<i64> {
        i64::try_from(ms)
            .ok()
            .and_then(|ms| now.checked_add(ms))
            .ok_or_else(|| StorageError::invalid_input("queue delay out of range"))
    }

    fn held(message: &Message, queue: &str, receipt: &Receipt) -> bool {
        message.queue == queue
            && matches!(&message.state, State::Leased { token, .. } if *token == receipt.token)
    }

    fn release_group(data: &mut Data, queue: &str, message: &Message) {
        if let Some(group) = &message.group {
            let key = (queue.to_owned(), group.clone());
            if data
                .groups
                .get(&key)
                .is_some_and(|(owner, _)| owner == &message.id)
            {
                data.groups.remove(&key);
            }
        }
    }
}

#[async_trait]
impl QueueStore for MemoryQueue {
    fn features(&self) -> QueueFeatures {
        QueueFeatures {
            dedupe: true,
            groups: true,
            priorities: true,
            delay: true,
        }
    }

    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()> {
        validate_queue_name(queue)?;
        validate_spec(spec)?;
        self.lock()?.specs.insert(queue.to_owned(), *spec);
        Ok(())
    }

    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let mut data = self.lock()?;
        let mut output = Vec::with_capacity(messages.len());
        for message in messages {
            validate_message(message)?;
            let ready_at = Self::after(now, message.delay_ms)?;
            let mut duplicate = None;
            if let Some(key) = &message.dedupe_key {
                let existing = data
                    .messages
                    .values()
                    .find(|stored| {
                        stored.queue == queue && stored.dedupe_key.as_deref() == Some(key.as_str())
                    })
                    .map(|stored| (stored.id.clone(), stored.state.clone()));
                if let Some(stored) = existing {
                    let expired = matches!(stored.1, State::Done { expires } if expires <= now);
                    if expired {
                        data.messages.remove(&stored.0);
                    } else {
                        duplicate = Some(stored.0);
                    }
                }
            }
            if let Some(id) = duplicate {
                output.push(Enqueued {
                    id,
                    duplicate: true,
                });
                continue;
            }
            data.sequence = data.sequence.saturating_add(1);
            let sequence = data.sequence;
            let id = crate::key::ulid();
            data.messages.insert(
                id.clone(),
                Message {
                    id: id.clone(),
                    queue: queue.to_owned(),
                    payload: message.payload.clone(),
                    priority: message.priority,
                    group: message.group.clone(),
                    dedupe_key: message.dedupe_key.clone(),
                    enqueued_ms: now,
                    seq: sequence,
                    attempt: 0,
                    state: State::Ready { at: ready_at },
                },
            );
            output.push(Enqueued {
                id,
                duplicate: false,
            });
        }
        Ok(output)
    }

    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> Result<Vec<Delivery>> {
        validate_queue_name(queue)?;
        if max == 0 {
            return Err(StorageError::invalid_input("receive at least one message"));
        }
        if visibility_ms == 0 {
            return Err(StorageError::invalid_input(
                "a visibility window must be positive",
            ));
        }
        let now = self.now()?;
        let until = Self::after(now, visibility_ms)?;
        let mut data = self.lock()?;
        let spec = data.specs.get(queue).copied().unwrap_or_default();
        let mut candidates: Vec<_> = data
            .messages
            .values()
            .filter(|m| {
                m.queue == queue
                    && match &m.state {
                        State::Ready { at } => *at <= now,
                        State::Leased { until, .. } => *until <= now,
                        _ => false,
                    }
            })
            .map(|m| (m.id.clone(), m.priority, m.seq, m.group.clone()))
            .collect();
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.2.cmp(&b.2)));
        let mut out = Vec::new();
        let mut skipped = Vec::<String>::new();
        for (id, _, _, group) in candidates {
            if out.len() >= usize::try_from(max).unwrap_or(usize::MAX) {
                break;
            }
            if group.as_ref().is_some_and(|g| skipped.contains(g)) {
                continue;
            }
            let Some(message) = data.messages.get(&id).cloned() else {
                continue;
            };
            if let Some(group) = &message.group {
                let key = (queue.to_owned(), group.clone());
                if data
                    .groups
                    .get(&key)
                    .is_some_and(|(owner, expiry)| owner != &id && *expiry > now)
                {
                    skipped.push(group.clone());
                    continue;
                }
            }
            let attempt = message.attempt.saturating_add(1);
            if attempt > spec.max_attempts {
                if let Some(current) = data.messages.get_mut(&id) {
                    current.state = State::Dead {
                        reason: "max attempts exceeded".to_owned(),
                    };
                }
                Self::release_group(&mut data, queue, &message);
                continue;
            }
            let token = crate::key::ulid();
            if let Some(current) = data.messages.get_mut(&id) {
                current.attempt = attempt;
                current.state = State::Leased {
                    until,
                    token: token.clone(),
                    consumer: consumer.to_owned(),
                };
            }
            if let Some(group) = &message.group {
                data.groups
                    .insert((queue.to_owned(), group.clone()), (id.clone(), until));
            }
            out.push(Delivery {
                receipt: Receipt { id, token },
                payload: message.payload,
                attempt,
                priority: message.priority,
                group: message.group,
                enqueued_ms: message.enqueued_ms,
                lease_until_ms: until,
            });
        }
        Ok(out)
    }

    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let mut data = self.lock()?;
        let Some(message) = data.messages.get(&receipt.id).cloned() else {
            return Ok(false);
        };
        if !Self::held(&message, queue, receipt) {
            return Ok(false);
        }
        let window = data
            .specs
            .get(queue)
            .copied()
            .unwrap_or_default()
            .dedupe_window_ms;
        if message.dedupe_key.is_some() && window > 0 {
            let expires = Self::after(now, window)?;
            if let Some(current) = data.messages.get_mut(&receipt.id) {
                current.payload = Value::Null;
                current.state = State::Done { expires };
            }
        } else {
            data.messages.remove(&receipt.id);
        }
        Self::release_group(&mut data, queue, &message);
        Ok(true)
    }

    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let mut data = self.lock()?;
        let Some(message) = data.messages.get(&receipt.id).cloned() else {
            return Ok(false);
        };
        if !Self::held(&message, queue, receipt) {
            return Ok(false);
        }
        let spec = data.specs.get(queue).copied().unwrap_or_default();
        let (state, group_until) = match retry {
            Retry::After(delay) if message.attempt < spec.max_attempts => {
                let at = Self::after(now, delay)?;
                (State::Ready { at }, Some(at))
            }
            Retry::After(_) => (
                State::Dead {
                    reason: "max attempts exceeded".to_owned(),
                },
                None,
            ),
            Retry::DeadLetter(reason) => (State::Dead { reason }, None),
        };
        if let Some(current) = data.messages.get_mut(&receipt.id) {
            current.state = state;
        }
        if let Some(group) = &message.group {
            let key = (queue.to_owned(), group.clone());
            if let Some(until) = group_until {
                data.groups.insert(key, (message.id.clone(), until));
            } else {
                Self::release_group(&mut data, queue, &message);
            }
        }
        Ok(true)
    }

    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool> {
        validate_queue_name(queue)?;
        if visibility_ms == 0 {
            return Err(StorageError::invalid_input(
                "a visibility window must be positive",
            ));
        }
        let until = Self::after(self.now()?, visibility_ms)?;
        let mut data = self.lock()?;
        let Some(message) = data.messages.get(&receipt.id).cloned() else {
            return Ok(false);
        };
        if !Self::held(&message, queue, receipt) {
            return Ok(false);
        }
        if let Some(current) = data.messages.get_mut(&receipt.id)
            && let State::Leased {
                token, consumer, ..
            } = &current.state
        {
            current.state = State::Leased {
                until,
                token: token.clone(),
                consumer: consumer.clone(),
            };
        }
        if let Some(group) = &message.group {
            data.groups
                .insert((queue.to_owned(), group.clone()), (message.id, until));
        }
        Ok(true)
    }

    async fn stats(&self, queue: &str) -> Result<QueueStats> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let data = self.lock()?;
        let mut stats = QueueStats::default();
        for message in data.messages.values().filter(|m| m.queue == queue) {
            match message.state {
                State::Ready { at } if at <= now => stats.ready += 1,
                State::Ready { .. } => stats.delayed += 1,
                State::Leased { until, .. } if until <= now => stats.ready += 1,
                State::Leased { .. } => stats.leased += 1,
                State::Dead { .. } => stats.dead += 1,
                State::Done { .. } => {}
            }
        }
        Ok(stats)
    }

    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>> {
        validate_queue_name(queue)?;
        let data = self.lock()?;
        let mut messages: Vec<_> = data
            .messages
            .values()
            .filter(|m| m.queue == queue && matches!(m.state, State::Dead { .. }))
            .collect();
        messages.sort_by_key(|m| m.seq);
        Ok(messages
            .into_iter()
            .take(limit)
            .map(|m| DeadLetter {
                id: m.id.clone(),
                payload: m.payload.clone(),
                attempt: m.attempt,
                reason: match &m.state {
                    State::Dead { reason, .. } => reason.clone(),
                    _ => String::new(),
                },
                group: m.group.clone(),
                enqueued_ms: m.enqueued_ms,
            })
            .collect())
    }

    async fn redrive(&self, queue: &str, id: &str) -> Result<bool> {
        validate_queue_name(queue)?;
        let now = self.now()?;
        let mut data = self.lock()?;
        let Some(message) = data.messages.get_mut(id) else {
            return Ok(false);
        };
        if message.queue != queue || !matches!(message.state, State::Dead { .. }) {
            return Ok(false);
        }
        message.attempt = 0;
        message.state = State::Ready { at: now };
        Ok(true)
    }

    async fn purge(&self, queue: &str) -> Result<u64> {
        validate_queue_name(queue)?;
        let mut data = self.lock()?;
        let before = data.messages.len();
        data.messages.retain(|_, message| message.queue != queue);
        data.groups.retain(|(name, _), _| name != queue);
        Ok(u64::try_from(before - data.messages.len()).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
