# QueueStore: durable message queues

**Status:** Implemented in the v2 queue primitive and storage bus. **Owner:**
tinystoragedrivers.

## Problem

Hosts take in large volumes of external events (chat messages, webhooks,
connector triggers) that must survive a restart, be processed at least once,
and be retried or set aside when processing fails. Today each consumer builds
this from `DocumentStore::claim` by hand (the run ledger in
[`storage-ports.md`](storage-ports.md)), without visibility timeouts, attempt
counts or dead letters, and nothing lets a deployment move the queue to a
broker such as RabbitMQ.

## Goals and non-goals

Goals:

- one port, `QueueStore`, scoped like the other ports;
- at-least-once delivery with leases, retries, dead letters and redrive;
- optional groups (one leased message per group), dedupe keys, priorities and
  delays, each reported in `QueueFeatures`;
- a generic implementation over any `DocumentStore`, so memory, SQLite, file
  and MongoDB need no driver code;
- a conformance suite every driver runs.

Non-goals: exactly-once delivery, pub/sub fan-out to several consumer groups
(use one queue per consumer), message filtering or routing (the caller's
concern), and payloads larger than a document.

## Proposed behavior

```rust
#[async_trait]
pub trait QueueStore: Send + Sync + Debug {
    fn features(&self) -> QueueFeatures;
    async fn ensure_queue(&self, queue: &str, spec: &QueueSpec) -> Result<()>;
    async fn enqueue(&self, queue: &str, messages: &[NewMessage]) -> Result<Vec<Enqueued>>;
    async fn receive(&self, queue: &str, max: u32, visibility_ms: u64, consumer: &str)
        -> Result<Vec<Delivery>>;
    async fn ack(&self, queue: &str, receipt: &Receipt) -> Result<bool>;
    async fn nack(&self, queue: &str, receipt: &Receipt, retry: Retry) -> Result<bool>;
    async fn extend(&self, queue: &str, receipt: &Receipt, visibility_ms: u64) -> Result<bool>;
    async fn stats(&self, queue: &str) -> Result<QueueStats>;
    async fn dead_letters(&self, queue: &str, limit: usize) -> Result<Vec<DeadLetter>>;
    async fn redrive(&self, queue: &str, id: &str) -> Result<bool>;
    async fn purge(&self, queue: &str) -> Result<u64>;
}
```

- Queue names are 1 to 64 bytes of `[a-z0-9._-]`.
- `receive` leases up to `max` deliverable messages for `visibility_ms`,
  highest priority (0 to 9) first, then oldest. Each `Delivery` carries a
  `Receipt` (message id plus a per-delivery token) and its `attempt`, starting
  at 1.
- `ack`, `nack` and `extend` return `false`, not an error, when the receipt no
  longer holds the message: its lease lapsed and another consumer has it, or it
  was settled already.
- A lapsed lease makes the message deliverable again with `attempt + 1`. A
  message whose next delivery would pass `QueueSpec::max_attempts` is
  dead-lettered with reason `max attempts exceeded`, as is a
  `Retry::After` nack on the last attempt. `Retry::DeadLetter(reason)` records
  the caller's reason.
- `redrive` puts a dead letter back with a fresh attempt count.
- **Groups.** Messages sharing `NewMessage::group` are leased one at a time,
  in priority then enqueue order. A `Retry::After` nack keeps the group held by
  that message until the retry is due, so later messages do not overtake it.
- **Dedupe.** A second enqueue of a `dedupe_key` that is still queued, leased
  or dead, or that was acknowledged less than `QueueSpec::dedupe_window_ms`
  ago, returns `Enqueued { duplicate: true }` with the held message's id and
  stores nothing.

## Invariants and constraints

- A message is leased to at most one receipt at a time.
- An acknowledged message is never delivered again.
- A message is either deliverable, delayed, leased, dead, or gone; `stats`
  counts the first four.
- Within a group, at most one message is leased at a time.
- Time comes from the handle's clock, never the database's, so the suite can
  drive it; `DocumentQueue::with_clock` injects one.
- Queues on different scopes never meet, as for every port.

## Acceptance criteria

The `tinystoragedrivers-testkit` queue conformance suite checks ordering,
acknowledgement, nacks, attempts, dead letters, redrive, extension, stats,
isolation and purge, plus delays, groups and dedupe when the engine reports
those capabilities. The in-memory queue runs without a service; RabbitMQ's
live acceptance uses a disposable broker.

The v2 `Storage` bundle contains a queue port for its scope. Applications can
use `QueueConfig` and `open_queues` to install a RabbitMQ implementation; the
manifest and declaration policy remain with the host that owns the queues.

## Open questions

None. The broker mapping, including why RabbitMQ reports `dedupe: false` and
`groups: false`, is settled in [`queue-rabbitmq.md`](queue-rabbitmq.md).
