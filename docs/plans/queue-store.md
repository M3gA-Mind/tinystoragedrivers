# QueueStore delivery plan

Specification: [`../specs/queue-store.md`](../specs/queue-store.md).

## Tasks

1. [x] Port and types in `crates/tinystoragedrivers-core/src/queue/`
   (`mod.rs`, `types.rs`), exported from `lib.rs`.
2. [x] `DocumentQueue` over any `DocumentStore`
   (`crates/tinystoragedrivers-core/src/queue/document.rs`), with collections
   `tsd.queue`, `tsd.queue_groups` and `tsd.queue_specs`.
3. [x] Conformance checks (`conformance/queue.rs`) wired into `run_ports`, so
   memory, SQLite, file and the bus client loopback run them.
3a. [x] Queue manifest: `queue/manifest.rs` (`QueueManifest`,
   `QueueDeclaration`), `schemas/queues.schema.json`,
   `docs/examples/queues.json`.
4. [x] MongoDB live job passes the queue checks.
5. [ ] Storage bus: a `Queues` object and selectors in
   `tinystoragedrivers-bus` (contract version bump), client and busproxy
   server, loopback conformance.
6. [ ] `tinystoragedrivers-rabbitmq`: native queue backend behind the facade's
   `rabbitmq` feature and an `amqp://` queue URL, with a live CI job.
7. [ ] `ScopedStorage::queues()` and facade wiring for a queue URL separate
   from the storage URL.

## Verification

```sh
cargo test -p tinystoragedrivers-core --all-features queue
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
```
