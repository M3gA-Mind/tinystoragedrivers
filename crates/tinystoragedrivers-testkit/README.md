# tinystoragedrivers-testkit

Shared testing tools for consumers and engine crates. Add this package as a
`dev-dependency`; its real memory engines and SQLite test support then stay out
of consumers' normal dependency trees.

Build a complete in-memory storage bundle from the six native primitive
engines, or request multiple cached handles from one scope-aware backend:

```rust,ignore
use tinystoragedrivers_testkit::{memory_backend, memory_storage};
use tinystoragedrivers_types::Scope;

let storage = memory_storage(Scope::new("test")?);
let backend = memory_backend();
let scoped = backend.for_scope(&Scope::new("tenant")?)?;
```

The bundle is not a document-backed approximation. Documents, streams, KV,
blobs, queues, and secrets each use their real memory engine.

The queue conformance suite can be run against any `QueueStore`:

```rust,ignore
tinystoragedrivers_testkit::queue::run(queue.as_ref()).await;
```

`NoNativeTtl` decorates a `KvStore` to exercise the fallback path for stores
without native expiration:

```rust,ignore
let kv = Arc::new(tinystoragedrivers_testkit::NoNativeTtl::new(kv));
assert!(!kv.capabilities().contains(Capability::Ttl));
```

`FaultPlan` and `Failing<T>` provide a shared deterministic schedule for
hand-written test adapters. `TestClock` supplies manually advanced millisecond
time without sleeps or wall-clock races.
