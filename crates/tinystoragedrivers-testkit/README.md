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

The crate also exports `blobs`, `kv`, `secrets`, `documents`, `validated`,
`watch`, `semantics`, `streams`, and `seal` suites. Run each suite from an
engine crate's dev-only test target with the corresponding real port handle;
the suite uses unique names and does not depend on a process-global database.
Optional behavior is checked only when the port advertises it. For example,
`blobs::run` verifies streamed writes reject short, long, and failed readers
without publishing partial objects, while `kv::run` exercises insert-only
preconditions, revisions across delete/recreate, and TTL only when supported.

The `memory_storage(&Scope)` factory builds all six ports from their native
memory engines. Consumers should add this crate only under `[dev-dependencies]`
and compose the real storage bundle in tests rather than substituting a fake
document store for unrelated ports.

`NoNativeTtl` decorates a `KvStore` to exercise the fallback path for stores
without native expiration:

```rust,ignore
let kv = Arc::new(tinystoragedrivers_testkit::NoNativeTtl::new(kv));
assert!(!kv.capabilities().contains(Capability::Ttl));
```

`FaultPlan` and `Failing<T>` provide a shared deterministic schedule for
hand-written test adapters. Wrap each adapter call with
`failing.call("put", inner.put(...)).await?`; the inner call is skipped when
the named failure point is reached. `ConflictOnce<T>::call(future)` injects
one `Conflict` before delegating later calls, which is useful for retry loops.
`Slow<T>` provides an explicit `before().await` hook (or
`run(future).await`) for tests that need a deliberately delayed adapter
operation. `TestClock` supplies manually advanced millisecond time without
wall-clock races.
