# tinystoragedrivers-testkit

Shared testing tools for consumers and engine crates. Add this package as a
`dev-dependency`; it has no storage engine dependency in its normal build.

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
