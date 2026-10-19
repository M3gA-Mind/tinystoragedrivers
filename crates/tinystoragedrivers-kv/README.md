# tinystoragedrivers-kv

This crate implements the scope-bound JSON `KvStore` port with independent
memory, filesystem, SQLite, and MongoDB storage. It owns only KV persistence;
consumer-specific namespacing, records, and migration rules stay with callers.

The default `memory` feature provides a cloneable store for ephemeral state and
tests. Enable `file`, `sqlite`, or `mongodb` to use the corresponding native
engine. SQLite and MongoDB adapters receive the shared `tinystoragedrivers-connect`
handle, so their data uses the same configured database as document and stream
adapters.

Each stored value is arbitrary JSON. Writes support unconditional replacement
and version preconditions, listing is stable and keyset paginated, and optional
TTL entries read as absent after expiry. SQLite and MongoDB use native KV
tables/collections; they never read or write the legacy `tsd.kv` document
collection.
