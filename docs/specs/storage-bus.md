# Storage bus

Status: accepted for contract design; implementation is staged by
[`../plans/storage-bus.md`](../plans/storage-bus.md). Owner: tinystoragedrivers.

## Problem and boundary

Compiled TinyBus modules need storage that follows the same tenant, schema,
atomicity and capability rules as native callers. `DocumentCollection1` is an
existing confined Get/Put/Delete callback, but it is not the complete
`StorageBackend` port. This contract defines the complete
`ai.tinyhumans.tinystorage.Storage` object family, with Documents, Streams,
Blobs and Secrets objects. The new Documents wire types
are synchronous serde data and do not depend on native ports, an async runtime,
or a database. The existing crate still houses the legacy TinyBus CRUD proxy
for compatibility; moving transport adapters out of the wire crate is part of
the staged implementation, not a property claimed for the current dependency
graph.

Goals are full Documents, Streams, Blobs and Secrets parity; one host-captured
caller authority; mechanically adapted clients and servers; and shared
conformance proving same-port behavior. The existing `tinystoragedrivers`
name remains. This does not move domain repositories into storage, let modules
choose storage URLs/scopes, or allow a module to create a host grant.

## Authority and dispatch

The host authenticates the module caller and binds the call to one principal,
scope, database, admitted backend, and immutable grant. The request has no
scope, database, URL, backend, generation, or grant selector. Unknown selectors
and unknown request fields fail closed. The host checks caller rights, object
rights, declared schema and resource limits on every call; a module's claimed
identity is not authority. Scope is never serialized in a request or inferred
from a document body.

Grants enumerate allowed object families, collections, stream names, blob key
prefixes, secret name prefixes, and operations. Documents schema rights list
permitted indexes, TTL fields, and search fields; modules may only use host
approved declarations. Host fences are captured at registration and checked
atomically by native writes, including empty checked batches. Stale authority
returns `Fenced`. Secrets always use the encrypted `SecretStore` port and never
cross a non-confidential route. Failure payloads carry only the sanitized
storage error category.

## Objects and behavior

`Storage` contains `Documents`, `Streams`, `Blobs`, and `Secrets`. Collection
names are explicit in Documents operation messages and are checked against the
captured host grant on every call. They select data within that grant; they do
not select a scope, database, backend, or schema authority. Each object
has a fixed versioned interface, finite selector list, and typed synchronous
serde messages. TinyBus request/reply provides bounded control traffic. Bulk
payloads over 16 MiB use an authenticated TinyBus `Stream`; there is no inline
fallback above that limit. Stream capabilities are bound to the same caller
grant and one operation, enforce byte limits, and are closed on completion or
failure. Watch delivery uses TinyBus `SIGNAL`s: `WatchOpen` and `WatchClose`
control a host-owned subscription, while `WatchChanged` carries only a
host-issued watch id and a content-free invalidation/reset notice. Its cursor
matches native `WatchOptions.after` resume semantics and its bounded quiet
interval is 1–60,000 milliseconds. Clients reread authoritative data through
Documents. Signals never carry documents, secrets, blobs, or stream contents.

Documents mirrors `DocumentStore`: get/put/delete, filter/sort keyset query,
count, delete-where, atomic claim, atomic batch, checked atomic batch, search,
schema ensure/validate, collection drop and enumeration. Requests preserve
native versions and preconditions. Cursors are opaque, query-bound and
host-grant-bound. Search uses declared fields and returns best matches first;
the contract promises stable ordering within one result list, not cross-driver
score equality. Schema changes are host-authorized only. Full-text index
rebuild/reindex is a host/driver maintenance operation, never a module selector.

FTS providers must apply a documented deterministic tokenization and ranking
strategy per driver; conformance pins token membership and ordering for ties,
while relevance scores remain backend-local. Reindexing a changed approved
search declaration is idempotent and does not expose index internals.

Vector search is intentionally not a Documents capability. Semantic memory
indexes, embeddings, candidate retrieval and relevance policy belong to
TinyMemory, whose storage implementation and TinyBus module contract own the
end-to-end meaning of a vector query. Keeping that query inside TinyMemory
ensures every host uses its same embedding model, metadata filters, recall
policy and ranking, while Storage supplies durable documents, streams and
blobs. A generic vector port here would otherwise leave embedding generation,
dimensions, distance metrics, filtering, ranking and index lifecycle undefined
across drivers. TinyMemory can propose a separately versioned vector capability
only if it needs a driver-neutral acceleration seam and defines conformance for
those semantics; the Storage bus will then adapt that accepted capability.

Streams mirrors append/read/range/length/truncate/delete and conditional batch
append when supported. Offsets are dense and never reused by truncation.
Blobs mirrors put/get/ranged-read/delete/prefix-list and sends large byte
payloads only over TinyBus Stream. Secrets mirrors get/set/delete/prefix-list
through encrypted native storage; secret bytes and names are only returned to
authorized confidential callers. Every operation reports unsupported
capabilities explicitly; adapters must not claim unimplemented capabilities.

The S3 driver remains a host-side driver. Hosts forward S3 configuration and
credentials into their own storage backend construction; neither these secrets
nor S3 endpoints are passed through a module request or module-controlled URL.

## Acceptance

- The new wire types use serde-only data dependencies, with no core, transport,
  async-trait or runtime dependency. The crate currently retains its legacy
  TinyBus proxy dependency for compatibility; it is not represented as a
  serde-only crate dependency graph.
- Every selector and message shape is pinned by wire roundtrip and unknown
  selector/field rejection tests; no message accepts scope selectors.
- Native and module callers run the same Documents/Streams/Blobs/Secrets
  conformance cases against one backend and observe the same committed state.
- Authentication, grant, schema, confidentiality, fencing and size checks run
  before native I/O. Watch signals contain no payload.
- Calls larger than 16 MiB use TinyBus Stream with caller-bound limits.
- Unsupported capabilities remain unsupported; no adapter fabricates success.

## Open questions

None for the accepted boundary. Selector versioning and any future streaming
extension require a new interface version rather than changing existing wire
shapes.
