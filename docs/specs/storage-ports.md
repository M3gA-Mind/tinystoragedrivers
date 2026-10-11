# Storage ports

Status: accepted (v0.3, ports, memory driver and conformance suite).

## Problem

OpenHuman persists data in more than a dozen places, and each one talks to its
backend directly:
- rusqlite databases opened per call: sessions, run ledger, flows, cron,
  approvals, devices, notifications, task sources, graph checkpoints
- JSON and JSONL files: turn states, threads, artifacts, costs, credentials
- the OS keyring

Some of the libraries it hosts (tinyagents, tinyflows) hard-wire SQLite in the
same way. Nothing can run against another backend, and a multi-tenant cloud
deployment on MongoDB is impossible without rewriting every store.

## Outcome

- One set of backend-neutral ports that every persisting crate builds on.
- A driver per backend: memory, SQLite, MongoDB, files, and secrets (the
  `SecretStore` contract is in [`secret-storage.md`](secret-storage.md)). Each is
  compiled in by Cargo feature and chosen at boot by URL.
- The same behavior on every driver, enforced by one conformance suite.
- Tenant isolation as a property of the port handle, not of each call site.

Non-goals:
- typed repositories (they stay with the crates that own the records)
- a query language richer than what both SQL and MongoDB evaluate natively
- cross-backend replication

## Ports

### `DocumentStore`

Versioned JSON **objects** in named collections.

- **Names.** Collection names are 1–120 bytes of `[A-Za-z0-9_.-]`, and the
  prefix `_tsd` is reserved for drivers. Ids are 1–512 bytes without NUL.
- **Versions.** A new document is version 1, and every write produces a
  strictly greater version. A write that would exceed `u64::MAX` fails with
  `ErrorKind::Backend` and changes nothing; a version is never reused. That
  holds across deletion: a deleted (or dropped) id that is written again
  continues from its last version, so a compare-and-swap prepared before the
  deletion fails instead of overwriting the new document.
- **Preconditions.** `Precondition::None` upserts. `Absent` inserts. `Version(v)`
  is a compare-and-swap. A failed precondition is `ErrorKind::Conflict`.
- **Filters.** `Filter` supports `eq`, `ne`, `in`, `range`, `exists`, `and`,
  `or` and `not`, over dotted paths. `_id` addresses the document id.
  - Values compare by the cross-type order in `value::compare`: null < bool <
    number < string < array < object. Numbers compare numerically.
  - A range bound of a different JSON type never matches.
  - `ne` matches documents where the field is absent.
- **Sorting.** Any number of keys. Ties, and an empty sort, fall back to id
  ascending. A missing field sorts as null.
- **Paging.** `Query.limit` caps a page and must be at least 1. Sort fields
  follow the same path rules as filters. `Page.next`
  is an opaque cursor, valid only for the same collection, filter and sort on
  the same driver; any other cursor is `ErrorKind::InvalidInput`.
- **Declarations.** `ensure_collection` merges into what is already declared:
  indexes and search fields accumulate, and an expiry field can be added but
  not changed. Redeclaring an index name with different fields is
  `InvalidInput`. Adding a unique index that stored documents already violate
  (in any scope) is `AlreadyExists`, and nothing is declared. A declaration is schema, written by host code rather than
  tenants, so it applies to the collection in every scope, the way a SQL or
  MongoDB index spans the whole table or collection.
- **Indexes.** Any field is queryable. An `IndexSpec` is a performance hint;
  when it is `unique`, every driver enforces it with `ErrorKind::AlreadyExists`.
  Documents missing an indexed field are unconstrained.
- **Claims.** `claim(filter, sort, patch)` picks the first match, applies an
  RFC 7396 merge patch, and returns the updated document, all as one atomic
  step. Two claims never return the same document when the patch makes it
  stop matching.
- **Optional capabilities.** A capability the driver lacks is
  `ErrorKind::Unsupported(capability)`.
  - `Ttl`: documents whose `ttl_field` (epoch ms) has passed read as absent.
  - `Transactions`: `atomic_batch`, all or nothing.
  - `CollectionEnumeration`: `collections(prefix)` lists collection names with
    stored records in this handle's scope. It filters by a literal string prefix
    and sorts names lexicographically. Tombstones count as stored records so
    complete scoped cleanup can discover deleted versions; declared collections
    with no records are omitted. Without the capability, the method returns
    `Unsupported(CollectionEnumeration)`. Enumeration never exposes another
    scope or named database.

  - `FullText`: `search` over the declared `SearchSpec` fields. Conformance
    checks single-token membership only, never ranking.
  - CrossDatabaseFencing: native guard addressing in the default or another
    named database through for_scope_fenced_in_database.
  - `Fencing`: `StorageBackend::for_scope_fenced`, see
    [`write-fencing.md`](write-fencing.md).

### `StreamStore`

Append-only logs of JSON values.
- Offsets are dense, start at 0, and are never reused.
- `truncate_before` discards old records but leaves `len` unchanged.

Optional ConditionalStreamAppend provides append_batch_if_offset(stream,
expected_next_offset, values). Compare the persistent next offset and append
the entire batch in the same atomic step; return its first offset. A missing
stream has next offset 0. An empty batch still checks the expected offset and
any native fence.

A mismatched offset returns Conflict without appending. Validation errors and
aborted operations append nothing. Native fence validation participates in the
same lock/transaction. Scope and named database isolation match other stream
operations. Unsupported adapters return Unsupported(ConditionalStreamAppend).
MongoDB advertises the capability only when transactions are available; file
atomicity remains within its documented process boundary.

A transport failure during MongoDB commit can leave the outcome unknown,
reported explicitly as a backend error. Read before retrying; repeating the
same expected offset cannot duplicate a committed batch. delete_stream resets
next offset to 0; truncate_before never does. A caller retaining a cursor across
deletion must separately track identity.

### `BlobStore`

Bytes by `/`-separated key, without empty, `.` or `..` segments, and with no
leading `/`. Supports ranged reads clamped to the blob's length, and prefix
listing.

### `StorageBackend` and `Scope`

- `for_scope(scope)` returns handles bound to one tenant. No port method takes
  a scope, so a call site cannot forget it.
- `Scope::local()` is the single-operator scope.
- A backend that serves only one scope (a desktop SQLite file) rejects other
  scopes in `for_scope`.
- `database(name)` returns an independent named database. Names are 1–64 bytes
  of lowercase `[a-z0-9_-]`. The same name always addresses the same data.
- `for_scope_fenced(scope, fence)` returns handles whose writes the driver
  checks atomically against a guard document (`Capability::Fencing`,
  `ErrorKind::Fenced`). See [`write-fencing.md`](write-fencing.md).
- OpenHuman's scope is the agent id. The cloud deployment runs one agent per
  user, so this is the user.

### Errors

`StorageError { kind, message, source }`.
- Callers branch on `ErrorKind` only.
- Only `Unavailable` is retryable.
- Messages never contain credentials. `StorageConfig`'s `Display` and `Debug`
  redact the password in a connection string.

### Synchronous callers

`Blocking` (feature `blocking`) runs a port future on a runtime thread it owns.
It is safe from plain threads and from inside any tokio runtime. A future
running on the bridge must not call back into it; doing so is a `Backend`
error, not a deadlock. The bridge is transitional: synchronous consumer traits
should become async.

## Acceptance

- `conformance::run` passes on every driver. The memory driver passes it in
  this release.
- `tinystoragedrivers-core` depends on no database client or transport, and CI
  asserts this.
- Every source file has at least 90% line coverage.


## Validated document batches

A document port advertising `ValidatedBatch` implements
`atomic_batch_checked(read_checks, write_ops)`. A `ReadCheck` names a collection
and document id in the bound scope and database. Its `ExpectedDocument` is
either `Absent` or an exact live `Version`. Expired documents count as absent.
All checks inspect the pre-write state, followed by the writes in their input
order, inside one native lock or transaction. Contradictory duplicate checks
return `Conflict`; equivalent duplicates are allowed. Empty and read-only
batches still validate the native fence. Dependency bodies and logical versions
are never rewritten. A failed check or write applies none of the batch.

The default trait implementation returns `Unsupported(ValidatedBatch)`.
Memory and SQLite support this operation. File storage refuses it until its
multi-document persistence has a crash-atomic commit contract. Mongo supports
it on transaction-capable deployments when public transactions are enabled;
standalone servers do not advertise it.

Mongo uses reserved metadata counters per named database and storage scope.
Every document mutation, including claims, deletion, expiry sweeping and writes
through independently opened handles, increments its scope counter inside the
same physical transaction. This prevents snapshot write skew for both version
and absence checks, without changing user documents. Ordinary writes in
unrelated scopes touch different counters. Collection-wide schema changes and
expiry burial lock registration and all registered scope counters atomically.
First admission of a scope also locks registration, so it cannot bypass a
concurrent schema change. Mongo does not create server TTL indexes; expiry is
filtered on reads and buried by driver mutations.

On replica sets, `without_transactions()` disables public transactional
capabilities, including validated batches and fencing. Ordinary document
mutations still use internal physical transactions to participate in validation
serialization. This narrows the former promise that the handle never used any
transactions. True standalone deployments keep ordinary single-document writes.

Mongo bounds transient transaction retries to 64 attempts with staggered,
bounded backoff. Exhaustion returns the typed last error; a changed dependency
returns nonretryable `Conflict`. An unknown commit result returns a
nonretryable backend error because the mutation may have committed: callers
must read their durable outcome before retrying. The API has no deadline
parameter. Scope serialization trades same-scope mutation throughput for
cross-document serializability.

## Scoped reset and writer admission

Reset targets an explicit host-owned manifest of root-relative
`DatabaseAddress::Default` and `DatabaseAddress::Named(name)` addresses.
Every first-party `database(name)` is root-relative. A receipt from one
address says nothing about the other addresses; a host reports a full reset
only after every owned address and external owner stage completes.

`ScopedStorage::purge(PurgeRequest { operation_id, mode })` is optional.
Unknown adapters return `Unsupported(ScopePurge)` before erasing data.
`ScopePurge` advertises native scope admission and reset support;
`AtomicScopePurge` additionally promises one atomic all-port operation.
A stable operation identity makes retries idempotent and its receipt survives
the erased data. `PurgeMode::Quiescent` is a trusted host assertion that every
writer has been stopped and drained; it is never a model-selectable option.
`PurgeMode::Atomic` requires native atomic erasure of all storage ports.

Memory and SQLite implement atomic erasure of documents, deleted-document
versions, document-backed secrets and wrapped-key envelopes, import receipts,
stream records and lifetime cursors, and blobs for exactly one scope.
SQLite also removes that scope's search mirror. Shared collection declarations,
indexes and native admission/receipt metadata remain: removing shared schemas
would alter other scopes. Query results become empty while declarations remain.
Standalone secrets backends, OS keyrings, arbitrary native SQL tables and other
external owner stores are outside these ports and require explicit owner stages.

The operation issues a monotonically increasing `ScopeGeneration`.
Generation zero is the legacy handle default. Every mutation, including empty
operations and schema declarations, validates its issued generation under the
same native lock/transaction as the mutation. Old handles therefore fail
`Fenced` after reset, even if the operation would otherwise change nothing.
Hosts persist the returned generation in authoritative profile state before
reopening through `for_scope_generation` or
`for_scope_fenced_in_database_generation`. Generations come from trusted
native receipts and profile admission, never model arguments or document
metadata. A matching receipt retry does not erase data written in the new
generation. Reusing an older operation identity after a later reset cannot
revive the old generation.

A fenced purge checks and holds the native guard through the operation. The
guard's scope must differ from the erased scope, including when the guard is
in the default or another named database. A stale guard changes nothing.
Memory and SQLite support this natively. File, Mongo GridFS, S3 and composite
storage do not currently advertise atomic all-port purge; they must not turn
an unsupported native guarantee into an unfenced best-effort success.

Reset invalidates application query caches, old version preconditions, stream
cursors, import receipts and scoped secret providers. Shared declaration caches
remain valid because declarations are retained. Hosts stop and drain KMS loads
and secret writers, call `KmsKeyProvider::invalidate(scope)` to zeroize the
local cached key, then rebuild/rebind providers over the admitted generation.
KMS async reads already revalidate the durable envelope before using cached
plaintext; the synchronous KMS path refuses to use unvalidated cached keys.
The new envelope must be created through a generation-admitted handle.
