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
  follow the same path rules as filters. `Page.next` is an opaque keyset cursor
  containing the last returned sort values and id, bound to the issuing driver,
  scope, collection, filter and sort; any other cursor is
  `ErrorKind::InvalidInput`. Legacy native offset cursors are invalid because
  their contents cannot be interpreted as a keyset position.
  Each continuation re-evaluates the query and starts strictly after that saved
  total-order position. Paging does not promise a snapshot: inserts before the
  saved position are not returned, inserts after it can be returned, and
  deletions or sort-key changes affect later pages according to their current
  values. A document moved from before the cursor to after it can therefore
  appear, while one moved from after it to before it can be omitted.
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
  - *Run-ledger patterns.* A task or run ledger is built from the document
    port alone: enqueue with `Precondition::Absent`, claim the next eligible
    task with `claim(eligible, [priority, seq], patch)`, complete with
    `Precondition::Version` of the claimed version (a stale holder gets
    `Conflict`), release a worker's claims on shutdown by claiming
    `state = running AND owner = me` until `None` with a patch that sets the
    state back and nulls `owner`, and prune finished tasks past retention with
    `delete_where`. Conformance (`conformance/ledger.rs`) pins these on every
    driver.
- **Optional capabilities.** A capability the driver lacks is
  `ErrorKind::Unsupported(capability)`.
  - `Ttl`: documents whose `ttl_field` (epoch ms) has passed read as absent.
    Expired documents are invisible to `get`, `query`, `count`, `claim` and
    `search` alike, and rewriting a document with a later expiry keeps it
    alive. Blobs have no expiry of their own: a cache of blobs keeps one
    document per blob in a TTL collection and removes the blob when the
    document expires (`delete_tree` removes a whole prefix).
  - `RecordMetadata`: every document carries `Versioned::meta`
    (`Meta { schema, created_ms, updated_ms, expires_ms }`), described in
    [`schema-v2.md`](schema-v2.md). `put_with(.., WriteOptions { schema,
    expires_ms })` declares the schema version and expiry; a plain `put`
    declares neither, so it clears an earlier expiry, and `claim` carries both
    over. The driver stamps `created_ms` on the first write (an update keeps
    it; creating the id again after a delete or expiry starts it afresh) and
    `updated_ms` on every write (never backwards). A record at or past
    `expires_ms` reads as absent from `get`, `query`, `count`, `claim` and
    `search`. Filters and sorts address the fields as `_schema`,
    `_created_ms`, `_updated_ms` and `_expires_ms`, next to `_id`; a reserved
    path never reads the body. A driver without the capability returns
    `Meta::unknown()` (all zero, no expiry) and refuses a `put_with` that
    declares a schema or expiry with `Unsupported(RecordMetadata)`. Batched
    puts (`WriteOp::Put`) declare no options yet.
  - `Transactions`: `atomic_batch`, all or nothing.
  - `CollectionEnumeration`: `collections(prefix)` lists collection names with
    stored records in this handle's scope. It filters by a literal string prefix
    and sorts names lexicographically. Tombstones count as stored records so
    complete scoped cleanup can discover deleted versions; declared collections
    with no records are omitted. Without the capability, the method returns
    `Unsupported(CollectionEnumeration)`. Enumeration never exposes another
    scope.

  - `FullText`: `search` over the declared `SearchSpec` fields. Hits come
    best first: scores never increase down the list, equal scores order by
    document id, a document matching more of the query ranks ahead of one
    matching less, and `limit` keeps the best prefix of the full ranking. Score
    values are backend-local. Declaring a field searchable later indexes
    documents written before the declaration (an idempotent reindex), and the
    index follows updates and deletes. Conformance
    (`conformance/semantics.rs`) pins all of this.
  - Vector search is not a port here; it stays with TinyMemory (see
    [`storage-bus.md`](storage-bus.md)).
  - `Fencing`: `StorageBackend::for_scope_fenced` checks each write atomically
    against a guard document, returning `ErrorKind::Fenced` when the guard fails.

### `StreamStore`

Append-only logs of JSON values.
- Offsets are dense, start at 0, and are never reused.
- `truncate_before` discards old records but leaves `len` unchanged.
- `seal` (capability `StreamSeal`) closes a stream to appends and stores its entries as one zstd blob at `_tsd/sealed/<escaped name>` in the same scope. Reads are unchanged; appends and `truncate_before` are `Conflict`; sealing is idempotent; a missing stream is `NotFound`; `delete_stream` removes the blob. See `schema-v2.md` section 6.

Optional ConditionalStreamAppend provides append_batch_if_offset(stream,
expected_next_offset, values). Compare the persistent next offset and append
the entire batch in the same atomic step; return its first offset. A missing
stream has next offset 0. An empty batch still checks the expected offset and
any native fence.

A mismatched offset returns Conflict without appending. Validation errors and
aborted operations append nothing. Native fence validation participates in the
same lock/transaction. Scope isolation matches other stream
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

**Tree view.** `list_dir(dir)` lists one level (blobs and the sub-directories
that hold more blobs, sorted by name, `a` never matching `ab/...`),
`put_tree(dir, files)` writes a bundle such as a skill (every path is validated
before the first write; the writes are not atomic across files, so publish a
manifest document last when all-or-nothing matters), and `delete_tree(dir)`
removes everything below a directory (the root is refused). They are provided
methods built on `list`/`put`/`delete`, so every driver, the S3 driver and the
bus client answer them identically.

### `StorageBackend` and `Scope`

- `for_scope(scope)` returns handles bound to one tenant. No port method takes
  a scope, so a call site cannot forget it.
- `Scope::local()` is the single-operator scope.
- A backend that serves only one scope (a desktop SQLite file) rejects other
  scopes in `for_scope`.
- A backend is one database; there are no named databases (removed in schema
  v2, see [`schema-v2.md`](schema-v2.md) section 7).
- `for_scope_fenced(scope, fence)` returns handles whose writes the driver
  checks atomically against a guard document (`Capability::Fencing`,
  `ErrorKind::Fenced`).
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

## Host conformance

A host that composes several scopes, shares one store between handles and
migrates its own collections should not repeat the driver matrix in its own
tests. `conformance::run` therefore also covers, on every driver:

- **Nested scopes** (`isolation.rs`, `nested`): a tenant `S`, `S/agent:a` and
  `S/module:m` hold the same collection, id, stream and blob key without
  seeing each other, and collection and scope listings do not leak between
  parent and child.
- **Document migrations** (`migrations.rs`): a two-step chain over several
  batches, a crash part way, a resume that writes no record twice, and a no-op
  re-run (drivers with `RecordMetadata`).
- **Holder fences**: writes through a fenced handle are checked atomically against its guard document.

External drivers should call the public
`conformance::run_with_process_capabilities(backend, single_scope,
shared_across_processes, cross_process_cas)` entry point with their declared
process guarantees. It checks both `StorageBackend::capabilities()` and the
`DocumentStore::capabilities()` on the local handle and, for multi-scope
drivers, the `conformance-b` handle. The declarations must match the supplied expectations
on both surfaces; `SharedAcrossProcesses` requires `CrossProcessCas` on each.
This catches a backend whose top-level declaration promises a guarantee that
its scoped document port does not expose, or vice versa. Built-in native
drivers use `conformance::run`, which supplies their fixed expectations.

`conformance::run_shared(open)` takes a closure that opens independent
backends on one store. A driver declaring `CrossProcessCas` must arbitrate
racing document claims, version-guarded puts, create-only (`Absent`) puts and
lease acquisitions across handles: each race has exactly one winner, and a
fresh lease starts at epoch 1. When it also advertises `Fencing`, a fenced
handle must reject a write after another handle changes its guard. CAS-only
drivers make no immediate-visibility promise for the other storage ports.

A driver declaring `SharedAcrossProcesses` promises all of those CAS and
fencing guarantees, and also immediate cross-handle visibility: document
creates, updates and deletes, stream appends and blob writes are visible to
other handles. The suite verifies these reads after the corresponding writes
and checks that all handles observe the winning claim, lease and version race.
Every driver, including ones with neither process capability, must show that
a backend opened after another was dropped reads the stored data and continues
its document versions, stream offsets and lease epochs. The memory driver (one
heap) and file driver (one process) do not report either process capability.
`tinystoragedrivers_secrets::testkit::document_secrets_conformance` runs the
secrets suite and the `enc2`-ciphertext-at-rest check over any driver's
documents; the SQLite, file and MongoDB crates run it.


## Validated document batches

A document port advertising `ValidatedBatch` implements
`atomic_batch_checked(read_checks, write_ops)`. A `ReadCheck` names a collection
and document id in the bound scope. Its `ExpectedDocument` is
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

Mongo uses reserved metadata counters per storage scope.
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
