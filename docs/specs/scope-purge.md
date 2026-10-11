# Native scope purge

Status: Accepted; implementation and publication gates remain in progress.
Owner: TinyStorageDrivers. Host integration: OpenHuman issue #7432.

## Problem

Removing a local directory does not erase data held by another backend.
Enumerating current records also misses stream lifetime state, document
tombstones, encrypted secret envelopes and blob chunks. A reset must retire
existing handles so they cannot read or write the replacement data.

## Boundary

The optional native port resets one scope in one addressed logical database.
The trusted host owns the complete database/scope manifest, authorization,
writer shutdown, operation journal and issuance of replacement handles.
DatabaseAddress resolves against the trusted root; it never discovers owner
namespaces or resolves relative to an arbitrary actor handle.

The port does not revoke external KMS keys, remove unrelated filesystem paths,
implement a root-wide transaction, or coordinate composite backends. A composite
that cannot erase all its ports must refuse the capability before mutation.

## Contract

PurgeRequest contains a stable host-issued operation_id and Atomic or Quiescent
mode. IDs contain 1–128 ASCII letters, digits, underscores or hyphens. The host
persists an ID before its first attempt and reuses it after interruption.

ScopePurge is bound to a database, scope, generation and any native fence.
StorageBackend generation constructors preserve those bindings. Ordinary
constructors select generation zero, rather than discovering the newest
generation and silently reviving old callers.

PurgeReceipt reports that operation's generation, status and resumable stage.
Complete means every owned native port in this database has finished. Pending
means admission remains closed and the same operation must resume. Native
admission and receipts live outside erased tenant data.

Completed receipts remain queryable after subsequent resets and reconnects.
Retrying a completed operation returns its original receipt and never erases
replacement data or issues a newer generation. Receipt retention is unbounded
in this version; there is no automatic pruning policy. An old schema can
backfill its latest completed admission, but cannot reconstruct earlier
receipts that the old implementation never retained.

Memory and SQLite support atomic all-port deletion and generation advancement.
SQLite also persists the receipt in that transaction. MongoDB replica sets
support explicitly quiescent staged reset: records, durable blob manifest,
chunks/files, then completion. Every generation is refused while pending.
GridFS uploads must be stopped and drained by the trusted host first; a late
file keeps the operation pending and causes manifest replanning.

MongoDB standalone deployments refuse ScopePurge. MongoDB Atomic mode and
native fenced GridFS purge refuse their unsupported guarantees before starting.
Native error kinds and causes remain intact. Corrupt authority fails closed.
A durable MongoDB reset witness is committed independently of the admission
row in the same transaction that first closes admission. Lost or recreated
admission remains corruption while that first reset is pending; completed
receipt history also detects authority loss from older schema versions.
A pre-reset registration may omit reset fields; once any reset marker exists,
all authority fields are required. Manifest publication, chunk deletion,
file/manifest removal and completion validate and write the same native
authority row in their transactions. A delayed overlapping reset stage cannot
commit destructive work against replacement data after completion.

## Invariants

- Scope and database siblings survive every reset stage and retry.
- Documents, tombstones, streams, stream lifetime cursors and blobs are erased.
- Shared collection declarations and native authority metadata remain.
- Old generations cannot read replacement payloads or mutate any port.
- Native fence validation still precedes historical receipt replay.
- Failed atomic erasure commits neither generation nor receipt.
- Partial staged work never returns Complete or opens admission early.
- Status queries do not start a reset; they can report an earlier generation.

## Acceptance

Conformance must exercise default and named databases, supported mode selection,
all-port replacement preservation, and historical status/retry after two resets.
Native tests must also prove persisted reopen, upgrade backfill, corruption,
rollback, staged interruption/resume, late GridFS files and stale-generation
reads. Missing reset authority fields and delayed overlapping planner/eraser
stages must fail closed while replacement blobs remain intact. Live MongoDB checks use local replica and standalone fixtures.

Before publication, run the owning workspace gates, strict Clippy/rustdoc and
per-file coverage. The [publication plan](../plans/scope-purge.md) tracks the
remaining work. Host end-to-end reset and external KMS/S3 deletion are separate
acceptance requirements, not established by native conformance alone.

## Open questions

External composite erasure needs its own durable coordinator and contract.
MongoDB fenced GridFS reset remains unsupported; host takeover-safe shared reset
must not bypass that refusal. Future watch ports must validate generations
before every delivery and terminate on reset, or refuse the combination.
