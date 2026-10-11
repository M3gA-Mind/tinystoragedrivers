# Scoped document invalidation

Status: accepted implementation target. Base: d032e0f2fc0dedd8a219727aaacc16af6ddcf4c1.

## Contract

DocumentStore::watch is optional (Watch). It returns an owned asynchronous
subscription after its subscribe-before-read barrier has completed. The caller
opens the subscription before its authoritative initial read. Notifications are
coalesced invalidation hints, never document bodies, decisions, or authority.
The caller always reads and validates current storage state before acting.

A subscription is bound to the handle's native database, scope, collection,
filter, and captured fence. Filters describe the affected query; conservative
extra invalidations are permitted. Missing an insert, replacement, edit,
delete, filter exit, or logical TTL expiry is forbidden. Backend reads and
notifications may observe different instants; this is not snapshot history.

next returns Invalidated or terminal ResetRequired. Resumption loss,
collection replacement, invalidation, and lost bounded buffers require a reset.
Reset requires dropping the subscription and subscribing before rereading.
Transport/backend failures retain their typed native source. A captured stale
fence returns Fenced; neither opening nor delivery borrows a newer owner.
Dropping a subscription drops its cursor; there is no application worker or
unbounded queue retained after drop. Cancellation of next must be safe.

An optional opaque cursor is redacted in Debug. Cursors carry no user document
payload, grant no authority, and are accepted only for their issuing backend and
exact database/scope/collection/filter/fence binding. Native cursors are also
bound to the connected backend context and are refused on a newly connected
backend; they are not durable restart checkpoints. Invalid, oversized, or
misbound cursors fail with InvalidInput. Native resume-history loss instead
produces terminal ResetRequired with its native source available.

## Native Mongo

Replica sets and sharded deployments use actual Mongo change streams. Scope
filtering uses the driver's documentKey._id.s, including delete events; no
full-document lookup or preimage is required. Collection-wide destructive
changes reset the subscription. Conservative periodic invalidations supplement
native changes so logical TTL does not wait for Mongo's physical TTL sweeper.
Standalone Mongo does not advertise Watch.

## Explicit polling

The separately selected PollingWatch adapter does not add Watch to a
backend's capabilities and does not promise mutation history or resumability.
It emits conservative invalidation hints at bounded caller-selected intervals.
Opening and each delivery perform an empty native checked batch to validate the
captured handle's fence. A backend without ValidatedBatch is refused. Polling
has no cursor; a supplied native resume cursor is rejected. Subscription must
still precede the initial authoritative read. No process persistent query cache,
collection layout, document copying, or ambient backend is involved.

## Generation admission

The canonical base has no native scope generation contract. This change does
not invent one. A future generation-admitted wrapper must refuse Watch until
it can validate its captured generation on opening and delivery. Generation
invalidation must terminally reset the old subscription, never expose a new
generation through it. Generation receipts and metadata belong to the separate
native ScopePurge contract.

## Verification

Tests cover unsupported defaults, validation and redaction, polling opening
and delivery refusals, drop/cancellation, and current-thread execution. Native
Mongo conformance covers subscribe-before-read, same-scope independent handles,
scope/database/filter/cursor isolation, deletion/filter exit, TTL, lost resume,
and stale fence refusal. Live tests use an explicitly supplied local replica
set fixture and retain the optional environment gate used by other drivers.
