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

## Local implementation evidence

The implementation branch preserves its feature checkpoint and merge of
canonical 89cc548. It does not change a host checkout or consume ScopePurge.

Behavioral failures were observed before each relevant fix:

- Polling accepted stale opening and stale delivery: four passing tests,
  two failures, exit 101. Native empty checked-batch admission now rejects both.
- Reset Debug exposed its native error payload: one failure, exit 101. Debug
  now records only the source kind while callers retain the original source.
- Native Mongo initially returned Unsupported for the actual subscribe/read,
  cross-scope, insertion, filter exit and deletion regression: exit 101. The
  same real local replica-set test passes through the native stream.
- Cancellation during native admission discarded a consumed hint: one failure,
  exit 101. Pending delivery now retains it until successful admission.
- Memory's backend capability inherited unsupported Watch, and a composite
  imported arbitrary document capabilities from a blob backend: both focused
  regressions failed with exit 101 before explicit capability masks.

Fresh local replica/standalone Watch tests pass (seven): scope and connected
context isolation, exact cursor binding, stale owner refusal, TTL boundary,
collection destruction/reopening, insertion/filter exit/deletion and native
history-loss terminal reset with the original Mongo error. History loss changes
only a returned token timestamp; no oplog truncation or server state mutation
is required. Wait cancellation and admission cancellation have separate tests.

At this point strict full-workspace Clippy, all-target/all-feature build,
573 all-feature tests, 530 default tests (including doctests, none ignored),
strict rustdoc, fmt and Rust 1.88 locked workspace check all passed. The fresh
full coverage gate also passed (exit 0): all 79 executable production source
files exceed 90%, with a minimum of 90.14%. Core Watch types cover 90.91%,
polling 100%, conformance 100%, and native Mongo Watch 95.36%. This gate includes
the instrumented local replica/standalone suite, eight injected native I/O
faults, the stopped two-link key publisher/concurrent reader, and the real
SecretService tests. It includes the pure CI-only helper correction from PR20
(a44e09f) through a preserved merge; no unpublished library API, copied profile
or fabricated filename alias is consumed. Disposable log filenames are not a
substitute for these retained behavior names and recorded terminal outcomes.
