# Document invalidation

Open native DocumentStore::watch before reading authoritative state. Await
DocumentWatch::next and reread on Invalidated; always apply current native CAS,
fence and business rules before acting. A notification is never a decision.
ResetRequired is terminal: drop, subscribe again, then rebuild the read view.
Storage errors preserve native sources; notice Debug logs only error kinds.

Native Watch is optional. Capability admission never opts into polling.
The polling-watch feature exports PollingWatch::open with explicit ScopedStorage.
It conservatively invalidates every configured interval, including logical TTL
and changes that enter and leave between reads. It has no snapshot cache,
resume cursor, ambient backend, or worker. Empty checked batches validate native
admission at opening and each delivery; unsupported/refused handles fail closed.

Native cursor validation belongs to the issuing driver. Cursors are opaque,
size bounded and redacted in Debug. Mongo binds them to connected context,
logical and physical database, scope, collection, filter and captured fence.
They are not portable reconnect or restart checkpoints. Subscribe-before-read
is a live notification boundary, not a transactionally consistent read snapshot.

Scope generation is a separate admission protocol. A generation wrapper must
refuse Watch until it can validate its captured generation at both boundaries;
it must terminally reset rather than forward a new generation through old state.

Spec: [document-watch](../../../../docs/specs/document-watch.md).
