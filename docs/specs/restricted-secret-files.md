# Restricted secret-file publication

Accepted owner contract for the Task 10 prerequisite. The encrypted JSON blob,
`enc2:` values and hex `.secret_key` formats remain unchanged.

New data, key and lock files must be restricted when the native create call
returns, before any bytes are written. Unix birth mode is 0600 even with a
permissive umask. Windows birth owner is the effective token user and its DACL
is protected, with only that user receiving file access; a broad inheritable
parent ACL must not expose a new empty handle. Existing unsafe objects must
fail admission rather than receive a post-exposure permission repair.

Reads and mutations require admitted, pinned namespace and file identities.
Locks cover the complete modification/publication/verification interval.
Verified publication distinguishes visibility, the supported persistence
boundary, and cleanup. Uncertain errors after publication must preserve
migration sources until exact destination and source identity reconciliation.
No file flush, rename or fault injection alone proves physical-power-loss
survival. Unsupported native guarantees must be reported explicitly.

Stage 1 records the current defects through production-private reservation
and public key/data regressions on disposable fixtures. It changes no
production behavior. Native Windows RED is required before semantic fixes.
See [delivery order](../plans/restricted-secret-files.md).

Imports use an additional encrypted `<secrets file>.imports` string-map ledger
under the same key and lock. It records an immutable operation/source/value
identity and encrypted source backup before changing the destination. This
permits an exact retry after uncertain publication and rejects a different
operation for the same destination name. The legacy formats are unchanged.
A typed acknowledgment binds exact source and destination objects and value.
Windows acknowledges visible, file-flushed publication only; cleanup returns
unsupported without removing its source. Unix cleanup first moves the source
into an operation-specific quarantine, verifies the captured object and target
again, then removes it and syncs its directory. An uncertain cleanup preserves
the encrypted source backup and reports its actual phase; it never claims
rollback after deletion. A caller chooses names, values, source selection and
consent. Source admission is bounded to regular private files up to 1 MiB.

Key initialization also holds an atomic-private `<key filename>.lock` across
its two-link no-clobber interval. This avoids transient hard-link admission
failures between cooperating initializers; arbitrary hard links still fail.
The default `.secret_key.lock` participates in native handle/DACL validation
and is excluded as an import source alongside key/data/ledger/store-lock files.

A cleanup receipt retains exact opened source/destination handles and is a
live process-bound object. Its retries reconcile only the persisted captured
removal phase, retaining encrypted backup bytes and rejecting replacements.
After process restart, the supported port recovers authenticated backup bytes
with the same key; it does not synthesize a pathname-based deletion receipt or
claim a durable cleanup boundary it cannot establish. Subprocess tests prove
backup recovery and refusal to bind a fresh source to the old import intent.
