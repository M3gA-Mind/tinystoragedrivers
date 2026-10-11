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
