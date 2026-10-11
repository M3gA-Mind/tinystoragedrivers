# MongoDB scope purge

This module owns durable reset admission, historical receipts and resumable
quiescent erasure for one scope in one logical database. Public callers use
the core `ScopePurge` port; `mod_tests.rs` exercises native replica behavior.

Reset closes admission, erases records, records a durable GridFS manifest,
erases chunks/files and completes with an immutable receipt. Manifest writes,
destructive commits and completion validate the same scope authority row in
their transactions. Overlapping stages cannot erase replacement data after
completion. Every port checks admission before and after reads.

An independent scope reset witness is committed when admission first closes.
It distinguishes missing authority from a scope that was never reset, even
before the first reset completes. Completed history provides the same refusal
for earlier schemas. Retry of a completed operation returns its original
receipt without another erasure.

The host must stop and drain writers before claiming quiescence. Standalone
MongoDB, Atomic mode and native fenced GridFS purge remain unsupported. This
module does not coordinate external blobs or revoke KMS keys.
