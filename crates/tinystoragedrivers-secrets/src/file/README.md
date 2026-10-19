# Private encrypted file storage

`EncryptedFileSecrets` keeps the existing raw nonce/ciphertext/tag JSON format.
`load_or_create_key_file` keeps the 64-character hex key format. The driver
owns private file creation, admitted namespaces, locking and verified
publication. The caller selects secret names, source files, values and consent.

Unix creates files with mode 0600 in the original open call and accesses the
admitted directory through retained descriptors, no-follow opens and relative
filesystem operations. Ancestors must belong to the current user or root and
must not allow foreign mutation; root-owned sticky temporary ancestors may
contain a private user-owned child. Files must be private regular single-link
objects owned by the current user.

Windows uses published `clack-private-fs` to pass an owner-only protected DACL
at the original native create call and obtain an owned `std::fs::File`.
Admission supports rooted ordinary and verbatim local NTFS disk paths with
persistent ACLs, preserving verbatim spelling for every component pin. UNC,
remote/device and drive-relative paths are rejected. It requires a process
identity without thread impersonation, trusted ancestor ownership and no
foreign effective mutation grants. Retained directory handles deny delete
sharing. Leaf handles retain actual FileIDs while allowing legitimate atomic
replacement. Reparse points, unsafe owners/ACLs and hard links are rejected.
Existing permissions are checked, never repaired after exposure.

Reads and mutations hold `<secrets filename>.lock`. Key creation holds
`<key filename>.lock`, including the short two-link no-clobber publication
interval. New staging, key, data and lock files are private before bytes.

`EncryptedFileSecrets` fails closed on decryption and serialization errors by
default. A host that wants desktop recovery can opt into
`CorruptFilePolicy::QuarantineAndContinue`: the driver moves the corrupt file
to a unique `.corrupt.*` sibling under the same held lock and syncs the
directory on Unix before continuing with an empty store. It never removes the
archived bytes. Permission, namespace and other I/O failures remain errors.
Publication flushes staged content, publishes, then verifies the actual
published identity and content. Unix also syncs the admitted directory.
Windows uses same-volume native replacement through `std::fs::rename`, whose
access-denied fallback preserves open destination handles with POSIX replacement
semantics. Full paths resolve within the pinned namespace; this does not provide
descriptor-relative source selection against uncoordinated same-user changes.
Namespace verification surrounds replacement. Published FileID verification,
explicit file flush and exact byte readback precede acknowledgment. Unsupported
extended replacement fails closed, potentially reporting the original access
denial. Replacement does not request native write-through.
These acknowledgments describe native APIs; they do not prove physical
power-loss survival. A failure after publication may leave visible state.
Failed staging cleanup is attempted and can itself fail; private leftovers
are never permission-broadened or treated as committed data.

## Bounded file imports

`FileImportSource::open` admits and retains an exact private source object of
at most 1 MiB. Its bytes are available for caller-owned parsing.
`prepare_file_import` binds an operation, destination name, source identity,
an opaque random live-source-instance token, source/value hashes and retained source bytes in an authenticated encrypted
`<secrets filename>.imports` ledger before destination publication. Exact
publication retries reconcile that intent. A different operation, source or
value conflicts; active destination/key/lock/ledger objects cannot be sources.

`VerifiedFileImport` retains the actual source and destination handles and
states its native acknowledgment boundary. Windows returns
`WindowsVisibleFileFlushed`; cleanup returns `UnsupportedBoundary` and retains
the source. Unix returns `UnixDirectorySync`. Cleanup verifies the destination,
atomically captures the source into an operation-specific quarantine, verifies
it again and persists the exact captured phase. After all ledger/publication
work, it rechecks the exact source identity, safety and bytes before unlinking. It never
deletes an unverified replacement. A failed capture restoration preserves the
captured object when the original name is occupied.

After removal, a failed directory barrier or ledger update returns
`RemovedUnacknowledged`. The encrypted backup remains available. A live receipt
can retry the barrier and ledger only for the persisted removal phase with no
replacement source occupying the name, and only when the retained exact
source handle reports zero remaining links. A moved-but-linked capture cannot
be acknowledged merely because both expected names are absent. Completed operations return
`AlreadyCompleted`. These states do not imply rollback or hardware survival.

Cleanup holds a nonblocking exclusive advisory lock on the retained source
file and serializes receipts sharing that source instance. Cooperating writers
must acquire the same file lock before changing bytes or source namespace
links. Callers must coordinate all source-name replacement/rename operations
through that agreement. A held cooperating writer makes cleanup fail closed;
retry after it releases the lock. Private namespaces and the final check cannot
exclude an uncoordinated same-user change between that check and unlink. The
API does not promise atomic protection against such actors. Detected mismatches
retain the current quarantine and the authenticated original backup.

`with_key_file` admits existing data, store lock and ledger before generating a
master. Existing store objects without their original master are a conflict;
restore that key instead of creating another. The key creation lock serializes
constructors and existing store locks cover store admission.

The receipt is process-bound. `recover_file_import_source` works in a fresh
process with the same key and returns authenticated retained bytes. It does
not reconstruct a deletion receipt from a pathname or recycled file identity,
restore plaintext files, select a migration slot or decide consent. A fresh
source at the original name cannot authorize cleanup for an old operation.
The caller owns recovery policy over the returned zeroizing bytes.

The native CI matrix executes the birth and final-object fixtures on Linux,
macOS and Windows. It checks actual native compiled-source inventory against
LLVM JSON, independently maps its executable segments to unique line
coordinates/counters, and requires exact agreement with LCOV before computing
changed-line coverage against the original base. Generic/closure summary counts
may exceed unique coordinates; partial, duplicate and conflicting reports fail.
Linux additionally observes real injected I/O failures and a paused two-link
publisher with a concurrent reader. Fault tests prove API failure handling,
not physical power-loss durability.

A separate native process under a different standard user reads, writes,
replaces and deletes a public control, then attempts those same operations on
the published key, ciphertext and both locks. All sixteen private operations
must fail with access denied and leave the objects unchanged. Unix uses the existing
unprivileged `nobody` account; the Windows CI fixture creates a random local
standard account and removes it in a `finally` block. No operator account or
operator credential is used by this fixture.

## Read-only legacy HEX key source

`ProtectedHexKeyFile::new` binds one absolute, host-selected path without I/O.
The object implements `HexKeyFileSource::read_encoded`, returning
`Option<Zeroizing<Vec<u8>>>`. Only a missing leaf under successfully admitted,
rechecked existing ancestors returns `None`; missing parents remain errors.
This source never creates a parent, master, lock or import ledger, repairs
permissions, writes or deletes. It is separate from `load_or_create_key_file`.

The original encoding, including surrounding Unicode whitespace and hex case,
is retained after validation as exactly 64 ASCII hex digits representing
32 bytes. The maximum encoding is 4096 UTF-8 bytes, including whitespace.
Metadata checks and streamed limit-plus-one reads bound growing files.
Invalid UTF-8 or size is `InvalidInput`, malformed keys are `Crypto`, native
admission failures are `Backend`, and transient I/O is `Unavailable`.
Diagnostics redact both path and material, including backend source chains.

Reads reuse native namespace and regular-file admission without weakening
owner, permission/DACL, single-link or no-symlink/reparse requirements.
Windows owner-only inherited DACLs remain admissible; a protected-DACL bit
is not a read requirement. After reading, the source rechecks opened-file
protection, namespace pins, named identity and exact bytes. These checks do
not promise immunity to uncoordinated same-user changes after the final
check. Cancellation can leave blocking reads finishing, with no mutation.
Reading grants no durable publication, migration or deletion acknowledgment.
Native Windows source tests require their own Windows execution; Linux
results do not establish that platform's admission.

## Native bounded single-secret transactions

FileTransactionResource captures a verified directory, sidecar and existing
data identity without a key. Its synchronous execute method borrows one
Zeroizing key for one typed Get, Set or Delete. It starts no hidden worker.
The native owner retains its own finite physical job permit through return and
supplies a private FileTransactionGuard bound to live authority and consent.

Every phase invokes that owner hook. A scoped publication lease spans the actual
atomic rename, flush and exact same-lock readback. Revocation/generation changes
must serialize with that lease. A final output lease covers acknowledgment.
Cancellation/deadlines stop later phases; they cannot interrupt OS calls that
already started. A mutation followed by failed readback/output fences that
resource. The owner must also fence uncertain jobs across reinitialization.

Budgets bound ciphertext, decoded and serialized JSON, entries, UTF-8 names and
values, returned bytes and conservative simultaneous allocation reservations.
The string-map decoder wipes decoded keys, values and scratch on all paths.
No map, raw key, generic closure, listing, import or recovery API is exposed.
External object replacement invalidates the pinned resource. Ordinary async
SecretStore behavior is unchanged. See the
[transaction specification](../../../../docs/specs/bounded-secret-transaction.md).
