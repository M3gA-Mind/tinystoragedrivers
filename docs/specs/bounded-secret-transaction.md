# Bounded native file secret transaction

The secrets driver provides a synchronous Get/Set/Delete API for a trusted native
owner. It preserves raw nonce/ciphertext/tag encryption of the existing JSON
string map. It does not expose the map, derive/export keys, implement consent,
spawn workers, or support arbitrary file formats, import, quarantine or listing.

Initialization captures a verified existing directory, a private sidecar lock
and the current data object (or absence). Every operation verifies these exact
objects. Replacing an admitted data/lock/directory object externally invalidates
the resource, even when replacement bytes are valid. Own successful publications
advance the pinned data object. A failed publication/readback cannot be treated
as permission to retry: the native owner must fence uncertain work.

The actual synchronous job borrows a zeroizing key and request data. Finite
ceilings cover ciphertext, plaintext JSON, entries, name/value bytes, output,
serialization and a conservative simultaneous allocation reservation. Arithmetic
overflow rejects admission. Reservations count buffer capacities and conservative
map overhead; allocator metadata and OS page/cache accounting are outside this
portable application-level budget. Native owners must additionally charge actual
worker/key/output lifetimes and retain their permit until physical quiescence.

The owner implements a trusted phase hook, privately bound to the initialized
resource, operation, request identity, live generation, consent and deadline.
Each invocation returns an RAII authority lease. In particular the publication
lease must serialize revocation/generation changes with the actual rename and
exact locked readback. It is not a caller receipt. Guards must themselves be
bounded/cancellation-aware. The driver retries advisory locks only until the
absolute operation deadline and invokes the guard between attempts. OS calls
already in progress cannot be force-canceled; deadline/guard denial prevents
ordinary later publication and acknowledgment.

Publication stages a unique private sibling through the captured directory,
flushes bytes, verifies target/lock/namespace again, then replaces atomically
under the live publication lease. It flushes and verifies the exact published
object and ciphertext bytes under the same exclusive lock before acknowledgment.
Errors after rename can mean a visible but unacknowledged mutation. Output uses
a final owner lease; the native owner controls subsequent confidential delivery.

The ordinary asynchronous SecretStore remains compatible and has no implied
native lifecycle guarantees. Only the new explicit API has this contract.
