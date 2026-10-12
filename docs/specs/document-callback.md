# Confined document callbacks

A trusted host captures an already-admitted DocumentStore and one validated
collection. A compiled module receives only an SDK-created confidential callback
destination, path, and fixed interface. Identifiers are document data, never
scope, collection, database, URL, schema, or admission selectors.

The minimal contract offers Get, Put, and Delete. Requests reject unknown fields.
Put/Delete preserve the native atomic Precondition; replies preserve versions
and distinguish absent documents from errors. Native error categories cross
the wire without messages, identifiers, documents, or backend sources.

The adapter registers only on canonical TinyBus HostCallbackBinding. Its private
service refuses unauthenticated or nonconfidential dispatch before storage I/O.
Native storage still owns schema, generations, leases, validation and atomicity.
The adapter does not open stores, declare schemas, infer authority, or implement
module business logic.

This foundation is not TinyMemes persistence, legacy import authority, or proof
of lifecycle draining beyond the pinned canonical TinyBus contract.
