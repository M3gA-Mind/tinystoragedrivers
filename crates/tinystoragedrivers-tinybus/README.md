# Captured document callback adapter

A trusted host calls endpoint, prepares a canonical TinyBus HostCallbackBinding,
then serves one already-admitted DocumentStore and fixed collection with
serve_collection. It loads the real verified module using that same binding.

The service implementation is private. There is no ordinary service constructor
or public direct dispatch method. Registration and calls cannot select another
collection, scope, URL, database or native admission. Native ports perform CAS,
data validation, generation checks and fenced writes; this crate only forwards.

Schema preparation and legacy import authority remain the owning host/module's
explicit responsibilities. Registration never calls ensure_collection. Native
error categories cross the wire, while backend messages and sources do not.
No persistence policy or module business reducer is implemented here.

Run the real artifact proof with scripts/test-document-callback.sh. This uses the
canonical callback binding, not an OpenHuman workaround or private API copy.
The proof does not establish TinyMemes persistence or unpublished lifecycle fixes.

The wire uses document_id and strictly parsed inline document_json text. This
preserves arbitrary business JSON without confusing id-only data with native
TinyBus stream references. No privacy check or stream API is bypassed.
