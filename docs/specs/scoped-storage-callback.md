# Scoped native callback profile

ScopedStorageRoute in the pure schema crate is routing data only:

```json
{"version":1,"destination":":1.2","object_root":"/storage","profile":"DocumentsStreamsSecretsV1"}
```

The version is required and must be 1. The profile uses the externally serialized
unit enum variant shown above. Unknown routing fields and profiles fail decoding.
The client accepts only unique destinations and valid TinyBus paths. No backend,
scope, grant, generation, fence or admission is supplied through this object.
A copied route is not an authority token.

The host declares scoped_storage_endpoints(root) on an SDK-created
HostCallbackBinding, then calls serve_scoped_storage(binding, root, server).
The server captures the host's already scoped, admitted and optionally fenced
and generation-bound ScopedStorage, plus the optional captured SecretStore.
No global backend is consulted or opened. Registration grants no schema rights.

The interface is ai.tinyhumans.tinystorage.Storage on <root>/Documents,
<root>/Streams and <root>/Secrets. Members are the existing protocol sets,
excluding Documents WatchOpen/WatchClose. Blobs have no endpoint. Every member
requires a confidential call. Dispatch checks the live binding guard before
StorageServer::handle(Admission::Confidential). The SDK binds source/destination,
path, interface and member to the verified module instance. Its guarded
registration owns revoke/drain tracking. Revocation stops new work; it does not
pretend to cancel a native operation already in flight. Hosts must drain and use
native epoch fencing for replacement.

The native server retains collection declaration validation, metadata, CAS,
expiry, JSON stream offsets, leases and generation checks. Native admission
controls EnsureCollection; the adapter never treats an empty declaration as a
successful no-op. JSON records use the existing bounded inline protocol and need
no blob streams or raw connection. Storage failures carry the static category;
callback transport failures become static unavailable errors without payloads.

Modules call connect_scoped_storage(connection, route) for BusStoragePorts:
documents, JSON streams, secrets and their capabilities. This partial type has
no StorageBackend implementation or blob handle. Watch, StreamingBlobs,
PresignedUrls and BlobTtl are removed from advertised capabilities. The transport
rejects blob/watch methods explicitly; the document port refuses watch when its
capability is absent. Unavailable destinations, secrets, grants and generations
never create fallback storage.

The legacy connect_storage(Connection) and collection-only callback API retain
their original well-known-host and attestation behavior.

The SHA-admitted native proof additionally gates a real secret write. Revocation
leaves it counted as running and drain pending until its future finishes.
Advancing the native lease before release rejects the late write; replacement
admission itself never claims to cancel that future.
