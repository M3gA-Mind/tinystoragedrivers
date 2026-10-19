# tinystoragedrivers-client public item destinations

Source symbols below include public definitions, associated methods, public fields, and re-export declarations. Line numbers refer to the audited epic source revision `ae2fc08`. A name repeated through a re-export is intentionally repeated: both public paths are API.

| Source | Public item(s) | v2 destination |
| --- | --- | --- |
| `src/client/blobs.rs` | `BusBlobs (L18)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/client/documents.rs` | `BusDocuments (L18)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/client/mod.rs` | `use blobs::BusBlobs (L23)`, `use documents::BusDocuments (L24)`, `use queues::BusQueues (L25)`, `use secrets::BusSecrets (L26)`, `use streams::BusStreams (L27)`, `BusStorage (L83)`, `connect (L108)`, `secrets (L132)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/client/queues.rs` | `BusQueues (L19)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/client/secrets.rs` | `BusSecrets (L11)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/client/streams.rs` | `BusStreams (L15)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/lib.rs` | `use client::{BusBlobs, BusDocuments, BusQueues, BusSecrets, BusStorage, BusStreams} (L42)`, `use loopback::Loopback (L43)`, `use server::StorageServer (L44)`, `use transport::{Admission, BlobBody, Object, StorageTransport, WireError} (L45)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/loopback.rs` | `Loopback (L26)`, `new (L36)`, `largest_frame (L47)`, `streamed_bytes (L53)` | `tinystoragedrivers-busproxy`; wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/server/mod.rs` | `StorageServer (L77)`, `new (L101)`, `with_queue_manifest (L121)`, `with_secrets (L128)`, `open_watches (L135)`, `signals (L141)`, `handle (L150)`, `put_blob_stream (L180)`, `get_blob_stream (L192)` | `StorageServer`/serving methods -> `tinystoragedrivers-busproxy`; remove `with_queue_manifest` because the manifest moves to OpenHuman host (#29); secrets are installed through the complete `Storage` bundle, replacing `with_secrets`. Wire DTOs/selectors remain in `tinystoragedrivers-bus`. |
| `src/transport.rs` | `BlobBody (L13)`, `Object (L17)`, `as_str (L33)`, `parse (L45)`, `Admission (L61)`, `WireError (L70)`, `StorageTransport (L93)` methods `call`, `put_blob_stream`, `get_blob_stream`, `signals` | `tinystoragedrivers-busproxy`; `BlobBody` adopts the `-api` bytes stream contract, object selector names/wire DTOs remain in `tinystoragedrivers-bus`. |
