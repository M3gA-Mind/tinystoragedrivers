# `tinystoragedrivers-blob`

Standalone implementations of the scoped `BlobStore` port. The crate owns
opaque blob persistence only; callers choose a scope and provide any shared
connection handles.

## Features

| Feature | Adapter | Additional engine dependency |
| --- | --- | --- |
| `memory` (default) | `MemoryBlobStore` | — |
| `fs` | `FileBlobStore` | — |
| `sqlite` | `SqliteBlobStore` | `tinystoragedrivers-connect/sqlite` |
| `mongodb` | `MongoBlobStore` | `tinystoragedrivers-connect/mongodb`, `mongodb` |
| `s3` | `S3BlobStore` | `tinystoragedrivers-connect/s3`, `object_store` |

Each adapter is selected independently. The filesystem adapter preserves the
v1 raw-bytes and JSON-sidecar layout. SQLite reads the existing v1 `blobs`
table unchanged and records new write ages in the shared `_tsd_blob_written`
table. MongoDB stores scoped objects in a dedicated GridFS bucket. S3-compatible
storage uses the shared configured object-store connection.

```rust,no_run
use tinystoragedrivers_blob::MemoryBlobStore;
use tinystoragedrivers_types::Scope;

let blobs = MemoryBlobStore::new(Scope::new("local")?);
# Ok::<(), tinystoragedrivers_types::StorageError>(())
```

`list_page` uses bytewise key ordering and an exclusive `next_after` cursor.
It includes expired physical objects so callers can walk stored data without
TTL filtering; legacy objects without a recorded write age have
`written_at_ms: None`. `put_stream` publishes only after the declared byte
length has been received successfully.
