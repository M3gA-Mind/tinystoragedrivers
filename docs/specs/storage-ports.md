# Storage ports

Status: implemented. This document points to the v2 public contract, which is
the normative specification for storage behavior.

TinyStorageDrivers v2 separates shared vocabulary, async ports, and engines.
Hosts select only the primitives they need, bind every handle to an explicit
`Scope`, and keep schema, migration, retention, and key-acquisition policy with
the component that owns those records.

The complete contract, including document, stream, KV, blob, queue, secret,
connection, and module-bus behavior, is in
[`primitives-v2.md`](primitives-v2.md). Its crate inventory is recorded in
[`primitives-v2-inventory/`](primitives-v2-inventory/README.md).

The optional facade composes complete document backends through
`StorageConfig`. Independent S3 blobs and RabbitMQ queues use scoped
`BlobConfig`/`open_blobs` and `QueueConfig`/`open_queues` helpers. Secrets on a
durable backend remain unsupported until the host supplies an explicit secret
engine and key provider.
