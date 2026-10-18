# tinystoragedrivers-schema

Pure storage declarations and version-one transform wire contracts for owners
that must describe storage without linking a runtime, backend, clock, hashing,
compression, native library, or memory driver.

`SchemaDeclaration` owns its `Owner`, `Item` declarations and ordered
`StepDeclaration` list. Each step names a `TransformDescriptor` with its
operation, record family and operation contract version (initially `1`).
Declaration construction and decoding do not execute an operation or validate
registry composition; those responsibilities belong to the runtime.

Document transforms receive `DocumentsTransformRequest` and return
`TransformResult<Vec<TargetRecord>>`. An empty vector drops the input; multiple
records support splitting. `SourceRecord` preserves metadata and `TargetRecord`
keeps optional creation time and the distinct carry, never and at-time expiry
states. Stream and blob transforms receive their corresponding request and
return `TransformResult<Option<StreamTransformOutput>>` or
`TransformResult<Option<BlobTransformOutput>>`. `Ok(None)` skips/drops an input;
`Err(TransformError)` reports invalid input, an unsupported operation, or an
unsupported operation version. Blob bytes have no text encoding requirement.

New enums use explicit snake-case `kind` tags. Existing wire types retain their
representations: for example `SealWhen::Idle(42)` is `{"idle":42}` and metadata
retains its existing field names, integer widths, defaults and omitted expiry.
Owner deserialization applies the same grammar and reserved-name validation as
`Owner::new`; it cannot construct an invalid owner through JSON.

`StorageError`, `ErrorKind` and capability vocabulary are ordinary Rust errors
and values shared with core. `StorageError` retains its boxed standard error
source and is not serializable. `TransformError` is a separate source-free wire
error. Sharing the ordinary error vocabulary preserves the exact existing
`Result<_, StorageError>` constructor and collection validation signatures.

Core reexports these types and retains executable `Schema`, closure-backed
`Step`, `Registry`, storage ports, `Key` and scope hashing. The facade forwards
these exports, including `declarations`, through core.

Run `python scripts/check-schema-dependencies.py --negative-test` from the
repository root to resolve this crate through an isolated consumer outside the
workspace's feature unification. Both default and all schema features must have
only serde, serde_json, thiserror and their implementation dependencies. The
negative fixture introduces Tokio and must be rejected. It writes metadata
fixtures under this checkout's `target/`; it does not compile them.
