# ADR 0002: Keep storage static and primitive

Status: accepted for the v2.0.0 restructure (2026-10-19).

## Context

The v1 workspace combines shared storage values and ports with memory and SQL
engines, schema registries, migrations, typed repositories, leases, counters,
scope policy, secret file formats, and migration transactions. A consumer that
needs a port can inherit engine dependencies or tsd-owned persistence policy.
The persisted records belong to tinyagents, tinyflows, tinyconnectors,
OpenHuman, and other consumers. Their owners know the record invariants and
migration history.

## Decision

Split storage into data-only `tinystoragedrivers-types`, trait-only
`tinystoragedrivers-api`, one crate per primitive, and a testkit. Engines are
features on the primitive that owns the port. An unpublished
`tinystoragedrivers-connect` shares one configured engine connection where
multiple primitive adapters use the same database. The facade composes
engines only for a host authorized to choose them.

Consumers own typed repositories, schemas, migration steps, leases, counters,
and their record formats. OpenHuman owns process-wide composition, URL and
scope conventions, legacy import, the queue manifest, secret formats,
credential naming, and host lifecycle. The bus contract shares the types
crate's wire vocabulary; a bus proxy adapts module calls to the API. No
consumer library depends normally or at build time on a storage engine.

## Consequences

- Libraries can depend on ports without linking SQLite, MongoDB, S3,
  RabbitMQ, filesystem implementations, keyrings, or cloud KMS clients.
- Tests select memory engines through dev-dependencies and run the same
  conformance suite as production engines.
- Engine selection is explicit at the composition root. A missing feature
  fails configuration rather than silently falling back to memory.
- Data-model changes and their migrations ship with the owning consumer.
- Existing `storage.db` compatibility remains an engine concern; migration
  policy and one-time legacy import remain host concerns.
- The v1 schema, purge, generation admission, fencing, migration delivery,
  and restricted secret-file contracts are historical and superseded by
  [primitives-v2](../specs/primitives-v2.md). The corresponding implementation
  leaves remove both code and obsolete documents.

## Alternatives rejected

- Keep a generic schema/migration framework in tsd: an infrastructure crate
  would own consumer model versions and upgrade policy.
- Keep ports and engines together in `-core`: every consumer would inherit a
  wide crate and its dependency graph even when it needs one primitive.
- Provide a production memory fallback: it can report successful writes that
  disappear on restart.
- Let every primitive open its own SQLite connection: adapters would disagree
  about database identity and lifecycle.

## Rollout

Implement the types/API spine first, then primitive engines and their
conformance suites, then transport and facade. Keep existing on-disk
`storage.db` files readable across the engine move. Consumers migrate models
and tests in their own repositories; OpenHuman updates submodule pins only
after the owning repository merges its change.
