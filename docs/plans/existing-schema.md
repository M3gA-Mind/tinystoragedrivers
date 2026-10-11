# Existing schema implementation

1. Add optional default-refusing backend and document methods with capability tests. Confirm Memory native behavioral regressions fail against defaults.
2. Implement native Memory declaration restriction and read-only validation; pin sticky named/fenced constructors and scoped deletion.
3. Pin SQLite/File missing named storage and declaration mutation regressions; implement native restricted construction and metadata validation without schema initialization.
4. Pin Mongo fresh-handle native catalog checks and lazy no-DDL behavior; retain fences and unsupported GridFS writes.
5. Verify default and all-feature tests, strict Clippy, build, formatting, rustdoc and live native fixtures where available. Document measured limitations and publish a canonical ready PR only after actual gates.

## Acceptance status (2026-10-11)

The branch preserves granular implementation history and canonical merges through
`89cc548` (PR15), including PR17, PR18 and PR19. PR15 supplies the owning Mongo
checked-read expiry and concurrent index declaration fixes; these are now
canonical prerequisites, rather than unpublished patches carried by this branch.

The fresh default-parallel workspace all-feature run passed 547 Rust tests and
27 doctests with both actual Mongo fixtures enabled:

- Replica set: `mongodb://127.0.0.1:29162/tsd_existing_schema_7422?directConnection=true`.
- Standalone: `mongodb://127.0.0.1:29164/tsd_existing_schema_standalone_7422?directConnection=true`.

Existing-schema live tests allocate unique logical namespaces inside the URL's
configured physical database and remove only their owned collections. The native
Default guard case keeps the actual default namespace, with unique lease/data
collections. They do not drop a physical database or change a configured guard
address to avoid a failure. A separate standalone command-trace test passed with
zero create/createIndexes/dropIndexes/drop/collMod commands after trusted schema
preparation. Scoped document deletion remains permitted and fenced.

Fresh strict all-target/all-feature Clippy, all-target/all-feature build,
warnings-denied rustdoc, formatting and diff checks pass. The default workspace
run passed 510 Rust tests and 25 doctests without live-service environment
variables. Fresh full coverage passed 547 Rust tests with both Mongo fixtures,
eight actual native syscall failure injections, the actual two-link key publisher
and concurrent reader proof, and actual isolated Secret Service. All 75 executable
production files have at least 90% line coverage (minimum 90.1515%). The linked
checkout coverage selector is an explicit pure-tooling dependency on PR20
(`a44e09f`), merged with preserved history while that PR awaits canonical publication.
It fixes all three native supplements, including the newly merged Secret Service
helper, and fails closed for an absent, empty or ambiguous current inventory.
Two Python tests passed, including a probe through the actual I/O helper entry
point with a misleading stale inventory present. No inventory alias, ignored
supplement or relaxed coverage threshold is used.

Earlier measured coverage (533 tests) predates this final canonical merge and is
historical evidence only. Constructor and native-port acceptance
here do not prove OpenHuman ProfileHost binding. That host composition, the
TinyAgents restricted initialization mode, and migration/import authority remain
separate integrations; no local-file fallback is introduced by this contract.

## PR21 review regressions

Four additional behavioral tests reproduced the review findings before the fix:
standalone deletion accepted missing persisted metadata, trusted GridFS preparation
created a non-unique chunk index, restricted blob reads accepted that non-unique
index, and malformed Memory names returned NotFound. The fixes retain scoped data
deletion while validating its admitted schema, share the required unique chunk
index between preparation and read-only validation, and validate names first.

Fresh validation after these fixes passed 551 Rust tests plus 27 doctests with
both actual Mongo servers enabled under normal parallel scheduling, and 514 Rust
tests plus 25 doctests in the default workspace run. Full coverage and every
native supplement passed again; all 75 executable production files remain above
90% (minimum 90.1515%). Strict Clippy, build, warnings-denied rustdoc, formatting
and diff checks passed. This evidence supersedes the earlier test counts for the
reviewed production source.
