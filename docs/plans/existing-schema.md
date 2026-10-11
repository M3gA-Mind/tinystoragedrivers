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
formatting and diff checks pass. Full fresh coverage is still required before
publication: the canonical Secret Service supplement currently reads a hardcoded
`tinystoragedrivers-profraw-list`, whereas cargo-llvm-cov names the inventory after
the linked checkout. The existing fail-closed `profile_directory` selector and
two tests cover this distinction for the I/O and key-race helpers; the newly
merged Secret Service helper needs the same selection. No inventory alias,
ignored supplement or relaxed coverage threshold is used.

Earlier measured coverage (533 tests, native failure/concurrency supplements,
75 executable production files each at least 90%) predates this final canonical
merge and is historical evidence only. Constructor and native-port acceptance
here do not prove OpenHuman ProfileHost binding. That host composition, the
TinyAgents restricted initialization mode, and migration/import authority remain
separate integrations; no local-file fallback is introduced by this contract.
