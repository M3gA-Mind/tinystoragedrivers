# Scope purge publication review

Specification: [native scope purge](../specs/scope-purge.md).

This plan reviews the existing imported implementation before publishing it.
Keep every original implementation commit and canonical merge individually.

1. Establish native conformance and optional capability refusal. Confirm atomic
   Memory/SQLite and quiescent MongoDB erase every native port, preserve sibling
   scopes/databases, and reject unsupported combinations before mutation.
2. Pin completed operation history with a failing test. Query the first receipt
   after two resets and retry it against replacement document/stream/blob data.
   Implement immutable per-scope/per-operation receipts with native atomic
   authority updates. Preserve any latest admission available during upgrade.
3. Add native restart, backfill, corruption and rollback tests. Execute real
   loopback MongoDB replica/standalone cases and interruption/resume checks;
   skipped live tests and zero-test filters are not verification evidence.
4. Integrate current canonical main without rewriting history. Run format,
   workspace tests, strict Clippy, rustdoc and build gates. Run the repository's
   per-file coverage check and close any uncovered production paths.
5. Review the API and implementation against the specification, record native
   unsupported guarantees, and open a ready upstream PR. Consume no host API or
   gitlink until the owning change is canonically available.

Memory history RED then GREEN, SQLite history RED then GREEN, and MongoDB
replica history RED then GREEN are established. SQLite persisted history,
upgrade backfill and corruption checks and MongoDB independent reconnect with
backfill now pass. Complete workspace/coverage gates remain pending.
