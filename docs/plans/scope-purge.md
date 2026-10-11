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
backfill now pass. On canonical base 89cc548, full workspace instrumentation
and every native supplement passed the 90 percent gate across 81 production
files. Strict workspace Clippy, build, rustdoc, formatting and default-feature
tests also passed, followed by all 27 all-feature doctests. Upstream publication
and hosted CI remain pending; host integration is outside this native review.

Independent review found two MongoDB authority defects before publication:
missing generation/pending fields admitted invalid handles, and delayed reset
stages could publish and erase replacement blob IDs after completion. Both
behavioral regressions failed against the unfixed implementation. Complete
authority decoding and transactions sharing the scope authority row now pass
all 13 focused real-replica purge tests, including partial chunk failure and
late-writer replanning. Independent read-only rereview approves these fixes.
The earlier full-gate results above precede these changes. Fresh full coverage
now passes all 81 production files at 90 percent or above, including Mongo purge
at 238/264 lines (90.15 percent), with real replica/standalone fixtures and all
native supplements. Fresh strict workspace Clippy, build and rustdoc pass;
default workspace tests pass (544 reported cases, live-only cases explicitly
gated without native URLs in that run), and all 27 all-feature doctests pass.
The full coverage run separately supplied actual replica/standalone fixtures.
All local publication gates are now green; hosted checks remain pending.

Hosted PR23 review subsequently found missing-admission corruption. Mongo
80935 and SQLite 76992 reproduced stale access after a completed reset. A
further first-pending-reset regression 67592 reproduced the same reopening
before any completed receipt existed. A durable per-scope Mongo witness now
commits with admission closure; both absent and recreated registrations refuse.
Focused replica purge suite 49858 passes all 15 cases. The Memory cross-database
report was disproved by the existing shared root gate and a passing deterministic
public takeover regression; no production workaround was added. Backend and
Mongo purge suites now use focused directories and sibling mod_tests.rs files.

After preserving canonical PR20 merge 2fe0b42, final run 84608 passes 559 native
workspace tests, all real native supplements, all 81 production files above
90 percent (Mongo purge 254/280, 90.71 percent), strict Clippy/build/rustdoc,
all 27 all-feature doctests and formatting/diff checks. Independent read-only
review confirms the pending-witness fix. Owner PR23 is ready; new hosted
checks remain pending after pushing review fixes. Host integration is separate.
