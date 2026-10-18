# Scoped native callback validation

Evidence was captured on macOS under target/scoped-storage-callback/. This is
local validation; this revision still requires canonical review, hosted CI, merge
and release.
No consumer gitlink or released artifact was changed.

RED: the metadata/CAS three-port regression failed to compile against the
foundation because BusStoragePorts did not exist (red.log). The separate
behavioral watch regression failed when a transport accepted WatchOpen despite
the partial profile advertising no Watch (watch-red.log). Both pass after the
additive client API and capability check (green.log and watch-green.log).

The real SDK fixture is built with its independent lock, hashed, allowlisted and
loaded using HostCallbackBinding. Seven native tests pass (native.log): native
metadata/CAS, cache-key Get/Set/Get, environment Set/Get/Delete, audit append/read/
len, private monitor exclusion, plaintext and unrelated-peer refusals, forged
path/destination/authority fields, invalid collection declarations, unavailable
secrets, native fence/generation/expiry and exact-instance replacement.

The gated native secret test proves revoke does not pretend an in-flight future
has stopped: running remains one and drain remains pending until release. Native
epoch fencing then rejects late publication and no secret appears.

Commands run through the host cancellation wrapper unless purely formatting or
metadata:

- cargo fmt --all -- --check; independent proof formatting also passes.
- cargo clippy --locked --all-targets --all-features -- -D warnings.
- cargo build --locked --all-targets --all-features.
- cargo test --locked --all-features, including legacy unattested-host tests.
- cargo test --locked --no-default-features.
- cargo doc --locked --no-deps --all-features with RUSTDOCFLAGS=-D warnings.
- scripts/test-document-callback.sh (locked builds and nine proof tests).
- Independent document-callback clippy and all-target/all-feature locked build.
- Independent schema-contract locked default and all-feature builds.
- python3 scripts/check-schema-dependencies.py --negative-test: isolated default
  and all-feature closures pass; both Tokio negative controls are rejected.

The first full suite exhausted macOS's default 256 file descriptors in existing
native-secrets concurrency tests. The complete suites pass with shell-local
ulimit -n 4096. No tests were disabled or changed to accommodate this limit.

Targeted llvm-cov execution covers the changed executable production files:

- tinystoragedrivers-busproxy/src/scoped.rs: 51/53 lines (96.23%).
- tinystoragedrivers-client/src/client/documents.rs: 58/60 lines (96.67%).
- tinystoragedrivers-client/src/client/mod.rs: 112/121 lines (92.56%).
- tinystoragedrivers-tinybus/src/scoped.rs: 112/117 lines (95.73%).

The schema callback DTO has derived serialization only; its exact representation
and unknown-field/profile rejection are exercised. The canonical Linux native
secrets/strace coverage gate was not run on macOS and is still required in CI.
The pure schema's dependencies remain unchanged (serde/serde_json/thiserror only).
Root lock package identities are unchanged; the native proof lock adds only
required packages and retains every prior identity/version.

Native fixture SHA-256 after review correction:

- Primary DocumentProof1:
  50fbfe8995c9abcd1949d7bd594aff8690532fce73a4efdd22cd4e0f06348a09
- Replacement ReplacementProof1:
  87cbd70d4b4f950ee755cdd86f0eb1aad43f53e2a9445b39e30326de1a9960ac

Independent review exposed two gaps in the original proof, without identifying a
production defect. Both were reproduced before correcting the fixture:
review-peer-red.log shows two separate brokers reusing destination :1.1;
review-monitor-red.log shows the original empty-or-error check accepted a closed
observer. The original cross-broker refusal did not prove instance isolation.

The corrected replacement proof uses one shared broker, two distinct SHA-admitted
SDK identities and distinct module/callback unique names. The replacement's
native marker is read successfully first. The original still-live verified
module and an ordinary outsider are then rejected against that same replacement
destination. Both attestation names and digests are checked as distinct. The
second artifact shares the entire relay implementation; only its SDK identity
wrapper differs, because the SDK reserves each provided bus name.

The corrected broad monitor observes public before/after signals around completed
confidential calls. The latter is an event barrier. Closed, lagged or timed-out
observers fail explicitly; controlled injections of plaintext and byte-encoded
secret/document/audit sentinels are rejected. Nine proof tests pass, including
seven real module tests and two monitor-oracle controls (review-native-green.log).
Fixture fmt, strict clippy and locked all-target/all-feature build pass. No
production source changed during this review correction, so the full workspace
suites and production coverage above were not unnecessarily repeated.

This proves the canonical callback prerequisite. The owning MCP consumer still
must adopt the public route/client and pass its ordinary retry acceptance; this
repository does not claim MCP's native migration acceptance is complete.


## Public review round 1 correction

The frozen PR #60 round-1 feedback identified a fixture confused-deputy issue,
not a production storage-profile defect. The actual outsider regression against
an existing cache_key reproduced the finding before the fix: a confidential
caller could ask ScopedCall to read the loader's secret through the module's
admitted callback identity (review-round-1-red.log). No secret bytes were logged.

The shared relay now captures the unique loading-host identity from immutable
SDK init configuration and compares it with CallContext's authenticated sender
before every fixture member. This is fixture caller authorization, independent
of the supplied route; it introduces no production profile API or host policy.
The runner supplies the identity when loading each SHA-admitted SDK artifact.

The original authorized host can therefore still issue a deliberate same-broker
cross-instance request. A test first reads the replacement's existing secret,
then observes the original module's dispatch counter increase by one, the SDK's
NotAttested error category, and no increase in the native secret-read counter.
The replacement succeeds afterward. This proves that the lower callback guard,
rather than the relay's new caller check, rejects that cross-instance attempt.

The outsider's ScopedCall Secrets.Get, ScopedPlainCall and NativeRoundtrip all
fail at caller authorization before dispatch, and leave the existing key,
documents and audit stream unchanged. Eleven proof tests pass under Rust 1.99,
including the two monitor-oracle controls. Explicit cargo +1.99 formatting,
strict clippy, locked all-target/all-feature proof build and native runner tests
pass. Evidence is in target/scoped-storage-callback/review-round-1-*.log.
The prior Rust 1.99 lint correction is preserved. Production libraries and their
API did not change, so unrelated full-workspace suites were not repeated.

Current Rust 1.99 proof artifact SHA-256 values, superseding the earlier local
fixture digests above:

- module: 8679fdc650d287429193238c3f502fc2bacdc9b641b5234f19fcba54df793069
- peer: f979391b8f3a17f283c2f43c168901ba0cc6669dfbcc27292350365e34cf2fad
