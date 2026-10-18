# Scoped native callback validation

Evidence was captured on macOS under target/scoped-storage-callback/. This is
local validation; canonical review, hosted CI, merge and release remain pending.
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
- scripts/test-document-callback.sh (locked native build and seven tests).
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

Native fixture SHA-256 at validation:

    6be072ab509071e89b2356429ab268aa398dd2923fa903d1fab91a145223a2ac

This proves the canonical callback prerequisite. The owning MCP consumer still
must adopt the public route/client and pass its ordinary retry acceptance; this
repository does not claim MCP's native migration acceptance is complete.
