# Document Watch implementation

Spec: [Scoped document invalidation](../specs/document-watch.md).

1. Add documented object-safe watch types and the optional unsupported port.
   Run existing core tests; test invalid inputs and redacted opaque cursors.
2. Add explicit feature-gated conservative polling with checked admission.
   Prove opening/delivery typed native refusal first, then implement checks.
   Test interval validation, cancellation, and unsupported cursor admission.
3. Add native Mongo scoped change streams and database/filter/fence-bound
   resume encoding. Prove subscribe-before-read and isolation against a local
   replica set before implementation; retain the actual failing test logs.
4. Add optional-capability conformance and cancellation/resume-loss tests.
   Refuse unsupported deployments explicitly; preserve native error sources.
5. Run full workspace tests, fmt, strict Clippy, rustdoc, build, MSRV and every
   source-file coverage gate. Integrate fresh canonical changes without squash
   before opening an independent canonical PR. No host consumption here.
