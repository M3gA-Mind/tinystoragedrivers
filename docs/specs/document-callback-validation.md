# Document callback validation record

This records scoped behavior evidence. Full canonical workspace acceptance,
coverage and Rust 1.88 validation are still pending; this is not publication
readiness or a TinyMemes persistence completion claim.

## Native forwarding baseline

The first build failed on fixture-only imports, borrowed port Arcs and a missing
collections prefix argument. No behavioral RED was claimed from that build.

After correcting those fixtures, the baseline executed eight tests:
three bus tests passed; three adapter controls passed; two adapter tests failed
at the unimplemented forwarding path. Those two tests pin native CAS/version
preservation and fixed captured collection/scope. This was an executed feature
baseline, not a reproduced defect in a previously shipped adapter.

Forwarding subsequently passed bus4 + adapter10 at d6db153.

## Actual compiled artifact exposed a wire defect

The excluded proof workspace builds the real canonical TinyBus SDK cdylib.
The runner computes its SHA-256, stages an allowlist beside the actual artifact,
loads it through HostCallbackBinding and uses confidential authenticated calls.
The module links the bus contract, not a storage implementation.

First actual artifact run: three executed, two passed, one failed. A legitimate
Get with a bare id field was conservatively detected as a stream reference.
A fourth typed business-data regression was added before fixing that encoding.
The next real artifact run executed four: two passed, two failed with the same
private-stream Protocol refusal. The second failure used nested business objects
containing id/content_type/len, not transport stream references.

Canonical TinyBus documents that conservative policy. No TinyBus parser,
privacy rule, native admission or storage implementation was changed.

## Connected wire adaptation

The new unpublished contract uses document_id and document_json containing
strictly parsed inline JSON text. Rust consumers still receive JSON Value.
All request structs reject unknown fields; the inline document text is ordinary
persisted data and is never interpreted as a stream handle or storage authority.

At ae19651, the actual final wire attributes were read back from committed source.
Fresh scoped tests passed bus6 + adapter10 (16 total, none ignored or filtered).
The actual rebuilt/hash-verified artifact then passed all four tests, covering:

- native conditional writes, conflicts, versions, deletion and missing documents;
- captured collection/scope, including another actual module/store instance;
- ordinary and confidential unauthorized peers, arbitrary selectors and revoke;
- stale native fence refusal without private backend prose;
- nested stream-shaped business JSON roundtrip, unchanged in the native store.

Zero-test doctest groups were not counted as evidence.

## Strict scoped checks

Strict two-crate Clippy and rustdoc passed after narrow corrections to doc
backticks, borrowed error mapping and empty-vector assertions. No lint or test
was disabled. Those changes are checkpoint b8ee8ec. Behavior reruns on that
checkpoint, full workspace gates, per-file coverage, MSRV and upstream review
remain required before publication. Actual artifact coverage here is local
Linux, not evidence of every platform's dynamic-loader behavior.

Original logs live under target/gates; this durable record survives build-output
cleanup. The proof establishes this document callback boundary. It does not
establish module business persistence, legacy import authority, read-fencing
beyond the supplied native port's contract, or PR59 lifecycle drain behavior.
