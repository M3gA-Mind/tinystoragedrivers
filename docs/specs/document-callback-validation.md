# Document callback validation record

This records executed behavior and canonical workspace acceptance at source
checkpoint 760230812e260fcfed9594dbf40dc06fbc9d4c6e. It establishes the confined
document transport, not TinyMemes business persistence or a host migration.

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
was disabled. Those changes are checkpoint b8ee8ec. The final 7602308 checkpoint
also supplies compiled documentation examples and runs the locked actual module
proof in the existing Linux CI job. Actual artifact evidence here is local
Linux, not evidence of every platform's dynamic-loader behavior.

## Final canonical acceptance

Fresh final-head gates passed, one build at a time:

- scoped bus6 + adapter10, plus two compiled doctests;
- locked rebuilt/hash-verified SDK module proof: four passed, none ignored;
- all-feature workspace: 676 unit/integration tests and 29 doctests;
- default workspace: 635 unit/integration tests and 28 doctests;
- strict all-target/all-feature workspace Clippy and full build;
- all-feature workspace rustdoc with warnings denied;
- Rust 1.88 all-target/all-feature workspace build;
- strict all-target/all-feature Clippy in the excluded proof workspace;
- root/proof formatting and diff whitespace checks;
- canonical coverage gate: 676 instrumented tests, profile helper two tests,
  eight actual injected native I/O failures, concurrent key publication probe
  and isolated admitted Secret Service probe, all passed.

All 90 production files exceeded the required 90% line threshold. New files:
client 100% (20/20), inline JSON 91.67% (11/12), wire types 100% (3/3), adapter
96.97% (128/132). The excluded ABI proof is executed separately; its source is
not included in the native workspace's coverage denominator.

The workspace suites and coverage used an owned loopback MongoDB 7 replica set
and verified local MinIO, including the 500 MiB streaming roundtrip. True
standalone MongoDB was not enabled; native Windows/macOS probes remain their
existing CI responsibility. No production endpoint or real credential was used.

The first full suite stopped with three MinIO connection-refused failures after
the setup shell's background fixture process disappeared. Its log is preserved.
The same verified binary was restarted in a persistent tool session; the entire
fresh suite then passed. No product code, test, timeout or assertion changed.

Independent review of 7602308 found no blocking authority, CAS or source issue.
At that historical checkpoint, TinyBus was pinned to canonical 1f255765
(0.1.4); PR59 was not a dependency and
its pending lifecycle correction is not claimed by this CRUD transport.

## Hosted source policy

PR27's first Supply chain job failed because the existing unknown-Git denial
correctly rejected the newly introduced canonical TinyBus source. A local
`cargo deny check sources` reproduced that refusal before changing policy.

The fix allows exactly https://github.com/tinyhumansai/tinybus and requires
revision-based Git specifications. Unknown Git repositories and registries
remain denied. Both workspace manifests were checked for the exact canonical
full 40-character revision. The revision-spec setting alone does not validate
the length of a revision; the checked manifests contain the full commit.

Fresh `cargo deny check sources` and `cargo deny check all` then passed, with
advisories, bans, licenses and sources all accepted. No Rust source or native
admission changed. Hosted checks must finish on the resulting policy head.

Original logs live under target/gates; this durable record survives build-output
cleanup. The proof establishes this document callback boundary. It does not
establish module business persistence, legacy import authority, read-fencing
beyond the supplied native port's contract, or PR59 lifecycle drain behavior.

## Canonical runtime dependency follow-up

The dependency-only follow-up from canonical TSD `d2f85b2` aligns the
production adapter and the excluded native proof with merged TinyBus
`40814a8279ff71b8fb411f9f29362c8ec0be51e2` (PR60), now pinned by the
released tag `v0.1.5` that contains it. This includes the host
callback lifecycle accounting from PR59 and the SDK diagnostic ownership fix.
It changes neither the document callback contract nor storage implementation.
The root graph has one TinyBus identity; the native proof has one TinyBus and
one matching SDK identity, without replacement directives or local runtime
patches. This is canonical source availability, not a new release claim.

The real native callback suite passed all four cases on the merged runtime.
The historical evidence above remains evidence for its original checkpoint;
it does not prove the later runtime's additional lifecycle behavior.
