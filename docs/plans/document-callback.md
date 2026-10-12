# Document callback implementation

Contract: [document callbacks](../specs/document-callback.md).

1. Add minimal bus types/client and host adapter crates, leaving core transport-free.
2. Stage wire-selector, CAS/version/missing-document, captured scope/collection,
   unverified-context, and sanitized-error tests. Run meaningful behavioral RED.
3. Forward Get/Put/Delete mechanically to the captured native port, preserving
   conditions, versions and categories. Rerun focused GREEN.
4. Build an actual module artifact against the bus-only contract. Verify admitted
   confidential CRUD, unauthorized peers, selector rejection, independent captured
   scope/collection bindings, and native refusal through compiled ABI.
5. Run default/all-feature suites, strict Clippy/docs, formatting and coverage.
   Publish canonical upstream only after independently verifying the contract.
   Host consumption and TinyMemes module persistence remain separate.
