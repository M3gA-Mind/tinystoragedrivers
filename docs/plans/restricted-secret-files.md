# Restricted secret-file delivery order

Contract: [restricted secret files](../specs/restricted-secret-files.md).

1. Tests-only checkpoint: inspect the actual empty production reservation
   handle under a native broad Windows parent; inspect public key/data/lock
   objects; inspect Unix birth mode in a child with umask zero. Add native
   Linux, macOS and Windows lanes, running the named birth regression first.
   Capture actual Windows and Unix behavioral RED before semantic changes.
2. Establish a supported safe Windows descriptor/create/handle/flush path
   within MSRV 1.88 and the unsafe prohibition. Extend a maintained upstream
   wrapper if needed. Establish the documented namespace persistence boundary
   separately; unsupported paths cannot issue durable receipts.
3. Implement minimal reusable owner mechanics for restrictive birth, admitted
   namespaces and reads, stable locks, verified publication and typed
   conditional migration cleanup. Preserve existing formats. Prove each
   behavior with meaningful failure regressions and native I/O evidence.
4. Run owner contract checks and real three-platform coverage: at least 90%
   per compiled production source file and 80% changed executable lines,
   rejecting omitted or malformed reports. Review the complete change and
   open one canonical owner PR before consumers change production pins.

Stage 1 intentionally fails against the baseline. Passing native acceptance,
Windows persistence feasibility and full coverage remain later prerequisites.
