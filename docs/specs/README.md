# Specifications

Specifications define what the system must do before implementation details
take over. Create one for behavior that changes a public API, crosses module
boundaries, introduces a durable data format, or has meaningful operational
constraints.

Use a short kebab-case filename such as `retry-policy.md`. Each specification
should contain:

1. **Status and owner** — Draft, Accepted, Implemented, or Superseded.
2. **Problem** — the user or system need, without prescribing a solution.
3. **Goals and non-goals** — the exact boundary of the work.
4. **Proposed behavior** — public API, inputs, outputs, errors, and examples.
5. **Invariants and constraints** — properties every implementation must keep.
6. **Acceptance criteria** — externally observable pass/fail conditions.
7. **Open questions** — unresolved decisions that block acceptance.

After the specification is accepted, create a linked implementation plan in
[`../plans/`](../plans/README.md). Keep code snippets small enough to clarify
the contract; production code still belongs under `src/`.

See [`primitives-v2.md`](primitives-v2.md) for the current storage contract,
[`storage-ports.md`](storage-ports.md) for its entry point, and
[`storage-bus.md`](storage-bus.md) for the loadable-module wire protocol.
Queue-specific behavior is in [`queue-store.md`](queue-store.md) and
[`queue-rabbitmq.md`](queue-rabbitmq.md); confidential secret handling is in
[`secret-storage.md`](secret-storage.md).
