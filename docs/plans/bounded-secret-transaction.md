# Bounded file transaction implementation

Spec: [bounded native transaction](../specs/bounded-secret-transaction.md).

1. Pin the existing secure directory, lock and data object without retaining a key.
2. Reproduce missing guard and value admission with runnable failing regressions.
3. Add bounded synchronous lock, read, map decode, serialization and publication.
4. Exercise owner cancellation/leases, resource replacement, corrupt/wrong-key
   files, budgets, names, UTF-8 and old-format compatibility in native tests.
5. Run focused and full gates, MSRV, strict lint and fresh coverage. Keep the
   existing file and changed-line coverage thresholds; collect Linux execution
   and wire tests into the existing three-OS native lane.
