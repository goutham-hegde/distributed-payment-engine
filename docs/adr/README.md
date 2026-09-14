# Architecture decision records

Each record states one decision: the problem that forced it, the options, what was chosen and why,
what it costs, and when to revisit it. Where a decision was changed by what testing found, the
record says so rather than being rewritten to look as if it had been right the first time.

| # | Decision | Milestone |
|---|---|---|
| [0001](0001-orchestration-not-choreography.md) | The saga is orchestrated, not choreographed | M3 |
| [0002](0002-idempotency.md) | Idempotency is a Postgres constraint; Redis is a cache in front of it | M4 |
| [0003](0003-shared-contracts.md) | Shared contracts: one module of records and topic names, versioned by topic | M2 |
| [0004](0004-shared-messaging-library.md) | A shared messaging library, but never a shared domain | M3 |
| [0005](0005-jwt-authentication.md) | Stateless JWT authentication, and where authorization actually happens | M5 |
| [0006](0006-trace-context-in-the-outbox.md) | The trace context is a column, not a thread local | M6 |
| [0007](0007-the-console-is-a-reader.md) | The console is a reader, and one origin sits in front of three services | M6.5 |
| [0008](0008-reconciliation-finishes-compensations.md) | Reconciliation finishes a compensation; it never originates a movement | M7 |
| [0009](0009-admission-control-and-the-request-bulkhead.md) | Admission control bounds work in flight; a bulkhead bounds requests | M8 |
| [0010](0010-sharded-clearing.md) | CLEARING is sharded, and each hold records its shard | M8 |
| [0011](0011-tests-have-no-route-to-the-running-stack.md) | Tests have no route to the running stack | M8 |
| [0012](0012-a-jvm-memory-budget.md) | A memory budget for the JVMs, not a heap cap; G1, and GC logs always on | M8 |
| [0013](0013-kubernetes.md) | Kubernetes: putting back what Compose was doing for free | M10 |
