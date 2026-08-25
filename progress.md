# Project Progress

An engineering log for this repository: what has been built, what decisions were made and why,
and what broke along the way. Newest entries at the bottom.

## Milestones

| # | Milestone | Status |
|---|---|---|
| M0 | Environment + multi-module skeleton | ✅ **done** |
| M1 | Ledger core — double-entry, `SELECT FOR UPDATE`, deadlock-safe lock ordering | ⬜ next |
| M2 | Transactional outbox + Kafka publishing + inbox dedup | ⬜ |
| M3 | SAGA orchestration — compensation, state machine, timeout sweeper | ⬜ |
| M4 | Idempotency keys + retry/backoff + Dead Letter Queue | ⬜ |
| M5 | JWT authentication and per-account authorization | ⬜ |
| M6 | Observability — Prometheus metrics, Grafana dashboards, distributed tracing | ⬜ |
| M7 | Chaos suite — 8 injected-failure scenarios | ⬜ |
| M8 | Load test — k6 to 1,000 concurrent transfers | ⬜ |
| M9 | Documentation, ADRs, README polish | ⬜ |
| M10 | Kubernetes manifests + Helm chart | ⬜ |

---

## Key design decisions

Decisions that shaped the architecture, with the reasoning behind each.

### Orchestration over choreography

The saga is driven by an explicit orchestrator rather than services reacting to each other's
events. Choreography has fewer moving parts, but the business process then exists nowhere in the
code — understanding one transfer means reading every service. For a payment flow, being able to
answer "where is transfer X right now?" with a single query is worth the central component.

### Postgres unique constraint for idempotency, not a Redis lock

The original design called for Redis distributed locks to prevent duplicate charges. This was
changed deliberately.

Redlock's safety depends on bounded clock drift and bounded GC pauses. If a process holding a
10-second lease stalls for 15 seconds, the lease expires, Redis hands the lock to a second
process, and both proceed — a double charge, with the lock behaving exactly as specified. That
risk is acceptable for a cache warm-up. It is not acceptable for money.

So correctness rests on `UNIQUE (client_id, idempotency_key)` in Postgres: insert first, and treat
a constraint violation as "duplicate — return the stored original response." ACID, no timing
assumptions. Redis remains as a fast-path response cache and an anti-stampede lock, but it is
**not load-bearing** — delete Redis and the system is still correct, only slower.

### At-least-once delivery, not "exactly-once"

Exactly-once delivery over an unreliable network is impossible — it reduces to the Two Generals
Problem. The transactional outbox provides *at-least-once* delivery; combined with an inbox table
that makes consumers idempotent, the result is **effectively-once processing**. The distinction is
stated explicitly rather than glossed over.

### Correctness enforced by database constraints, not application logic

Uniqueness is enforced by `UNIQUE` / `PRIMARY KEY` and detected via constraint violation, never by
a `SELECT`-then-`INSERT` check. A check-then-act sequence has a race window between the two
statements; a constraint is evaluated atomically at commit and has none.

### Double-entry ledger

Balances are derived from immutable, append-only entries that sum to zero, rather than stored as
mutable numbers. This makes money creation *structurally detectable* — a bug breaks a global sum
that a script can verify in milliseconds — and gives a complete audit trail for free.

`accounts.balance` is also stored as a denormalized column for fast reads and row locking, updated
in the same transaction as the entries. Invariant I2 exists specifically to assert the two never
drift apart.

### Redpanda for local development, Apache Kafka available on demand

Both speak the Kafka protocol and the application code is 100% Kafka API either way. Redpanda
starts in about 2 seconds versus roughly 30 for Kafka, which matters a great deal when the chaos
suite restarts the broker repeatedly. A Compose profile swaps in real Apache Kafka in KRaft mode
to verify the system runs unmodified on both.

---

## Correctness invariants

These five conditions define "correct" for this system. A script asserts all of them, and every
chaos scenario and load-test run ends by calling it.

```
I1  SUM(ledger_entries.amount) = 0                    global double-entry balance
I2  accounts.balance = SUM(its ledger_entries)        denormalized column agrees with truth
I3  SUM(balances) + SUM(active holds) is constant     conservation across a run
I4  no saga non-terminal after quiescence             nothing stuck, no stranded money
I5  no account balance < 0                            no overdraft under concurrency
```

---

## Log

### M0 — Environment and skeleton · 2026-08-25

**Built**

Maven multi-module reactor targeting Java 21 and Spring Boot 4.1.1:

```
distributed-payment-engine/
├── common-events/          shared command/event contracts and versioned topic names
├── payment-orchestrator/   port 8081
├── account-service/        port 8082
├── payment-gateway/        port 8083
└── infra/ chaos/ loadtest/ scripts/ docs/ k8s/
```

Configuration choices worth recording:

- **`ddl-auto: validate`** — Flyway owns the schema and Hibernate may never alter it. Drift
  between entities and migrations then fails loudly at boot instead of corrupting data quietly.
- **`open-in-view: false`** — stops the persistence session leaking into view rendering, which
  otherwise hides N+1 queries and holds database connections far longer than necessary.
- **Every datasource URL is `${DB_URL:localhost-default}`** — the same jar runs from an IDE and
  from Compose, where the database host is `postgres` rather than `localhost`.
- **Kafka, Redis, Security and tracing dependencies are deliberately absent.** Each arrives in
  the milestone that needs it, so the commit history explains why every dependency exists.

**Environment**

Java 21.0.11 LTS, Docker 29.7.2 with Compose v5.4.0, WSL2 capped at 8 GB. No system Maven
required — the Maven wrapper is committed. k6 and psql run as containers rather than installs.

Available RAM is the binding constraint on this machine, so JVM heaps are explicitly capped in
Compose rather than left to default sizing.

**Problems hit**

1. **Spring Boot 3.5.x is no longer offered by Spring Initializr** and is out of OSS support. The
   plan had specified 3.5.x for better tutorial coverage; that was overridden, because shipping an
   end-of-life framework in a portfolio project is a worse problem than thinner documentation.

2. **Initializr's version identifier is not the Maven artifact version.** Initializr reports
   `4.1.1.RELEASE`; Maven Central publishes `4.1.1` — the `.RELEASE` suffix was dropped after Boot
   2.x. The first build failed with `Non-resolvable parent POM`. Worth noting that
   `search.maven.org` returned stale results claiming 3.5.3 was the latest; the authoritative
   source is `repo.maven.apache.org/maven2/.../maven-metadata.xml`.

3. **Spring Boot 4.x renamed most starters.** Pre-2025 examples will not copy-paste:

   | Boot 3.x | Boot 4.x |
   |---|---|
   | `spring-boot-starter-web` | `spring-boot-starter-webmvc` |
   | `spring-kafka` | `spring-boot-starter-kafka` |
   | `flyway-core` | `spring-boot-starter-flyway` |
   | `spring-boot-starter-test` (single artifact) | per-module: `...-webmvc-test`, `...-data-jpa-test`, … |
   | `org.testcontainers:postgresql` | `org.testcontainers:testcontainers-postgresql` |
   | `org.testcontainers.containers.PostgreSQLContainer` | `org.testcontainers.postgresql.PostgreSQLContainer` |

**Verified**

`./mvnw -B -ntp compile` — BUILD SUCCESS, all 5 modules, 36s.

**Next**

Compose stack (PostgreSQL + Redpanda + Redis) and all three services reporting a healthy
`/actuator/health`, then M1: the ledger core.
