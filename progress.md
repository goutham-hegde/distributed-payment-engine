# Project Progress

An engineering log for this repository: what has been built, what decisions were made and why,
and what broke along the way. Newest entries at the bottom.

## Milestones

| # | Milestone | Status |
|---|---|---|
| M0 | Environment + multi-module skeleton | ✅ **done** |
| M1 | Ledger core — double-entry, `SELECT FOR UPDATE`, deadlock-safe lock ordering | ✅ **done** |
| M2 | Transactional outbox + Kafka publishing + inbox dedup | ⬜ next |
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

`accounts.balance_minor` is also stored as a denormalized column for fast reads and row locking,
updated in the same transaction as the entries. Invariant I2 exists specifically to assert the two
never drift apart.

### Money is an integer count of minor units

Amounts are `BIGINT` paise, never a floating-point type: 0.1 + 0.2 is not 0.3 in binary floating
point, and a ledger that cannot represent its own amounts exactly cannot be reconciled.
`BigDecimal` is exact as well but carries scale and equality pitfalls that an integer count of the
smallest indivisible unit simply does not have.

The cost is that every boundary must name its unit, so the fields are `amountMinor` and
`balance_minor` rather than `amount` and `balance`. A factor-of-100 error is among the most
expensive money bugs there is, and the field name is the cheapest available defence.

### Where money enters a closed ledger

If every posting sums to zero, the first rupee has no way in: crediting a newly opened account is
a credit with no matching debit, which is money from nothing and I1 broken on the first request.

The answer is the one every real ledger uses — a single **issuance account** (equity, in
accounting terms). Money enters by being debited from it, so its balance is negative and its
magnitude is exactly the total issued. That is a liability, not an overdraft, which is why I5 is
stated as *no CUSTOMER account may go negative* and the CHECK constraint reads
`balance_minor >= 0 OR account_type = 'SYSTEM'`. Exactly one such account may exist, enforced by a
partial unique index rather than by convention.

Funding an account goes through the ordinary transfer path rather than writing a balance
directly. There is deliberately no privileged code path that can move a balance without a
counterpart — such a path would be able to create money, and no invariant would notice.

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
I1  SUM(ledger_entries.amount_minor) = 0              global double-entry balance
I2  accounts.balance_minor = SUM(its entries)         denormalized column agrees with truth
I3  SUM(balances) + SUM(active holds) is constant     conservation across a run
I4  no saga non-terminal after quiescence             nothing stuck, no stranded money
I5  no CUSTOMER account balance_minor < 0             no overdraft under concurrency
```

`scripts/verify-invariants.sh` asserts these against a running stack and exits non-zero on any
violation. I4 reports as skipped until the saga tables exist, and I3 needs a baseline recorded at
the start of a run — both are stated explicitly rather than passing silently, because a check that
did not run must never look like a check that passed.

I5 is scoped to customer accounts. See "Where money enters a closed ledger" below for why exactly
one account is exempt.

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

**Compose stack**

One `docker compose up -d --build` brings up PostgreSQL and all three services. A single
multi-stage `Dockerfile` builds every service, selected by a `MODULE` build argument; a BuildKit
cache mount on `~/.m2` means the three images share one dependency download and rebuilds are
fast. Runtime images carry a JRE and a jar only — no Maven, no JDK, no source — and run as an
unprivileged user.

Only PostgreSQL is in the stack at this point. Redpanda arrives at M2, Redis at M4, and
Prometheus/Grafana/Jaeger at M6, so every container is introduced by the commit that first needs
it.

Two details worth recording:

- **`depends_on` is not enough.** It waits for a container to exist, not to be ready, and
  PostgreSQL accepts TCP connections several seconds before it can serve queries. Without
  `condition: service_healthy` against a `pg_isready` probe, the services crash-loop at startup
  and it looks like a Flyway problem.
- **Database-per-service needed more than ownership.** The first version created a database and a
  role per service and assumed that was isolation. It was not: PostgreSQL grants `CONNECT` on
  every database to `PUBLIC` by default, so any of the three roles could connect to any database
  and read it. Verified by connecting as `accounts` to `payments_db` — it succeeded. The fix is
  `REVOKE CONNECT ON DATABASE <db> FROM PUBLIC` followed by an explicit grant to the owner. The
  isolation matrix is now a clean diagonal, asserted rather than assumed.

**Verified**

```
./mvnw -B -ntp compile                    BUILD SUCCESS, 5 modules, 36s
docker compose up -d --build              all 4 containers healthy in ~20s
/actuator/health x3                       UP, db UP
cross-database connection matrix          6/6 denied, 3/3 own-database connected
```

**Next**

M1: the ledger core — double-entry schema, `SELECT ... FOR UPDATE`, deadlock-safe lock ordering,
and the invariant verification script.

---

### M1 — Ledger core · 2026-09-03

**Goal**

A single service that moves money correctly under concurrency: double-entry postings, pessimistic
row locking, deadlock-safe lock ordering, and a script that proves the five invariants hold.

**Decisions**

| Decision | Choice | Reasoning |
|---|---|---|
| Money representation | `BIGINT` minor units (paise) | Exact by construction; no rounding mode or `BigDecimal` scale traps. Cheapest thing to lock and sum. |
| Ledger entry amounts | Signed — debits negative, credits positive | Makes I1 a plain `SUM()` rather than a `CASE` expression that could itself be written wrongly. |
| Concurrency control | Pessimistic `SELECT ... FOR UPDATE` | Account rows are genuinely hot. Optimistic retries thrash under contention; `SERIALIZABLE` needs retry handling for serialization failures. |
| Optimistic `@Version` column | Deliberately absent | It would mask a missing `FOR UPDATE` rather than expose it, and exposing that is what the concurrency tests are for. |
| Deadlock avoidance | Sort account ids, lock the lower first | Turns deadlock from *detected* into *structurally impossible*. Ordering depends only on identity, never on which side is sending. |
| Locked reads | Two single-row queries, not one `IN (...)` | Lock acquisition order for an `IN` list is not guaranteed even with `ORDER BY`, which silently reintroduces the cycle. |
| `holds` table | Deferred to M3 | A hold is meaningless until something reserves funds and later commits or releases them. Each table arrives with the commit that needs it. |
| Uniqueness of a posting | `UNIQUE (transfer_id, account_id, entry_type)` | Blocks a double-posted leg while still permitting the opposite-direction reversal that compensation will need in M3. |

**Built**

`V1__ledger_core.sql` creates `accounts` and `ledger_entries`. Every rule is a constraint rather
than an application check — non-negative customer balances, sign matching entry type, non-zero
amounts, one leg per account per transfer, one issuance account. A check-then-act sequence in
application code has a race window between the check and the act; a constraint evaluated at write
time does not.

`TransferService.transfer` is the core: validate, lock both accounts in sorted id order, re-derive
which locked account is the sender, check the balance *after* the lock is held, then write two
entries and two balance deltas in a single transaction.

Two REST endpoints, RFC 9457 problem responses, and status codes chosen to say something true
about retryability, because from M3 the saga reads them to choose between retrying and
compensating: 404 not found, 422 insufficient funds (a business "no", the compensation trigger),
400 malformed, 409 constraint violation.

`scripts/verify-invariants.sh` asserts the invariants against a running stack using the same SQL
as the test-suite assertions, so what the tests prove and what the chaos suite will prove cannot
drift apart.

**What broke**

1. **Issuance was rejected as an overdraft.** The affordability check was applied to every source
   account, the issuance account included. Since it starts at zero and is *designed* to go
   negative, opening the first funded account was impossible: `POST /accounts` with an opening
   balance returned `422 insufficient funds in 00000000-...-0001: balance=0 requested=100000`.
   The fix was to mirror the CHECK constraint in the service — the affordability rule applies to
   customer accounts only. One rule stated consistently in two places, rather than two rules
   contradicting each other.

   The more useful half of this: **fourteen tests were green when this bug shipped.** Test
   fixtures seed accounts with raw SQL, deliberately bypassing the service so the setup cannot be
   corrupted by the code under test. That is the right call, but it meant no test ever routed an
   issuance through the transfer path. Wherever a fixture takes a shortcut past production code is
   exactly where "all tests pass" stops meaning "the system works". `AccountIssuanceTest` now
   covers that path.

2. **`PostgreSQLContainer` is no longer generic.** Testcontainers 2.x moved the class to
   `org.testcontainers.postgresql` and dropped the self-type. Every existing example's
   `new PostgreSQLContainer<>("postgres:16-alpine")` fails with *"cannot use '<>' with non-generic
   class"*.

3. **`HttpStatus.UNPROCESSABLE_ENTITY` is deprecated in Spring Framework 7.** RFC 9110 renamed 422
   to "Unprocessable Content"; the constant is now `UNPROCESSABLE_CONTENT`. Same status code.
   `PAYLOAD_TOO_LARGE` → `CONTENT_TOO_LARGE` likewise.

4. **A wrong assumption, corrected by testing it.** A partial unique index was initially written
   as `ON accounts ((true)) WHERE account_type = 'SYSTEM'`, then changed on the belief that
   Postgres rejects constant index expressions. It does not — PG 16 accepts it. Indexing
   `account_type` is still clearer, but the stated reason was wrong, and it was worth ten seconds
   in psql to find that out rather than carrying a false fact forward.

**Verified**

```
./mvnw -B -ntp verify                    BUILD SUCCESS, 5 modules
                                         AccountIssuanceTest              5/5
                                         LedgerConstraintTest             7/7
                                         TransferServiceConcurrencyTest   7/7
                                         Tests run: 19, Failures: 0, Errors: 0

docker compose up -d --build             dpe-account healthy; Flyway applied V1;
                                         ddl-auto=validate accepted every entity mapping

POST /accounts alice opening=100000      201, id returned
POST /accounts bob   opening=50000       201, id returned
POST /transfers 30000 alice -> bob       200  alice=70000  bob=80000
POST /transfers  5000 bob   -> alice     200  bob=75000    alice=75000
POST /transfers 999999 alice -> bob      422  insufficient funds, balance=75000

SELECT ... FROM accounts                 alice 75000, bob 75000, system -150000
SELECT COUNT(*), SUM(amount_minor)       8 entries, sum = 0

./scripts/verify-invariants.sh           I1 PASS  I2 PASS  I3 PASS  I4 SKIP  I5 PASS   exit 0
```

The verification script was also tested against a deliberately corrupted ledger — an unbalanced
credit inserted straight into the table — and correctly reported `I1 FAIL` and `I2 FAIL` with exit
code 1 before the row was removed. A checker that has never failed is not yet a checker. I3
correctly stayed green throughout that test, because the fabricated money was in an entry rather
than a balance, which is precisely the seam between those checks.

The concurrency tests are the substance of this milestone. They run against real PostgreSQL rather
than an in-memory database, because every mechanism involved — `FOR UPDATE` blocking semantics,
deadlock detection, CHECK and UNIQUE behaviour, the READ COMMITTED default — is database-specific,
and a test that passes against H2 while production is wrong is worse than no test. Threads are
released by a latch so they genuinely contend; without it the first thread finishes before the
last is scheduled, the lock is never contested, and a completely broken implementation passes.

**Next**

M2: the transactional outbox. Business state and the outbox row written in one local transaction,
a relay claiming rows with `FOR UPDATE SKIP LOCKED`, and an inbox table for consumer idempotency.
