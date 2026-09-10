# Project Progress

An engineering log for this repository: what has been built, what decisions were made and why,
and what broke along the way. Newest entries at the bottom.

## Milestones

| # | Milestone | Status |
|---|---|---|
| M0 | Environment + multi-module skeleton | ✅ **done** |
| M1 | Ledger core — double-entry, `SELECT FOR UPDATE`, deadlock-safe lock ordering | ✅ **done** |
| M2 | Transactional outbox + Kafka publishing + inbox dedup | ✅ **done** |
| M3 | SAGA orchestration — compensation, state machine, timeout sweeper | ✅ **done** |
| M4 | Idempotency keys + retry/backoff + Dead Letter Queue | ✅ **done** |
| M5 | JWT authentication and per-account authorization | ✅ **done** |
| M6 | Observability — Prometheus metrics, Grafana dashboards, distributed tracing | ⬜ |
| M6.5 | Demo console — React UI: transfer tracker, system view, chaos controls | ⬜ |
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

So correctness rests on `PRIMARY KEY (client_id, idempotency_key)` in Postgres: insert first, and
treat a conflict as "duplicate — return the stored original response." ACID, no timing
assumptions. Redis remains as a fast-path response cache and an anti-stampede lock, but it is
**not load-bearing** — delete Redis and the system is still correct, only slower.

There is a second reason, discovered while building it, that turns out to be the stronger one. A
duplicate arriving while the first request is still in flight does not merely lose a race: Postgres
**blocks** the second `INSERT ... ON CONFLICT DO NOTHING` on the first request's uncommitted index
tuple until that transaction ends — replaying the stored response if it committed, and taking over
the work if it rolled back. That is mutual exclusion between concurrent duplicates, with correct
handoff on failure, out of an index. It is precisely what a distributed lock is usually reached
for, without the lease, the TTL or the clock assumption. Full argument in
`docs/adr/0002-idempotency.md`.

### A demo console, and why it comes after observability

The system is asynchronous and its most interesting behaviour is invisible from an HTTP response:
money sitting in a `CLEARING` hold, a command travelling from one service's outbox to another
service's inbox, a saga being compensated by a timeout sweeper nobody called. All of it is already
recorded — `saga_steps` deliberately writes two rows per step, one when the command is issued and
one when its reply lands, so the gap between them is the step latency — but reading it means
reading three databases.

M6.5 renders it: a transfer tracker showing the stage timeline and the compensation branch, the
balances at each stage, and the message trail by message id; a system view with outbox backlog,
in-flight sagas, DLQ depth and the five invariants as five lights; and controls onto the gateway's
existing simulation endpoint, so a decline can be forced and the compensation watched.

It is sequenced after M6 rather than earlier for two reasons. Tracing has to exist first, or the
per-transfer view cannot link out to the trace that explains it. And it has to exist before M7, or
the chaos scenarios are verified by reading logs when they could be watched.

The nginx container in front of it reverse-proxies all three services under one origin rather than
having the browser call three ports with CORS enabled on each. That is one place to configure
instead of three, and it is closer to how an edge actually sits in front of services.

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

---

### M2 — Transactional outbox and idempotent consumer · 2026-09-07

**Goal**

Get an event out of account-service and into payment-orchestrator without ever being in a state
where one of them is wrong. Specifically: no message describing a transfer that did not commit, no
transfer that commits without its message, and no message applied twice on the far side.

**Decisions**

| Decision | Choice | Reasoning |
|---|---|---|
| Publishing a message | An `INSERT` into `outbox`, in the business transaction | A database write and a broker publish cannot be made atomic. Making the message a row removes the second system entirely: one write, one commit. |
| Message identity | The outbox row's own primary key, generated in Java before the insert | It must be stable across redelivery. A Kafka offset changes when the relay republishes after crashing between the send and the `published_at` update — which is exactly the case dedup exists to absorb, so dedup would never fire. |
| Relay claim query | `FOR UPDATE SKIP LOCKED`, native SQL | `SKIP LOCKED` is what turns a table into a work queue: N relays each get a disjoint batch immediately, with no coordination. JPQL cannot express it, and Hibernate's dialect hint degrades silently to plain `FOR UPDATE` where unsupported. |
| Publish/mark ordering | Send, wait for the broker ack, *then* mark published | Mark-then-publish can lose a message outright; publish-then-mark can duplicate one. Duplicates are absorbed by the consumer's inbox. Losses are absorbed by nobody. |
| Kafka sends inside the claim transaction | Accepted, with a small batch and a bounded send timeout | The claim's locks are what stop a second relay publishing the same rows. Nothing else ever touches these rows, so holding them blocks no user-facing work; the cost is a long-running transaction, which is bounded rather than eliminated. |
| A failed send in a batch | Record the failure, skip that aggregate's remaining messages, keep going | Aborting the batch would let one poison message stall every unrelated transfer behind it. Continuing blindly would let a later message of the same aggregate overtake the failed one — losing the per-aggregate ordering the partition key was chosen to provide. |
| Consumer dedup | `INSERT ... ON CONFLICT DO NOTHING`, in the same transaction as the business write | A `SELECT`-then-`INSERT` has a race window; a primary key does not. `save()` plus a caught `DataIntegrityViolationException` does not work at all — the violation marks the transaction rollback-only, so the business write that was about to be skipped to would fail too. |
| Read-model upsert | Deliberately **not** idempotent — a repeat increments `apply_count` | If it were idempotent, a completely broken dedup gate would still produce a correct-looking table and the test would be asserting the primary key's behaviour rather than the inbox's. Production would prefer defence in depth; this trades that for provability. |
| Offset commit | Manual, after the handler's transaction commits | The default auto-commit runs on a five-second timer with no knowledge of whether the work succeeded. A crash after an auto-commit and before the write is durable loses the message permanently. |
| Broker | Redpanda by default, Apache Kafka behind a `--profile kafka` | Same wire protocol, no JVM, starts in about a second on a 16 GB machine. "Passes on Redpanda" is evidence, not proof, so the real thing stays one flag away. |

**Built**

`V2__outbox.sql` adds the `outbox` table to `accounts_db`: message identity, routing (`topic`,
`event_type`), the aggregate id that becomes the Kafka partition key, a `jsonb` payload serialized
at write time, and `published_at` / `attempts` / `last_error` for the relay. The index on
unpublished rows is **partial** — `WHERE published_at IS NULL` — so it grows with the backlog
rather than with the archive, and the relay's claim query stays flat no matter how many million
messages have already been sent.

`V1__inbox_and_read_model.sql` adds `inbox` and `transfer_projection` to `payments_db`.

On the producing side, `TransferService` appends the event through `OutboxWriter`, which has no
`@Transactional` of its own precisely so that it joins the caller's transaction. `OutboxRelay`
claims a batch, publishes each record keyed by aggregate id with the message id and event type in
headers, waits for the acknowledgement, and only then marks the row. Its `@Scheduled` trigger
lives in a separate bean: had both annotations been on one class, the scheduled method calling
the transactional one internally would bypass Spring's proxy and run with no transaction at all —
the claim's locks would be released the instant the query returned, and two relays would publish
the same rows.

On the consuming side, `AccountEventConsumer` decodes, routes and acknowledges — nothing else —
and `AccountEventHandler` writes the inbox row and the projection row in one transaction.

Serialization deserves a note. The payload is serialized inside the business transaction rather
than at publish time. A payload that cannot be serialized then fails while the transaction can
still roll back, instead of being discovered after the money has already moved and the message is
undeliverable.

**What broke**

1. **A consumer can create the topic it is about to read — with the wrong shape.** The first live
   run published four events and the orchestrator applied exactly one. Nothing errored. The cause
   was a startup race: the orchestrator's consumer subscribed before account-service declared the
   topic, and Redpanda auto-created it with the broker default of **one partition**. The consumer
   was assigned that one partition; account-service's `NewTopic` then grew the topic to three
   behind its back. The producer keyed messages across all three exactly as designed, and roughly
   two thirds of the traffic was never delivered — no exception, no lag alert on the partition
   being read, nothing above DEBUG in the log. It looks identical to a system with less traffic
   than it has, and it heals itself silently five minutes later when `metadata.max.age.ms` forces
   a refresh and a rebalance.

   `rpk group describe` is what made it visible: partitions 1 and 2 had lag and an **empty
   member-id** — assigned to nobody.

   Fixed in three places, because one is not enough. The partition count is now a constant in
   `Topics` that both services declare, so whichever boots first creates the topic correctly and
   the two cannot drift. `allow.auto.create.topics=false` stops the consumer requesting creation.
   And the broker runs with `auto_create_topics_enabled=false`, so a topic nobody declared is a
   visible error rather than a silent wrong default.

2. **`jsonb` does not store the bytes you give it.** Two tests asserted on the text of the stored
   payload and failed against perfectly correct rows: Postgres parses `jsonb` into a binary tree
   and renders it back with its own key order and its own `": "` spacing, so `payload::text` is
   Postgres's serialization of the document, never Jackson's. Any test that string-matches a
   `jsonb` column is asserting on the database's serializer. The assertions now read fields
   through `->>`.

3. **A compression codec can wedge a partition below the reach of every error handler.** Replaying
   a message by hand with `rpk topic produce` — which compresses with **snappy by default** —
   permanently stalled a partition. `snappy-java` unpacks a bundled native library and dlopens it;
   the service image is `eclipse-temurin:21-jre-alpine`, which is musl, and the `.so` is
   glibc-linked:

   ```
   UnsatisfiedLinkError: libsnappyjava.so:
       Error loading shared library ld-linux-x86-64.so.2: No such file or directory
   ```

   What makes it worth recording is *where* it fails. Decompression happens inside
   `consumer.poll()`, below the listener, so it surfaces as a `KafkaException` carrying no record —
   and `DefaultErrorHandler` refuses it outright: *"This error handler cannot process
   'KafkaException's; no record information is available."* No error handler can intercept it, and
   neither will the dead letter topic in M4. The consumer spins on that record forever; recovery
   was a manual `rpk group seek` past the offset with the service stopped.

   Every Kafka codec except gzip goes through JNI, so `compression.type` is now pinned to `none`
   explicitly, with the reason written beside it. Turning compression on for throughput requires a
   glibc base image first.

4. **Redpanda will not start when its `--memory` equals its container limit.** `--memory=512M`
   under a 512M cap fails with `insufficient physical memory: needed 536870912 available
   500000000`. Seastar allocates the full amount up front, and the runtime's own overhead comes
   out of the same cgroup. Now 384M under a 512M cap.

5. **`./mvnw -pl account-service test` stopped working.** account-service depends on
   `common-events` as of this milestone, and `-pl` alone does not build it:
   `Could not find artifact com.dpe:common-events:jar:0.0.1-SNAPSHOT`. It needs `-am`.

**Verified**

```
./mvnw -B -ntp verify                    BUILD SUCCESS, 5 modules
                                         AccountIssuanceTest              5/5
                                         LedgerConstraintTest             7/7
                                         OutboxWriteTest                  5/5
                                         OutboxRelayTest                  6/6
                                         TransferServiceConcurrencyTest   7/7
                                         InboxDedupTest                   4/4
                                         AccountEventConsumerTest         3/3
                                         Tests run: 37, Failures: 0, Errors: 0
```

End to end on the Compose stack — five containers healthy, no test doubles:

```
POST /accounts x5, POST /transfers x5, one 422 overdraft

outbox      10 rows | 10 published | 0 backlog | 0 attempts | worst relay lag 834ms
inbox       10 rows
projection  10 rows | all apply_count = 1
rejected transfer (422)                     left NO outbox row
```

Duplicate delivery was tested against the running system rather than only in a unit test: an
already-consumed record was republished to the topic carrying its original message id.

```
rpk topic produce ... -H dpe-message-id:4d32d3dd-... --compression none
  -> Produced to partition 2 at offset 5

orchestrator log:  message 4d32d3dd-... (FundsTransferred) skipped as duplicate
transfer_projection.apply_count:  still 1
```

The recovery from failure (1) is itself evidence for the delivery guarantee: the three messages
stranded on unassigned partitions were **not lost**. Restarting the consumer rebalanced it onto
all three partitions and it drained the backlog immediately — total lag 0.

Invariants, baselined before the run and checked after three transfers:

```
./scripts/verify-invariants.sh baseline   I3 baseline recorded: 400001
./scripts/verify-invariants.sh            I1 PASS  I2 PASS  I3 PASS  I4 SKIP  I5 PASS   exit 0
```

**Terminology**

This system provides **at-least-once delivery with idempotent consumers, giving effectively-once
processing**. Not exactly-once delivery, which is impossible here: the relay cannot make "the
broker acknowledged" and "the row is marked published" atomic, so a crash between them
republishes. The whole design is choosing *which* failure to have, and then absorbing it on the
other side.

**Next**

M3: the saga. Orchestration across all three services, compensation on decline, and a timeout
sweeper for sagas that stall — plus invariant I4, which has been skipped since M1 because
`saga_instances` does not exist yet.

---

### M3 (part 1) — Saga foundations · 2026-09-07

**Goal**

Everything the saga needs to exist: the schema it runs on, the message contracts it speaks, the
transport that carries them, and the simulated payment provider that gives compensation a real
cause. The state machine itself is deliberately the next commit — this one ends with its
specification written as failing tests.

**Decisions**

| Decision | Choice | Reasoning |
|---|---|---|
| Where money in flight lives | A third account type, `CLEARING` | Both obvious designs break an invariant. See "the reserve problem" below — the most consequential decision in the milestone. |
| Outbox/inbox code across three services | Extracted to a new `common-messaging` module | All three become producers and consumers at M3. Sharing infrastructure that carries no business meaning; each service still owns its own tables. ADR 0004. |
| Outbox/inbox *tables* | Duplicated per database, deliberately | A shared table would be a shared database — the coupling database-per-service exists to prevent. |
| Saga state: stored or derived | A `status` column, with `saga_steps` as the full history | The timeout sweeper needs an indexed query for "non-terminal and past deadline". A state folded from an event log cannot be indexed, so every sweep would scan every saga ever run. |
| Rejected reserve vs. declined gateway | Different terminal paths | A rejection moved no money, so there is nothing to compensate. Routing it through compensation would emit a release naming a hold that does not exist. |
| Saga deadline | One deadline for the whole saga, set at creation | A per-step deadline is more precise but must be reset on every transition, and a missed reset produces a saga that can never time out — the exact failure the deadline exists to prevent. One deadline fails safe. |
| Consumer groups per service | Exactly one, so exactly one inbound listener | Forced by the inbox's key. See "what broke" (1). |
| `POST /transfers` response | `202 Accepted`, not `201 Created` | The money has not moved when it returns. 201 would claim the transfer is done; 202 says it has been accepted and the outcome comes later. That is the saga in one status code. |

**The reserve problem, and why there is a CLEARING account**

Reserve-then-commit has to satisfy four invariants at once, and the two obvious implementations
each break one:

- *"A hold is just a row; leave the balance alone."* Breaks **I1**'s companion **I3** —
  `SUM(customer balances) + SUM(active holds)` climbs by the reserved amount on every reserve,
  because the balance never moved. It also pushes the overdraft rule back into application code
  where I5's CHECK constraint cannot see it.
- *"Debit the sender now; the hold records it."* Breaks **I1** — a debit with no matching credit
  means the ledger no longer sums to zero.

Both fail for the same reason: in double-entry there is no such place as "in transit" unless you
make one. So a reserve debits the sender and credits a `CLEARING` account, which is what real
payment rails do:

```
reserve   DEBIT  sender   -X    CREDIT clearing  +X    hold ACTIVE
commit    DEBIT  clearing -X    CREDIT recipient +X    hold COMMITTED
release   DEBIT  clearing -X    CREDIT sender    +X    hold RELEASED
```

Every invariant then holds with no special-casing. `CLEARING` is not a `CUSTOMER` account, so its
balance sits outside I3's sum and the hold accounts for that money instead.

The property this produced for free is the best structural guarantee in the milestone. Commit and
release write the **identical** ledger leg — `(transfer_id, clearing, DEBIT)` — and that triple is
covered by a UNIQUE constraint. So for any one transfer the database permits a commit or a release
and never both, and never either one twice. A saga that tried to complete and compensate the same
transfer — through a bug, a redelivered command, or a timeout sweeper racing a late approval —
cannot double-spend. **The mutual exclusion is a property of the schema, not a promise made by the
orchestrator.**

**Built**

- `common-messaging`: the outbox entity, repository, writer, relay and scheduler, plus the inbox
  entity and repository, moved out of the two services that had them. It holds no domain type; the
  payload is an opaque string in both directions.
- Migrations: `V3__holds_and_inbox.sql` (accounts_db — `holds`, `inbox`, the CLEARING account, the
  widened account-type constraint), `V2__saga.sql` (payments_db — `transfers`, `saga_instances`,
  `saga_steps`, `outbox`), `V1__gateway_charges.sql` (gateway_db).
- Ten new contracts in `common-events`: `ReserveFunds` / `FundsReserved` / `ReserveRejected`,
  `ChargeGateway` / `GatewayApproved` / `GatewayDeclined`, `CommitFunds` / `FundsCommitted`,
  `ReleaseFunds` / `FundsReleased`.
- `payment-gateway` became a real service: charge domain, idempotent `ChargeService`, command
  consumer, and runtime-tunable fault injection (`POST /admin/simulation`) for decline rate,
  latency, timeout rate and duplicate callbacks. Its `transfer_id` is UNIQUE — an external charge
  is the one step in this system that cannot be undone by writing an opposite row, so the
  protection has to be structural rather than a promise made by the caller.
- Orchestrator: `Transfer` and saga entities, repositories including the sweeper's
  `FOR UPDATE SKIP LOCKED` claim, `POST /api/v1/transfers`, and the single inbound listener.
- account-service: `Hold`, `HoldRepository`, the command consumer and its transactional gate.

Deliberately left unimplemented, with their tests written: `SagaOrchestrator` (the state machine),
`SagaTimeoutSweeper.sweep`, and `ReservationService` (reserve, commit, release).

**What broke**

1. **Two consumer groups over one topic would have silently swallowed every saga reply.** The saga
   reply consumer was first written as a second `@KafkaListener` alongside M2's projection
   consumer, with its own group id. Both subscribe to `dpe.account.events.v1`, and two consumer
   groups each receive *every* message — while the inbox's primary key is the message id **alone**.

   So whichever group committed first would insert the inbox row, and the second would find the
   message already recorded and skip it as a duplicate. Correctly, by its own rules. Every saga
   would have stalled in `STARTED` and the sweeper would have compensated perfectly healthy
   transfers — with nothing logging an error, because from each component's point of view nothing
   went wrong.

   Caught before it ran, by a column comment written at M2 warning about exactly this case. Fixed
   by collapsing to one listener that routes by event type, so every message passes through
   exactly one gate. The general rule is worth more than the fix: **a dedup key must be unique
   across everything that shares the table.** A key that is unique per message but not per consumer
   works right up until someone adds a second consumer, and then it fails by being too effective
   rather than by erroring.

2. **`@EntityScan` moved package in Spring Boot 4.** No longer
   `org.springframework.boot.autoconfigure.domain.EntityScan` — Boot 4 split `autoconfigure` into
   per-technology modules and it now ships in `spring-boot-persistence` as
   `org.springframework.boot.persistence.autoconfigure.EntityScan`. `@EnableJpaRepositories` did
   not move, being Spring Data rather than Boot. Symptom: *"package
   org.springframework.boot.autoconfigure.domain does not exist"*.

3. **`@EntityScan` and `@EnableJpaRepositories` replace the default scan rather than extending
   it.** Declaring either one makes Boot back off its own scanning entirely. So `common-messaging`
   cannot declare them on a service's behalf — naming only the library would silently unregister
   the service's own repositories, and the library would work while the application broke. Each
   application class names both packages instead, where a reader looks first.

4. **The outbox `payload` column holds the envelope, not the event.** Three test helpers reached
   for `payload->>'transferId'`, which is always NULL — the business fields are one level down, at
   `payload->'payload'->>'transferId'`. The failure mode is nasty inside a test: a `WHERE` clause
   on it matches nothing, so an assertion that "no such row exists" passes for entirely the wrong
   reason. Only one of the four surfaced as a real failure; the rest were found by grep afterwards.
   Related to M2's lesson about `jsonb` and worth stating alongside it: reading a `jsonb` column
   requires knowing the shape you actually stored, not the shape you had in mind.

**Verified**

```
./mvnw -B -ntp verify        BUILD SUCCESS, 6 modules
                             43 passing, 18 failing (the unwritten specification)

passing  AccountIssuanceTest            5/5    LedgerConstraintTest    7/7
         OutboxWriteTest                5/5    OutboxRelayTest         6/6
         TransferServiceConcurrencyTest 7/7    InboxDedupTest          4/4
         AccountEventConsumerTest       3/3    ChargeServiceTest       6/6

failing  ReservationServiceTest         6      every one an
         SagaFlowTest                   6      UnsupportedOperationException
         SagaTimeoutTest                6
```

Every one of M2's 37 tests still passes through the `common-messaging` extraction and the three
new migrations, which is the check that mattered most — the refactor touched every service's
Spring wiring and both existing databases.

The 18 failing tests are the specification for the state machine, the sweeper and the reservation
logic, written before the implementations they describe. They fail with
`UnsupportedOperationException` rather than assertion errors, so the build distinguishes "not
written yet" from "written wrong" — which is worth the small effort of stubbing the methods rather
than leaving them absent.

**Next**

Implement the three files the tests specify: `ReservationService` (reserve / commit / release with
sorted lock ordering), `SagaOrchestrator` (the state machine and its two failure branches), and
`SagaTimeoutSweeper.sweep`. Then the end-to-end run on the Compose stack — a happy-path transfer, a
forced decline through the gateway's simulation endpoint, and a stalled participant to exercise the
sweeper — each followed by `verify-invariants.sh`, which can finally check I4 now that
`saga_instances` exists.

---

### M3 (part 2) — The saga, working end to end · 2026-09-07

**Goal**

Implement the three pieces part 1 left specified but unwritten — the reservation logic, the saga
state machine, and the timeout sweeper — and prove all four saga paths on the running stack rather
than only in tests.

**Decisions**

| Decision | Choice | Reasoning |
|---|---|---|
| Transaction propagation on the saga and reservation services | `@Transactional` with the default `REQUIRED` | The handler opens the transaction and writes the inbox row; these join it. `REQUIRES_NEW` would commit the ledger independently of the inbox row that makes it idempotent. `REQUIRED` also makes both callable directly from a test, which is why the specification tests can drive the state machine without a broker. |
| A reply that does not match the saga's current state | Record a `SKIPPED` step and return | A late approval arriving after the sweeper compensated is normal operation. Throwing would roll back the inbox row and redeliver forever. |
| A `CommitFunds` for a hold that is already `RELEASED` | Log at ERROR and return, do not throw | Same reasoning. The money was never at risk — the UNIQUE constraint on `(transfer_id, clearing, DEBIT)` already made the double settlement impossible — so the only thing left to choose is whether the anomaly is loud or infinite. |
| Sweeper failure handling | Catch per saga, continue the batch | One saga that cannot be compensated must not stop every other stranded saga from being rescued. Same principle as the relay blocking only the aggregate that failed. |
| `onTimeout` transaction boundary | None of its own; runs inside the sweeper's batch transaction | The claim's locks are the only mutual exclusion between sweeper instances and must be held to the end of the batch. |

**Built**

- `ReservationService.reserve/commit/release` — validate, lock the sender and CLEARING in sorted id
  order, post both ledger legs, move the hold, write the reply to the outbox. All in the caller's
  transaction.
- `SagaOrchestrator` — `start` plus six reply handlers plus `onTimeout`, each loading the saga
  `FOR UPDATE`, guarding the expected state, and recording a `saga_steps` row.
- `SagaTimeoutSweeper.sweep` — claims expired sagas `FOR UPDATE SKIP LOCKED` and drives each one.

**What broke**

1. **The new module was invisible to Docker.** `./mvnw verify` was green while
   `docker compose build` failed with a bare `dependency:go-offline` exit code 1. The Dockerfile
   copies each module's POM and `src` **by name**, so `common-messaging` existed in the reactor and
   not in the image build. Worth internalising: a green local build is not evidence that the
   container build works, and the error message names nothing useful.

2. **Compose merges `ports` across `-f` files rather than replacing them.** Another project's kind
   cluster held host port 19092. An override file with `ports: []` changed nothing — sequence
   fields are merged by default and `!override` is required to replace one. The host binding is
   only there for IDE and host-side `rpk` access; services reach the broker at `redpanda:9092` on
   the internal network, so dropping it for the verification run cost nothing.

3. **Services raced the network on a partially failed `up`.** After the port failure above, the
   three service containers had been created but the broker had not started, and starting them
   afterwards produced `No resolvable bootstrap urls given in bootstrap.servers` — a DNS failure
   that reads like a configuration error. `--force-recreate` after the broker was healthy resolved
   it. `depends_on: service_healthy` protects a clean `up`, not a resumed one.

**Verified**

Unit and integration suite, real Postgres and Redpanda via Testcontainers:

```
./mvnw -B -ntp verify        BUILD SUCCESS, 6 modules, 61 tests, 0 failures

AccountIssuanceTest 5   LedgerConstraintTest 7   OutboxWriteTest 5
OutboxRelayTest 6       ReservationServiceTest 6 TransferServiceConcurrencyTest 7
AccountEventConsumerTest 3   InboxDedupTest 4    SagaFlowTest 6   SagaTimeoutTest 6
ChargeServiceTest 6
```

Then all four saga paths on the Compose stack — five containers, no test doubles. Alice opened with
₹1,000.00, Bob with zero; the I3 baseline was 100000 minor units throughout.

**1. Happy path** — ₹300.00 Alice → Bob:

```
POST /api/v1/transfers            HTTP 202, status PENDING, sagaStatus STARTED
after 10s                         status COMPLETED, sagaStatus COMPLETED
saga steps    ReserveFunds STARTED/SUCCEEDED, ChargeGateway STARTED/SUCCEEDED,
              CommitFunds STARTED/SUCCEEDED
hold          COMMITTED 30000
ledger        alice DEBIT -30000 | clearing CREDIT +30000
              clearing DEBIT -30000 | bob CREDIT +30000
balances      alice 70000, bob 30000, clearing 0
```

**2. Gateway decline** — `POST /admin/simulation {"failureRate":1.0}`, then ₹250.00:

```
transfer      status FAILED, failureReason GATEWAY_DECLINED, sagaStatus COMPENSATED
gateway       DECLINED, reason card_expired
hold          RELEASED 25000
ledger        alice DEBIT -25000 | clearing CREDIT +25000
              clearing DEBIT -25000 | alice CREDIT +25000     <- four entries, not zero
balances      alice back to 70000, clearing 0
```

The four ledger entries are the point. **Compensation is not rollback**: the reserve's debit is
still there, and the reversal sits beside it. Alice's statement shows the money leaving and coming
back, which is what actually happened.

**3. Timeout** — `{"timeoutRate":1.0}`, so the PSP never answers at all, then ₹150.00:

```
t+10s   saga RESERVED, hold ACTIVE 15000, alice 55000, clearing 15000
        I3 PASS  total money conserved (100000)
        I4 FAIL  1 saga(s) stuck in a non-terminal state - money may be stranded
```

That pair of results is exactly why both invariants exist. No money was lost — I3 balances,
because the hold accounts for it — and it is nonetheless stranded, which only I4 can see.

```
t+55s   orchestrator log: "saga sweeper compensated 1 stalled saga(s)"
        transfer FAILED / SAGA_TIMEOUT, saga COMPENSATED, sweep_attempts 1
        saga steps  ChargeGateway TIMED_OUT -> COMPENSATING, ReleaseFunds STARTED/SUCCEEDED
        hold RELEASED, alice back to 70000, clearing 0
        I1 I2 I3 I4 I5 all PASS
```

**4. Rejected reserve** — ask for ₹9,999.99 against a ₹700.00 balance:

```
transfer      status FAILED, failureReason INSUFFICIENT_FUNDS, sagaStatus FAILED
saga          hold_id NULL, went to FAILED directly, never COMPENSATING
ledger        0 entries      holds 0 rows      ReleaseFunds emitted: 0
saga steps    ReserveFunds STARTED, ReserveFunds FAILED -> FAILED
```

No money moved, so nothing was compensated — and no `ReleaseFunds` naming a hold that does not
exist.

Messaging health after the run, with the counts reconciling exactly across three databases:

```
payments_db  outbox=10 published=10 backlog=0 attempts=0 inbox=10
accounts_db  outbox=8  published=8  backlog=0 attempts=0 inbox=7
gateway_db   outbox=2  published=2  backlog=0 attempts=0 inbox=2

sagas   COMPLETED 1, COMPENSATED 2, FAILED 1
```

`gateway inbox=2` against three `ChargeGateway` commands is correct rather than a gap: the timed-out
charge rolled its inbox row back with the transaction, exhausted the container's retry budget, and
was never recorded — which is precisely why the sweeper had to be the thing that resolved it.

Final invariant check:

```
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (100000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
All invariants hold.
```

I4 has been skipped since M1 because `saga_instances` did not exist. This is the first run in which
all five are actually checked.

**Next**

M4: idempotency keys at the API edge (a client retry of `POST /transfers` currently starts a second
transfer), retry with backoff, and the dead letter queue — including the honest note from M2 that
not every poison message is reachable by a DLQ.

Debt carried forward, deliberately recorded rather than hidden: when a saga times out in `STARTED`,
the sweeper marks it `FAILED` without releasing anything, because there may be no hold. If the
reserve did in fact succeed and only its reply was lost, that leaves an `ACTIVE` hold with no live
saga — I3 still balances and the money is accounted for, but it is stranded and no timeout will
find it. The fix is a reconciliation job that scans for holds with no corresponding saga; it is out
of scope here and is called out in `SagaOrchestrator.onTimeout`.

---

### M4 (part 1) — The idempotency gate, specified · 2026-09-08

**Goal**

Everything the API-edge idempotency gate needs to exist: the table, the statements it is built
from, the Redis client, the HTTP contract, and the specification written as failing tests. The
gate itself and the Redis cache are the next commit — the same split M3 used, and for the same
reason: the specification is easier to argue about before there is an implementation to defend.

**Decisions**

| Decision | Choice | Reasoning |
|---|---|---|
| Where the guarantee lives | `PRIMARY KEY (client_id, idempotency_key)` on `idempotency_records` | ACID, no clock assumptions. The pair is the identity of the row, not a UNIQUE index beside a surrogate id — a surrogate lets a second row for the same pair be inserted by any code path that forgets to check. ADR 0002. |
| Redis's role | Response cache and `SET NX PX` anti-stampede lock, both optional | Placed where its failure mode is harmless. A lease measured in time cannot protect a resource that does not check the lease. |
| Claim and work | One transaction — the claim, the transfer, the saga, the outbox row and the stored response all commit together | Either split leaves a broken state: a claimed key pointing at a transfer that does not exist (and every retry refused forever), or a transfer whose key was never recorded (and the retry makes a second one). |
| The transaction boundary | An injected `TransactionTemplate` rather than a second `@Transactional` method | Self-invocation goes down the `this` reference and never reaches the proxy, so the transaction silently does not exist. It passes every single-threaded test. |
| An `IN_PROGRESS` state column | Not included | The work is a local commit, so the insert and the completion share a transaction and any committed row already has its response. A state column that is always in the same state is machinery pretending to be a design. The migration records the case that would need one: work that happens outside the transaction, such as a synchronous call to an external provider. |
| The replayed body | The stored JSON, verbatim | Re-rendering from current state would answer a retry `COMPLETED` where the original answered `PENDING` — two different answers to what the client believes is one request, which is the ambiguity idempotency exists to remove. |
| A key reused with a different body | `409`, never a replay | Replaying answers the second request with the first request's receipt: a 5,000 transfer confirmed with the record of a 300 one, silently, and in the direction the caller is happy to believe. |
| A request that fails validation | Rolls back the claim; the key remains usable | Caching failures (as some payment APIs do) requires claiming the key in a separate transaction that survives the rollback, plus a decision about what to answer a duplicate arriving mid-flight. The smaller mechanism was chosen deliberately rather than by omission. |
| `Idempotency-Key` header | Required; `400` when absent | This endpoint moves money. A caller without a key has no safe way to retry, and accepting the request anyway lets it assume a guarantee it never asked for. |
| `X-Client-Id` header | Required; replaced by the JWT subject in M5 | Keys live in per-client namespaces. A shared namespace makes a common key like `"1"` collide, and a collision does not fail — it hands one client another client's response body. |
| Fingerprint input | The parsed fields, separator-joined, SHA-256 | Hashing raw bytes turns key order, whitespace and `30000` vs `30000.0` into false conflicts. Omitting the separator lets account `...ab` sending `1` and account `...a` sending `b1` hash identically. |
| Redis in Compose | `depends_on: service_started`, no volume, `maxmemory 64mb`, `allkeys-lru` | `service_healthy` would make the cache a hard startup dependency in the easiest place to introduce one by accident. A persisted cache can disagree with the database after a restore; empty on boot is the correct state. The default `noeviction` policy makes a full Redis refuse writes, which this design survives but silently. |

**Built**

- `V3__idempotency.sql` — `idempotency_records`, with the reasoning for each column in the file.
- `IdempotencyRecord` (read-only entity, composite `@IdClass`) and `IdempotencyRepository`:
  `claim` (`INSERT ... ON CONFLICT DO NOTHING`), `complete`, and a bounded `deleteExpired`.
- `IdempotencyProperties` and `IdempotencyConfig` — retention, cache TTL, lock TTL, lock wait, and
  the `TransactionTemplate` that makes the transaction boundary visible.
- `RequestFingerprint`, `CachedResponse`, `IdempotentOutcome`, `IdempotencyConflictException`.
- `TransferController` now requires both headers and returns the stored response body as raw JSON
  with an `Idempotency-Replayed` header; `ApiExceptionHandler` maps the 409 and the missing-header
  400 into the existing error shape.
- Redis: `spring-boot-starter-data-redis` with 200ms client timeouts, and `redis:7-alpine` in
  Compose.
- 16 new tests across `IdempotencyGateTest`, `IdempotencyCacheTest` and
  `IdempotencyWithoutRedisTest`, plus `AbstractRedisIT`.
- `docs/adr/0002-idempotency.md`.

`IdempotencyGate.execute` and the four methods of `IdempotencyCache` are stubs that throw. The
tests that specify them fail with `UnsupportedOperationException`, so the build distinguishes "not
written yet" from "written wrong".

**The test that justifies the architecture**

`IdempotencyWithoutRedisTest` enables the cache and points it at a port with nothing behind it, so
every `lookup`, `store`, `acquireLock` and `releaseLock` fails on the request path under
concurrency. It then asserts that 100 concurrent identical requests still produce exactly one
transfer and exactly one `ReserveFunds` command.

The failure it exists to catch is a common one: a cache added in front of a database with its
exceptions left to propagate does not degrade, it **amplifies** — the system now fails whenever
*either* component is down, so a component added to make things faster has made them less
available. "Redis is not load-bearing" is a claim that should be executable, and this is what makes
it so.

**What broke**

1. **An `@IdClass` cannot be a record.** The persistence provider instantiates the id class through
   a public no-argument constructor, and a record has only its canonical one. It fails at
   bootstrap, not at the first query.

2. **There is no `org.testcontainers:testcontainers-redis` module.** Redis was never an official
   Testcontainers module; the widely-used artifact is third-party. It is not needed — Boot's
   `RedisContainerConnectionDetailsFactory` accepts any container whose *image name* is `redis`, so
   `@ServiceConnection` on a plain `GenericContainer` wires `spring.data.redis.*` correctly. Note
   the counterpart to M1's trap: `GenericContainer<SELF>` **is** still generic in Testcontainers
   2.x, unlike `PostgreSQLContainer`, so the diamond compiles for one and not the other.

3. **`@SpringBootTest` properties do not merge down a class hierarchy.** Spring finds the first
   `@SpringBootTest` in the hierarchy and uses it alone, so a subclass that re-declares it must
   repeat the parent's properties in full. Omitting one silently restores a production default —
   here, a background outbox relay racing the assertions.

4. **A primitive `boolean` in a `@ConfigurationProperties` record binds to `false` when the key is
   absent**, which would have shipped the fast path silently disabled anywhere the key was not set.
   Boxed to `Boolean` so "absent" and "false" are distinguishable, then defaulted to true.

5. **Spring Data Redis attempted to claim the JPA repositories.** Adding the starter enables Redis
   repository scanning, which inspects every repository in the service and logs five "could not
   safely identify store assignment" lines on each boot. Disabled via
   `spring.data.redis.repositories.enabled: false` rather than tolerated: multi-store scanning
   resolves ambiguity by guessing, and a wrong guess would be silent.

6. **`TRUNCATE transfers` began failing on a foreign key** once `idempotency_records` referenced
   it. Added to the same single `TRUNCATE` statement as the saga tables — with foreign keys, no
   ordering of separate truncates works.

**Verified**

```
./mvnw -pl payment-orchestrator -am test -Dtest=InboxDedupTest -Dsurefire.failIfNoSpecifiedTests=false
  Tests run: 4, Failures: 0, Errors: 0, Skipped: 0     BUILD SUCCESS
```

The check that mattered: Flyway applied V3 and Hibernate's `ddl-auto: validate` accepted the new
entity against it, `jsonb` column and composite key included.

```
./mvnw -pl payment-orchestrator -am test -Dtest=IdempotencyCacheTest -Dsurefire.failIfNoSpecifiedTests=false
  Tests run: 6, Failures: 0, Errors: 6
  java.lang.UnsupportedOperationException: M4: SET NX PX with a fresh token, ...
```

Six errors, every one from an unimplemented method and none from the context — so the Redis
container starts, the service connection wires it, and the application boots with the cache
enabled. The only thing missing is the implementation.

**Open / next**

- Implement `IdempotencyGate.execute` and `IdempotencyCache`; done when all 16 new tests pass,
  including both 100-thread cases.
- M4 part 2: retry with exponential backoff and jitter, `DefaultErrorHandler` with
  `DeadLetterPublishingRecoverer`, a DLQ replay endpoint, and the expiry sweep for
  `idempotency_records`. M2's caveat still stands — a decompression failure happens inside
  `poll()`, below every error handler, and no dead-letter topic can reach it.
- Carried from M3: a reconciliation job for holds left with no live saga.
- `transfer_projection` is now redundant with `transfers`, but its `apply_count` column is the only
  thing in the schema that can distinguish "the inbox worked" from "a primary key happened to save
  us". Whether that is worth keeping a redundant table for is a decision for part 2.

---

### M4 (part 1b) — The gate and the cache, implemented · 2026-09-09

**Goal**

Turn the 16 failing tests from part 1b's specification into 16 passing ones: write
`IdempotencyGate.execute` and the four methods of `IdempotencyCache`, and keep the property the
specification exists to defend — that removing Redis leaves the system correct.

**Decisions**

| Decision | Choice | Reasoning |
|---|---|---|
| What `acquireLock` returns | A sealed `LockOutcome` — `Acquired(token)`, `HeldByAnother`, `Unavailable` — not `Optional<String>` | Three outcomes, and the caller must branch differently on each. See "What broke" #1: collapsing two of them into `Optional.empty()` compiles, passes every test, and adds a fixed latency penalty to every request whenever the cache is off or Redis is down. |
| Release under Lua, not `DEL` | `GET`-compare-`DEL` in one script | A holder that stalls past the lease wakes up and deletes whichever lock is now there — its successor's. Redis is single-threaded, so a script runs with nothing interleaved, which is what makes compare-and-delete atomic. |
| Where the fingerprint is checked | On both replay paths — Redis and Postgres | Two routes to one decision is two places to forget the check, and the fast one is the one that runs under load. |
| Cache written after commit, lock released after that | Ordering, not preference | Storing before the commit lets a rollback leave Redis asserting a transfer that does not exist, and the fast path then serves that assertion to every retry — a lie Postgres can no longer correct, because nothing reaches Postgres any more. Releasing last means the loser wakes to a populated cache. |

**Built**

- `IdempotencyGate.execute` — fingerprint, cache fast path, advisory lock, one transaction holding
  the claim and the work, then store-and-release.
- `IdempotencyCache` — `lookup`, `store`, `acquireLock`, `releaseLock`, plus the sealed
  `LockOutcome`. Every method catches broadly and degrades to "no answer".
- `IdempotencyCacheTest` updated to assert the `LockOutcome` variant rather than presence, so the
  distinction the type exists to carry is itself under test.

**What broke**

1. **`Optional.empty()` was the wrong return type for a three-outcome operation, and no test could
   see it.** `acquireLock` returned empty for "another caller holds the lock", "Redis is
   unreachable" and "the cache is switched off". The gate could only branch on empty, so it waited
   for a winner in all three cases — but in the last two there is no cache for a winner to publish
   into, so `waitForWinner` polled a lookup that returns empty by construction until its deadline
   and then returned null. Net effect: **with the cache off or Redis down, every write request
   slept `lockWait` (500 ms) before it was allowed to reach Postgres.**

   Nothing failed. `IdempotencyGateTest` runs with `dpe.idempotency.cache=false` and all seven of
   its tests were paying the penalty and passing. It is the mirror image of the failure
   `IdempotencyWithoutRedisTest` was built to catch — a cache that degrades the system when absent
   — except expressed as latency instead of as an exception, and latency is invisible to an
   assertion that only checks the answer. Fixed by making the three outcomes three types.

   The general lesson is about `Optional` specifically: it models "a value or nothing", and it is
   the wrong shape the moment "nothing" has more than one cause that callers must distinguish.

2. **Changing the signature broke two tests at *runtime*, not at compile time.** `mvn verify`
   reported `Unresolved compilation problems` inside surefire rather than a compiler error, because
   the two call sites that still assigned to `Optional<String>` were compiled by ecj into classes
   that throw on entry. A test source file that does not compile can therefore look like a test
   failure, in a build that otherwise says nothing.

**Verified**

```
./mvnw -B -ntp verify

Tests run: 36, Failures: 0, Errors: 0, Skipped: 0     account-service
Tests run: 35, Failures: 0, Errors: 0, Skipped: 0     payment-orchestrator
Tests run:  6, Failures: 0, Errors: 0, Skipped: 0     payment-gateway
BUILD SUCCESS
```

The two that carry the milestone:

```
IdempotencyGateTest.oneHundredConcurrentRetriesProduceExactlyOneTransfer      PASSED
IdempotencyWithoutRedisTest.theLockWasNeverTheGuarantee                       PASSED
```

The second runs 100 concurrent identical requests with Redis pointed at a closed port, so every
`lookup`, `store`, `acquireLock` and `releaseLock` on the request path fails — and still produces
exactly one transfer and exactly one `ReserveFunds` command. The Redis lock is an optimization,
and this is the executable form of that claim.

**Open / next**

- M4 part 2, unchanged from part 1a: retry with exponential backoff and jitter,
  `DefaultErrorHandler` with `DeadLetterPublishingRecoverer`, a DLQ replay endpoint, and the expiry
  sweep for `idempotency_records`.
- The DLQ replay endpoint needs a decision made before it is written: replaying a dead-lettered
  message is a second delivery of a message the inbox may already have recorded, so replay either
  goes through the inbox (and may be a no-op, which the operator must be told) or around it (and is
  no longer idempotent).

---

### M4 (part 2) — Retry, dead letters and replay, specified · 2026-09-09

**Goal**

Close the last hole in the consumer path. Up to this point every listener in the system ran on
Spring Boot's default error handling, and that default is worth stating plainly because it is not
what most people assume it is: `DefaultErrorHandler` retries a failed record **ten times with no
delay between attempts**, and then **logs the exception and commits the offset**. A `ReserveFunds`
command that fails ten times inside a few milliseconds — a two-second database failover is more
than enough — is discarded. The saga is never told, so it sits in `STARTED` until the timeout
sweeper compensates it, and the only surviving evidence is a stack trace.

So: bounded retry with exponential backoff, a classifier that separates transient failures from
poison ones, a dead letter topic, a queryable dead letter table, a replay endpoint, and the expiry
sweep that stops `idempotency_records` growing without limit.

**Decisions made**

| Decision | Choice | Reasoning |
|---|---|---|
| Dead letter transport | A `.dlt` Kafka topic **and** a `dead_letters` table fed by a listener on it | The topic is what makes the failure durable at the moment of failure, in the same infrastructure, with no database needed. But a log answers none of an operator's questions — how deep is the queue, what have I already replayed, show me every failure for this transfer. Those are queries. Depth becomes a `COUNT`, replay becomes a row update, and the console planned for M6.5 needs no Kafka client in the browser. |
| Dead letter dedup key | `(original_topic, original_partition, original_offset)`, **not** the message id | A replayed message that fails again is genuinely a second failure and must produce a second row; keying on the message id would silently discard it, and the operator would see an empty queue while the same message kept dying. Kafka coordinates are unique forever, so they identify the *failure* rather than the message. Same rule as the inbox — a dedup key must be unique across everything sharing the table — applied to a table whose rows are events *about* messages. |
| Does a replay go through the inbox? | Through it, deliberately | Left open at the end of part 1; it resolves cleanly in both directions. A technical failure throws, which rolls back the inbox row with it, so a dead-lettered message left no trace of consumption and its replay is a first delivery. In the rarer case where the handler committed but the acknowledgement did not, the replay is a duplicate — exactly what the inbox exists to absorb. Replay is therefore safe without the operator having to know which case they are in, and that property is bought entirely by the idempotent-consumer work in M2 and M3. |
| Shape of the classifier | A predicate over the whole cause chain, not a list of exception classes | Spring hands the error handler a `ListenerExecutionFailedException` every time; the exception that decides the outcome is one or two levels down. `addNotRetryableExceptions(...)` matches on the top of the chain, so it would see one type for the entire system and classify nothing — and it would fail silently, in whichever direction the default happened to point. |
| Dead letter listener gets its own container factory | Yes | If it inherited the main error handler, a failure while recording would publish to `<topic>.dlt.dlt`, which nothing consumes: not lost exactly, but somewhere nobody will ever look, which is worse than lost because it looks handled. Its handler retries forever and never recovers, so a database outage blocks that partition rather than dropping the last copy. Blocking is the right failure mode for the one listener that has nowhere further to fall. |
| Payload column | `TEXT`, not `jsonb` | A message can be here *because* it is not valid JSON. A `jsonb` column would reject exactly the rows the table exists to hold, so the insert recording the failure would itself fail. |
| Backoff ceiling | Bounded, and bounded well below `max.poll.interval.ms` | The default backoff handler sleeps in the listener thread, so `maxAttempts x maxInterval` is time the consumer is not polling. Exceed five minutes and the broker evicts the consumer and rebalances — handing the partition to an instance that starts the retry budget again at zero. An unbounded backoff does not produce patient retries, it produces a rebalance loop. |
| Sweep the idempotency table, but not the inbox | Only `idempotency_records` | Retention on an idempotency key is a *published contract*: a retry after 24 hours is documented as a new payment, so deleting an expired key changes nothing that was promised. An inbox row has no such contract — the thing that might redeliver is the broker, and its redelivery window is broker retention plus consumer lag plus however long a partition can stall. Delete an inbox row while redelivery is still possible and money moves twice. Retention policy is a statement about what can still happen, not housekeeping. |

**Built**

- `V4__dead_letters.sql` in all three service databases (`V2` in the gateway), each service owning
  its own table exactly as it owns its own `outbox` and `inbox`.
- `common-messaging/deadletter/` — the entity and repository, `DeadLetterProperties`,
  `KafkaErrorHandlingConfig` (the `DefaultErrorHandler`, the `DeadLetterPublishingRecoverer`, the
  delivery-attempt container customizer, and the separate factory for the dead letter listener),
  `DeadLetterRecorder`, `DeadLetterConsumer`, and `DeadLetterController` exposing depth, listing,
  by-key lookup and replay under `/admin/dead-letters`.
- `RetryClassifier` and `DeadLetterReplayService` — specified, with their tests; implementation
  follows.
- `IdempotencySweeper` and `IdempotencySweepScheduler` in the orchestrator, plus three new
  `dpe.idempotency.*` keys (`sweep-interval`, `sweep-batch-size`, `scheduled`).
- Each service's `KafkaTopicsConfig` gained a `NewTopic` for its dead letter topics and a
  `dltTopics` bean. The listener resolves its topic list from that bean via `#{@dltTopics}` rather
  than from a property, so the names it subscribes to derive from the same constants the admin
  client creates them with and cannot drift apart.

The web starter is an **optional** dependency of `common-messaging`: `DeadLetterController` needs
the annotations to compile, but a library that ships the outbox must not force an HTTP stack on
whatever depends on it. `@ConditionalOnWebApplication` keeps the bean from being registered where
those classes are absent, and because the condition is read from annotation metadata the class is
never loaded there at all.

Config choices worth remembering: four deliveries total (not four retries), a 500 ms initial
interval doubling to a 5 s ceiling, 100 ms of jitter, replay batches of 50, and a five-minute
sweep of at most 500 expired keys per statement.

**What broke**

1. **`ExponentialBackOffWithMaxRetries` does not exist in Spring Framework 7.** Every Spring Kafka
   tutorial written before 2025 uses it in exactly this position. It was folded into
   `ExponentialBackOff.setMaxAttempts(int)`. Note the off-by-one that comes with it:
   `setMaxAttempts(n)` yields *n* intervals and then stops, so total deliveries are *n + 1* — the
   first attempt was not a retry.

2. **`ExponentialBackOff.setJitter` takes milliseconds, not a fraction.** `setJitter(0.2)`
   expecting "20% spread" does not compile, which is the lucky outcome; `setJitter(1)` expecting
   the same compiles and adds one millisecond of spread, which is indistinguishable from working.

3. **The DLT origin headers are binary, not text.** Spring writes `kafka_dlt-original-partition`
   and `-offset` as big-endian `int`/`long`, while a header set by hand or by another client
   library is usually the decimal string. A reader that only parses text does not throw on the
   binary form — it returns 825373492, which is the ASCII bytes of `"1234"` read as an int. The
   recorder handles both encodings and the test fixture deliberately writes the binary one.

4. **A dead letter topic left to auto-creation fails in the cruellest available place.** Broker
   auto-creation is off in this project, so an undeclared `.dlt` topic makes the *recoverer's* own
   publish fail — and `DefaultErrorHandler` logs and swallows that. The message would be lost by
   the machinery built to save it, and only ever on a day when something else was already going
   wrong. Every dead letter topic is now declared with a `NewTopic` alongside the topic it shadows.

**Verified**

The scaffolding compiles, every context starts with the new beans, the dead letter topics are
created, and the listener is assigned all three of their partitions:

```
./mvnw -B -ntp compile                                     BUILD SUCCESS

./mvnw -pl account-service -am test -Dtest=DeadLetterRecorderTest \
       -Dsurefire.failIfNoSpecifiedTests=false
  account-service: partitions assigned: [dpe.account.commands.v1.dlt-0,
                                         dpe.account.commands.v1.dlt-1,
                                         dpe.account.commands.v1.dlt-2]
  Tests run: 6, Failures: 0, Errors: 0, Skipped: 0         BUILD SUCCESS
```

The three unwritten methods fail, and fail only for their own reason rather than on a context
error:

```
RetryClassifierTest      Tests run: 13, Errors: 13   UnsupportedOperationException
DeadLetterReplayTest     Tests run:  6, Errors:  6   UnsupportedOperationException
IdempotencySweepTest     Tests run:  5, Errors:  5   UnsupportedOperationException
```

Nothing else is claimed as verified yet.

**Open / next**

- `RetryClassifier.isRetryable`, `DeadLetterReplayService.replay` / `replayPending`, and
  `IdempotencySweeper.sweep`.
- `PoisonMessageTest` — the end-to-end proof that a poison message leaves its partition and that a
  good message queued behind it on the same key is still processed — goes green only once the
  classifier exists, since the error handler consults it on every failure.
- One question deliberately left open in `IdempotencySweeper`: whether a single transaction around
  the whole batch loop is right, given that batching exists precisely to avoid one long
  lock-holding statement against a table that sits on the write path.
- Not every poison message is reachable by a DLQ. A record whose failure happens inside
  `consumer.poll()` — a deserializer that throws, or the compression codec that could not load its
  native library in M2 — fails below the listener with no record attached, so no recoverer runs and
  the partition spins. The defence there is a deserializer that cannot throw
  (`ErrorHandlingDeserializer`), which is a different mechanism at a different layer and is not
  built yet.

---

### M4 (part 2b) — Retry, dead letters and replay, implemented · 2026-09-09

**Goal**

Write the three methods left open by part 2 — the retry classifier, the dead letter replay, and the
idempotency expiry sweep — and verify the whole path against the real stack rather than only
against Testcontainers.

**Decisions made**

| Decision | Choice | Reasoning |
|---|---|---|
| Where the sweeper's transaction goes | One per batch, via an injected `TransactionTemplate` | Three shapes were possible and two are wrong. **No transaction** does not run at all: a Spring Data `@Modifying` query without one throws `InvalidDataAccessApiUsageException: No active transaction for update or delete query`. **One around the loop** runs and quietly undoes the batching — every batch's row locks are then held until the last batch finishes, which is exactly the long lock-holding statement against a hot table that `LIMIT` existed to prevent, and it makes the sweep all-or-nothing for rows that have no relationship to each other. **One per batch** commits and releases as it goes, so a failure costs one batch and keeps the progress before it. A `TransactionTemplate` rather than a second `@Transactional` method, because a self-invocation never reaches the proxy and would fail as case one while looking correct. |
| Classifier walks the cause chain with an identity-set guard | Yes | The container hands over a `ListenerExecutionFailedException` every time, so a classifier reading only the top of the chain sees one type for the whole system. The visited-set is not paranoia: `getCause()` returning `this` is legal, and a naive loop hangs inside a listener while holding a partition — a hang, not a failure, which is the hardest thing to diagnose from outside. |
| `DataAccessResourceFailureException` listed explicitly | Yes | It sits under `NonTransientDataAccessResourceException`, so an `instanceof TransientDataAccessException` test alone misses it — and would classify an unreachable database as poison and dead-letter it on the first attempt, which is the worst possible answer for the most common transient fault there is. |
| A letter with no `message_id` is refused, not repaired | Refused, permanently and loudly | Those rows exist by design: a record with a missing or unparseable message id is dropped before any handler sees it, which is how it reaches the table. Replaying it puts a message on the topic that no consumer can dedupe. Minting a replacement id is worse — it makes the message look new to every inbox in the system, which is a deliberate double-spend dressed as a fix. The row stays pending and stays visible; republishing the intent is a decision for a human. |
| `exception_type` stores the cause, not the caught exception | Cause, falling back to the wrapper | Spring stamps `DLT_EXCEPTION_FQCN` with what it caught, which for anything thrown out of a listener is `ListenerExecutionFailedException`. A column whose purpose is "group failures by kind" would then read identically on every row and answer nothing. `DLT_EXCEPTION_CAUSE_FQCN` carries what actually broke. |

**Built**

- `RetryClassifier.isRetryable` — cause-chain walk with an `IdentityHashMap`-backed visited set,
  poison checked before transient at each level, defaulting to non-retryable for an unrecognised
  type.
- `DeadLetterReplayService.replay` / `replayPending` — original topic, original key, original
  message id and event type; publish-then-mark with the wait bounded by `sendTimeout`; per-letter
  failures logged and skipped so one bad row cannot roll back the batch's successes, with an
  interrupt breaking the loop instead, since that is the JVM shutting down rather than one bad
  message.
- `IdempotencySweeper.sweep` — batches until one comes back short, capped at 1000 batches, one
  transaction per batch.
- `DeadLetterRecorder` now prefers the exception cause over the listener wrapper.
- Four new tests covering the refusal path, the bulk-replay skip, and the cause-vs-wrapper column.

**What broke**

1. **A hand-produced test message wedged a partition, in a system whose producers are all correctly
   configured.** `rpk topic produce` **compresses with snappy by default**. The consumer runs on
   `eclipse-temurin:21-jre-alpine`, so `libsnappyjava.so` cannot load against musl, and the failure
   happens inside `consumer.poll()` — below the listener, with no record attached. `DefaultErrorHandler`
   refuses it outright ("this error handler cannot process `org.apache.kafka.common.KafkaException`s;
   no record information is available"), the retry policy never runs, the dead letter topic never
   sees it, and the partition spins on that offset producing roughly 600,000 identical stack traces
   before it was noticed.

   This is the M2 compression trap arriving from an unexpected direction: not from a producer
   setting, which is pinned to `none` in all three services, but from the CLI tool used to test
   them. It is also the live proof of a claim that had until now only been asserted — **not every
   poison message is reachable by a DLQ.** Recovery is the documented one and it does work: stop the
   consumer (seek needs an empty group), `rpk group seek <group> --to end --topics <topic>`, start it
   again. Producing with `--compression none` reproduces the intended payload-level poison instead.

2. **Removing `@Transactional` from the sweeper made it fail, not run unbounded.** See the decisions
   table. Worth recognising on sight because the message names the symptom rather than the cause.

3. **A test asserting on a global `count(*)` measured the suite's history, not its own behaviour.**
   `PoisonMessageTest` passed alone and failed after `DeadLetterReplayTest`. A Kafka topic is a log
   and consumer group offsets outlive a test class, so messages one class published and never
   consumed are delivered to the next class's listener, fail there, and land in the table under
   assertion. Fixed by scoping every count to the test's own aggregate id.

4. **A fixture cannot insert an already-expired idempotency key with the default `created_at`.**
   The table carries `CHECK (expires_at > created_at)` and `created_at` defaults to `now()`. That is
   the constraint doing its job — in production the pair is always written together as `now()` and
   `now() + retention` — so the fixture backdates both rather than working around it.

**Verified**

Full suite:

```
./mvnw -B -ntp verify

common-messaging       Tests run:  13, Failures: 0, Errors: 0
account-service        Tests run:  54, Failures: 0, Errors: 0
payment-orchestrator   Tests run:  40, Failures: 0, Errors: 0
payment-gateway        Tests run:   6, Failures: 0, Errors: 0
BUILD SUCCESS
```

Live on Compose — `docker compose build`, `up -d`, all six containers healthy, all eight topics
present at three partitions each including the four `.dlt` topics.

A poison message produced by hand onto `dpe.account.commands.v1`:

```
{"pending":1}

  "messageId":      "392d0ee0-5755-4413-81c7-4f5965b77b13",
  "originalTopic":  "dpe.account.commands.v1",
  "originalOffset": 0,
  "payload":        "{\"messageId\":\"392d...\",\"eventType\":\"ReserveFunds\",",
  "exceptionType":  "tools.jackson.core.exc.StreamReadException",
  "attempts":       1,
```

`attempts: 1` is the classifier working: unparseable JSON is poison, so it went straight to the dead
letter topic without spending the retry budget. `exceptionType` is the cause rather than
`ListenerExecutionFailedException`, which is the recorder change.

The partition kept moving — a transfer submitted afterwards reached `COMPLETED`, with the money
where it should be:

```
alice  457500      bob  42500
```

Replay, and the case that justifies the dedup key:

```
POST /admin/dead-letters/{id}/replay   HTTP 202
POST /admin/dead-letters/{id}/replay   HTTP 409     (already replayed)

original_partition@offset   attempts  replayed_at             replay_count
1@0                         1         2026-09-09 08:16:08     1
1@3                         1         NULL                    0
```

The replayed message failed again — the payload is still broken — at a **new offset**, and produced
a **second row**. Keying the table on the message id would have swallowed that and left the operator
looking at an empty queue while the same message kept dying.

The expiry sweep, with a key backdated 48 hours:

```
swept 1 expired idempotency key(s)
```

And the invariants, after all of it:

```
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (500000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
All invariants hold.
```

**Open / next**

- M5: JWT. `/admin/dead-letters` is unauthenticated and one of its endpoints republishes payment
  commands, so it is first in line behind an authenticated role.
- The gap this milestone proved rather than assumed: a failure inside `consumer.poll()` is below
  every error handler and no DLQ can reach it. The defence is a deserializer that cannot throw
  (`ErrorHandlingDeserializer`, which turns the failure into a poison-pill value the listener can
  see). Not built, and worth doing before the chaos suite at M7 starts breaking things on purpose.
- `outbox`, `inbox` and `dead_letters` all still grow without limit. Only `idempotency_records` has
  a sweeper, and the inbox deliberately cannot share its schedule — see the retention argument in
  `IdempotencySweeper`.


---

## Session 9 — 2026-09-09

### Goal

Start M5: authenticate every caller, and make it impossible to move money out of an account you do
not own. Landed end to end — the mechanism, the schema, the ownership event, the filter chains, the
two ownership checks and the tests — and verified against the running stack. RS256 is part 2.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Session vs token | Stateless JWT, validated locally in all three services | A session store sits on the hot path of every request in every service and makes instances non-interchangeable. The price is that a token cannot be revoked — mitigated with a 15-minute TTL and a `jti` claim so a denylist is possible later without invalidating tokens already in the wild. |
| Algorithm | HS256 now, RS256 next | A symmetric key means the verification key and the signing key are the same bytes, so every service that can validate can also forge. Shipping it that way first, visibly, then splitting the key. |
| Token issuance | A development `/auth/token` endpoint in the orchestrator | Stands in for an identity provider. Because validation is local, replacing it with Keycloak/Auth0 later changes exactly one thing in the other services: which key to trust. |
| Where ownership is checked | Both: a local projection at the API edge, and the authoritative column under the row lock | A synchronous call to account-service would make *accepting* a transfer depend on account-service being up, which is the coupling the saga exists to remove. Checking only in account-service means the caller gets 202 and then a FAILED saga, with authorization errors indistinguishable from insufficient funds. |
| Trusting a projection for an authorization decision | Safe **because ownership is immutable** | An account is opened once by one owner and there is no transfer-of-ownership operation, so the projection can only ever be missing a row, never holding a wrong one — and a missing row denies. Staleness fails closed. If ownership ever becomes mutable, the edge check becomes advisory and the decision has to move entirely to account-service. |
| Roles | `USER` and `OPERATOR`, disjoint | An operator reads operational surfaces and replays dead letters, and cannot move money: no single human should be able to move another human's money by holding a role. No `SERVICE` role — services talk over Kafka, where the trust boundary is the broker's. |
| Catch-all rule | `denyAll()`, not `authenticated()` | They differ only for an endpoint nobody wrote a rule for, which is next month's new controller. `authenticated()` opens it to every customer holding a token; `denyAll()` opens it to nobody and surfaces as a failing test. |
| Signing key default | None, anywhere in the repository | A default is a published signing key. `DPE_SECURITY_SECRET` is required and a service without it refuses to start, naming the key and the minimum length — better than inventing one or running open. |
| account-service `POST /transfers` | Denied to every role | It writes ledger entries with no saga, no idempotency key and no ownership check — a second door into the money that bypasses three milestones of machinery. The service behind it stays (the M1 concurrency tests drive it directly); only the HTTP exposure closes. |
| Gateway `/admin/simulation` | Now operator-only | Previously open, on the grounds that it is a test affordance on a simulated third party. It can also take the payment path down for every customer at once, and an availability control is a security control. |

### Built

- **`common-security`**, a fourth Maven module holding the *mechanics* of token validation and
  nothing about authorization: `SecurityProperties` (fails fast on a missing or too-short secret,
  at startup rather than at first signature), `JwtConfig` (a decoder with timestamp, issuer and
  audience validators, and the algorithm pinned so a token cannot choose its own), `Roles`, and
  `TokenIssuer`. Which endpoints require which role stays in each service's own `SecurityConfig` —
  a shared filter chain would mean loosening one endpoint quietly loosens all three services.
- **`AccountOpened`** in `common-events`, published by `AccountService.open()` through the outbox in
  the same transaction as the account row. An account whose ownership never reached the orchestrator
  would be an account nobody can spend from, with no retry able to fix it.
- **The ownership projection** — `V5__authorization.sql` (`account_owners`, plus
  `transfers.initiated_by`), an inbox-gated handler, and a third branch in `SagaReplyConsumer`.
  Still one consumer group and one listener in the service: the inbox's primary key is the message
  id alone, so a second group would receive every message and skip everything the first recorded.
  The repository upserts with `ON CONFLICT DO NOTHING` — ownership is decided once, and an
  overwrite would let a replayed event hand an account to someone else.
- **`ReserveFunds.initiatedBy`** and `ReserveRejected.NOT_ACCOUNT_OWNER`, so account-service can
  answer the ownership question itself rather than assuming the request came through the front door.
  `dpe.account.commands.v1` is reachable by anything that can produce to it.
- **A development token endpoint** (`POST /auth/token`), the demo directory in configuration, and
  `scripts/token.sh` so the chaos scenarios and the k6 run can authenticate in one line.
- **Filter chains for account-service and payment-gateway**, the `403` error mapping, and the
  transfer API rewritten to take its identity from the token instead of the `X-Client-Id` header.
- **`docs/adr/0005-jwt-authentication.md`** — the full argument, including what was rejected.
- **The orchestrator's filter chain**, the `roles` → `ROLE_` authorities converter, the edge
  ownership guard, and the authoritative check inside `ReservationService.reserve`. The converter
  reads the claim defensively rather than with `getClaimAsStringList`: that helper throws when the
  claim is present but malformed, which would turn an authorization question into a 500 from inside
  the filter chain. Anything unexpected collapses to no authorities — authenticated, holding
  nothing, denied by every role rule.

The M4 idempotency namespace is the clearest single change: `idempotency_records.client_id` used to
come from a header the caller chose, so sending another tenant's client id with their key returned
*their* stored response body — another customer's transfer receipt. The column is unchanged; the
value now comes from the token's `sub` claim and cannot be forged without the signing key.

### What broke

1. **Spring Boot 4 renamed the resource-server starter and kept the old name working.**
   `spring-boot-starter-oauth2-resource-server` still resolves; it is a deprecated alias, and its
   own POM says so in the description — *"deprecated in favor of
   spring-boot-starter-security-oauth2-resource-server"*. Nothing warns at build time, and every
   pre-2025 tutorial uses the old coordinate. The whole security family moved under the `security-`
   prefix (`-security-oauth2-client`, `-security-saml2`, and so on).

2. **`@AutoConfigureMockMvc` moved package.** It is no longer
   `org.springframework.boot.test.autoconfigure.web.servlet` — Boot 4 split test autoconfiguration
   per technology and it now lives at
   `org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc`, shipped in
   `spring-boot-webmvc-test`. Same reorganisation that moved `@EntityScan` at M3. Symptom:
   *"package org.springframework.boot.test.autoconfigure.web.servlet does not exist"*.

3. **`JwtClaimsSet` refuses to build a token whose `exp` is not after its `iat`.** The first
   expired-token fixture applied one negative offset to both, making them equal:
   *"expiresAt must be after issuedAt"*. The library is right, and the distinction is worth keeping
   — an expired token is a different object from an impossible one, and a fixture producing the
   impossible one would be testing the builder rather than the validator. Rewritten as a token
   issued an hour ago with a thirty-minute lifetime.

4. **Maven reported `test-compile` SUCCESS with stale test classes.** Changing a constructor in a
   main source did not trigger recompilation of the test sources that call it — main sources logged
   *"Recompiling the module because of changed dependency"* and test sources did not, so a test
   class two hours old kept calling a signature that no longer existed. It surfaces later as a
   runtime error rather than a compile failure, which is the same trap family as
   `Unresolved compilation problems` being reported by surefire as a test *error*.

### Verified

Full suite:

```
./mvnw -B -ntp verify

common-messaging       Tests run:  13, Failures: 0, Errors: 0
account-service        Tests run:  64, Failures: 0, Errors: 0
payment-orchestrator   Tests run:  64, Failures: 0, Errors: 0
payment-gateway        Tests run:   6, Failures: 0, Errors: 0
BUILD SUCCESS
```

Live on Compose — `docker compose build`, `up -d`, six containers healthy. The health endpoints
stayed unauthenticated, which the container healthchecks prove by passing at all.

Tokens, and the four surfaces:

```
./scripts/token.sh alice        251-byte token
./scripts/token.sh operator     260-byte token

POST /api/v1/transfers                  no token              401
POST /accounts            (8082)        operator              201
POST /accounts            (8082)        customer              403
POST /transfers           (8082)        operator              403     the direct ledger door
GET  /admin/dead-letters/depth          anon / customer / operator    401 / 403 / 200   on all three services
GET  /admin/simulation    (8083)        anon / customer / operator    401 / 403 / 200
```

The ownership projection arriving over Kafka, seconds after the accounts were opened — outbox to
relay to broker to inbox-gated handler:

```
             account_id              | owner_id | account_type |         projected_at
--------------------------------------+----------+--------------+-------------------------------
 438a9787-15f2-441e-b7a7-171a92f071d7 | bob      | CUSTOMER     | 2026-09-09 10:23:55.573877+00
 e38e6b70-8a3b-48c8-892e-429a0488acfa | alice    | CUSTOMER     | 2026-09-09 10:23:55.116094+00
```

A transfer alice owns, and the same request against bob's account:

```
POST /api/v1/transfers  from=alice's account   202  {"status":"PENDING","sagaStatus":"STARTED"}
                        polled                      {"status":"COMPLETED","sagaStatus":"COMPLETED"}

POST /api/v1/transfers  from=bob's account     403  {"code":"ACCOUNT_FORBIDDEN",
                                                     "message":"You may not use that account"}
```

Reads are scoped, and the idempotency namespace still works now that it comes from the token
rather than a header:

```
GET  /api/v1/transfers/{id}   as alice              200
GET  /api/v1/transfers/{id}   as bob                404      not 403 - the id is the secret
POST /api/v1/transfers        same Idempotency-Key  202  Idempotency-Replayed: true
```

**The check that matters most**, because it is the one an API-level guard cannot make: a
`ReserveFunds` command produced straight onto the topic, bypassing the orchestrator entirely, with
a subject that does not own the account (`rpk topic produce --compression none`, per the M4 trap):

```
Produced to partition 0 at offset 1

   event_type    |      reason       |                          detail
-----------------+-------------------+-------------------------------------------------------
 ReserveRejected | NOT_ACCOUNT_OWNER | subject 'mallory' does not own account e38e6b70-...

ledger_entries for that transfer: 0
holds for that transfer:          0
```

Refused, committed, replied — no ledger entry, no hold, no infinite redelivery. Balances after
everything, showing only the legitimate movement:

```
 owner_id | balance_minor
----------+---------------
 alice    |        457500
 bob      |         42500
```

And the invariants. The first run **failed I3 correctly**: opening a funded account issues money
into the ledger, so the total moved from 500,000 to 1,000,000 while the baseline still held the old
figure. That is the invariant doing its job — I3 is conservation *across a run*, not a constant —
so the baseline was re-recorded and money moved across it:

```
./scripts/verify-invariants.sh baseline      I3 baseline recorded: 1000000
POST /api/v1/transfers  7500                 202
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (1000000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
All invariants hold.
```

### Committed

`M5: JWT authentication, and per-account authorization that a valid token is not enough for`
— one commit, so the history never contains a state in which the API is unreachable.

### Open / next

- **M5 part 2: RS256.** The orchestrator keeps a private key and signs; the other two get a public
  key and can only verify. Today all three hold the same secret and can therefore mint each other's
  tokens.
- `/actuator/prometheus` is operator-only, so the M6 scraper will need a credential or a network
  exemption. Left closed rather than pre-opened, so the decision is made when the scraper exists.
- Nothing rate-limits `POST /auth/token`, which is the one endpoint that mints credentials.
- A newly opened account is briefly unspendable while its `AccountOpened` event is in flight —
  bounded, and the direction the projection is allowed to be wrong in.
- Still outstanding from M4: `ErrorHandlingDeserializer`, so a failure inside `consumer.poll()`
  becomes a poison-pill value the listener can see rather than an unreachable partition stall; and
  retention for `outbox`, `inbox` and `dead_letters`, which still grow without limit.


---

## Session 10 — 2026-09-09

### Goal

M5 part 2: replace the shared HS256 secret with RS256, so that the ability to *verify* a token
stops implying the ability to *mint* one. Part 1 shipped that defect deliberately and visibly;
this session removes it.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Key distribution | A JWKS endpoint on the orchestrator, fetched by the other two | It is what every identity provider serves and every resource server knows how to consume, so replacing this issuer with Keycloak later changes a URL and nothing else. It is also the only mechanism that supports rotation: the issuer publishes old and new keys together and validators follow by `kid`, with no coordinated restart. |
| Where `TokenIssuer` lives | Moved out of `common-security` into payment-orchestrator | Under HS256 it belonged in the shared library and was a bean in all three services — that was honest, because the verification key and the signing key were the same bytes. Under RS256 it would be a lie. The move follows the fix rather than substituting for it: the class needs a private key, and there is no import that brings one into the other services. |
| How the orchestrator verifies | Directly, with its own public key in memory | It could fetch its own JWKS endpoint over HTTP and be symmetrical with the others. That would make verification depend on its own HTTP port being up, turning an in-memory operation into a startup-ordering question. |
| Choosing the decoder | An explicit `@Import`, not `@ConditionalOnMissingBean` | Outside auto-configuration, `@ConditionalOnMissingBean` is evaluated in bean-registration order. Ordering that decides which key verifies your tokens is not something to leave to processing order. |
| Where the signing key comes from | Configured if present, generated at startup otherwise, with a WARN | Keeps `git clone && docker compose up` working with no key ceremony, and is safe in a way a committed key is not: it never existed before the process and does not outlive it. What it cannot do is survive a restart or be shared by two instances — both are stated in the log line and in `SigningKeys`. |
| Algorithm pinning | `RS256` pinned on every decoder | Now that the verification key is *public*, a verifier that let a token choose its own algorithm would accept one HMAC-signed with the published key. Pinning is not defence in depth here; it is the thing that stops a public key being a signing key. |
| Test key material | Generated per JVM, injected via `@DynamicPropertySource` | No private key is committed to this repository, including test fixtures. account-service and payment-gateway generate a pair, hand the public half to the application as `dpe.security.public-key`, and mint with the private half — which is exactly what an operator pointing these services at a different issuer would do. |

### Built

- **`SigningKeys`** — the system's private key, in payment-orchestrator and nowhere else. Loads a
  configured PEM/base64 pair or generates RSA-2048 at startup. `kid` is the RFC 7638 thumbprint,
  derived from the key material, so two processes given the same key agree on its id without
  coordinating.
- **`JwksController`** — `GET /.well-known/jwks.json`, unauthenticated. The load-bearing line in
  the whole change is `toPublicJWK()`: it strips the private exponent and the primes. Serving the
  full key would still be valid JSON, still return 200, and still pass every test that only checks
  that a token verifies.
- **`TokenIssuer`**, moved, now signing RS256 and stamping `kid` into the header.
- **`JwtDecoderConfig`**, split out of `JwtConfig`: one decoder bean that branches on whether a
  JWK set URI or a static public key is configured, imported only by the services whose key comes
  from configuration.
- **`SecurityProperties`** rewritten around a public key — the type no longer has a method that
  returns anything capable of signing.
- **`JwksEndpointTest`** — four tests, two of which are the milestone: the served key set contains
  no private field (`d`, `p`, `q`, `dp`, `dq`, `qi`), and a token HMAC-signed with the published
  public key is rejected.

### What broke

1. **`@ComponentScan` in a shared library swept up the configuration class it was meant to opt
   into.** `JwtConfig` carried `@ComponentScan(basePackageClasses = JwtConfig.class)` to register
   its one component. A `@Configuration` class *is* a component, so the scan also registered
   `JwtDecoderConfig` in every service — including the issuer, which supplies its own decoder —
   producing two `jwtDecoder` definitions and a `BeanDefinitionOverrideException` at startup.

   Loud, and it could have been much worse: with bean overriding enabled, one decoder would have
   silently replaced the other and the service would have verified tokens with a key nobody chose.
   **An "import this to opt in" design is not opt-in if a scan can find the class anyway.** Fixed
   by dropping the scan and declaring the one component as a `@Bean`.

2. **Two sources of truth for a verification key, introduced by the test harness.** The test base
   added `dpe.security.public-key` via `@DynamicPropertySource` while `application.yml` still
   configured `dpe.security.jwk-set-uri`, so both were set. `SecurityProperties` refuses that
   combination at startup by design — the failure was the check working. The test now blanks the
   URI explicitly.

3. **A retired key keeps working until the validator's cache turns over, and then stops
   abruptly.** Restarting the orchestrator generated a new key. A token minted before the restart
   was still accepted by account-service, because its cached JWK set still held the old key; once
   a token carrying the new `kid` forced a re-fetch, the old key was gone and the same pre-restart
   token began failing with 401 — while still being well within its 15-minute lifetime.

   Both halves are worth keeping in mind. **Rotating a key is not revocation** (tokens signed by a
   retired key survive in warm caches), and **retiring one can also cut valid tokens short**
   (once the cache turns over, they fail early). Real rotation therefore publishes both keys for
   an overlap window at least as long as the token TTL, and removes the old one only afterwards.

### Verified

Full suite — 151 tests:

```
./mvnw -B -ntp verify

common-messaging       Tests run:  13, Failures: 0, Errors: 0
account-service        Tests run:  64, Failures: 0, Errors: 0
payment-orchestrator   Tests run:  68, Failures: 0, Errors: 0
payment-gateway        Tests run:   6, Failures: 0, Errors: 0
BUILD SUCCESS
```

Live on Compose, all six containers healthy. The orchestrator says what it did with its key:

```
WARN  com.dpe.orchestrator.auth.SigningKeys : no dpe.auth.private-key configured - generated an
ephemeral RSA key (kid y0cF2k1GIb04DXpkZKHnSa2TGawDQQ6UO7aKXSWbDVY). Tokens will not survive a
restart, and a second instance would sign with a different key. Configure a key pair for anything
real.
```

What the endpoint publishes, and what it does not:

```
GET /.well-known/jwks.json          200, no credential
fields served: ['e', 'kid', 'kty', 'n', 'use']
private fields present: none
```

Tokens are RS256 and name the key that signed them:

```
{'kid': 'y0cF2k1GIb04DXpkZKHnSa2TGawDQQ6UO7aKXSWbDVY', 'alg': 'RS256'}
```

**The property the milestone was for:** account-service and payment-gateway hold no key material at
all, and verify tokens the orchestrator issued using a public key they fetched over HTTP:

```
POST /accounts            (8082)  operator token   201
GET  /admin/simulation    (8083)  operator token   200
```

The rest of M5 still holds, on the new algorithm:

```
POST /api/v1/transfers   from alice's own account   202 -> polled COMPLETED
POST /api/v1/transfers   from bob's account         403
```

Rotation, demonstrated by restarting the issuer so its ephemeral key changed:

```
old kid: y0cF2k1GIb04DXpkZKHnSa2TGawDQQ6UO7aKXSWbDVY
new kid: WSz5LnLqUS2dFFj0IoDMZGk0asSmZcnOvuq-kX8I8EE

a token issued BEFORE the restart, presented afterwards:
  to the issuer (in-memory key)          401
  to account-service (cached JWK set)    200, then 401 once a new kid forced a re-fetch

a freshly issued token, at account-service, which was never restarted:   200
```

That last line is the point: account-service picked up a brand-new signing key with no restart, no
redeploy and no configuration change.

And the invariants, after all of it:

```
./scripts/verify-invariants.sh baseline      I3 baseline recorded: 1300000
POST /api/v1/transfers  5000                 202
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (1300000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
All invariants hold.
```

### Committed

`M5 (part 2): RS256 — the ability to verify a token stops implying the ability to mint one`

### Open / next

- **M6: observability.** Metrics, dashboards, tracing. Note the debt M5 left it:
  `/actuator/prometheus` is operator-only, so the scraper needs a credential or a network
  exemption — a decision deliberately deferred to when the scraper exists.
- Key rotation is *possible* but not *practised*: the orchestrator publishes one key at a time. A
  real rotation publishes the outgoing and incoming keys together for at least one token lifetime.
- Nothing rate-limits `POST /auth/token`.
- Still outstanding from M4: `ErrorHandlingDeserializer`, so a failure inside `consumer.poll()`
  becomes a poison-pill value the listener can see rather than an unreachable partition stall; and
  retention for `outbox`, `inbox` and `dead_letters`, which still grow without limit.

## Session 11 — 2026-09-09

### Goal

M6 part 1: stand up the metrics pipeline. Prometheus scraping all three services, Grafana
provisioned from the repository, and the cardinality controls that make a metrics system survive
contact with production traffic. The custom `dpe.*` business meters are deliberately not in this
session — the dashboard was written first, as their specification.

The decision deferred since M5 — *how does a scraper authenticate to an operator-only actuator?* —
had to be answered before anything else could be built.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| How Prometheus reaches `/actuator/prometheus` | A separate `management.server.port`, unpublished in Compose | The alternatives were a static bearer token or basic auth. A never-expiring token in a config file is a published credential and contradicts the 15-minute TTL the whole of M5 argued for; basic auth adds a second authentication mechanism to a system that just spent a milestone arguing there should be one. A port the host cannot reach makes the network the boundary — which is the same answer this system already gives for `dpe.account.commands.v1`, where nobody authenticates to produce either. A system with two different stories about where its perimeter sits has neither. |
| Where the port rule lives | The `permitAll` in each service's own `SecurityConfig`; only the port *matcher* is shared | `common-security`'s stated contract is that it holds token-validation mechanics and no authorization rule. "Which connector did this arrive on" is a fact about a socket and is shared; "therefore permit it" is a rule, and a `permitAll` that arrives from a library is the single most dangerous kind of reuse. |
| Whether the scraper gets its own credential | No | Adding one would have meant the actuator stayed reachable from the host, and the credential would have become the thing protecting it. Removing host reachability removes the need for the credential; that is a smaller system, not a looser one. |
| One Prometheus job or three | One, with `application` as a meter tag | Three jobs would make `job` duplicate the `application` tag each service already stamps on its own meters, and two labels that always agree are two labels that can one day disagree. `job` is the scrape file's opinion; `application` is the application's own claim, and it survives federation and `remote_write`. |
| Quantiles | Histogram buckets, never client-side percentiles | A percentile computed inside one JVM cannot be aggregated — averaging two p99s is not the p99 of anything. Buckets can be summed, so the same query stays correct behind a load balancer with three replicas. |
| Bucket range | Bounded explicitly, 5ms–5s for requests and 50ms–60s for sagas | Every bucket boundary is a time series multiplied by every other label. The default range emits roughly seventy per meter; bounding it to what these operations actually do gives about thirty. |
| Grafana auth | Anonymous, with the Admin role | Stated in the Compose file rather than hidden in a volume, because it is exactly the setting that must never be copied to a deployment. It is only defensible because port 3000 is reachable from this machine alone. |
| Dashboard storage | Provisioned from files, `allowUiUpdates: false` | A dashboard that exists only in Grafana's volume cannot be reviewed, diffed, or restored, and `docker compose down -v` deletes it along with every query in it. |
| Prometheus retention | 24h | Retention is a statement about which questions can still be asked. A day covers "what happened during this morning's chaos run" and promises nothing longer, which is all a laptop should. |

### Built

- **`ManagementPortMatcher`** (`common-security`) — a `RequestMatcher` that answers whether a
  request arrived on the management connector, using `HttpServletRequest.getLocalPort()`: the port
  the connector actually accepted on, not a header, not `X-Forwarded-Port`, nothing a client can
  set. That is the only reason a port is usable as a trust boundary. Fails closed — if
  `management.server.port` is unset, zero, or equal to `server.port`, there is no second connector,
  it matches nothing, and the actuator stays operator-only.
- **A second `SecurityFilterChain` in each service**, `@Order(0)`, selected by that matcher and
  permitting everything on it. It has to be a whole chain rather than a `requestMatchers` rule
  because `requestMatchers` only ever sees a path and cannot tell two sockets apart.
- **`micrometer-registry-prometheus`** in all three services. Actuator alone gives the metrics API
  but no registry that can serialise it, and without this artifact `/actuator/prometheus` is simply
  absent — which reads like an exposure typo rather than a missing dependency.
- **Metrics configuration** in all three `application.yml`: the `application` common tag,
  `max-uri-tags: 50` as the cardinality backstop, and bounded histogram buckets.
- **Prometheus and Grafana containers**, 384M and 320M caps. Prometheus depends on the services
  with `service_started`, not `service_healthy`, so it comes up and *shows targets down* rather
  than hiding the startup window worth watching.
- **`infra/prometheus/prometheus.yml`**, **Grafana datasource and dashboard provisioning**, and
  **`infra/grafana/dashboards/dpe-payments.json`** — 26 panels in five sections: RED for
  request-driven work, a section of its own for the saga, the messaging spine, the idempotency
  edge, and USE for resources. Every panel description names the meter it reads. The panels reading
  `dpe_*` are empty until those meters exist; the dashboard is their specification, not a
  decoration over them.
- **`ManagementPortSecurityTest`** — five assertions covering the two-connector posture, including
  the one that bounds the whole design: the business API returns 404 on the management port,
  because the management child context has its own `DispatcherServlet` carrying only actuator
  mappings. The API is not merely denied there; it is not mapped.

### What broke

1. **A separate management port is not unprotected — Boot puts the application's own security on
   it.** This was the assumption the entire design rested on, and it is false.
   `ServletManagementChildContextConfiguration$ServletManagementContextSecurityConfiguration`
   reaches into the *parent* bean factory and registers the parent's `springSecurityFilterChain`
   on the management connector:

   ```java
   springSecurityFilterChain(HierarchicalBeanFactory bf) {
       return bf.getParentBeanFactory().getBean("springSecurityFilterChain", Filter.class);
   }
   ```

   So every M5 rule applied to port 9091 too and the scraper got a 401. It fails in the safe
   direction, which is exactly why it survives review: the natural response is to give Prometheus
   a credential rather than to ask why a port with "no security on it" is refusing.

2. **A test class named `*IT` never runs.** `ManagementPortSecurityIT` compiled, passed when run
   explicitly with `-Dtest=`, and was silently skipped by `./mvnw verify` — no Failsafe plugin is
   configured and Surefire's default includes are `*Test`, `Test*`, `*Tests`. The only signal was
   the suite total not going up, from 151 to 151, which is the one number nobody checks. Renamed
   to `ManagementPortSecurityTest`; the count then went to 156.

3. **`AdminEndpointSecurityTest` had been passing for the wrong reason since M4.** It asserts
   `GET /actuator/health == 200` to show the probe needs no token. Boot's `RedisHealthIndicator`
   probes Redis regardless of `dpe.idempotency.cache=false` — switching the cache off does not
   remove the connection factory — and one DOWN component makes the whole endpoint 503. These
   tests run with no Redis, so the assertion only held when something happened to be listening on
   `localhost:6379`, which in practice meant "Docker Compose was up in another window". Confirmed
   directly: with nothing on 6379 the test fails; start a bare `redis:7-alpine` on 6379 and it
   passes. It survived all of M4 and M5 that way. The test is about authorization, so the health
   of a dependency it deliberately does not run is noise in it —
   `management.health.redis.enabled=false` in the test properties.

4. **`DynamicPropertyRegistry.add` takes a supplier that is invoked on every resolution, not
   once.** Passing `TestSocketUtils::findAvailableTcpPort` directly returned a *different* free
   port each call, so the web server bound one port and `ManagementPortMatcher` was constructed
   with another. The matcher then compared against a port nothing was listening on, matched
   nothing, and every request fell through to the M5 chain — presenting as a 401 from a connector
   that is supposed to be open, which reads exactly like the security rule being wrong. A dynamic
   property supplier must be idempotent; the port is now chosen once into a constant.

5. **The application port cannot return 404 to an anonymous caller at all.** With the actuator
   moved, `GET /actuator/health` on port 8081 has no handler; the 404 becomes a servlet ERROR
   dispatch to `/error`; and `/error` matches no rule, so `anyRequest().denyAll()` denies it. Every
   unmapped path therefore answers 401. That is a real and desirable property of `denyAll()` — the
   port cannot be walked for its routes — but it means the expected status for "this endpoint
   moved" is 401, not 404, and a probe left on the old port reports unhealthy forever with nothing
   in the log to say the endpoint merely moved. The Compose healthcheck moved to `MANAGEMENT_PORT`
   with it; the same correction is due for the K8s probes at M10.

6. **`--web.enable-lifecycle=false` is rejected by Prometheus 3** with `error: unexpected false`.
   These are presence flags; the negative form is `--no-web.enable-lifecycle`. Absent is already
   the secure default, so the flag was dropped rather than negated.

7. **Compose merges list values by concatenation.** An override file supplying a different `ports:`
   entry *adds* a mapping rather than replacing it, so the original conflicting binding was still
   attempted. `ports: !override` is the replacement form. (Hit while working around host port
   19092 being held by an unrelated container on this machine — the services themselves reach the
   broker at `redpanda:9092` on the Compose network and were never affected.)

8. **Boot 4 moved `TestRestTemplate` and made it opt-in.** It is no longer
   `org.springframework.boot.test.web.client.TestRestTemplate` from the test starter; it lives in a
   `spring-boot-resttestclient` module no starter pulls in, needs `@AutoConfigureTestRestTemplate`
   for the bean, and then fails at context load with `NoClassDefFoundError: RestTemplateBuilder`
   until `spring-boot-restclient` is added as well. Three modules and an annotation to issue a GET
   and read a status code. The test uses the JDK's own `HttpClient` instead, which needs none of
   them and — unlike a bare `RestTemplate` — does not throw on a 4xx, which this test depends on.

### Verified

Full suite, 156 tests (151 at M5, plus the five new ones once the class was named so they would
actually run):

```
./mvnw -B -ntp verify

common-messaging       Tests run:  13, Failures: 0, Errors: 0
account-service        Tests run:  64, Failures: 0, Errors: 0
payment-orchestrator   Tests run:  73, Failures: 0, Errors: 0
payment-gateway        Tests run:   6, Failures: 0, Errors: 0
BUILD SUCCESS
```

Live on Compose, eight containers healthy. All three targets scraped:

```
payment-orchestrator:9091          up       ok
account-service:9092               up       ok
payment-gateway:9093               up       ok
localhost:9090                     up       ok
```

The scrape path, and the boundary that replaces the credential:

```
# from inside the network, no credential of any kind
docker exec dpe-prometheus wget -qO- http://payment-orchestrator:9091/actuator/prometheus
  hikaricp_connections_active{application="payment-orchestrator",pool="orchestrator-pool"} 0.0
  jvm_memory_used_bytes{application="payment-orchestrator",area="heap",id="Eden Space"} 1.9368464E7

# the same endpoint from the host - 9091 appears in no `ports:` block
curl http://localhost:9091/actuator/prometheus        -> connection failed (000)

# and the application port is untouched by any of this
curl http://localhost:8081/actuator/prometheus        -> 401
```

Thirty-four transfers driven through the stack, and the built-in RED metrics that came with them:

```
sum by (uri,method) (rate(http_server_requests_seconds_count{application="payment-orchestrator"}[5m]))
  method=POST uri=/api/v1/transfers        0.1036
  method=POST uri=/auth/token              0.0114
  method=GET  uri=/.well-known/jwks.json   0
  method=GET  uri=UNKNOWN                  0

sum(http_server_requests_seconds_count{uri="/api/v1/transfers",status="202"})   34
```

Quantiles computed by Prometheus from `_bucket` series, which is the point of choosing histograms:

```
histogram_quantile(0.50, ...)   0.0383
histogram_quantile(0.95, ...)   0.0551
histogram_quantile(0.99, ...)   0.0657
```

`uri=UNKNOWN` in that first output is the cardinality guard working: paths that match no handler
collapse into one series instead of minting one apiece.

Grafana provisioned from the repository, and querying end to end through its datasource:

```
GET /api/datasources    Prometheus  prometheus  http://prometheus:9090  uid=dpe-prometheus  default=True
GET /api/search         dash-db | DPE - Payments | uid=dpe-payments | folder=DPE

GET /api/datasources/proxy/uid/dpe-prometheus/api/v1/query?query=up{job="dpe"}
  account-service:9092           up=1
  payment-gateway:9093           up=1
  payment-orchestrator:9091      up=1
```

Invariants, before the traffic and after it:

```
./scripts/verify-invariants.sh baseline     I3 baseline recorded: 1900000
  ... 34 transfers ...
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (1900000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
```

Memory, since RAM is this project's binding constraint — the two new containers cost 147 MB
together, well under their caps:

```
dpe-prometheus     37.8MiB / 384MiB
dpe-grafana       108.8MiB / 320MiB
```

### Committed

Not yet — the business meters the dashboard specifies belong in the same commit as the pipeline
that carries them.

### Open / next

- **The `dpe.*` business meters**, which the dashboard already names and queries: saga starts,
  terminal outcomes by status, saga duration, in-flight sagas by state, outbox backlog and oldest
  age, DLQ depth, inbox duplicates suppressed, and idempotency gate outcomes. Each gauge must ride
  the partial index its worker already uses — `idx_outbox_unpublished`, `idx_dead_letters_pending`,
  `idx_saga_instances_in_flight` — because a gauge is re-evaluated on every scrape, forever,
  against tables on the write path of every payment.
- **M6 part 2: tracing.** The interesting problem is that the transactional outbox destroys
  automatic trace propagation — the message is written inside the request's transaction and
  relayed later on a different thread, so the `traceparent` has to be persisted in the outbox row
  and restored by the relay, or one payment becomes five disconnected traces.
- **M6 part 3:** the read endpoints M6.5 needs.
- Grafana's anonymous-Admin setting is correct for a laptop and must not survive contact with
  anything else.
- Still outstanding from M4: `ErrorHandlingDeserializer`, and retention for `outbox`, `inbox` and
  `dead_letters`, which still grow without limit.

## Session 12 — 2026-09-09

### Goal

Finish M6 part 1 by writing the business meters the dashboard was built as a specification for.
Session 11 landed the pipeline and left every `dpe_*` panel empty on purpose; this session fills
them, and the interesting work is not the counters themselves but deciding *when* each one is
allowed to fire and *what* it must still say when nothing is happening.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| When a counter increments | On `afterCommit`, never inline | The dual-write problem again, one level down: the meter is in process memory, the saga is in Postgres, nothing spans both. The increment and the commit cannot be atomic, so the only choice is which way it fails. Inline over-counts on every rollback — and rollbacks are routine here, because a redelivered message losing the inbox race rolls back by design — so the metric reads permanently high. After commit under-counts by one if the process dies in the gap, once. Bounded and rare beats unbounded and permanent. |
| How gauges are read | `AtomicLong` fields refreshed by a scheduler; the scrape never touches the database | A Micrometer gauge over a lambda calls it on *every* scrape. Pointed at a repository that means queries against the write path every 10s forever, and the scrape now blocks on Postgres — precisely when the metrics are all you have left. Reading a field makes the scrape unable to block or fail, and fixes database load to the refresh interval rather than to the number of scrapers. |
| Where the shared meters live | `common-messaging`, component-scanned | Outbox backlog, outbox age and DLQ depth are the same question in all three services. `micrometer-core` at compile scope, not the actuator starter: the module instruments, it does not expose. Which registry the meters land in stays the service's decision. |
| The inbox dedup seam | A new `InboxGate` replacing five direct `insertIfAbsent` calls | Five handlers across three services each wrote the same three lines, and there was nowhere to observe *the* rule that makes at-least-once delivery safe. The guarantee did not move: still `ON CONFLICT DO NOTHING` against the primary key, still decided by Postgres under the row lock. The class adds a counter and a name, not a check. |
| Which labels exist | `status`, `state`, `outcome`, `result`, `topic` — and nothing else | Every one is drawn from a fixed vocabulary that requires a code change to extend. `transfer_id`, `account_id` and the idempotency key are deliberately absent: those are one series per payment, and "what happened to this transfer" is a question for a trace or `saga_steps`. |
| Meters that may legitimately stay at zero | Pre-registered at startup | `FAILED`, `conflict`, and inbox duplicates are all meters whose healthy reading is zero. Registered lazily they do not exist until they first fire, so the panel reads "No data" on a working system — which is the fastest way to teach an operator to ignore a panel. Worse for alerting: `absent()` then cannot tell "healthy" from "the exporter died". |
| Where terminal transitions are recorded | One private `finish()` helper in `SagaOrchestrator` | There are four terminal call sites in four branches. A fifth added later would otherwise be invisible to `dpe.saga.terminal`, the compensation rate would silently under-report, and no test would catch it. One door, so the meter cannot be forgotten. |

### Built

**Shared, in `common-messaging`:**

- `MessagingMetrics` — `dpe.outbox.backlog`, `dpe.outbox.age`, `dpe.dlq.depth`, refreshed by
  `MessagingMetricsScheduler` every 5s against a 10s scrape. Each query rides the partial index
  its own worker already uses (`idx_outbox_unpublished`, `idx_dead_letters_pending`), so it reads
  only the active set and its cost stays flat as the archive grows.
- `OutboxRepository.oldestUnpublishedAgeSeconds()` — `MIN(created_at)` under the same predicate as
  the partial index, so it is an index scan that stops at the first entry.
- `InboxGate` — `dpe.inbox.accepted` and `dpe.inbox.duplicate`, tagged by topic.

**In `payment-orchestrator`:**

- `SagaMetrics` — `dpe.saga.started`, `dpe.saga.terminal{status}`, `dpe.saga.duration{status}`
  (a Timer, with buckets from `application.yml` so Prometheus computes the quantiles), and
  `dpe.saga.inflight{state}` gauges refreshed from one `GROUP BY` query.
- `SagaInstanceRepository.countInFlightByStatus()` — native, with the terminal list written out to
  match `idx_saga_instances_in_flight` character for character, because a parameterised `NOT IN`
  cannot be proven to imply the partial index's predicate and the planner would fall back to a
  sequential scan over every saga ever run — on a timer, forever. That makes it the fifth place the
  terminal set is written down: `SagaStatus`, the `saga_status_known` CHECK, the partial index,
  `verify-invariants.sh`, and now the two native queries in this repository. They change together
  or I4 stops meaning anything.
- `dpe.idempotency.request{outcome}` in `IdempotencyGate` — `new`, `replay`, `in_flight`,
  `conflict`, all four pre-registered.
- `dpe.idempotency.cache{result}` in `IdempotencyCache` — `hit`, `miss`, `unavailable`, kept as
  three and not two for the same reason the M4 `LockOutcome` refactor existed: "switched off" and
  "threw" are not misses, and an operator reading a 0% hit ratio needs to know which they have.

**Tests:** `SagaMetricsTest` (5) and `MessagingMetricsTest` (4). 156 → 165.

### What broke

1. **Micrometer appends `baseUnit` to the Prometheus metric name.** `.baseUnit("messages")`
   publishes `dpe_outbox_backlog_messages`, not `dpe_outbox_backlog` — so every dashboard panel and
   every future alert written against the documented name silently matches nothing. Nothing warns,
   the meter is exported, the scrape is healthy, and the panel just says "No data". Caught only by
   diffing the live `/actuator/prometheus` output against the dashboard's queries. Units dropped
   from the three count gauges; `seconds` kept on `dpe.outbox.age`, where the suffix is the
   Prometheus convention and the dashboard already expected it.

2. **A test that passes alone and fails under `verify`, which is the worst way round.**
   `SagaMetricsTest` asserted `timer().count() == 1`. The `MeterRegistry` is a singleton in the
   shared Spring test context, so every other class that had run a saga to completion in that JVM
   had already incremented it. Green on the machine where it was written, red in a full build.
   Every assertion now captures a before-value and asserts the delta.

3. **A `@Modifying` query still needs a transaction, and the error does not say so.**
   `MessagingMetricsTest` called `InboxGate.claim` directly and got
   `TransactionRequiredException: No active transaction for update or delete query`. Same family as
   the M4 sweeper trap — "no transaction" is not a third option. Fixed with a `TransactionTemplate`
   per claim, which is also the more honest test: two deliveries of a message are genuinely two
   transactions, and running both in one would let the second see the first's uncommitted row.

4. **A counter registered lazily does not exist until it first fires.** `dpe_inbox_duplicate_total`
   was missing from Prometheus entirely after a clean run, because no duplicate had arrived. The
   healthy reading for that meter is zero — and a missing series and an explicit zero look identical
   on a graph while meaning opposite things. `InboxGate` now touches both counters and increments
   one. Same fix applied to the three terminal saga counters and the four idempotency outcomes.

5. **Counting a cache hit before parsing the cached value double-counted it.** The first version
   incremented `hit` and then called `readValue` inside the same `try`; a stored value that no
   longer deserialises would increment `hit` and then fall into the catch and increment
   `unavailable`, so one lookup appeared twice and the hit ratio read above 100% of what happened.
   Parse first, count second.

6. **A verification race that looked like a broken feature.** Forcing `failureRate=1.0`, issuing
   six transfers and immediately restoring `failureRate=0.0` produced six *approved* charges and
   zero compensations — the API returns 202 immediately and the gateway is reached asynchronously
   several hundred milliseconds later, by which time the failure injection had already been turned
   off. Nothing was wrong with the gateway. Worth remembering before M7 writes eight chaos
   scenarios against exactly this endpoint: **a chaos scenario must wait for the sagas to reach a
   terminal state before it restores the fault**, or it asserts on a system that was healthy again
   by the time the work arrived.

7. **`verify-invariants.sh baseline` records I3 at the moment it runs.** Baselining and then
   opening two funded accounts fails I3 by exactly the amount opened. Correct behaviour — the
   invariant caught real money entering the system — but the baseline has to be taken after the
   fixtures exist, not before.

### Verified

165 tests:

```
./mvnw -B -ntp verify

common-messaging       Tests run:  13, Failures: 0, Errors: 0
account-service        Tests run:  64, Failures: 0, Errors: 0
payment-orchestrator   Tests run:  82, Failures: 0, Errors: 0
payment-gateway        Tests run:   6, Failures: 0, Errors: 0
BUILD SUCCESS
```

Live on Compose. 27 transfers across three shapes — fresh, retried, and forced to compensate — and
every meter cross-checks against the database independently:

```
dpe_saga_started_total                    27
sum by (status) (dpe_saga_terminal_total) COMPENSATED=6  COMPLETED=21  FAILED=0
                                          -> 6 + 21 = 27, and psql agrees: COMPENSATED 6

compensation rate                         0.2222   (= 6/27)
saga p95 from _bucket series              3.11s

sum by (outcome) (dpe_idempotency_request_total)
                                          new=27  replay=5  in_flight=0  conflict=1
                                          -> exactly the 27 distinct keys, 5 retries of one key,
                                             and one deliberate reuse with a different amount (409)

sum by (result) (dpe_idempotency_cache_total)
                                          hit=6  miss=27  unavailable=0
                                          -> 6 hits = 5 replays + the conflict, which was also
                                             detected from the cache
```

The inbox gate is live on all four consumer paths across the three services:

```
sum by (application, topic) (dpe_inbox_accepted_total)
  payment-orchestrator   dpe.account.events.v1     58
  account-service        dpe.account.commands.v1   54
  payment-gateway        dpe.gateway.commands.v1   27
  payment-orchestrator   dpe.gateway.events.v1     27
```

Every metric name the dashboard queries resolves, and the meters whose healthy value is zero say
zero rather than nothing:

```
dpe_saga_started_total              1 series      dpe_outbox_backlog             3 series
dpe_saga_terminal_total             3 series      dpe_outbox_age_seconds         3 series
dpe_saga_duration_seconds_bucket   50 series      dpe_dlq_depth                  3 series
dpe_saga_inflight                   4 series      dpe_inbox_duplicate_total      4 series
dpe_idempotency_request_total       4 series      dpe_inbox_accepted_total       4 series
dpe_idempotency_cache_total         3 series

sum by (application,topic) (dpe_inbox_duplicate_total)
  account-service        dpe.account.commands.v1   0
  payment-gateway        dpe.gateway.commands.v1   0
  payment-orchestrator   dpe.account.events.v1     0
  payment-orchestrator   dpe.gateway.events.v1     0
```

Invariants, after the compensations:

```
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (2900000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
```

### Open / next

- **M6 part 2: tracing.** The transactional outbox destroys automatic trace propagation — the
  message is written inside the request's transaction and relayed later on a different thread, so
  the `traceparent` has to be persisted in the outbox row and restored by the relay, or one payment
  becomes five disconnected traces.
- **M6 part 3:** the read endpoints M6.5 needs.
- Alert rules. Every threshold on the dashboard is currently a colour, not a rule — the obvious
  first three are outbox age, DLQ depth above zero, and a compensation rate step change.
- Note for M7: chaos scenarios must wait for terminal sagas before restoring an injected fault,
  per "what broke" item 6.
- Still outstanding from M4: `ErrorHandlingDeserializer`, and retention for `outbox`, `inbox` and
  `dead_letters`.

## Session 13 — 2026-09-10

### Goal

Start M6 part 2: make one payment one trace. Part 1 added metrics, and metrics answer *how many
sagas compensated* — they cannot answer *why this one*. That needs the causal chain across three
processes, and the transactional outbox is precisely the thing that breaks the mechanism which
would otherwise supply it for free.

Built specification-first, the same shape as M3 and M4 part 2: the mechanism, the schema, the
configuration, the Jaeger container and the tests went in first, with the restore inside
`OutboxRelay.drainBatch` left red until it was written against them. It is written, and the suite
is green. What remains is the live run.

### The problem, stated once

Automatic trace propagation rests on an assumption that is never written down: **the outbound call
happens on the thread that is currently inside the span.** Spring MVC honours it. `RestClient`
honours it. Even a plain `kafkaTemplate.send()` honours it — Spring Kafka injects a W3C
`traceparent` header at send time and the consumer extracts it, and tracing across Kafka costs
nothing.

The transactional outbox is built to make that assumption false, and that is the entire point of the
pattern. Nothing is sent on the request thread. A *row* is written inside the business transaction,
and `OutboxRelay` turns it into a Kafka record later, on a scheduled thread, in a different
transaction, possibly after a restart. By then the producing span has ended.

Both ways of ignoring that are silent:

| What the relay does | What Jaeger shows |
|---|---|
| Sends with no `traceparent` | The consumer starts a **new root trace**. One payment becomes four traces, one per hop, joined by nothing. Each looks healthy in isolation. |
| Sends with **its own** context | Every message in one drained batch becomes a child of one `drainBatch` span, so unrelated payments are parented under each other — and it renders as a real trace, which is worse. |

Neither logs anything. Neither fails a test. It is found by opening Jaeger during an incident and
discovering the thing you came for is not there.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Where the context lives between write and send | Two columns on `outbox` — `trace_parent`, `trace_state` | It has to survive a thread, a transaction and possibly a restart. Only a row does that, and putting it on *this* row makes it atomic with the message it describes: a row can never carry the trace of a transaction that rolled back. The outbox pattern applied to itself. |
| Named columns vs a `jsonb` carrier | Two named W3C columns | A jsonb map accepts whatever keys the propagator of the day emits, so a later switch to B3 would leave old and new rows silently disagreeing with nobody obliged to notice. Named columns make a propagator change a migration. `management.tracing.propagation.type: w3c` is pinned to match. |
| What the relay puts on the record | A **child** publish span, not a verbatim copy of `trace_parent` | The copy is two lines and erases the relay hop, which deletes relay lag — the interval between the row committing and the send — from the timeline. A broker stall would then read as a slow producer. |
| Consumer side | `spring.kafka.listener.observation-enabled: true` | This half genuinely is free: Spring Kafka extracts `traceparent` into a consumer observation, so the handler, its JPA calls and its own outbox write all continue the producing trace. |
| Producer side | `spring.kafka.template.observation-enabled: false` | It injects whatever the *sending* thread holds — the relay context, not the payment one — and would overwrite the header the relay just set from the row. Two mechanisms writing one header, the wrong one winning, nothing logged. |
| Behaviour with no tracing bridge | Full no-op, and asserted by a test | The rule Redis is already held to. `Tracer.NOOP` / `Propagator.NOOP`, no service `depends_on` Jaeger, and `OutboxTracingWithoutABridgeTest` proves capture, inject and close are all inert. If deleting the trace backend could fail a payment, the instrumentation has joined the payment path. |
| Sampling | 1.0 locally; decided once at the edge and carried, never re-rolled downstream | The sampled flag is the last byte of `traceparent`, so it rides in the column with everything else. A payment sampled at the orchestrator and dropped at account-service would produce a trace that lies about where the work stopped. |
| Jaeger image | `all-in-one:1.76.0`, not `jaeger:2.x` | The v2 binary configures its memory store from a YAML file and leaves it unbounded by default, which under a 384M container limit is an OOM kill in the middle of a chaos run. `MEMORY_MAX_TRACES=20000` is the same decision as `--maxmemory 64mb` on Redis. |
| Dependencies | `spring-boot-micrometer-tracing-opentelemetry` + `micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp`, not the starter | `spring-boot-starter-opentelemetry` also brings `micrometer-registry-otlp`: a second live metrics registry pushing to localhost:4318 on a timer, in a system that is scraped by Prometheus. |

### Built

**In `common-messaging`:**

- `OutboxTracing` — `capture()` for the producing side, `beginPublish(...)` returning an
  `AutoCloseable` `PublishSpan` with `injectInto(Headers)`, `error(Throwable)` and `close()` for the
  relay side. `io.micrometer:micrometer-tracing` at compile scope: the API only, exactly as
  `micrometer-core` was added at part 1. The module instruments; it does not decide where the data
  goes, and a service with no bridge still builds and still runs.
- `CapturedTrace`, a two-field record with a `NONE` constant. The two values are meaningless apart —
  a `tracestate` without its `traceparent` names a sampling decision for a trace nobody can identify
  — so keeping them together makes it impossible to persist half of one.
- `OutboxMessage.traceParent` / `traceState`, both `updatable = false`. A republish after a failed
  send must carry the *same* parent; re-capturing at publish time would parent the retry under the
  relay instead of under the payment.
- `OutboxWriter.append` now captures, inside the business transaction, on the producing thread.
- `OutboxRelay` takes `OutboxTracing`, and its javadoc records why this class is the one component
  in the system that cannot get tracing for free.

**Schema:** `account-service V5`, `payment-gateway V3`, `payment-orchestrator V6` — the same two
columns in each. Nullable (a message written by the timeout sweeper has no span, and that is normal
rather than exceptional) and unindexed: nothing ever queries *by* trace context, since the row is
already claimed by primary key, and an index on a high-cardinality column no predicate mentions is
write amplification on the busiest table in the service.

**Infrastructure:** Jaeger on `localhost:16686`, OTLP/HTTP receiver published on 4318 so a service
run from an IDE reports to the same collector as one in Compose. No service depends on it, by
design.

**Configuration:** sampling probability, `propagation.type: w3c`, the OTLP endpoint, and both Kafka
observation switches, in all three services. The test classpath sets sampling to 1.0 (a
probabilistic sampler makes a propagation assertion flaky in the worst way — green locally, red
once in ten runs in CI, for a reason that looks like the code) and turns export off, so no test JVM
opens a batch exporter against a port nothing is listening on.

**Docs:** ADR 0006 — the trace context is a column, not a thread local.

**Tests:** `OutboxTracingWithoutABridgeTest` (3) and `OutboxTracePropagationTest` (5). 165 → 173.

### What broke

1. **Boot 4.0 moved the entire OTLP property family and deprecated the old keys at level ERROR.**
   `management.otlp.tracing.endpoint` is now
   `management.opentelemetry.tracing.export.otlp.endpoint`, and
   `management.otlp.tracing.export.enabled` is now `management.tracing.export.otlp.enabled`. The
   old names are what every tutorial and every pre-2025 answer uses, and they are what I wrote
   first.

   The failure is the quiet kind. The key does not bind, so the exporter falls back to the OTLP
   **default** of `http://localhost:4318` — which inside a container is the container itself. The
   service starts happily, exports into nothing, and Jaeger stays empty in a way that looks like an
   instrumentation bug rather than a renamed property. Found by reading the deprecation level out of
   the jar's `spring-configuration-metadata.json` instead of assuming a deprecated key still works;
   "deprecated" and "still binds" are different claims, and at level `error` only the first is true.

2. **There is no `spring-boot-starter-micrometer-tracing`.** Metrics got
   `spring-boot-starter-micrometer-metrics`, so the symmetric guess is the natural one — and it 404s
   on Maven Central. Boot 4 names the tracing starter after the *backend* instead:
   `spring-boot-starter-opentelemetry`. Same split-by-technology reorganisation that moved
   `@EntityScan`, `@AutoConfigureMockMvc` and `TestRestTemplate`, but with a naming asymmetry that
   makes it harder to guess than any of those.

3. **A specification test that fails with a `NullPointerException` teaches nothing.** The first
   draft of `thePublishSpanIsAChildAndNotACopy` split the `traceparent` header without first
   checking it existed, so against an unwritten relay it errored inside `String.split` rather than
   reporting the missing header. Fixed by asserting presence first, with a message naming which of
   the three failures to fix first. The failure message is the whole product of a test written
   before the code.

4. **A stale `target/test-classes` presented as a missing third-party class.** Once the relay
   landed, `payment-orchestrator` failed at test *discovery* with `NoClassDefFoundError: RSAKey`,
   thrown out of `TestTokens` — a class untouched since M5. Reproducible across two runs;
   `mvn clean` on that module fixed it with no source change.

   Worth recording for two reasons. The name in the error is the **bare** `RSAKey`, not
   `com.nimbusds.jose.jwk.RSAKey`, and `nimbus-jose-jwt` was on the dependency tree at compile
   scope throughout — so the error is not describing a real missing dependency, and investigating
   it as one costs time. And it surfaced immediately after a POM change added three tracing
   artifacts, which is exactly when the instinct is to blame the new dependency. The rule that
   generalises: **when a module fails and its sources did not change, `clean` it before believing
   anything the error says.**

5. **Two mistakes made while verifying, both in the harness around the system rather than in it.**

   `FAILED` is not the opposite of `COMPENSATED`. The declined transfer returned
   `status: FAILED, sagaStatus: COMPENSATED, failureReason: GATEWAY_DECLINED`, and those answer
   different questions: the transfer status is the customer-facing outcome (the payment did not
   happen), the saga status is what the mechanism did (it compensated, the money came back). A poll
   loop watching only `status` cannot distinguish "compensated cleanly" from "rejected outright" —
   a distinction the chaos scenarios at M7 will need.

   And I3 fails legitimately if a funded account is opened *after* the baseline is recorded. I3 is
   money conserved; funding issues new money into the ledger, debited from the SYSTEM account, which
   is why I1 still passed while I3 reported 3600000 against a 3400000 baseline that predated the new
   account. The rule, which M7 and M8 both have to follow: **record the I3 baseline once the account
   set is fixed and before any transfer runs.**

### Verified

```
./mvnw -B -ntp clean test

  common-messaging       Tests run: 16, Failures: 0, Errors: 0, Skipped: 0
  account-service        Tests run: 69, Failures: 0, Errors: 0, Skipped: 0
  payment-orchestrator   Tests run: 82, Failures: 0, Errors: 0, Skipped: 0
  payment-gateway        Tests run:  6, Failures: 0, Errors: 0, Skipped: 0

BUILD SUCCESS          173 total

docker compose -f infra/docker-compose.yml config -q     -> OK
```

`OutboxTracePropagationTest` is green in both halves: a message written inside a span stores that
span's `traceparent` on the row, a message written outside one stores NULL, and the relay puts the
stored context back onto the record as a **child** span — the test asserts the trace id matches the
row and the span id does *not*, which is what separates a real publish span from a verbatim header
copy.

### Verified live

Nine containers healthy, Jaeger among them, three migrations applied against the real databases.
One transfer, `COMPLETED`, and **one trace: 23 spans across all three services**, read back from
the Jaeger API:

```
SPAN                                       SERVICE                   START     DURATION
http post /api/v1/transfers                payment-orchestrator     +0.0ms     275.56ms
  dpe.account.commands.v1 publish          payment-orchestrator   +437.3ms      89.91ms
    dpe.account.commands.v1 process        account-service        +530.3ms     101.49ms
      dpe.account.events.v1 publish        account-service       +1061.8ms       9.97ms
        dpe.account.events.v1 process      payment-orchestrator  +1073.6ms      48.16ms
          dpe.gateway.commands.v1 publish  payment-orchestrator  +1553.9ms      12.91ms
            dpe.gateway.commands.v1 proc.  payment-gateway       +1597.8ms     205.27ms
              dpe.gateway.events.v1 publ.  payment-gateway       +2225.9ms      84.78ms
                dpe.gateway.events.v1 pr.  payment-orchestrator  +2296.6ms      27.72ms
                  dpe.account.commands.v1  payment-orchestrator  +2588.9ms       8.60ms
                    ... process            account-service       +2600.3ms      35.52ms
                      ... events publish   account-service       +3105.6ms       8.48ms
                        ... process        payment-orchestrator  +3115.7ms      18.78ms
```

The interesting number is the **first gap**. The HTTP span ends at 275ms; the reserve command is not
published until +437ms. That 162ms is relay lag — the row sitting committed in the outbox waiting
for the next poll — and every later hop shows the same 500ms poll interval as a span of its own. A
verbatim copy of `trace_parent` onto the record would have hidden all of it: the consumer would hang
directly off the request, and there would be no span to hold the interval. That was the argument for
making the relay open a child span, and it is now the observed behaviour rather than an argument.

The compensation branch, forced with `failureRate: 1.0`, is a second complete trace (22 spans), the
`dpe.event.type` tag naming each hop:

```
http post /api/v1/transfers              payment-orchestrator     +0.0ms
  dpe.account.commands.v1 publish        payment-orchestrator    +98.8ms  ReserveFunds
    dpe.account.commands.v1 process      account-service        +107.9ms
      dpe.account.events.v1 publish      account-service        +131.7ms  FundsReserved
        dpe.account.events.v1 process    payment-orchestrator   +140.3ms
          dpe.gateway.commands.v1 publ.  payment-orchestrator   +617.0ms  ChargeGateway
            dpe.gateway.commands.v1 pr.  payment-gateway        +626.6ms
              dpe.gateway.events.v1 pub. payment-gateway        +836.0ms  GatewayDeclined
                dpe.gateway.events.v1 p. payment-orchestrator   +843.6ms
                  dpe.account.commands.. payment-orchestrator  +1138.7ms  ReleaseFunds
                    ... process          account-service       +1148.8ms
                      ... events publish account-service       +1660.9ms  FundsReleased
                        ... process      payment-orchestrator  +1669.5ms
```

Every outbox row written during the run carries a trace context — all three services, every event
type, no exceptions:

```
payments_db   ChargeGateway 5/5   CommitFunds 4/4   ReleaseFunds 1/1   ReserveFunds 5/5
accounts_db   FundsReserved 5/5   FundsCommitted 4/4  FundsReleased 1/1
              AccountOpened 4/4   FundsTransferred 2/2
gateway_db    GatewayApproved 4/4  GatewayDeclined 1/1
```

Invariants after three further transfers, checked at quiescence:

```
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (3600000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
```

### Open / next

- **M6 part 3:** the read endpoints M6.5 needs.
- Alert rules. Every threshold on the dashboard is still a colour, not a rule — outbox age, DLQ
  depth above zero, and a compensation rate step change are the obvious first three.
- Note for M7: chaos scenarios must wait for terminal sagas before restoring an injected fault.
- Still outstanding from M4: `ErrorHandlingDeserializer`, and retention for `outbox`, `inbox` and
  `dead_letters`.

---

## Session 14 — 2026-09-10

### Goal

M6 part 3: the read endpoints. Everything built so far makes *writes* correct — the saga, the
outbox, the idempotency gate, the ownership checks. Nothing yet lets anyone see the result except
by opening psql. These are the endpoints the demo console is built on, and they are landed now
rather than during the UI build so they get designed rather than improvised.

Reads turn out to have three constraints the write path never raised:

1. **Offset pagination is broken on a table that is still being written to.** `OFFSET 5000` makes
   Postgres produce and discard five thousand rows, so the deepest page is the most expensive one
   — and, worse, a row inserted between page 1 and page 2 shifts every row down a position, so one
   row is returned twice and one is never returned at all.
2. **A read endpoint queries the write path's tables.** Same rule as the M6 gauges, different
   budget: a gauge runs every 10s forever, an operator query runs on demand. The difference has to
   be *stated*, or somebody turns the invariants endpoint into a gauge.
3. **A read endpoint is an authorization surface.** The timeline exposes topics, message ids and a
   trace id — operational detail about one customer's money.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Pagination | Keyset, opaque base64 cursor | An offset names a *count of rows*; a keyset names a *place in the data*. Only the second still means the same thing after a concurrent insert. Opaque so the cursor's shape is not a published API. |
| Transfer cursor | `(created_at, id)` | `created_at` alone is not unique. Two transfers committed in the same microsecond are ordered arbitrarily and a boundary between them loses or repeats one. |
| Ledger cursor | `id` alone | `ledger_entries.id` is a BIGSERIAL, already a total order. No tiebreak needed — and the table is append-only, so a cursor can never point at a row that moved. |
| Cursor codecs shared? | No — one per service | They encode different things. A shared abstraction would be the union of both and would couple two services' pagination contracts to save about fifteen lines. |
| List response shape | Summary, no `sagaStatus` | Including it means joining `saga_instances` for every row of every page. A list answers "which ones"; a detail endpoint answers "what happened to this one". |
| Total count | Omitted | An unbounded `COUNT` on every page request, for a number that is stale as it is rendered. |
| Page size | Clamped, not rejected | `?size=1000000` is a denial-of-service parameter with a friendly name. Clamping still returns data and a cursor. |
| Invariants endpoint | One per service, per database | I1/I2/I3/I5 are statements about `accounts_db`; I4 about `payments_db`. A single endpoint answering all five needs one process with credentials to both — a shared-database architecture reintroduced through the monitoring door. |
| I3 on that endpoint | A total, not a verdict | Conservation is a statement about *two instants*; the endpoint only sees one. Returning `holds: true` would show five green lights and be a lie in the one place this system claims to prove something. |
| Reading an account | Opened to customers, scoped in-request | The role rule says a customer may read *some* account; which one is a path variable no matcher has bound. `AccountService.getVisibleTo` decides, so a second caller of the service cannot skip it. |
| Not-yours on a read | 404, not 403 | Same call as `GET /transfers/{id}`. Folding "not yours" into "not found" means no code path can tell them apart, so none can leak the difference later. |
| Timeline assembly | Pure function, no Spring | The pairing rule is the only real logic on the read path, and its interesting inputs are the shapes a *broken* system produces — which an integration test cannot easily create. |

### Built

**payment-orchestrator**

- `GET /api/v1/transfers?cursor=&size=` — keyset-paged, scoped by `initiated_by` **in the WHERE
  clause**. Filtering after the LIMIT would return short pages, and the number of rows removed is
  itself information about other people's traffic.
- `GET /api/v1/transfers/{id}/timeline` — the saga row, the stages, and the message behind each.
  `saga_steps` is append-only with two rows per step, so per-step latency comes for free and a
  stage with a start and no end *is* the stall. Each stage carries the outbox row (with
  `publishedAt − createdAt` as relay lag) and the inbox row that closed it, joined in one query by
  `message_id` — a left join to both tables fills in exactly one side, and which side it filled in
  is the direction of the message. The W3C trace id is parsed off `outbox.trace_parent` for a
  Jaeger deep link.
- `GET /admin/invariants` — I4, flagged `requiresQuiescence`, with the in-flight breakdown by state.
- `V7__read_paths.sql` — `idx_transfers_initiated_by` rebuilt as `(initiated_by, created_at DESC,
  id DESC)`; new `idx_outbox_aggregate (aggregate_id, created_at, id)`.

**account-service**

- `GET /accounts/{id}` now scoped to the owner (operator may read any).
- `GET /accounts/{id}/ledger?cursor=&size=` — signed amounts passed through exactly as stored, with
  the authoritative `balance_minor` alongside. Both queries in one read-only transaction, so the
  balance and the entries are consistent with each other.
- `GET /admin/invariants` — I1, I2, I5 as checks; I3 as a `conservation` block
  (`customerBalanceMinor + activeHoldsMinor = totalMinor`).
- `V6__read_paths.sql` — `idx_ledger_entries_account (account_id, id DESC)` replacing the
  `account_id`-only index.
- The `/accounts/**` security rule split: `POST /accounts` stays OPERATOR; the two GETs are
  `hasAnyRole(USER, OPERATOR)` with the resource check inside the request.

### What broke

**Postgres plans the row-value comparison as an index condition and the hand-expanded OR as a
filter.** This was an argument in a comment until it was measured. `(created_at, id) < (?, ?)` and
`created_at < ? OR (created_at = ? AND id < ?)` are logically identical; the planner will not
reassemble the second into a range scan:

```
-- row-value form
Index Scan using idx_transfers_initiated_by on transfers
  Index Cond: ((initiated_by = 'alice') AND (ROW(created_at, id) < ROW(now(), '000...'::uuid)))

-- hand-expanded OR form
Index Scan using idx_transfers_initiated_by on transfers
  Index Cond: (initiated_by = 'alice')
  Filter: ((created_at < now()) OR ((created_at = now()) AND (id < '000...'::uuid)))
```

Same answer, different cost, and the difference only appears once the table is large enough that
nobody is watching. It is also why the query is native: HQL cannot express a row-value comparison.

**Two schema constraints refused the test fixtures, and both were right.** A helper inserting
`saga_instances` rows for the I4 test was rejected by `saga_completed_at_iff_terminal`
(`completed_at` must be set exactly when the status is terminal — a *seventh* place the terminal
set is written down) and then by `saga_compensation_needs_a_hold` (a saga that compensated must
name the hold it released). Both are the schema refusing to hold a state the system could never
reach. That is the payoff of putting correctness in constraints: a fixture cannot fabricate an
impossible row, so an assertion cannot accidentally be about one.

**I5 cannot be forced red from outside the service — `accounts_customer_balance_non_negative`
enforces it in the database.** A test written to corrupt a balance and watch the check fail was
refused by the constraint. The test now asserts the true and more interesting fact: the overdraft
I5 looks for is *structurally impossible*, and the endpoint is a second, independent read of
something already enforced. I1 and I2 are the opposite — their enforcement is the discipline of
writing balanced pairs, so they *can* be broken by going around the service, and the test does
exactly that to prove the check is real.

**Three services crashed on startup with `No resolvable bootstrap urls given in
bootstrap.servers`.** Compose recreated Redpanda while the services were already starting, so DNS
for `redpanda` failed at exactly the wrong moment. Not a code fault and not a `depends_on` gap —
the containers came up clean on a plain restart. Worth recognising because the message sounds like
a configuration error and is a startup race.

### Verified

Nine containers healthy, `V7` and `V6` applied against the real databases.

Keyset paging, live, `size=3` over one subject's history — page 2 shares nothing with page 1 and
the order is strictly newest-first:

```
page 1  9818cdd7 COMPLETED 40000  2026-09-10T09:56:43.766786Z
        c33611eb COMPLETED 30000  2026-09-10T09:56:43.559571Z
        e5bcf166 COMPLETED 20000  2026-09-10T09:56:43.261293Z
page 2  53e31917 COMPLETED 10000  2026-09-10T09:56:42.891219Z
        cf4c9b1b COMPLETED 15000  2026-09-10T09:03:15.166505Z
        46057471 COMPLETED 15000  2026-09-10T09:03:14.979173Z
```

A completed transfer's timeline — three stages, per-step latency, and relay lag as its own number:

```
saga COMPLETED   traceId 3ac18056097ec9309567d762724f37df
STAGE           OUTCOME    TO           LAT ms  RELAY ms  COMMAND        REPLY
ReserveFunds    SUCCEEDED  RESERVED        738       149  ReserveFunds   FundsReserved
ChargeGateway   SUCCEEDED  CHARGED        1223       488  ChargeGateway  GatewayApproved
CommitFunds     SUCCEEDED  COMPLETED       828       321  CommitFunds    FundsCommitted
```

The compensation branch, forced with `failureRate: 1.0` — the same shape, and the decline visible
as a `FAILED` stage that moves the saga to `COMPENSATING`:

```
transfer FAILED  saga COMPENSATED  reason GATEWAY_DECLINED
ReserveFunds    SUCCEEDED  RESERVED      608 ms  ReserveFunds  -> FundsReserved
ChargeGateway   FAILED     COMPENSATING  639 ms  ChargeGateway -> GatewayDeclined
ReleaseFunds    SUCCEEDED  COMPENSATED   382 ms  ReleaseFunds  -> FundsReleased
```

That transfer in the sender's ledger, as the money view will draw it — out and back, same transfer
id:

```
 370 CREDIT    25000  c66fa6f3-aa47-4419-87ed-0fd605718fa5
 367 DEBIT    -25000  c66fa6f3-aa47-4419-87ed-0fd605718fa5
```

Authorization, live:

```
alice -> bob's ledger                404
bob   -> bob's ledger                200
operator -> bob's ledger             200
alice -> bob's transfer timeline     404
malformed cursor                     400
no token on the list                 401
customer token on /admin/invariants   403
```

Both invariants endpoints, with the conservation total agreeing with the baseline the shell script
recorded before the run:

```
account-service / accounts_db
  I1  holds   sum 0 over 370 entries
  I2  holds   0 of 17 accounts drifted
  I5  holds   0 customer account(s) negative
  conservation  customerBalance 4200000 + activeHolds 0 = 4200000

payment-orchestrator / payments_db
  I4  holds   0 saga(s) in flight   (requiresQuiescence: true)
```

Query plans for all four new read shapes are index scans — see "What broke" for the row-value
comparison, and:

```
Index Scan using idx_outbox_aggregate on outbox
Index Scan using idx_ledger_entries_account on ledger_entries
  Index Cond: ((account_id = '...') AND (id < 400))
```

Full suite: **209 tests green** (was 173), and the invariants after the run:

```
./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (4200000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
```

### Committed

`2f6028a` — M6 (part 3): the read path - keyset pages, the timeline, and invariants per database

### Open / next

- **M6 is done.** Next is M6.5, the demo console — every endpoint it was specified to need now
  exists.
- Alert rules are still outstanding from part 1: outbox age, DLQ depth above zero, and a
  compensation-rate step change.
- Note for M7: chaos scenarios must wait for terminal sagas before restoring an injected fault.
- Still outstanding from M4: `ErrorHandlingDeserializer`, and retention for `outbox`, `inbox` and
  `dead_letters`.
- The transfer list has no status or account filter. When one is wanted it arrives *with the index
  that serves it*, not before — a filter over the existing index returns short pages and makes the
  cursor's meaning depend on the filter.
