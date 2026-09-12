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
| M6 | Observability — Prometheus metrics, Grafana dashboards, distributed tracing, read endpoints | ✅ **done** |
| M6.5 | Demo console — React UI: transfer tracker, system view, chaos controls | ✅ **done** |
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

---

## Session 15 — 2026-09-10

### Goal

Close two items carried forward from earlier milestones — Prometheus alert rules and
`ErrorHandlingDeserializer` — then build M6.5, the demo console, on the read endpoints M6 part 3
landed.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Alerting pipeline | Rules only, **no Alertmanager** | A notification pipeline with nowhere to notify is theatre, and RAM is the binding constraint. Prometheus evaluates the rules itself, shows them under Status → Rules, and publishes the synthetic `ALERTS` series — queryable, graphable and testable. Routing is configuration downstream of the judgement, and the judgement is what these rules carry. |
| Outbox alert signal | **Age**, not backlog | Depth measures load; age measures liveness. A backlog of 5,000 that is draining is a busy system; a backlog of one that is not draining is a stopped relay. Picking depth is the commonest way to build a dashboard that stays green through an outage. Backlog still gets a warning-level rule, at a threshold that means "cannot keep up" rather than "has stopped". |
| DLQ threshold | `> 0` | The only rule in the file whose number needs no defence but needs an argument for why there is no number to pick. A dead letter is a message that exhausted its full retry budget. There is no healthy rate of that; "alert above 10" would mean nine lost payments are fine. |
| Compensation alert | Relative step change, not an absolute rate | The healthy compensation rate is a property of the payment mix, not of the code — a portfolio with more declining cards legitimately compensates more, and a threshold tuned for one is either deaf or screaming for the other. What is always abnormal is the rate *changing*. Compares the last 5m against the preceding hour and fires on a doubling, with `clamp_min` on both denominators and a throughput floor so an idle system cannot alert. |
| Stuck-saga alert | `min_over_time(...)[15m:] > 0`, excluding `STARTED` | Any in-flight level is legitimate under load, so a threshold on the gauge is meaningless. What is not legitimate is the level never coming *down*. `STARTED` is excluded because the timeout sweeper is the designed answer for a saga stuck there, and a sweeper doing its job would otherwise page every time it worked. |
| `ErrorHandlingDeserializer` | Added, wrapping `StringDeserializer` on **both** key and value, and documented as inert today | See "What broke" — the honest finding is that it changes no observable behaviour in the current configuration. It guards a layer that is currently unreachable, and the reason to place it now is that the change which makes the layer reachable is a one-liner somebody will make for tidiness. |
| Console framework | React + Vite + TypeScript, hand-written CSS | TypeScript so `tsc -b` fails the image build on a type error rather than shipping one. No component library and no CDN: ~80 KB gzipped total, and the console renders with no network access beyond its own origin. |
| Console to services | **nginx, one origin**, prefixes stripped | The alternative was CORS on three services. Rejected because it puts a browser concern into three server configurations, and because CORS is a policy about who may *read a response* and is routinely mistaken for an authorization mechanism — this repo has spent M5 putting every such decision in one place per service. |
| Token handling in the UI | Never parsed | Roles and lifetime come from the `POST /auth/token` body, which the server populates precisely so a client need not open its own credential. Decoding the JWT would be one line and would create a second, subtly different reading of a claim the server already interpreted — the `ROLE_` prefix is exactly where such readings diverge. |
| Role to UI | Tabs **disabled**, not hidden | "OPERATOR sees every operational surface and cannot move money" is a true and interesting statement about this system. Hiding the tab replaces it with the impression that different users get a different product. |
| Invariants panel | Two endpoints, joined in the browser | No service can answer all five, and building one that could would mean a process holding credentials to both databases — a shared-database architecture reintroduced through the monitoring door. `Promise.allSettled`, so one service being down still shows the other half. |
| I3 and I4 in the UI | I3 a total with no light; I4 never red | Conservation compares two instants and a page sees one. I4 counts sagas in flight, which is what a working system under load looks like. Five green lights with a sixth that quietly asserts something it could not check would be a lie in the one place this system claims to prove something. |
| Console metrics tiles | PromQL, not repository calls | Same rule as the M6 gauge budget, one level up: a console querying the payment write path every five seconds would be the observability becoming the outage. Costs one unauthenticated nginx route to Prometheus — accepted and bounded in ADR 0007. |
| Updates | Polling, re-armed after each response | Pushing these numbers would mean a publisher on the write path — a second write beside the business transaction, which is the dual-write problem arriving through the UI door. Re-arming after the response rather than on a fixed interval means a slow backend gets *fewer* requests, not more. |
| Pagination UI | Cursor **stack**, no page numbers | Keyset pagination gives you forward, and back to where you have been, and no "page 7". Offering numbered pages would promise something the API cannot do — and over an OFFSET API, something the database cannot do correctly while rows are being inserted above the window. |

### Built

**`infra/prometheus/alerts.yml`** — six rules in three groups. `OutboxRelayStalled` (age > 30s for
1m, critical), `OutboxBacklogGrowing` (> 1000 for 5m, warning), `DeadLettersPresent` (> 0 for 2m,
critical), `CompensationRateStepChange` (5m against the preceding hour, warning),
`SagasStuckInFlight` (`min_over_time` over 15m, warning), and `TargetDown` — the last of which is
what makes the other five trustworthy, since a crashed service produces no series at all and every
rule written over its metrics then goes silent, which renders identically to healthy.

Mounted as its own file (`rule_files:` in `prometheus.yml`, a second read-only bind in Compose) so
that editing a threshold does not touch the scrape config.

**`ErrorHandlingDeserializer`** on all three consumers, wrapping `StringDeserializer` as the
delegate for both key and value. The full argument is in `payment-orchestrator`'s
`application.yml`; the short version is that a deserializer throws *inside* `consumer.poll()`,
below the listener, so the exception reaches the container with no record attached — the same shape
as the compression trap, and equally uncatchable by `DefaultErrorHandler`. Wrapping converts it
into a record whose headers carry the exception, which the container rethrows *above* the listener
where the error handler can dead-letter it.

**`ui/`** — a React + Vite + TypeScript console, served by nginx as a fourth Compose container
(~15 MB resident; the node build stage is discarded). Not a Maven module: absent from the parent
POM and from the root `Dockerfile`'s `COPY` lists, with its own `ui/Dockerfile`.

- **Transfers** — send a payment; the keyset-paged list; and per transfer the stage timeline built
  from `saga_steps`, each stage carrying the outbox row that took the command out (with its relay
  lag) and the inbox row that absorbed the reply, plus a deep link into the Jaeger trace. Below it
  the money view: both accounts' ledger rows, with this transfer's legs highlighted.
- **System** — the five invariants from both services, the I3 conservation total, non-terminal
  sagas broken down by state, four messaging tiles from PromQL, and a panel listing whatever is
  currently firing out of the `ALERTS` series.
- **Chaos** — the gateway's simulation knobs as sliders, plus five named scenario presets whose
  button text is the claim each one makes about the system.

The idempotency gate is demonstrated in the UI rather than described: the key is generated once per
*attempt* and held across retries, and pressing Send twice with one key returns the first response
with `Idempotency-Replayed: true`. That header is the only place the fact exists, because the body
of a replay is the first request's response byte for byte and therefore cannot mention that it is
one.

**`docs/adr/0007-the-console-is-a-reader.md`** — the four decisions above, with the bound on the
unauthenticated Prometheus route written down rather than left as an oversight.

### What broke

**`main` was already red at `08bc85f`, and `clean` was the whole fix.** `./mvnw verify` failed in
`payment-gateway` with `ClassNotFoundException: ImmutableJWKSet` — and it reproduced with the
working tree stashed, so it predated this session's changes. Two details make it worth recording.
The exception carries the **simple** name, not the fully-qualified one, which is the signature of
an incrementally compiled class whose constant pool holds an unresolved reference; and it surfaces
during surefire's **discovery** phase, so the module reports `Tests run: 0` and `BUILD FAILURE`
with no failing test to look at. Nimbus was on the classpath the entire time (`dependency:tree`
confirms `nimbus-jose-jwt:10.9.1` transitively via `common-security`). `./mvnw clean test` on the
same tree passes. Same family as the two traps already recorded about believing the build's
cheerfulness over the source — with the new wrinkle that the *category* is wrong too: it is not a
test failure and it is not a compile failure, and it is reported as neither.

**`ErrorHandlingDeserializer` turned out to close nothing that is currently open.** This was
carried forward from M4 as a real gap and it is not one. The delegate is `StringDeserializer`,
which decodes bytes as UTF-8 and substitutes replacement characters rather than throwing — there is
no input it rejects. Malformed JSON already fails one layer higher, in Jackson inside the handler,
where `DefaultErrorHandler` has always been able to see it and `RetryClassifier` already files
`JacksonException` as poison. It is still worth having, for one reason stated in the config: the
change that makes the layer reachable is somebody switching the delegate to `JsonDeserializer` to
"clean up the handler", which silently moves parsing below the listener and re-opens a partition
stall. It also does **not** cover decompression, which fails below the deserializer as well —
nothing configured in Spring can intercept that, only `compression.type: none` and a glibc base
image. The general lesson is the one worth keeping: *an item on a to-do list is a hypothesis about
the code, and it ages.*

**The console came up healthy and reported itself unhealthy.** The container healthcheck
(`wget http://localhost/index.html`) failed with `can't connect to remote host` while nginx was
listening correctly on `0.0.0.0:80` — confirmed with `netstat` inside the container, and `nginx -T`
was clean. The cause is IPv6: the config has one `listen 80;` and no `listen [::]:80;`, so nginx
binds IPv4 only, while `localhost` inside the container resolves to `::1` first. busybox wget gets
`ECONNREFUSED` and does not fall back to IPv4. Fixed by using `127.0.0.1` in both the Dockerfile
`HEALTHCHECK` and the Compose healthcheck. Worth remembering because the symptom accuses the wrong
component — it reads as "nginx failed to start", and nginx had started perfectly.

**The README's health-check commands had been wrong since M6.** They still said
`curl localhost:8081/actuator/health`, from before the actuator moved to the unpublished management
ports. Nothing failed, because nothing ran them. Corrected to go through a container. The same
correction is still due for the K8s probes at M10.

**One flaky test, confirmed as a flake rather than assumed to be one.**
`IdempotencyCacheTest.releasingIsConditionalOnTheToken` errored with
`RedisCommandTimeoutException: Connection initialization timed out after 200 millisecond(s)` during
a `verify` run that was sharing the machine with an npm install and a Vite build. Re-run alone:
6/6 green. The 200ms connect timeout is deliberate elsewhere in this system, but it does mean this
class is sensitive to machine load, which is worth knowing before M8 runs k6 on the same laptop.

### Verified

All six alert rules load and evaluate, and one of them was already correct about the system:

```
$ docker exec dpe-prometheus wget -qO- http://localhost:9090/api/v1/rules
  dpe-messaging   OutboxRelayStalled           inactive   health ok
  dpe-messaging   OutboxBacklogGrowing         inactive   health ok
  dpe-messaging   DeadLettersPresent           PENDING    health ok
  dpe-saga        CompensationRateStepChange   inactive   health ok
  dpe-saga        SagasStuckInFlight           inactive   health ok
  dpe-platform    TargetDown                   inactive   health ok
```

`DeadLettersPresent` pending is not a bug — it is the rule correctly seeing dead letters left in
the table by an earlier poison-message run. Both config files pass `promtool`:

```
$ promtool check config /etc/prometheus/prometheus.yml
  SUCCESS: 1 rule files found
  SUCCESS: /etc/prometheus/prometheus.yml is valid prometheus config file syntax
  Checking /etc/prometheus/alerts.yml
  SUCCESS: 6 rules found
```

The deserializer is live, read out of the running container's own startup log:

```
$ docker logs dpe-account | grep deserializer
  key.deserializer   = class org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
  value.deserializer = class org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
```

Full suite after a `clean`: **209 tests, 0 failures**, plus the one Redis flake above that passes
on re-run.

Console build: `tsc -b && vite build` clean — 254 KB raw, **79 KB gzipped**, plus 5.6 KB of CSS.

Every console route exercised through nginx on `:5173`, not against the services directly:

```
index.html                                     200 text/html
POST /api/orchestrator/auth/token   (alice)    200
GET  /api/orchestrator/api/v1/transfers        200
GET  /api/orchestrator/admin/invariants        403   <- alice is USER, not OPERATOR
POST /api/orchestrator/auth/token   (operator) 200
GET  /api/orchestrator/admin/invariants        200   I4 holds, 0 in flight
GET  /api/accounts/admin/invariants            200   I1 I2 I5 hold, conservation 4200000
GET  /api/gateway/admin/simulation             200
GET  /api/prom/api/v1/query?query=...          200   success
```

The idempotency gate, twice with one key:

```
POST 1   HTTP/1.1 202   Idempotency-Replayed: false
POST 2   HTTP/1.1 202   Idempotency-Replayed: true
$ diff r1.json r2.json   ->   BODIES IDENTICAL BYTE FOR BYTE
```

A live transfer read back through the timeline endpoint — three stages, each with its outbox hop,
its relay lag and the inbox row that absorbed the reply, and a trace id for the Jaeger link:

```
transfer 600ffb68  COMPLETED   saga COMPLETED   traceId 4eba7c02924ba0d3fd2608f47b9a9822

  ReserveFunds   SUCCEEDED -> RESERVED   1741ms
      out  ReserveFunds    -> dpe.account.commands.v1   relay lag 679ms
      in   FundsReserved   <- dpe.account.events.v1
  ChargeGateway  SUCCEEDED -> CHARGED     602ms
      out  ChargeGateway   -> dpe.gateway.commands.v1   relay lag  67ms
      in   GatewayApproved <- dpe.gateway.events.v1
  CommitFunds    SUCCEEDED -> COMPLETED   315ms
      out  CommitFunds     -> dpe.account.commands.v1   relay lag  83ms
      in   FundsCommitted  <- dpe.account.events.v1
```

The compensation path forced through the console's own chaos route (`failureRate: 1.0`), then
reset:

```
transfer 2efa139e  status FAILED   saga COMPENSATED   reason GATEWAY_DECLINED
  ReserveFunds   SUCCEEDED -> RESERVED
  ChargeGateway  FAILED    -> COMPENSATING
  ReleaseFunds   SUCCEEDED -> COMPENSATED
```

and the money visibly returned to the sender in the ledger — a `-5000` DEBIT followed by a `+5000`
CREDIT against the same `transfer_id`, which is the point: nothing was rolled back, a second and
opposite pair of entries was written.

Invariants after all of it:

```
$ ./scripts/verify-invariants.sh
  PASS  I1  global ledger sum is zero
  PASS  I2  every account balance equals the sum of its ledger entries
  PASS  I3  total money conserved (4200000)
  PASS  I4  no saga left in a non-terminal state
  PASS  I5  no customer account holds a negative balance
```

### Committed

`4c0deed` - M6.5: the console - and two carried-forward items, one of which was already fixed

### Open / next

- **M7, the chaos suite.** The console now makes the scenarios watchable, which was the reason to
  build it first. The standing note applies: a scenario must wait for sagas to reach a terminal
  state before restoring an injected fault, or the result describes neither the fault nor the
  healthy system.
- Retention for `outbox`, `inbox` and `dead_letters` is still outstanding from M4, and is not a
  copy of `IdempotencySweeper`: an `inbox` row is not deletable on the same reasoning an
  `idempotency_records` row is, because the thing that might redeliver is the broker. The policy
  has to be argued per table before any code is written.
- `OutboxBacklogGrowing`'s threshold of 1000 is the weakest number in `alerts.yml` and is honestly
  a placeholder. M8's k6 run is what should set it, by measuring what the relay actually sustains.
- The wire types in `ui/src/api/types.ts` are hand-transcribed from the Java records and nothing
  checks them against the server — a renamed field is `undefined` at runtime, not a build failure.
  If M9 produces an OpenAPI document, generate them from it.
- The transfer list still has no status or account filter, and still only gets one together with
  the `(initiated_by, status, created_at DESC, id DESC)` index that serves it.

---

## Session 16 — 2026-09-11

### Goal

Start M7: a chaos harness that runs against the real Compose deployment, the eight scenarios on it,
and the first runs. The plan's hypothesis for every scenario was the same sentence — "the
invariants still hold" — and the first thing the suite did was show that sentence is not strong
enough.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Where scenarios run | Bash against the live Compose stack, not JUnit + Testcontainers | The faults are container-level — SIGKILL, `docker pause`, `docker network disconnect` — and the thing under test is the deployment, including its restart times, healthchecks and DNS aliases. A test that owns the containers it breaks can make them behave better than the real ones do. |
| Starting condition | Refuse to start unless **quiescent** | Quiescent = no saga in a non-terminal state AND no unpublished row in any of the three outboxes. A scenario that inherits in-flight work from the last one asserts about two faults at once and proves neither. Exit code 2, distinct from a refuted hypothesis. |
| Fault restore order | Wait for every saga in the batch to be terminal, **then** restore | Carried from M6: the API answers 202 before the gateway is reached, so restoring straight after the last POST tests a system that has already recovered. |
| I3 baseline | Recorded after the scenario's accounts are opened | Carried from M6.5: funding an account issues new money, and I3 would correctly call a later baseline a violation. |
| Checks beyond I1–I5 | Four more, S1–S4, run after quiescence | The five invariants are statements about one database at a time. S1 (no `ACTIVE` hold), S2 (every approved PSP charge belongs to a `COMPLETED` transfer), S3 (the converse), S4 (no `PENDING` transfer under a terminal saga). |
| Cross-database reads | Allowed in the harness, still forbidden in services | The harness is an operator's tool holding a superuser connection for the length of a test, not a component on the payment path. The M6 rule — no service may hold credentials to two databases — is about what a service may be, and is unchanged. |
| Scenario 3's window | Broker **paused** first, so committed-but-unpublished rows are a certainty rather than a race | The real window is the relay's 500 ms poll interval, far too narrow to hit by timing a kill. Pausing makes the precondition checkable: the scenario asserts the rows were stranded before it kills anything. |
| Scenario 8's heal | `docker network connect --alias account-service --alias dpe-account` | Reconnecting without the alias restores the container's network but not its service name, so every client addressing it as `account-service` stays partitioned after the "heal" — which reads as a bug in the system under test. |

### Built

**`chaos/lib.sh`** — the shared skeleton: `begin_scenario` (health, quiescence, fault reset, and a
trap-based cleanup that unpauses, reconnects and restarts whatever a dying scenario left broken),
`open_account` (which waits until the orchestrator's ownership projection has the account —
otherwise the first transfer is refused 403 during replication lag), `fire_transfers` (N POSTs, P
in flight, through `xargs -P`), `wait_terminal`, `wait_quiescent`, and `run_checks` (I1–I5 through
`scripts/verify-invariants.sh`, then S1–S4).

**Eight scenarios**, `chaos/01` to `chaos/08`, each opening with its hypothesis and each ending in
`finish_scenario`. Two take a parameter because one fault has two meaningfully different timings:
`01` takes the outage length (under or over the 30 s saga deadline) and `02` takes whether the
participant dies before the reserve or after the charge. `06` runs with Redis stopped as well as
running, since only the Redis-less run proves the unique index is the guarantee. `chaos/run-all.sh`
runs them sequentially and summarises HELD / REFUTED / NOT RUN; `chaos/README.md` lists them.

Also corrected `SimulationController`'s Javadoc, which still described the endpoint as
unauthenticated two milestones after it was put behind `OPERATOR`, and fixed the I3 baseline in
`scripts/verify-invariants.sh` (item 9 below).

### What broke

**1. The fault injector failed silently and the scenario nearly passed.** The first run of scenario
4 set `failureRate: 1.0` with an unauthenticated POST, got a 401 it did not check, and fired thirty
transfers through a gateway that approved every one. I1–I5 and S1–S4 all passed — they are true of
thirty successful transfers. Only the scenario's own assertion ("were they compensated?") failed.
The cause was a stale comment: `SimulationController`'s Javadoc still said "deliberately
unauthenticated", and M5 had put `/admin/**` behind `OPERATOR`.

The rule taken from it: **a fault injector that can fail quietly makes every scenario pass.**
`gateway_set` now requires a 200 and logs the knobs the gateway reports holding *after* the call,
so the log records what was actually in force. And every scenario asserts the fault's *effect*,
not only the invariants — invariants cannot tell a fault that was survived from a fault that never
happened.

**2. Scenario 2 refuted the saga design, as predicted from reading the code before the run.**
account-service was killed after every reserve had completed and while the gateway (slowed to 3 s)
was still charging. The PSP approved with the participant dead, so each saga reached `CHARGED` with
its `CommitFunds` waiting in the topic. Then the deadline passed. One saga's trail:

```
06:48:13  ReserveFunds   SUCCEEDED  -> RESERVED
06:48:23  ChargeGateway  SUCCEEDED  -> CHARGED
06:48:23  CommitFunds    STARTED       CHARGED
06:48:47  ChargeGateway  TIMED_OUT  -> COMPENSATING     no reply before the deadline
06:48:47  ReleaseFunds   STARTED       COMPENSATING
06:48:52  ReleaseFunds   TIMED_OUT     COMPENSATING     (re-emitted by the sweeper)
   ... three more, every 5 s ...
06:49:07  ReleaseFunds   TIMED_OUT     COMPENSATING     sweep_attempts = 5, never swept again
06:49:28  CommitFunds    SKIPPED       COMPENSATING     arrived while the saga was COMPENSATING
```

account-service came back and consumed its partition in order: `CommitFunds` first — the hold
settled, bob was paid — then all six `ReleaseFunds`, each finding a `COMMITTED` hold and returning
**without a reply** (30 log lines of `ReleaseFunds ... ignored: already COMMITTED`). Its
`FundsCommitted` reached a saga that had already moved to `COMPENSATING` and was skipped. Final
state: holds `COMMITTED`, bob +42000, PSP charges `APPROVED`, transfers `PENDING`, sagas
`COMPENSATING` forever.

```
  FAIL  I4  6 saga(s) stuck in a non-terminal state
  FAIL  S2  6 transfer(s) CHARGED at the PSP but not COMPLETED
```

Three distinct defects, and the first is the real one:

- **The sweeper compensates past the pivot.** In saga terms the gateway charge is the *pivot
  transaction*: the last step that can fail, and the first that cannot be undone by writing an
  opposite row in our own ledger. Before it the saga may go backward; after it, only forward.
  `onTimeout` treats `RESERVED` and `CHARGED` alike, so a timeout in `CHARGED` "compensates" by
  releasing the hold — refunding the customer out of our books while the card network keeps the
  charge. Here the money happened to go the right way only because per-partition ordering delivered
  the commit before the release. The saga's own decision was wrong.
- **A participant answers a command it will not act on with silence.** `release()` on a settled
  hold logs and returns. The saga cannot tell "not yet processed" from "will never be processed",
  so the only thing left to learn from is a deadline — which had already passed. The gateway does
  the opposite, correctly: a repeated charge republishes the *original* outcome.
- **The sweep budget is counted in sweeps, not time.** Five attempts at a 5 s interval is 20 s, and
  all five were spent while account-service was still booting. It changed nothing here — the
  commands were queued in the topic regardless — but a budget that expires inside one container
  restart is not a budget.

I1, I2, I3 and I5 passed throughout. The ledger was never wrong; the *saga* was.

The six were reconciled by hand after checking all three databases per transfer (hold `COMMITTED`,
one 7000 credit to the recipient and no refund leg, PSP `APPROVED`): one transaction that aborts
unless exactly six sagas and six transfers change, and writes a `saga_steps` row on each recording
the evidence. That is the operator's procedure, and it is what the orchestrator's own `STARTED`
branch already anticipates when it says "the honest fix is a reconciliation job".

**3. A timeout in `STARTED` strands the customer's money — three scenarios, three routes.** The
sweeper fails a saga whose reserve has not replied, but it cannot un-send the `ReserveFunds`. When
that command lands late, account-service reserves, and the `FundsReserved` reply is skipped by a
terminal saga. The customer is told the transfer failed; their money is in CLEARING; nothing will
ever move it.

```
02 before-reserve   transfers: FAILED 6    holds: ACTIVE 6    alice: 958000 (42000 short)
  PASS  I1-I5
  FAIL  S1  STRANDED - ACTIVE hold(s) with every saga terminal: 6 new
```

Reached three ways: the participant dead at the reserve (scenario 2 `before-reserve`), the
orchestrator's own consumer unable to hear replies after a restart (scenario 3, item 5), and a
consumer that silently stopped fetching after a broker restart (scenario 1, item 6). I3 passes in
every case because it adds held money back into the total, which is correct mid-run and exactly why
it cannot see money held forever. **This is the reason S1 exists.** At the end of the session 26
holds totalling 110000 were stranded, with I1–I5 green.

**4. A compensated transfer can still be charged by the PSP.** Scenario 5 part B: the PSP stops
answering, each charge dead-letters, the sweeper compensates all ten. The operator then replays the
dead letters with the PSP healthy, and it charges all ten.

```
{"replayed":10}
gateway charges for the compensated transfers: APPROVED 10
transfers: FAILED 10
  PASS  I1-I5
  FAIL  S2  CHARGED at the PSP but not COMPLETED: 10 new
```

The same outcome came from restarting the stalled gateway in item 6 (twelve more). Compensation
here only reaches our own ledger; nothing tells the gateway that the transfer is void, so a late
charge is indistinguishable from a first one. A compensation has to be able to arrive *before* the
thing it compensates and still win — the gateway needs to remember a void for a transfer it has not
charged yet.

**5. The orchestrator's sweeper runs while the orchestrator cannot hear.** Scenario 3 held on its
own claim — every stranded `ReserveFunds` was published exactly once by the restarted relay, one hold
each — and failed on what happened next:

```
08:09:07.7  saga created, deadline 08:09:37
08:09:10.2  orchestrator container started
08:09:25.9  application started: HTTP, relay and sweeper all running
08:09:26.5  ReserveFunds published by the relay
08:09:26.7  account-service reserves and replies
08:09:41.3  sweeper: ReserveFunds TIMED_OUT -> FAILED
08:09:49.6  orchestrator's consumer finally joins the group
08:09:49.9  the FundsReserved reply is read, and SKIPPED
```

For 23 seconds the orchestrator could send commands and could not receive a single reply, and its
sweeper spent that window judging other services by a clock. The join came ~41 s after the SIGKILL,
consistent with the dead instance's group members being held until their session expired (the
client's default `session.timeout.ms` is 45 s and is not overridden) — consistent with, not proven.
A timeout is only meaningful while the thing measuring it can hear the answer.

**6. After a broker restart, one service's consumer sometimes stops fetching, silently.** Scenario 1
(15 s outage, under the saga deadline) was run four times:

| Run | Result |
|---|---|
| 1 | gateway consumer stalled — group `Stable`, all partitions assigned, lag 12, no error, no rebalance, no log line after the reconnect. The sweeper compensated all twelve transfers and the first version of the scenario reported **HELD** |
| 2 | held — 12/12 `COMPLETED` |
| 3 | orchestrator consumer stalled — lag 12, all twelve sagas `FAILED` in `STARTED`, twelve holds stranded |
| 4 | refused to start: not quiescent |

A thread dump of the stalled gateway showed the listener thread alive inside
`KafkaConsumer.poll` → `NetworkClient.poll`, receiving nothing, while `rpk topic consume` could read
the waiting records at exactly the committed offsets — so nothing was lost or truncated. A restart
cleared it each time. Root cause **not established**. Every affected client logged `Resetting the
last seen epoch ... since the associated topicId changed from null to ...` on reconnect, which points
at the fetch session's handling of topic ids across a broker restart (kafka-clients 4.2.1, Redpanda
v25.3.17), but services that recovered logged the same line. The next experiment is to reproduce it
against Apache Kafka through the `kafka` Compose profile, which is the reason that profile exists.

What makes it dangerous is the combination: no error anywhere, and a timeout sweeper that turns a
stalled consumer into a column of perfectly tidy compensations. It surfaced only because the first
run's vacuous pass was examined rather than accepted.

**7. Redpanda is running in developer mode with write caching on.** `rpk cluster config get
write_caching_default` returns `"true"`: the broker acknowledges a write before it is fsynced, so on
a single node `acks=all` does not mean the message survives a SIGKILL — and the outbox marks a row
published on exactly that acknowledgement. No loss was observed in these runs, but scenario 1 is
testing a weaker broker than the design assumes. Also found: Compose's
`--set redpanda.auto_create_topics_enabled=false` is a no-op — it is a cluster property placed in
node config, which Redpanda reports at startup as `Unknown property auto_create_topics_enabled`.
Auto-creation is off only because that is Redpanda's default.

**8. A dead-lettered record's offset is never committed.** `KafkaErrorHandlingConfig` builds its
`DefaultErrorHandler` without `setCommitRecovered(true)`, and under `manual_immediate` that means the
committed offset stays behind a recovered record until a later record on the same partition succeeds.
Scenario 5 showed it as ten messages of lag with nothing in flight, cleared only when the replay's
new messages were acknowledged past it. A restart inside that window redelivers every dead-lettered
command as if new, in addition to whatever the operator replays.

**9. `verify-invariants.sh` compared two different sums for I3.** The check added active holds to
customer balances; the `baseline` command recorded balances alone. They agreed only when no hold was
active at baseline time — true of every run until item 3 left holds stranded, after which I3
"failed" by exactly their total (48000) on a system that had conserved every paisa. Both now call
one function.

**10. Two more ways the harness nearly lied.** Quiescence was defined as "no saga in flight and every
outbox drained", and the stalled gateway met that definition with twelve commands unread in its
topic — a published, unconsumed message is as much work in flight as an outbox row, so consumer lag
is now the third condition. And S1–S4 were global, so one stranded hold would refute every later
scenario; they now snapshot the violations present at the start and fail only on new ones, printing
the inherited count rather than hiding it.

### Verified

Scenario 4, second run (with authenticated injection):

```
gateway now: {"failureRate":1.0,"latencyMs":50,"timeoutRate":0.0,"duplicateCallbackRate":0.0}
firing 30 transfers
all 30 sagas terminal
  PASS  every POST accepted (30)
  PASS  sagas COMPENSATED (30)
  PASS  transfers FAILED with GATEWAY_DECLINED (30)
  PASS  alice's balance restored exactly (1000000)
  PASS  bob received nothing (0)
  PASS  alice's ledger shows 2N entries (debit + credit per transfer) (60)
  PASS  no new dead letters (a decline is not an error) (1)
  PASS  I1-I5, S1-S4
04 gateway declines everything: HYPOTHESIS HELD
```

Every scenario, against the stack as it stands (no saga fixes yet). Predictions were written down
before each run.

| Scenario | Result | Against the prediction |
|---|---|---|
| 01 broker dies, 15 s | 2 of 4 runs stalled a consumer (item 6); run 2 held, 12/12 `COMPLETED` | Predicted to hold. Refuted by a defect nobody predicted |
| 02 `after-charge` | **Refuted** — I4, S2 (item 2) | As predicted |
| 02 `before-reserve` | **Refuted** — S1, 6 holds stranded, I1–I5 green (item 3) | As predicted |
| 03 orchestrator dies before relay | Outbox claim **held** (8 published once, 8 holds); saga **refuted** — S1 (item 5) | Predicted a race; it happened |
| 04 gateway declines 100% | **Held** — 30/30 compensated, sender restored exactly | As predicted |
| 05 A duplicate callbacks | **Held** — inbox took 20 rows for 10 transfers, state guard skipped 10, bob credited once | As predicted, including that the inbox did *not* dedupe |
| 05 B silent PSP + DLQ replay | **Refuted** — S2, 10 charges for compensated transfers (item 4) | As predicted |
| 06 key stampede ×100, Redis on | **Held** — 100 × 202, one transfer id, one row, bob paid once | As predicted |
| 06 key stampede ×100, Redis off | **Held** — identical | As predicted; the unique index alone is the guarantee |
| 07 hot account, 90 for room of 60 | **Held** — exactly 60 `COMPLETED`, 30 `INSUFFICIENT_FUNDS`, sender exactly 0, recipients exactly 60000, 0 deadlocks | As predicted |
| 08 partition, 20 s | **Held** — 12/12 `COMPLETED`, no restart needed | The half-open-lock case was **not exercised**: the cut landed on no open transaction |
| 01 `OUTAGE=45` | Not run | Its predicted mechanism (item 3) was shown three other ways |

Scenario 7, the arithmetic version of "no lost update":

```
  PASS  every POST accepted (90)
  PASS  exactly CAPACITY completed (60)
  PASS  exactly EXTRA refused for insufficient funds (30)
  PASS  sender drained to exactly zero (0)
  PASS  recipients received exactly the sender's balance (60000)
  PASS  deadlocks in the postgres log during the run (0)
07 hot account (90 transfers, room for 60): HYPOTHESIS HELD
```

Final state: I1–I5 hold; 26 holds totalling 110000 stranded `ACTIVE`; 22 PSP charges for transfers
reported `FAILED`. Both are open reconciliation items, and both are invisible to the five
invariants.

### Committed

Committed with Sessions 17 and 18 as `df330a4` — "M7: the chaos suite - five saga defects it found, their fixes, and the lock that outlives its owner".

### Open / next

In the order they would change the results table:

1. **Forward recovery past the pivot** (item 2). A timeout in `CHARGED` re-emits `CommitFunds` and
   stays `CHARGED`.
2. **Participants always answer** (item 2). A commit or release for a settled hold replies with the
   outcome that actually happened.
3. **A `STARTED` timeout must be safe whether or not the reserve happened** (item 3). The
   compensation has to be addressed by transfer id, not hold id, and account-service has to remember
   it — so a `ReserveFunds` arriving afterwards is refused rather than obeyed.
4. **Compensation must reach the gateway** (item 4) — a void it remembers for a transfer it has not
   charged yet, for the same reason.
5. **The sweeper must not run deaf** (item 5): gate it on the reply consumer holding its partitions,
   and/or give the consumer static membership (`group.instance.id`) so a restart rejoins without
   waiting out the dead member's session.
6. **Isolate the fetch stall** (item 6) on the Apache Kafka profile, and alert on consumer lag that
   does not fall — the existing rules cannot see it, because the sweeper converts it into
   compensations.
7. **Broker durability** (item 7): `write_caching_default: false` via a Redpanda bootstrap file, and
   move `auto_create_topics_enabled` there too so the setting that claims to disable auto-creation
   actually does.
8. **`setCommitRecovered(true)`** (item 8).
9. **Reconcile** the 26 stranded holds and 22 orphaned charges once 3 and 4 exist to say what the
   right answer is.
10. Decide whether S1–S4 join `verify-invariants.sh`, and what that means for "five invariants" on
    both `/admin/invariants` endpoints and the console.

---

## Session 17 — 2026-09-11

### Goal

Close the five saga defects Session 16's chaos run found, fix the three infrastructure findings
that came with them, and re-run the whole suite against the result.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Timeout in `CHARGED` | Re-send `CommitFunds`, stay `CHARGED`, never compensate | The gateway charge is the saga's pivot: the one step that cannot be undone by writing an opposite row. Before it, recovery runs backward; after it, only forward. Compensating after it refunds the sender out of our own books while the PSP keeps the charge — scenario 2 caught exactly that. |
| Attempt cap after the pivot | None; ERROR log on every re-send past `max-sweep-attempts` | A forward step has no alternative to give up in favour of. Capping it leaves the PSP holding the money and the hold unsettled forever; logging makes it the alert. |
| Retry pacing | Each re-send pushes `deadline_at` out one step-timeout | The old budget was attempts × sweep interval (5 × 5 s), spent entirely while a participant was still restarting. Now 5 × 30 s for a compensation, and one commit per 30 s — not per 5 s — to a participant that is down. A forward transition still never resets the deadline, so the fail-safe property is unchanged. |
| A command that finds its work already settled | Reply with what actually happened to the hold | Silence left the saga to learn only from a clock; throwing loops forever. A COMMITTED hold answers `FundsCommitted` whichever command asked, a RELEASED one `FundsReleased`. The release reason moved onto the hold row so a repeated question gets the original answer. |
| Replies that do not match the command sent | Accepted; the saga finishes where the money is | `FundsCommitted` in `COMPENSATING` → `COMPLETED` (the recipient has it); `FundsReleased` in `RESERVED`/`CHARGED` → `COMPENSATED` plus a gateway void. Unreachable from the orchestrator's own commands after the first fix, reachable from a replayed dead letter or a hand-produced command. |
| Compensation addressing | By transfer id; `ReleaseFunds.holdId` optional | A saga that timed out before hearing `FundsReserved` has no hold id — which is why the old `STARTED` branch sent nothing, and why three scenarios stranded money through that gap. |
| account-service tombstone | New `transfer_voids` table + transaction-scoped advisory lock per transfer | A release that finds no hold records a void; a later reserve finds it and is refused. The two sides read different tables, so no unique constraint spans them; the lock (taken first, before any row lock) makes the two check-then-write sequences serial. Partition order makes the race rare, not impossible. |
| Gateway tombstone | A `gateway_charges` row with status `VOIDED`, no new table | The existing UNIQUE on `transfer_id` then *is* the mutual exclusion. The void inserts with `ON CONFLICT DO NOTHING`, which blocks on a concurrent uncommitted charge and then sees it, so the void cannot lose the race; the charge can, and losing means "no charge". Cost: an APPROVED row may now move to VOIDED, once. |
| Sweeper while the orchestrator cannot hear | Gated on the reply listener holding partitions for a 10 s grace | A timeout is a statement about the other side only while this side can hear it. Static group membership was rejected: a static member sends no LeaveGroup on a clean shutdown either, so every deploy would orphan its partitions for a session timeout, and it does nothing when the broker is what restarted. |
| Redpanda cluster properties | `infra/redpanda/bootstrap.yaml`, not `--set` | See What broke 2 and 3. |
| Dead-lettered offsets | `setCommitRecovered(true)` | Under `manual_immediate`, the offset of a record that was dead-lettered was never committed; a restart re-delivered it as new. |
| Reconciling the 26 stranded holds and 22 orphaned PSP charges from Session 16 | Not done this session | The fixes provide the mechanism — a transfer-addressed release and a void, both idempotent — but choosing to move money for 48 historical transfers is a reconciliation decision, not a code change. Left open with a proposal. |

### Built

**The pivot, in code** — `SagaOrchestrator.onTimeout` now has four distinct branches instead of
three. `STARTED`: fail, and send a `ReleaseFunds` with no hold id. `RESERVED`: compensate, and send
a `VoidCharge` to the gateway. `CHARGED`: re-send `CommitFunds`, stay `CHARGED`. `COMPENSATING`:
re-send the release. `SagaInstance.extendDeadline` paces the re-sends; `claimExpired` exempts
`CHARGED` from the attempt cap.

**Truthful replies** — `ReservationService.answerWithWhatHappened` replies to a commit or release
on a settled hold with the event describing how it settled. The recipient of a committed hold is
recovered from the commit's CREDIT leg, since the hold does not record it. The orchestrator's
`onFundsCommitted` / `onFundsReleased` accept the replies that can now arrive in unexpected states.

**Tombstones** — account-service `V7__transfer_voids.sql` (`transfer_voids`, plus
`holds.release_reason`) and `TransferVoidRepository.lockTransfer`; payment-gateway
`V4__charge_voids.sql` (status `VOIDED`, `voided_at`, `void_reason`, and CHECKs tying them
together), `ChargeService.voidCharge` and `GatewayChargeRepository.insertTombstone`. New messages
in `common-events`: `VoidCharge`, `ChargeVoided`, and the rejection code `TRANSFER_VOIDED`.

**The sweeper gate** — `ReplyListenerReadiness` (reads the reply listener's assignment from the
`KafkaListenerEndpointRegistry`) and `dpe.saga.listen-grace: 10s`; `SagaSweepScheduler` skips a
tick while it is closed and logs each transition.

**Infrastructure** — `infra/redpanda/bootstrap.yaml` mounted at `/etc/redpanda/.bootstrap.yaml`
(write caching off, auto-creation off) replacing the `--set` flag; `setCommitRecovered(true)` in
`KafkaErrorHandlingConfig`; a `KafkaConsumerFetchSpin` alert rule.

**Tests** — 15 new: forward recovery and the uncapped `CHARGED` sweep, the `STARTED` release, the
gateway void on a `RESERVED` timeout, both unexpected-reply cases, both orders of release and
reserve, truthful replies for a repeated commit and for a release that finds a committed hold, the
three gateway void outcomes, and the sweeper gate (a stub container, no broker). One existing test
was rewritten because it asserted the old behaviour: `SagaTimeoutTest` used to assert that a
`STARTED` timeout sends *no* release, with a comment conceding the outcome could be wrong; three
chaos scenarios proved it was.

**Chaos harness** — scenarios 2, 3 and 5 now assert the outcome the design promises (completed after
the pivot, sender whole before it, nothing timed out while deaf, the replay charged nobody) rather
than only that no invariant was broken.

### What broke

1. **`@KafkaListener(id = ...)` silently changes the consumer group.** Naming the reply listener so
   the sweeper gate could look it up would, by Spring Kafka's default, have made the id the
   `group.id` — overriding `spring.kafka.consumer.group-id` and moving the orchestrator to a brand-new
   group reading from `earliest`. Every reply ever sent would be re-delivered and every one absorbed
   by the inbox, so nothing would fail and no test would go red. `idIsGroup = false`; confirmed after
   deploy that `rpk group list` still shows exactly the three service groups.

2. **Every Redpanda `--set redpanda.<cluster property>` in Compose has been a no-op since M0.**
   Redpanda writes the value into the node config and then logs `Ignoring value for
   'write_caching_default' in redpanda.yaml: use rpk cluster config edit` — so the auto-creation flag
   never did anything (the behaviour was Redpanda's default all along), and adding one for write
   caching changed nothing: `rpk cluster config get write_caching_default` still said `"true"`,
   developer mode's default. Cluster properties belong in `.bootstrap.yaml`.

3. **Recreating the Redpanda container does not create a new cluster.** The Compose file declares
   no volume for it, so a recreate looked like a clean start — and the bootstrap file appeared not to
   work. The image itself declares an anonymous volume on `/var/lib/redpanda/data`, which Compose
   carries across a recreate; the giveaway was the controller log opening at `start_offset:60` on a
   "fresh" broker. Fixed live with `rpk cluster config set write_caching_default false`, then proven
   on a genuinely new cluster (`up -d --renew-anon-volumes redpanda`, then a service restart so the
   topics are re-declared): `"false"`, read from the bootstrap file.

4. **The consumer stall recurred, and turned out not to be idle.** Repeat run 2 of scenario 1, with
   write caching off: account-service's command consumer stopped consuming after the broker restart
   while staying `Stable`, assigned and heartbeating, with nothing logged. Its client metrics showed
   `fetch_total` at 2,984,401 and climbing ~5,900/s with zero records consumed, against 2/s for the
   healthy consumer in the same JVM. The broker had logged `no session with id 2 found` and `... id 3
   ...` for the two pre-restart fetch sessions; one consumer recovered and one did not. Root cause
   still not established. Two outcomes:
   - `KafkaConsumerFetchSpin` (fetch rate > 50/s and zero consumed, for 2 m), checked against the
     live stall before it was committed — it selected exactly the stuck client, then fired.
   - After `docker restart dpe-account`, with no manual intervention, all twelve affected transfers
     settled correctly: 3 `CHARGED` → `COMPLETED` by forward recovery, 9 `FAILED` with their holds
     `RELEASED (SAGA_TIMEOUT)` because the queued reserve and the transfer-addressed release arrived
     in that order. The same stall in Session 16 stranded money.

5. **An incremental `./mvnw verify` failed all 89 account-service tests after a one-line change in
   `common-messaging`** - every context failed with `NoClassDefFoundError: DeadLetterProperties`
   (note: no package in the name) while introspecting `KafkaErrorHandlingConfig`. Nothing was wrong
   with the code; `./mvnw clean verify` passed everything. Same family as the earlier stale-test-class
   trap: after changing a shared module, a failure that names a class the code plainly has is the
   build, not the change - `clean` before believing it.

### Verified

```
./mvnw -B -ntp clean verify
    BUILD SUCCESS - 224 tests, 0 failures (common-messaging 16, account-service 89,
    payment-orchestrator 110, payment-gateway 9)

docker exec dpe-redpanda rpk cluster config get write_caching_default    "false"
docker exec dpe-redpanda rpk topic describe -c dpe.account.commands.v1   write.caching false

./chaos/run-all.sh      10 / 10 HELD, exit 0
```

| Scenario | Session 16 | Session 17 |
|---|---|---|
| 01 broker dies, 15 s | 2 of 4 refuted (consumer stall) | 4 of 5 held; 1 stall, settled correctly after a restart |
| 01 broker dies, 45 s | not run | held — 10 FAILED, 2 COMPENSATED, no stranded hold, no orphaned charge |
| 02 account-service dies after the charge | refuted — compensated past the pivot | held — 6 COMPLETED, 0 releases sent |
| 02 account-service dies before the reserve | refuted — 42000 stranded in CLEARING | held — 6 FAILED, holds RELEASED, sender exactly whole |
| 03 orchestrator dies before relay | refuted — sweeper failed sagas while deaf | held — 8 COMPLETED; sweeper paused once, resumed once |
| 04 gateway declines everything | held | held |
| 05 A duplicate callbacks | held | held |
| 05 B silent PSP, dead letters replayed | refuted — replay charged 10 refunded transfers | held — 10 `VOIDED` tombstones, 0 approvals |
| 06 key stampede (Redis on / off) | held | held |
| 07 hot account | held | held — exactly 60 / 30 / 0 |
| 08 network partition | held | held (the open-transaction case still not exercised) |

After the stall recovery above: `verify-invariants.sh` I1–I5 all pass, account-service lag 0.

### Committed

`df330a4`, with Sessions 16 and 18 (see Session 18).

### Open / next

1. **Reconcile Session 16's leftovers** — 26 holds (110000) still `ACTIVE` under `FAILED` sagas and
   22 `APPROVED` charges on `FAILED` transfers. The idempotent mechanism exists now (a
   transfer-addressed `ReleaseFunds`, a `VoidCharge`), but no sanctioned way for a person to issue
   one does; operators deliberately cannot move money. That is a policy decision to make first.
2. **Consumer stall root cause** — reproduce on the Apache Kafka profile (Compose hard-codes
   `KAFKA_BOOTSTRAP: redpanda:9092`, which needs an override). Then decide whether an automatic
   listener restart on `KafkaConsumerFetchSpin` is worth the risk of restarting a healthy consumer.
3. Scenario 8's open-transaction case.
4. Whether S1–S4 belong in `scripts/verify-invariants.sh`.
5. Commit M7.

## Session 18 — 2026-09-11

### Goal

Close everything Session 17 left open: repair the money Session 16's chaos run stranded, decide
whether S1–S4 belong in the invariant script, reproduce the consumer stall on Apache Kafka, and
finally exercise scenario 8's open-transaction case.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| How to repair 26 stranded holds and 22 orphaned PSP charges | `POST /admin/transfers/{id}/reconcile` on the orchestrator (OPERATOR), driven per transfer by `scripts/reconcile.sh` | A terminal saga is never swept again, so the fixes could not reach back. SQL was rejected: hand-written ledger rows go around the only code that keeps I1/I2 true. See ADR 0008. |
| Does that break "an operator moves no money"? | No, and the endpoint is shaped so it cannot | It is refused for a `COMPLETED` saga (reversing that would be a movement) and for a live one (the sweeper owns it), and it chooses no amount, source or destination. It can only finish the compensation a failed transfer already implies. A stolen operator token still cannot pay anyone. |
| Release and void together, or in order? | In order: release first, void only on an answer proving the sender whole | The orchestrator cannot see holds. If the hold was COMMITTED, voiding the charge would pay the recipient out of our own books. `FundsCommitted` stops it and records `Reconcile FAILED` for a person. |
| S1–S4 in `verify-invariants.sh`? | Yes, defined once in `scripts/lib/stranded.sh`; `--no-stranded` for the chaos harness | Every defect the chaos suite found passed I1–I5. A definition of "correct" that a double charge passes is not one. The harness keeps its own new-violations-only judgement, so it opts out of the absolute version. |
| Running on Apache Kafka | `infra/docker-compose.kafka.yml` override, and the harness detects the broker | `--profile kafka` only started a broker no service talked to; every service hard-coded `redpanda:9092` and `depends_on` it. `!override` replaces `depends_on` wholesale instead of merging into it. |
| Automatic listener restart on `KafkaConsumerFetchSpin` | Not built | The stall did not reproduce on Kafka (below). An alert-driven restart that fires on a healthy consumer is a worse failure than the one it treats. |
| Orphaned transactions | `idle_in_transaction_session_timeout=30s` and TCP keepalives, as pgjdbc startup options on each pool | Server-side, because the client is gone; set by the service, so they travel with it into every environment. |
| The relay's transaction | A wall-clock `batch-budget` (10 s) and `max.block.ms: 3000` | See What broke 3. The relay is the only transaction with I/O inside it, so it is the one a session timeout has to be sized against. |

### Built

**Reconciliation** — `SagaOrchestrator.reconcile` and `ReconcileOutcome`; `ReconciliationController`;
`ReleaseFunds.RECONCILIATION`. The answer is handled in the three reply handlers that can carry it
(`FundsReleased`, `ReserveRejected(TRANSFER_VOIDED)`, `FundsCommitted`), and only while a request is
outstanding - counted from `saga_steps`, so no migration. `scripts/reconcile.sh` is a dry run unless
given `--apply`. ADR 0008.

**`scripts/lib/stranded.sh`** — S1–S4, now read by `verify-invariants.sh`, the chaos harness and
`reconcile.sh`.

**The Kafka override** — `infra/docker-compose.kafka.yml` (Redpanda disabled, Kafka enabled with
auto-creation off, all three services pointed at `kafka:9092`). `chaos/lib.sh` detects which broker
is running and kills, pauses and reads lag from that one (`kafka-consumer-groups.sh`, one JVM start
for all three groups).

**Scenario 8, rewritten** — the cut is aimed: pause the container, ask `pg_locks` whether one of its
backends holds a `transactionid` while idle in a transaction, cut only then, otherwise thaw and try
again 100 ms later, with traffic running until the cut lands (What broke 10). `MODE=crash` kills
the process while it is cut off. Both modes are in `run-all.sh`.

**Scenario 1** measures the outage the system actually saw (What broke 4).

**One scenario at a time** — `chaos/lib.sh` takes a lock in `begin_scenario` (What broke 9).

**Orphaned-transaction defences** — the pool options in all three services;
`OutboxProperties.batchBudget` and the budget check in `OutboxRelay.drainBatch`; `max.block.ms` on
all three producers.

**Tests** — 9 new: seven for reconciliation (the two real leftover shapes, the never-reserved case,
the committed hold that must not be voided, both refusals, and a reply with no request outstanding),
the relay budget, and one asserting the session settings actually bind. 233 in total.

### What broke

1. **The orphaned transaction, measured.** Scenario 8's first two versions cut the network at a
   random moment and landed on no open transaction; a transaction here lasts milliseconds. Aimed,
   and with the process killed while cut off, the orphan held its locks for as long as anyone
   watched: 150 s, against a two-hour TCP keepalive. The restarted account-service's only consumer
   thread blocked on the orphan's *uncommitted inbox row* for the redelivered command - the same
   "insert that blocks" property that makes idempotency safe, turned into a total stall - and lag
   climbed until the backend was terminated by hand. The same run left the dead instance's other
   nine pooled connections idle from its old IP: no locks, but each a `max_connections` slot
   (100, shared by three pools of 10) for two hours. With the fixes, both modes held and Postgres
   reaped the orphan itself (29 s after the cut in partition mode).

2. **Postgres prints `tcp_keepalives_idle` as `60`, not `1min`** - unlike the timeout next to it,
   which prints `30s`. The test asserting the setting bound was written with the wrong expectation
   and failed first; the setting was right.

3. **The relay could not coexist with a session timeout until its batch had a time budget.** No
   SQL runs between the relay's claim and its commit, so to Postgres the whole batch is one idle
   stretch - 100 sends × 5 s against a dead broker is over eight minutes. Any timeout short enough
   to reap an orphan would have killed a healthy relay mid-batch; under a slow broker, forever. The
   budget stops a drain from starting new sends after 10 s, and never before the first send, since
   a budget shorter than one send would otherwise publish nothing, ever - an outage that looks idle.
   `max.block.ms` (default 60 s) bounds the part of a send that `send-timeout` cannot see. Ceiling
   10 + 5 + 3 s, under the 30 s timeout. `DeadLetterReplayService` still sends up to 50 × 5 s in one
   transaction and will be reaped if a replay meets a broker outage; it is operator-invoked and safe
   to repeat, so left as is.

4. **"A 15 s outage" was a 31 s outage on Kafka.** One run of scenario 1 refuted: four sagas
   compensated. Their deadline passed at 16:12:10; the first publish after the restart was at
   16:12:10.852. Apache Kafka took ~14–22 s after `docker start` to accept writes, which Redpanda
   does in a couple, and `OUTAGE` only measures kill-to-start. The system was right - they were
   compensated and every check passed - and the scenario's assertion was wrong. It now measures
   kill to the first publish of a command written after the kill, and asks for "all COMPLETED"
   only below 25 s. Its first version took the first publish after the kill instant and read 0 s on
   four runs out of six: sends in flight at the kill were acknowledged as it landed.

5. **`curl -o /tmp/...` fails in Git Bash under `MSYS_NO_PATHCONV=1`** with `(23) Failure writing
   output` on every request - a Windows `curl` given a POSIX path. `reconcile.sh`'s first live run
   printed 48 of them over 48 successful requests. Body and status are now captured with `-w`.

6. **Counting lines of `$(...)` with `wc -l` is one short.** The substitution strips the trailing
   newline; the first `verify-invariants.sh` with S1–S4 reported 25 stranded holds against 26.

7. **A known inaccuracy, not fixed.** A hold released before migration V7 has no stored release
   reason, so its truthful reply echoes the command's. The 22 reconciled charges' steps say "hold
   released (RECONCILIATION)" about holds released in Session 16 for other reasons. The money is
   right; the label is not.

8. **The consumer stall happened again, on Redpanda, inside the suite.** Scenario 01's broker kill
   at the start of `run-all.sh`; this time the gateway's dead-letter listener
   (`consumer-payment-gateway-2`) - a third kind of consumer to stall, after the gateway's command
   listener and account-service's. 5,120 fetches/s, nothing consumed, 10 records of lag on
   `dpe.gateway.commands.v1.dlt`, and `KafkaConsumerFetchSpin` firing on exactly that client. It
   surfaced two scenarios later as 05's "each silent charge dead-lettered: got 0" - the check counts
   rows the stalled consumer writes - and every scenario after it refused to start on the lag. A
   restart of the one service cleared it. Redpanda is now 4 stalls in 10 broker restarts; Apache
   Kafka 0 in 13.

9. **Two harnesses ran at once, and nothing said so.** Stopping `run-all.sh` stopped its wrapper
   and not its loop, which went on to run 07 and 08 on top of a re-run of 05 and 06. The results
   were plausible and wrong: 05 miscounted duplicate callbacks and dead letters; 06 reported I3
   6,000 short, because the other harness funded accounts after 06 took its baseline (the ledger
   showed 08's 1,500-paise traffic inside 06's window); 08 failed to aim. Run alone, all of them
   held. `chaos/lib.sh` now takes a lock (`mkdir`, atomic, with the holder's pid so a lock left by
   a killed run is recognised as stale) and a second scenario exits 2 naming the first.

10. **The aimed cut was aimed at a fixed batch.** Scenario 8 caught an open transaction on try 1 in
    three runs, then missed 300 times in a row: 40 transfers drain in seconds, and each try - two
    docker CLI calls and a psql - takes most of a second. Traffic now runs until the cut lands.
    Caught on try 8 and try 23 since.

### Verified

```
./mvnw -B -ntp clean verify              BUILD SUCCESS - 233 tests, 0 failures, 0 errors
                                         (common-messaging 16, account-service 91,
                                          payment-orchestrator 117, payment-gateway 9; was 224)

./scripts/reconcile.sh                   dry run: S1 26, S2 22, S3 0, S4 0 - 48 would be reconciled
./scripts/reconcile.sh --apply           HTTP 202 x48; after: S1 0, S2 0, S3 0, S4 0; exit 0
  saga_steps  Reconcile STARTED 48, SUCCEEDED 48;  VoidCharge REVERSED 22, PRE_EMPTED 26
  holds       RELEASED (RECONCILIATION) 26, 110000
./scripts/verify-invariants.sh           I1-I5 PASS, S1-S4 PASS, exit 0
./scripts/reconcile.sh                   (second run) Nothing to reconcile.
```

Scenario 8, the open-transaction case:

| Run | Result |
|---|---|
| `MODE=crash`, before the fix (experiment) | orphan idle in transaction at +15, +45, +90, +150 s; consumer blocked on its inbox row; lag 85 → 156; cleared only by `pg_terminate_backend` |
| `MODE=crash`, after | HELD — orphan reaped by Postgres, 10 COMPLETED / 30 FAILED (a 40 s outage, past the deadline), I1–I5 and S1–S4 pass |
| `MODE=partition`, after | HELD — reaped 29 s after the cut, 37 COMPLETED / 3 COMPENSATED, all checks pass |

Scenario 1 on Apache Kafka 4.3.1 (`infra/docker-compose.kafka.yml`), 13 broker kills:

| Batch | Result |
|---|---|
| `OUTAGE=15` × 5 | 4 held; 1 refuted by the scenario's own timing assumption (What broke 4), not a stall |
| `OUTAGE=8` × 7 | 7 held, 12/12 COMPLETED each |
| `OUTAGE=45` × 1 | held — 12 FAILED, S1–S4 clean |
| Consumer fetch rate, every run | 1.7–3.8/s (the stall's signature is thousands per second) |

Zero silent stalls, against 3 in 9 broker restarts on Redpanda.

The whole suite on Redpanda, after every change above:

| Scenario | Result |
|---|---|
| 01, 02 `after-charge`, 02 `before-reserve`, 03, 04 | held (in `run-all.sh`) |
| 05 | refuted in `run-all.sh` by a silent consumer stall that began at 01's broker kill (What broke 8); held run alone after a restart of payment-gateway |
| 06 Redis on / off, 07 | held (run alone - What broke 9) |
| 08 `partition` | held — cut on try 8, orphan reaped 27 s after the cut, 92 COMPLETED |
| 08 `crash` | held — cut on try 23, orphan reaped 41 s after the cut, 140 COMPLETED / 44 FAILED |

The stalled dead-letter consumer's ten records were replayed once it recovered: approved charges
882 before, 882 after — every one found its transfer's `VOIDED` tombstone. I1–I5 and S1–S4 pass.

### Committed

`df330a4` — "M7: the chaos suite - five saga defects it found, their fixes, and the lock that outlives its owner". Sessions 16–18 in one commit: the harness, the five fixes and this session's work are one milestone, and several files carry changes from all three.

### Open / next

1. The consumer stall: only ever reproduced on Redpanda. A comparison, not a root cause.
2. `DeadLetterReplayService` holds a transaction across up to 50 sends; bound it like the relay if
   replays ever become routine.
3. ~~Commit M7~~ — done, `df330a4`.

---

## Session 19 — 2026-09-11

### Goal

Move the broker's host-facing port off 19092. A Kind cluster from another project on this machine
publishes its own Kafka on 19092, so the two could not run at the same time (the collision behind
the port workarounds recorded under M3 part 2 and Session 11), and a service started from the IDE
while the other cluster held the port would have connected to the wrong broker with nothing saying
so. Then move the console off 5173 for the same reason: the other project pins its dashboard's dev
server there.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Which project moves | This one | Nine lines and one container recreate. The other project's port is fixed at Kind cluster creation and is referenced in its scripts, README and five services; moving it means destroying and recreating that cluster. |
| New port | 29092 | Keeps the `x9092` pattern. Free on this machine, and outside the TCP ranges Windows reserves for Hyper-V (49816–50465 here), where a bind fails with an access-permissions error rather than "port already allocated". |
| Container side of the mapping | Also 29092 (`29092:29092`) | The advertised address is `localhost:29092`, and a client reconnects to the advertised address after bootstrap. Host and container ports must therefore be equal, or bootstrap succeeds and every produce and fetch after it hangs. |
| A Compose variable for the port? | No — nine literal lines | `${VAR:-29092}` in Compose cannot reach the `KAFKA_BOOTSTRAP` default in the three `application.yml`s, so overriding it would make Compose and the IDE defaults disagree silently. A literal that one `grep` finds is harder to get wrong. |
| Services in Compose | Unchanged | They use the internal listener, `redpanda:9092` / `kafka:9092`. Only the host path moved. |
| Console | 8084 (container), 8085 (Vite dev server) | Beside the services' 8081–8083. The other project's dev server is pinned to 5173 with `strictPort` and a CORS allowlist naming that origin, so this side is the one that can move freely. |
| Is a port clash always loud? | No — tested, and the answer changed the decision | Docker Desktop here publishes on IPv6 only (`::`, `::1`). With the console on 5173, a plain socket still bound `127.0.0.1:5173` and `0.0.0.0:5173` without error; only `::1` was refused. The other project's Vite happens to bind `::1` (Node resolves `localhost` to it on this machine), so its failure would have been loud — by luck of resolver order, not by design. |
| Dev server `strictPort` | On | Vite's default on a taken port is to move to the next one quietly. A dev server that is not where the README says is found by opening a different app. |

### Built

- `infra/docker-compose.yml` — external listener, advertised address and port mapping moved to
  29092 for both `redpanda` and the profile-gated `kafka`, with a comment at the mapping naming all
  the places that must agree.
- `KAFKA_BOOTSTRAP` default moved to `localhost:29092` in the `application.yml` of all three
  services.
- `infra/docker-compose.kafka.yml` — comment updated.
- The console: `ui` published on `8084:80`; `ui/vite.config.ts` on 8085 with `strictPort`; README.

### What broke

1. **A one-off transfer failed I3, correctly.** The live check opened a funded account after the
   stored I3 baseline was taken; funding an account is money entering the ledger, so the total rose
   by exactly the opening balance (71,500,000 → 71,505,000) and I3 reported it. I1, I2 and S1–S4
   passed. The chaos harness avoids this by recording the baseline *after* opening its accounts
   (`record_baseline`); ad-hoc traffic has to follow the same order. Baseline re-recorded.

### Verified

```
docker compose -f infra/docker-compose.yml config                          renders; 29092 target and published
docker compose -f infra/docker-compose.yml -f infra/docker-compose.kafka.yml config    renders; same
grep -rn 29092 --include=*.yml .                                           9 lines, all consistent
docker compose -f infra/docker-compose.yml up -d redpanda                  recreated, healthy; same data volume
                                                                           (774dd999...); 8 topics present;
                                                                           write_caching_default "false",
                                                                           auto_create_topics_enabled false
```

With the other project's Kind cluster running on 19092 at the same time, from the Windows host:

```
kafka-broker-api-versions.sh --bootstrap-server localhost:29092    advertised: localhost:29092 (id: 0)
kafka-topics.sh --list        --bootstrap-server localhost:29092    dpe.* topics only
kafka-broker-api-versions.sh --bootstrap-server localhost:19092    advertised: localhost:19092 (id: 1)
kafka-topics.sh --list        --bootstrap-server localhost:19092    the other project's topics only
```

After restarting the three services (a Redpanda restart has preceded every silent consumer stall
seen so far, and a restart of the service clears one):

```
rpk group describe (all three)     Stable, 2 members each, total lag 0
one transfer, alice -> bob, 1234   HTTP 202 -> saga COMPLETED in ~3 s; balances 3766 / 1234
./scripts/verify-invariants.sh     I1-I5 and S1-S4 PASS (after re-recording the baseline, What broke 1)
```

The console:

```
docker compose ... up -d ui               recreated alone (services untouched), healthy on 8084
GET  localhost:8084/                      200
POST localhost:8084/api/orchestrator/auth/token     token issued (alice)
GET  localhost:8084/api/orchestrator/api/v1/transfers   200, with that token
GET  localhost:8084/api/prom/api/v1/query 200
localhost:5173                            no answer; ::1 and 127.0.0.1 both bindable again
npm run typecheck (ui)                    exit 0
npm run dev (ui)                          serves on 8085; /api/prom proxied, 200
a second npm run dev                      "Error: Port 8085 is already in use" - refused, not moved
```

### Committed

`f86e462` — "infra: move the broker to host port 29092 and the console to 8084".

### Open / next

1. The two earlier port workarounds (M3 part 2, Session 11) are history now: both stacks run at once.
2. Carried over: the consumer stall (only reproduced on Redpanda) and `DeadLetterReplayService`'s
   unbounded transaction.

## Session 20 — 2026-09-11

### Goal

Start M8: a load harness that judges a run by the invariants rather than by HTTP status codes, and a
first experiment to find the throughput at which the saga stops keeping up — and what the system does
past it. The expected ceiling was worked out from the code and written down before the first run.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| What one iteration is | Pay, then poll `GET /transfers/{id}` until the transfer settles, then think | `POST /transfers` answers 202 once three rows are committed, before any money moves. A test that reads only the POST measures the one component that cannot fall behind. |
| Where the settle time comes from | `saga_instances`, accept → terminal, per 30 s window; k6's polled figure as the customer's view | Polling quantises to the poll interval and sees only transfers a VU was still watching. The table sees every saga, exactly. |
| Load models | `knee` is open (ramping arrival rate); `users` is closed (1,000 VUs with think time) | An open model keeps arriving when the system slows down, so it measures latency honestly past saturation; a closed model backs off and under-reports it (coordinated omission). "1,000 concurrent" is a statement about sessions, so the closed profile prints its Little's-law rate beside the user count. |
| Where k6 runs | In a container on the Compose network, image pinned (`grafana/k6:2.2.0`), capped at 2 GB | Through the host it would measure Docker Desktop's userspace port forwarder. |
| Runner structure | `loadtest/run.sh` sources `chaos/lib.sh` | A load run is a chaos scenario with no fault: the same lock (a load run and a chaos run can never read each other's traffic), the same quiescence gate, accounts opened *before* the I3 baseline, and the same verdict — I1–I5 plus new S1–S4 violations. |
| Correctness vs latency | Reported separately. Exit 1 = an invariant refuted, 3 = correct but SLO missed | A capacity finding and a correctness bug are different kinds of news. |
| Idempotency under load | 2% of accepted POSTs re-sent with the same key; threshold `replay_consistent rate==1` | A retry answered with a different transfer is a double payment. |
| Stack configuration during the run | As shipped (100% trace sampling, DEBUG for `com.dpe`) | One variable at a time; the ceilings found are structural. |

### Expected ceiling, from the code

- **payment-gateway, ~18 charges/s.** One listener thread, and the simulated PSP call
  (`Thread.sleep(50)`) runs inside the `@Transactional` handler, holding the thread and a connection.
- **Orchestrator relay, ~37 transfers/s.** One send at a time, each blocking on its ack
  (`linger.ms` 5 plus a round trip), then a 500 ms sleep even after a full batch; three commands
  per transfer.
- **The API edge, far higher,** and blind to both.

So: knee near 18/s, edge latency flat throughout, the queue forming as consumer lag on the gateway's
group, and — past the knee — the 30 s step-timeout compensating sagas that were only waiting.

### Built

- `loadtest/transfers.js` — profiles `smoke`, `knee`, `users`; custom metrics `transfer_settle_ms`,
  `transfer_outcome{outcome}`, `transfer_completed`, `replay_consistent`; per-VU token renewal inside
  the 15-minute TTL; `name` tags on URLs so a transfer id never becomes a time series.
- `loadtest/run.sh` — seeds 200 funded accounts and waits for the orchestrator's ownership
  projection, records the baseline, runs k6, waits up to 900 s for the pipeline to drain, then
  reports outcomes, per-window settle percentiles, accepted-vs-settled throughput, Prometheus peaks
  (outbox backlog and age, consumer lag, pool waits, GC) and container CPU from a 5 s sampler.
- `loadtest/README.md`. Results go to `loadtest/results/<run>/` (ignored).

### The knee run

Open model, 5 → 10 → 15 → 20 → 30 → 40 arrivals/s, 60 s per step.

```
window    started    /s   done timedout     p50     p95     p99     max   (s, accept -> terminal)
17:28:50      300  10.0    300        0    2.40    2.90    3.10    3.26
17:29:50      435  14.5    435        0    3.29    4.28    4.45    4.62
17:30:20      446  14.9    446        0    3.74    4.29    4.47    4.64   last flat window
17:30:50      534  17.8    534        0    4.34    6.76   14.19   15.03   the knee
17:31:20      453  15.1    453        0   15.56   20.63   22.00   22.35
17:31:50      592  19.7    555       46   20.16   33.66   35.34   35.66   timeouts begin
17:32:20      681  22.7    409      409   37.14   69.35   73.16   76.15
17:32:50      697  23.2      0      697   81.63   89.27   91.64   92.54   nothing completes
17:34:20       44   1.5      0       44   35.37   35.64   35.66   35.67   1.5/s, still all timed out
```

5,382 transfers accepted (and 2,822 arrivals k6 could not start, all 1,000 VUs being busy): 3,943
COMPLETED, 1,401 COMPENSATED and 38 FAILED — every non-completion a saga timeout. Zero HTTP errors
in 248,628 requests; all 112 idempotent retries answered with the original transfer. **I1–I5 and
S1–S4 all held.** Capacity on this machine is about 15 transfers/s, with the knee between 15 and 18.

**Why throughput collapsed instead of plateauing** (Prometheus, 20 s steps):

```
                   31:20 31:40 32:00 32:20 32:40 33:00 33:20 33:40 34:00 34:20
GET poll/s           144   237   452   569   843  1131  1424  1531  1616  1857
settled/s             17     8    14    16    14    17    21     5     1     5
orch pool waiting      0     0     0     0     0     0    59   187   187   145
orch outbox rows      22    51    31    36    90    47   331   697   883   861
```

The gateway saturated first, as expected. Customers then waited longer, and each waiting customer
polls: status requests rose from 144/s to 1,857/s and made up 97% of all traffic. Those requests
took the orchestrator's ten database connections — 187 threads queued for one — and the outbox relay
and the reply consumer draw from the same ten. The orchestrator could neither send commands nor
read replies, so nothing completed, and every saga past 30 s was compensated, adding more messages
to the starved pipeline. Arrivals at a tenth of capacity still timed out. That is a metastable
failure: the system's own recovery work, and its clients' reaction to its slowness, kept it down
after the load that caused it had gone.

The pool was exhausted two minutes after the knee, so it was a consequence, not the trigger — and
enlarging it would only have let the status requests take more connections. The missing pieces are
a **bulkhead** (the pipeline must not queue behind read traffic) and **admission control** (the API
accepted 20–26/s against a pipeline doing 15/s with nothing bounding the difference; a 202 is a
promise, and the edge had no way to know it could not keep it).

### What broke

1. **Seeding opened 200 accounts and recorded 109.** Each parallel worker wrote the owner and then
   the id as two separate writes to a shared pipe; writes under `PIPE_BUF` are atomic, pairs of
   writes are not, and interleaved lines were discarded. Fixed by building each line and writing it
   once.
2. **The Windows Python on this machine cannot open a POSIX path** (`/g/project/...`). The seed file
   came out empty and k6 failed parsing it. Fixed: the JSON is built with awk; Python only reads
   stdin.
3. **Stop-the-world pauses of several seconds on a ~60 MB live heap** — 4.8 s (account-service),
   7.9 s (gateway), 1.3 and 2.6 s (orchestrator). All but one are full collections recorded as
   `CodeCache GC Threshold`: the JIT's code cache growing as new code paths got hot under load, not
   heap pressure (about 55 MB used of 247). Swap is ruled out (none in the container's cgroup). Why a
   full collection of that heap takes seconds is not yet established. The gateway's 7.9 s pause fell
   exactly on the knee, and with one consumer thread a pause is a stalled partition.
4. **The orchestrator reached 505 of its 512 MiB container limit.** The heap is capped at 256 MB;
   the rest is native (up to 200 request threads, metaspace, code cache, buffers). It was one step
   from an OOM kill, and nothing alerts on container memory.
5. **Jaeger was OOM-killed during the run.** Its store is bounded by `MEMORY_MAX_TRACES`, which
   counts traces, not bytes; 248k requests at 100% sampling overran 384 MB well before 20,000
   traces. The Compose comment calling it a hard ceiling was wrong. Every payment was unaffected —
   the exporters logged failures and carried on, which is the property the tracing design requires.
6. **The knee run reported `SLO: MET` over a collapse,** because the knee profile deliberately sets
   no latency thresholds and k6 therefore exited 0. It now reports "not judged".
7. **Unexplained: the first five requests of the smoke run took ~600 ms**; every later one took
   17–51 ms. Five virtual users firing at the same instant after a long idle. Not Redis, not pool
   acquisition (1.8 ms max), not reproducible sequentially.

### Verified

```
./loadtest/run.sh                (smoke)   42 transfers, all COMPLETED; settle p95 3.77 s;
                                           I1-I5, S1-S4 PASS; exit 3 (accept p99 607 ms - What broke 7)
PROFILE=knee ./loadtest/run.sh             5,382 accepted: 3,943 COMPLETED, 1,401 COMPENSATED, 38 FAILED;
                                           0 / 248,628 HTTP errors; replay_consistent 112/112;
                                           drained 16 s after k6 stopped; I1-I5, S1-S4 PASS; exit 0
docker inspect dpe-jaeger                  OOMKilled=true (exit 137); restarted, healthy
```

### Committed

`5c43f2b` — "M8 (part 1): the load harness, a collapse that conserved every paisa, and admission control" (together with part 2).

### Open / next

1. Admission control on `POST /transfers`, bounded by work in flight (Little's law: capacity ×
   step-timeout is the ceiling, and the bound belongs well below it).
2. A bulkhead in the orchestrator: the relay, reply consumer and sweeper must not share a pool with
   request threads.
3. Gateway throughput: the PSP call out of the transaction; listener concurrency equal to the
   partition count.
4. Relay pipelining: send a batch, then await the acks together; keep draining while batches are full.
5. The `CodeCache GC Threshold` pauses — cause first, fix second.
6. A real memory bound for Jaeger, or lower sampling under load.
7. The `users` profile (1,000 concurrent) is not yet run.

## Session 20, part 2 — 2026-09-12

### Goal

Fix the two defects the first knee run exposed: an API that accepted work with no idea whether the
pipeline could finish it, and API reads that could take every database connection the pipeline
needs. Then prove, by switching one off, which fix does what.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| What admission control bounds | Sagas in flight, counted from `saga_instances` | Little's law: at fixed throughput, bounding the number in the system bounds the wait, which is what the 30 s saga deadline cares about. A requests-per-second limit knows nothing about whether the pipeline keeps up. Counted from the database, the bound is global across instances. |
| The limit | 150 | ~15/s measured capacity × a 10 s wait target (a third of the deadline, so no healthy saga is swept). A measurement of this deployment, to be re-derived when throughput changes. |
| Where it is checked | Inside `createTransfer`, after the idempotency claim | Only new work reaches it. A retry of an accepted payment is replayed at any load; refusing it would tell a client "not accepted" about a payment that was. The refusal rolls back the claim, so the key is not consumed. |
| How it counts | Recounted at most once a second, lazily, in the admitting request's own transaction; between counts, last count + admissions since | No query per request, no extra connection, no dependence on the service's single scheduler thread. Errs toward refusing early, never late. A soft bound, and documented as one. |
| Status code | 503 with `Retry-After`, code `AT_CAPACITY`, body telling the client to retry with the same `Idempotency-Key` | The refusal is about server state, not client behaviour (429). A fresh key after a lost 202 is the way to pay twice. |
| Bulkhead | A fair semaphore on `/api/*` and `/admin/*`: 6 request permits against a pool of 10 | Each request uses at most one connection at a time, so 6 permits hold at most 6 and 4 are always free for the scheduler thread, the reply listener, the dead-letter listener and the health check. Boot refuses to start if permits ≥ pool size. |
| Why not two connection pools | Rejected | A routing DataSource over two Hikari pools means defining DataSource beans, which in Boot 4 switches off the auto-configuration that wires connection details — including `@ServiceConnection` in every Testcontainers test. The semaphore gives the same isolation with one pool. |
| Filter position | Just after Spring Security | Unauthenticated requests are answered 401 without spending a permit. |
| Admission in the test suite | Effectively off by default; enabled at 3 in its own test with `@TestPropertySource` | The estimate is a singleton and the test base truncates the saga table under it; a real limit would make test order matter. |
| Load client | 503 treated as an expected status; the customer waits `Retry-After` plus jitter and retries with the same key, giving up after 5 | Shedding filed as an HTTP failure makes a system protecting itself indistinguishable from one failing. |

### Built

- `admission/` — `AdmissionControl`, `AdmissionProperties`, `AdmissionRefusedException`; one call in
  `TransferService.createTransfer`; a 503 handler in `ApiExceptionHandler`.
- `web/RequestBulkhead`, `BulkheadProperties`, `BulkheadConfig` (registration and the startup guard).
- Metrics `dpe.admission.refused`, `dpe.admission.limit`, `dpe.bulkhead.rejected`,
  `dpe.bulkhead.in_use`, `dpe.bulkhead.permits`. Deliberately no gauge for the in-flight estimate:
  it is recounted lazily, so on an idle system it would sit at its last value indefinitely.
  `dpe.saga.inflight` is the refreshed number.
- A "Load shedding" row on the Grafana dashboard.
- `AdmissionControlTest` (refusal writes nothing; a replay is never refused; a refused key is reusable;
  the HTTP 503 with `Retry-After`) and `RequestBulkheadTest` (refusal at capacity, permit returned
  on exception, the startup guard).
- The load client and runner updated for 503s and the new metrics.

### Results: three runs of the same knee profile (5 → 40 arrivals/s)

| | Neither | Bulkhead only | Both |
|---|---|---|---|
| Accepted | 5,382 | 6,211 | 5,741 |
| Completed | 3,943 (73%) | 4,554 (73%) | **5,741 (100%)** |
| Compensated or failed after acceptance | 1,439 | 1,657 | **0** |
| Worst per-window settle p99 | 91.6 s | 59.9 s | **13.3 s** |
| Settled/s at 40/s offered | ~3 | ~2 | **15.5–17.8** |
| Orchestrator pool, threads waiting | 187 | 0 | 0 |
| Orchestrator reply-listener lag | 294 | 8 | 0 |
| Orchestrator outbox, peak rows | 883 | 864 | 43 |
| Refused up front | — | — | 14,495 responses; 1,984 of 7,725 payments abandoned after 5 attempts |
| I1–I5, S1–S4 | pass | pass | pass |

- **Admission control prevents the collapse.** In flight flattened at the limit, the wait stayed at
  the 10–13 s design target, and throughput held at twice the offered load the pipeline can carry.
  The excess was told "not accepted, retry later" instead of "accepted" followed by a timeout.
- **The bulkhead removes the starvation, and on its own that is not enough.** No thread ever
  queued for a connection and the reply listener kept up, but with nothing bounding accepted work
  the queue still outgrew the deadline. Bounding concurrency does not bound work.
- **The next bottleneck is visible.** With its connection no longer contended, the orchestrator's
  outbox still reached 864 rows: the relay's one-send-at-a-time ceiling.
- The bulkhead never had to refuse a request in any run; its refusal path is covered by tests, not
  yet observed live.

### What broke

1. **`mvn clean` could not delete `target/surefire-reports`** — a shell's working directory was
   inside it, and Windows will not delete a directory in use. Stopping that background build then
   left its Maven JVM running; it had to be found and killed before a clean re-run.
2. **An edited provisioned Grafana dashboard was not reloaded** despite a 10 s update interval and
   the new file being visible in the container; nothing was logged. A Grafana restart loaded it.
   The cause (most likely change detection on a Docker Desktop bind mount) is not established.
3. **k6 reported a negative request duration** in the third run: the VM's clock stepped backwards.
4. **Jaeger was OOM-killed again**, during the second run, with a third of the first run's traffic.
   Payments were unaffected, as before; it was restarted after the session. Two kills in three runs
   make its trace-count bound the next thing to fix on the observability side.

### Verified

```
./mvnw -pl payment-orchestrator -am test -Dtest='AdmissionControlTest,RequestBulkheadTest'   7 of 7 pass
./mvnw -B -ntp -pl payment-orchestrator -am clean verify     common-messaging 16, orchestrator 124, 0 failures
docker compose up -d --build payment-orchestrator            healthy; "request bulkhead: 6 permits against a pool of 10"
PROFILE=knee ./loadtest/run.sh  (both)                        5,741 of 5,741 COMPLETED; I1-I5, S1-S4 pass
PROFILE=knee ./loadtest/run.sh  (DPE_ADMISSION_ENABLED=false) 1,657 COMPENSATED; pool waits 0; I1-I5, S1-S4 pass
orchestrator recreated with the normal configuration          healthy; verify-invariants.sh passes
```

### Committed

`5c43f2b` — "M8 (part 1): the load harness, a collapse that conserved every paisa, and admission control".

### Open / next

1. Relay pipelining — the measured next bottleneck.
2. Gateway throughput (PSP call out of the transaction, listener concurrency 3), then re-derive
   `max-in-flight` from the new capacity.
3. The `CodeCache GC Threshold` pauses (2.7 s on account-service again).
4. A real memory bound for Jaeger.
5. The `users` profile (1,000 concurrent).

## Session 21 — 2026-09-12

### Goal

Pipeline the outbox relay: define precisely what a pipelined `drainBatch` must preserve, write that
down as tests before the rewrite, change the tracing API that a pipelined relay could not use, then
rewrite the relay against the tests.

A correction to the previous session's summary first. The relay is the next bottleneck *under
overload* (the bulkhead-only run's outbox reached 864 rows), but the knee (~15/s) is set by the
gateway, predicted at ~18 charges/s against the relay's ~37 transfers/s; with both fixes on, the
orchestrator's outbox peaked at 43 rows. Pipelining the relay will not move a knee run on its own.
The gateway has to be lifted first for a load run to show the relay change.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Ordering under pipelining | At most one unacknowledged message per aggregate; pipeline across aggregates only | The idempotent producer preserves order across its *own* retries. The relay's retry is a new record sent by a later poll; if A1 fails and A2 is already in flight and succeeds, A1's retry lands after A2. The serial loop avoided this by never sending A2 before A1's outcome was known. |
| Waiting for acks | One deadline for the whole wait, not `sendTimeout` per future | Against a wedged broker, per-future waits sum to N × `sendTimeout` with the claim transaction open, while `idle_in_transaction_session_timeout` is 30 s. |
| Batch budget | Checked before every `send()`, not only between waits | `send()` itself can block for `max.block.ms` (3 s) on metadata or a full buffer before returning a future. |
| Where a row is marked | On the relay thread, after `get()` | The future completes on the producer's network thread; the entity belongs to the relay thread's persistence context and transaction. |
| Tracing | A publish span no longer holds a thread-local scope; the scope covers only the `send()` call | With many spans open on one thread and acks arriving in any order, nested scopes cannot be closed in the order they were opened. |
| How the tests observe pipelining | A scripted `MockProducer` recording in-flight sends per key; real Postgres | Counts ("5 in flight", "never two unacked for one key") rather than timing, where possible. |
| Shape of the pipelined relay | Waves: each wave sends the next message of every unblocked aggregate, then awaits them all | Satisfies the ordering rule by construction. The simpler alternative (send each aggregate's first message, leave the rest for the next poll) costs a whole poll per extra message of one transfer. |
| Draining again when a batch is full | Up to `max-batches-per-poll` (5) per tick, and only while each batch is full and fully acknowledged | A pipelined batch takes milliseconds, so sleeping the poll interval with a backlog wastes the speed-up. Bounded because the orchestrator's relay shares one scheduler thread with the saga sweeper and the metrics refresh; a dedicated thread would be a connection taken from the bulkhead's reserve. A short batch means drained, failing or out of budget, and only the first is a reason to continue. |

### Built

- `OutboxTracing.PublishSpan`: no scope held; `inScope(Supplier)` makes the span current for one
  call; `close()` ends the span. The serial relay wraps its `send()` in `inScope`, so its behaviour is
  unchanged.
- `OutboxRelayPipeliningTest` (account-service, 6 tests). Two specify the new behaviour and fail
  against the serial relay (`sendsTheWholeBatchBeforeWaitingForAnyAck`,
  `oneDeadlineForTheWholeWaitNotOnePerMessage`). Four pass against it and guard what the rewrite
  must keep: no two unacked sends for one aggregate, a failure holds back only its own aggregate,
  only acknowledged rows are marked, and the budget is checked before every send.
- The pipelined `OutboxRelay.drainBatch`: per-aggregate queues in claim order, waves, one ack
  deadline per wave (past it the wait is zero, so an ack that already arrived still counts), the
  budget checked before every send, rows marked on the relay thread, a synchronous `send()` failure
  blocking only its aggregate, and an interrupt resolving the current wave without waiting. The
  worst-case open-transaction stretch is unchanged: 10 s budget + 3 s `max.block.ms` + 5 s wait.
- `OutboxRelayScheduler.poll()` drains again while batches come back full and fully acknowledged, up
  to `dpe.outbox.max-batches-per-poll` (new, default 5, set in all three services).
  `OutboxRelaySchedulerTest` (4 tests, no container) pins the three cases.
- A wait timeout's `last_error` now reads "no broker acknowledgement within PT5S" rather than
  "TimeoutException: null".

### What broke

1. **`KafkaTemplate` closes the producer after every non-transactional send.** A real producer
   factory returns a close-safe wrapper; `MockProducerFactory` returns the bare `MockProducer`, which
   then refuses the next send ("MockProducer is already closed"). The test double ignores `close()`.
2. **`MockProducer` without a partitioner takes partition 0 of its `Cluster`**, and the default
   cluster is empty: `IndexOutOfBoundsException` on the first send.
3. **The first version of the deadline test would have failed a correct relay.** The first send in
   the JVM carried ~850 ms of warm-up (2.05 s measured for 3 × 400 ms). A warm-up drain and a wider
   margin fixed it; the serial relay then measured 2.68 s against a predicted 2.5 s, leaving ~180 ms
   of overhead, so a pipelined relay should finish in about 0.7 s against the 1.5 s bound.

### Verified

```
./mvnw -B -ntp -q -pl account-service -am clean test-compile     exit 0
OutboxTracingWithoutABridgeTest     3/3 pass
OutboxRelayTest                     8/8 pass
OutboxTracePropagationTest          5/5 pass
OutboxRelayPipeliningTest           4 pass, 2 fail as intended against the serial relay
                                    (1 in flight where 5 expected; 2.68 s against a 1.5 s bound)

After the rewrite:
OutboxRelayPipeliningTest 6/6, OutboxRelayTest 8/8, OutboxTracePropagationTest 5/5,
OutboxTracingWithoutABridgeTest 3/3, OutboxRelaySchedulerTest 4/4
./mvnw -B -ntp clean verify         BUILD SUCCESS in 14:24 - common-messaging 20, account-service 97,
                                    payment-orchestrator 124, payment-gateway 9: 250 tests, 0 failures
```

Not yet measured under load: the stack was down, and a knee run cannot show the relay change until
the gateway stops being the bottleneck.

### Committed

`d129fc7` — "M8 (part 2): pipeline the outbox relay without reordering an aggregate".

### Open / next

1. Gateway throughput: listener concurrency 3, then the PSP call's place in the transaction; then a
   knee run, which re-derives `max-in-flight` and is the first live measurement of the pipelined
   relay.
2. The `CodeCache GC Threshold` pauses, a memory bound for Jaeger, the `users` profile.

## Session 22 — 2026-09-12

### Goal

Lift the gateway, which set the knee, so a load run can finally show the pipelined relay; then
follow the bottleneck wherever it moved and re-derive the admission limit from the new capacity.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Gateway consumers | 3, one per partition of `dpe.gateway.commands.v1` (`dpe.gateway.command-concurrency`) | Each charge holds its thread through the PSP call, so 50 ms of latency capped the whole system at 20/s. Per-transfer order comes from the key, not the thread count; the concurrent-charge race was already settled by `UNIQUE(transfer_id)`. A fourth consumer would be assigned nothing. |
| How the concurrency is declared | On the `@KafkaListener`, from a property with no default | Scoped to one listener (the dead-letter listener stays at one). A mis-nested yml key fails the boot instead of quietly running one thread. |
| Moving the PSP call out of the transaction | Not done | Three threads hold 3 of 10 gateway connections through the sleep, and the gateway pool never had a waiting thread. The case for moving it is correctness under a slow PSP, not throughput, and it needs a pending-charge state and recovery path of its own. |
| Orchestrator reply consumers | 3, all in the one group (`dpe.saga.reply-concurrency`) | The single reply thread was 93–99% busy once the gateway was lifted. Every saga transition already loads the saga `FOR UPDATE` (the sweeper and reconcile race the replies), and the two projections are single-row upserts under a constraint, so more threads change no outcome. |
| Orchestrator pool | 10 → 12 | Two more reply threads are two more connections. The request bulkhead's reserve must still cover everything background can hold at once. |
| Bulkhead startup guard | `permits + (3 + reply threads) ≤ pool`, replacing `permits < pool` | The old check would have gone on passing with the reserve two connections short. A thread that touches the database is a connection, so the boot now does the arithmetic. |
| `max-in-flight` | Re-derived with two runs; **stays 150** | "Capacity × 10 s" said 300. At 300 the worst window p99 was 19.9 s (two-thirds of the deadline) for 1.7% more completions. Little's law gives the mean, and capacity falls as the queue deepens (below). |
| Account-service consumers | Not changed | Every reserve and commit locks the CLEARING row and holds it to commit, so extra threads there would queue on one row lock. That is a ledger design question, not a configuration one. |

### Built

- `GatewayCommandConsumer`: named listener (`idIsGroup = false`), `concurrency` from
  `dpe.gateway.command-concurrency: 3`. `GatewayCommandConcurrencyTest` asserts 3 consumers, at most
  the partition count, in group `payment-gateway`.
- `SagaReplyConsumer`: `concurrency` from `dpe.saga.reply-concurrency: 3`; javadoc records why it is
  safe. `ReplyListenerConcurrencyTest` (real broker): three consumers in `payment-orchestrator`,
  each holding one partition of each reply topic, all six covered, and the sweeper's readiness gate
  opening on the union.
- `BulkheadConfig.requireReserve(permits, pool, replyConcurrency)`; `RequestBulkheadTest` pins
  "6 permits, pool 10, 3 reply threads" as a boot failure.
- The `max-in-flight` derivation in `application.yml` rewritten around the three measurements.

### Results: the same knee profile (5 → 40 arrivals/s), three configurations

| | S20 baseline | + gateway ×3 | + replies ×3 | + limit 300 |
|---|---|---|---|---|
| Completed | 5,741 of 5,741 | 6,365 of 6,365 | 7,417 of 7,417 | 7,542 of 7,542 |
| Best 30 s window started/s | 17.8 | 24.2 | 36.6 | 39.8 |
| Worst window p99 | 13.3 s | 12.4 s | 9.1 s | **19.9 s** |
| Refused (503) | 14,495 | 11,197 | 4,626 | 2,025 |
| Peak sagas in flight | ~150 | 149 | 148 | 286 |
| Busiest stage at the top | gateway (predicted, not measured) | orchestrator replies, 93–99% | account-service, 90–99% | account-service, 99% |
| I1–I5, S1–S4 | pass | pass | pass | pass |

Where the time goes, decomposed per hop from the outboxes' `created_at`/`published_at` and the
consuming transaction's start:

- **The relay is not a bottleneck at any load.** Its wait is flat at p50 ~0.28 s, p95 ~0.54 s,
  from 4/s to 40/s: uniform over the 500 ms poll interval. Six relay hops account for ~1.7 s of the
  ~1.9 s unloaded settle time. The poll interval, not throughput, sets unloaded latency.
- **Gateway ×3:** each gateway thread ~45% busy at the top; its hop stayed ~0.3 s. The queue moved
  to the orchestrator's single reply thread: 0.7–2.3 s per reply hop, three hops per saga.
- **Replies ×3:** orchestrator hops back to ~0.1 s, each reply thread ~40% busy. The queue moved to
  account-service's single consumer (1.6–2.2 s per hop in the worst windows).
- **Offset commits** (`ack-mode: manual_immediate`) average ~4 ms, a quarter of the orchestrator's
  ~15 ms per reply.

### What broke

1. **The first re-derivation of `max-in-flight` was wrong, and only a run could show it.**
   30/s × 10 s = 300 doubled the tail. Customers poll until their payment settles, so GETs grow with
   the number in flight: 46/s at low load, ~500/s at 286. All three databases share one Postgres,
   and account-service's per-command time went from 11 ms to 18–23 ms while its own thread had no
   contention. Throughput at 286 in flight was ~22/s against 36/s at ~110. Capacity is a function of
   the queue the limit allows, which is the S20 collapse's feedback loop in a milder form. (The link
   through Postgres is consistent with the timing, not isolated.)
2. **A stage at 95% utilisation turns every pause into a long queue.** Backlog drains at (1 − ρ) of
   capacity, so a pause costs about ρ/(1 − ρ) times its own length: ~20× at 95%. The erratic windows
   in the second run line up with a 0.57 s GC pause on account-service and a 1.8 s one on the
   orchestrator.
3. **The `CodeCache GC Threshold` pauses are not code-cache exhaustion.** The code heaps held ~41 of
   116 MiB. The trigger is a growth threshold, and under SerialGC it runs as a full stop-the-world
   collection (0.57 s once on account-service). The orchestrator's 1.8 s pause was an ordinary
   allocation failure, which points at heap pressure under load instead.
4. **Jaeger was OOM-killed in all three runs** (exit 137), and in two of the three S20 runs. Payments were
   unaffected. It needs a real memory bound before the `users` profile.
5. **An uncontrolled variable:** a Kind cluster from another project was running throughout
   (mean 1.3–1.75 cores, peaks above 4 of 16). It was not running during the S20 runs.

### Verified

```
./mvnw -pl payment-gateway -am test -Dtest='GatewayCommandConcurrencyTest,ChargeServiceTest'   10/10 pass
  same, with idIsGroup = false removed      FAILS: expected "payment-gateway" but was "gateway-commands"
docker compose up -d --build                   3 gateway consumers, one partition each, group payment-gateway
PROFILE=knee ./loadtest/run.sh  (gateway x3)                  6,365/6,365 COMPLETED; I1-I5, S1-S4 pass
./mvnw -pl payment-orchestrator -am test -Dtest='ReplyListenerConcurrencyTest,RequestBulkheadTest,
    ReplyListenerReadinessTest,AccountEventConsumerTest'      1 + 4 + 4 + 3 pass
orchestrator rebuilt                "6 permits against a pool of 12 - 6 reserved ... which needs 6";
                                    saga-replies-0/1/2 each hold one partition of each reply topic
PROFILE=knee ./loadtest/run.sh  (+ replies x3)                7,417/7,417 COMPLETED; I1-I5, S1-S4 pass
PROFILE=knee ./loadtest/run.sh  (+ max-in-flight 300)         7,542/7,542 COMPLETED; I1-I5, S1-S4 pass;
                                                              worst p99 19.9 s -> reverted to 150
orchestrator rebuilt at 150         dpe_admission_limit 150.0, dpe_bulkhead_permits 6.0
./scripts/verify-invariants.sh                                I1-I5, S1-S4 pass
./mvnw -B -ntp clean verify         BUILD SUCCESS in 06:05 - common-messaging 20, account-service 97,
                                    payment-orchestrator 126, payment-gateway 10: 253 tests, 0 failures
```

### Committed

`93d7d87` — "gateway: one command consumer per partition, which moved the knee from ~17/s to ~24/s".
`094896f` — "orchestrator: three reply consumers, a bulkhead check that does the arithmetic, and a limit
that stayed at 150".

### Open / next

1. **account-service is the knee (~35/s), and its limit is a hot row.** Every reserve and commit
   locks CLEARING and holds it to commit, so consumer concurrency alone will queue on that lock.
   First measure it (concurrency 3, then `pg_locks` under load); the candidate designs are sharded
   clearing accounts, a derived rather than maintained CLEARING balance, or taking the CLEARING lock
   last in a new global order.
2. Per-record offset commits (`manual_immediate`, ~4 ms of ~15 ms per reply): `manual` would batch
   them, with the inbox absorbing the wider redelivery window.
3. Unloaded latency is set by the relay poll interval (~1.7 s of ~1.9 s), not throughput.
4. A memory bound for Jaeger (OOM-killed in five of the last six knee runs); the orchestrator's 1.8 s
   allocation-failure pause; the `users` profile.

## Session 22, part 2 — 2026-09-12

### Goal

account-service had become the knee (~35/s), and its limit was a hot row: every reserve and commit
locked the one CLEARING account and held it to commit. Measure that contention, remove it, and prove
the new concurrency cannot deadlock.

### Decisions made

| Decision | Choice | Reasoning |
|---|---|---|
| Measure first | account-service consumers ×3 with one CLEARING row, a lock sampler on `pg_locks` / `pg_stat_activity` | A hot row is a hypothesis until sessions are seen queued on it. The sampler counts tuple locks on account rows and matches them to the CLEARING row's ctid. |
| The fix | Shard CLEARING: eight accounts (`V8__sharded_clearing.sql`) | Keeps every invariant literally true: each shard is an ordinary account (I2), legs still balance (I1), shards are outside I3's sum as before. A derived CLEARING balance would have changed what I2 means; a "CLEARING last" lock order would have been a global rule change that still serializes on the commit's WAL flush. |
| Which shard | `hash(transfer_id) mod N`, chosen at RESERVE and recorded on the hold (`holds.clearing_account_id`) | Commit and release read the shard from the hold and never recompute it. Recomputing is correct only while the shard list never changes: add a shard and older holds would settle against a different account than they were parked in, and the commit/release mutual exclusion (both write the same `(transfer_id, clearing, DEBIT)` leg) would stop covering them. |
| Enforcing that a hold is parked in a CLEARING account | Composite foreign key `(clearing_account_id, clearing_account_type) → accounts (id, account_type)`, the type column pinned to `'CLEARING'` by a CHECK | A plain foreign key would accept a customer account, whose balance would then carry someone else's money in flight. |
| Existing holds | Backfilled to the original CLEARING (now shard 0), then `NOT NULL`, with no DEFAULT | A default would quietly supply the original for any future insert that forgot the column: the recompute-instead-of-record bug in another form. |
| Shard count | 8, as data (rows), not configuration | With three consumer threads, two concurrent transfers share a shard 1 time in 8. Adding shards later is safe because holds record theirs. |
| A currency with no shard | Refused as `CURRENCY_MISMATCH`, before any lock | The same answer the reserve gave when the single CLEARING account had a different currency. |
| account-service consumers | 3, one per partition (`dpe.account.command-concurrency`) | Only worth it once CLEARING stopped serializing every transaction. |
| Test isolation | Test contexts get a bootstrap and datasource address (and, in the orchestrator, a Redis address) that reach nothing (`127.0.0.1:9`), and the Kafka test bases turn topic creation back on | See What broke 1. `auto-startup=false` does not stop a restarted context's listeners. |

### Built

- `V8__sharded_clearing.sql`; `Hold.clearingAccountId`; `ClearingAccounts` (the shard for a
  transfer, from the CLEARING rows in its currency); `ReservationService` reserves into the chosen
  shard and settles against the hold's; `AccountCommandConsumer` concurrency 3.
- `ClearingShardTest`: reserves spread over the shards with each shard's balance equal to its own
  ACTIVE holds; a commit and a release of a pre-V8 hold settle against the original account, not the
  one the transfer id maps to now; the database refuses a hold parked in a non-CLEARING account;
  45 transfers round a ring of three accounts (every account both paying and being paid) reserved
  and committed in parallel with exact balances and no deadlock. `AccountCommandConcurrencyTest`
  pins 3 consumers in group `account-service`.
- `LedgerInvariants.assertAll` now also checks the per-shard identity (shard balance = ACTIVE holds
  naming it). It is stronger than the single-account total: a hold settled against the wrong shard
  leaves the total right and two shards wrong in opposite directions, and I1 and I2 cannot see it.
- Test configuration in all three modules; account-service's Postgres test base now switches its
  listeners off, as the other two services' do.

### Results: the knee profile (5 → 40 arrivals/s), continuing from part 1

| | replies ×3 (part 1) | + account ×3, one CLEARING | + CLEARING ×8 |
|---|---|---|---|
| results dir | 20260912T134310Z | 20260912T144105Z | 20260912T150159Z |
| Completed | 7,417 of 7,417 | 7,852 of 7,852 | 7,944 of 7,944 |
| Settle p99 in the ~30/s windows | — | 4.0–4.7 s | **2.8–3.1 s** |
| Worst window p99 | 9.1 s | 6.1 s | 9.4 s (a 6.7 s GC pause, below) |
| Refused (503) | 4,626 | 1,769 | 1,055 |
| Sessions waiting on an account-row lock (mean, busy samples) | — | 1.24 of 2.03 active | 0.33 of 1.78 |
| …of which on CLEARING | — | ≥ 0.81 | 0.08 |
| account-service consumer busy | 0.90–0.99 (one thread) | — | 0.2–0.4 per thread |
| I1–I5, S1–S4 | pass | pass | pass |

- Three threads helped even with one CLEARING row, because part of each command (deserialising, the
  inbox insert, the ~4 ms offset commit) happens outside the lock. But roughly one thread ran while
  the others queued, mostly on CLEARING.
- With the shards the curve stays flat (p99 ≈ 3 s) up to 31 starts/s. The two top windows were set
  by a **6.7 s full GC on the gateway** (SerialGC, one core at 101% during the pause) and a 1.9 s one
  on the orchestrator, not by locks. The gateway's consumers, at 0.72–0.87 busy, are the next stage
  to saturate.
- The orchestrator container reached **491 of 512 MiB** during the run.

### What broke

1. **Running the test suite with the Compose stack up put test consumers into the live consumer
   groups.** One full verify in part 1 joined the live `account-service` group 18 times and the live
   `payment-orchestrator` group 24 times. A test consumer in a live group takes partitions, processes
   live commands against the *test* database, and commits their offsets, so the running service
   never sees them. No traffic was flowing at the time and the next load run's quiescence check
   passed, so nothing is believed lost. There were two causes:
   - account-service's Postgres test base never set `spring.kafka.listener.auto-startup=false`, and
     every default address in `application.yml` is `localhost`, where Compose publishes the broker,
     Postgres and Redis.
   - The orchestrator's test base *did* set it, and its consumers still started. Spring Framework
     7.0.9 pauses cached test contexts and restarts them on reuse (`spring.test.context.cache.pause`,
     default `ON_CONTEXT_SWITCH`), and spring-kafka 4.1.1's registry starts **every** container on a
     `start()` after refresh (`startIfNecessary`: `(contextRefreshed && alwaysStartAfterRefresh) ||
     isAutoStartup()`, with `alwaysStartAfterRefresh` defaulting to `true`). `auto-startup=false`
     only governs a context's first start. Both were confirmed from the bytecode.

   The fix is that tests have no route to anything: `127.0.0.1:9` for the bootstrap and the
   datasource in every module's test properties, and for Redis in the orchestrator's (the only
   service that uses it), overridden by `@ServiceConnection` wherever a container exists. After it, with the stack up, the full verify made zero contacts with the live
   broker, and the restarted listeners can be seen in the logs failing to reach port 9 instead.
   The same verify also ran faster (4:42 against 6:05). Plausibly because non-Kafka contexts no
   longer create topics or join groups at startup, but that has not been isolated.
2. **The first parallel test could not have caught a deadlock.** Its senders and recipients were
   disjoint, so a per-role lock order (sender, then clearing, then recipient) happened to be global
   and the test would have passed it. Rewritten as a ring (A pays B, B pays C, C pays A). With the
   order mutated to per-role, Postgres reported 9 deadlocks and the test failed; with the global order
   it passes. Before that, the same test had asserted 16 transfers per recipient where its own
   distribution gave 18/15/15.
3. **Settling against a recomputed shard was caught, but not by the check expected to catch it.**
   With commit mutated to recompute the shard, the test failed on
   `accounts_customer_balance_non_negative`. Despite its name, that constraint is
   `balance_minor >= 0 OR account_type = 'SYSTEM'`, so it covers CLEARING too: the wrong shard was
   empty and could not go negative. Under load the wrong shard would usually hold other transfers'
   money and the debit would succeed. The per-shard identity is the check that catches that case.
4. **Jaeger was OOM-killed again**, and the gateway's 6.7 s pause shows that GC under load, not lock
   contention, now sets the tail.

### Verified

```
knee run, account ×3 + one CLEARING, lock sampler    7,852/7,852 COMPLETED; I1-I5, S1-S4 pass;
                                                     1.24 of 2.03 active sessions waiting on a row lock
./mvnw -pl account-service -am clean verify (stack up)   before the test fix: 18 live-group joins in part 1;
                                                     after: 0 contacts with localhost:29092
ClearingShardTest 5/5, AccountCommandConcurrencyTest 1/1, ReservationServiceTest 10/10,
ReserveOwnershipTest 4/4, InvariantsEndpointTest 7/7
  mutation: commit recomputes the shard          FAILS (accounts_customer_balance_non_negative)
  mutation: per-role lock order                   FAILS - 9 x "deadlock detected"
docker compose up -d --build account-service     "Successfully applied 1 migration ... now at version v8";
                                                 8 CLEARING accounts, 48,429 holds backfilled; invariants pass
knee run, CLEARING ×8, lock sampler              7,944/7,944 COMPLETED; I1-I5, S1-S4 pass;
                                                 0.33 of 1.78 active sessions waiting, 0.08 on CLEARING
./chaos/07-hot-account.sh                        HYPOTHESIS HELD - exactly 60 completed, 30 refused,
                                                 sender at 0, 0 deadlocks in the Postgres log
./mvnw -B -ntp clean verify (stack up)           BUILD SUCCESS in 04:42 - common-messaging 20,
                                                 account-service 103, payment-orchestrator 126,
                                                 payment-gateway 10: 259 tests, 0 failures;
                                                 0 contacts with the live broker
```

### Committed

`44e55a4` — "test: give no test a route to the running Compose stack".
`e471be1` — "account-service: shard CLEARING, and record each hold's shard so it can never be recomputed".

### Open / next

1. GC under load now sets the tail: a 6.7 s SerialGC full collection on the gateway, 1.9 s on the
   orchestrator, and the orchestrator at 491 of 512 MiB. Heap sizing and the collector choice are
   next.
2. The gateway's consumers (0.72–0.87 busy at ~36/s) are the next stage to saturate.
3. `max-in-flight` has not been re-derived since CLEARING was sharded; do it with a knee run, not
   arithmetic.
4. Per-record offset commits; the relay poll interval as the unloaded-latency floor; a memory bound
   for Jaeger; the `users` profile.
