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
