# ADR 0003 — Shared contracts: one module of records and topic names, versioned by topic

**Status:** accepted (M2); extended at M3, M5 and M7 with new messages
**Supersedes:** nothing
**Related:** ADR 0001 (orchestration), ADR 0004 (shared messaging library — the line between
sharing contracts and sharing behaviour)

## Context

At M2 a message first crossed a service boundary: account-service published `FundsTransferred` and
payment-orchestrator consumed it. From M3 every service is both a producer and a consumer. A
producer and a consumer have to agree on three things, and a disagreement on any of them is silent
rather than loud:

- **the shape of each message** — a field one side renames is `null` on the other;
- **the topic names** — a typo subscribes to a topic nobody writes to;
- **each topic's partition count** — found the hard way at M2. The consumer subscribed before the
  producer had declared the topic, the broker auto-created it with its default of **one**
  partition, and the producer's own declaration then grew it to three. About two thirds of the
  traffic was keyed onto partitions the consumer had not been assigned. No exception, no error
  log, and it healed itself five minutes later at `metadata.max.age.ms`.

## Options considered

**1. Each service defines its own copy of the messages it reads** (tolerant reader,
consumer-driven contracts). No shared artifact, truly independent builds. Nothing but review
catches drift, and at M2 drift had just been demonstrated to fail silently.

**2. A schema registry** with Avro or Protobuf and a compatibility mode enforced on publish. The
industry answer once services are deployed independently. It adds a container, a code-generation
step and a second serialization format, on a machine where memory is the binding constraint.

**3. A shared module of plain Java records and constants.** Chosen.

## Decision

`common-events` holds the contracts and nothing else:

- **Records only, with no behaviour.** Commands (`ReserveFunds`, `ChargeGateway`, `CommitFunds`,
  `ReleaseFunds`, `VoidCharge`), their replies, and broadcast facts (`AccountOpened`,
  `FundsTransferred`). The module has **no dependencies at all** — not Spring, not Jackson — so it
  cannot grow a helper that does something.
- **One envelope for every message.** `EventEnvelope` carries `messageId`, `eventType`,
  `aggregateId`, `occurredAt` and the payload. The message id is the outbox row's primary key,
  generated inside the business transaction, so it is identical on every redelivery — which is what
  makes it a correct dedup key where a Kafka offset is not. The aggregate id is the partition key.
  The id and type are also sent as headers, so a consumer can dedupe and route a message it cannot
  yet deserialize.
- **Topic names in `Topics`, versioned in the name:** `dpe.<context>.<kind>.v<N>`. Commands and
  events are on separate topics, so a new subscriber to events cannot receive a command it must not
  act on.
- **Partition counts as constants beside the names**, and every service that touches a topic
  declares a `NewTopic` from the constant. Broker auto-creation is off and consumers set
  `allow.auto.create.topics=false`, so an undeclared topic is an error at startup, not a
  one-partition default that half-works.
- **Machine-readable reason codes are string constants**, with a separate free-text `detail` that
  nothing parses. The orchestrator branches on the code; the sentence can be reworded freely.

### Evolution rules

Within a topic version, changes are **additive**:

- A **new field** is `null` in messages from a producer that predates it, so the consumer must
  handle `null` (M7 made `ReleaseFunds.holdId` nullable on purpose: a saga that times out before
  hearing `FundsReserved` has no hold id to send).
- A **field a consumer does not know** is ignored. That is Jackson 3's default
  (`FAIL_ON_UNKNOWN_PROPERTIES` is off; checked against jackson-databind 3.1.5), and no service
  sets a `spring.jackson` property that changes it.
- A **renamed or retyped field, or a changed meaning,** is a breaking change: it goes to `.v2`, and
  `.v1` stays up until every consumer has moved.

## Consequences

**The compiler is the compatibility check — while there is one build.** With every service built
from one commit, renaming a field breaks the build of every service that reads it, at once. That is
a stronger guarantee than a registry gives, and it lasts exactly as long as the services are
deployed together. Once they are deployed independently, a green build proves nothing about the
versions actually running, and the evolution rules above become the only protection.

**Reason codes stay strings, never an enum.** Checked against jackson-databind 3.1.5: an enum value
the consumer does not know fails deserialization with `InvalidFormatException`. That failure is
technical, not a business outcome, so the message would be retried and then dead-lettered —
turning "the producer added a rejection reason" into stuck payments. A string arrives intact.

**Every service rebuilds when a contract changes.** Accepted: a contract change is by definition a
change to what two services say to each other.

**The contracts are shared; the handlers are not.** What a service *does* with a message lives in
that service. ADR 0004 draws the same line for the messaging mechanics: shared infrastructure and
shared vocabulary, never a shared domain.

## Revisit if

- The services start being released on separate schedules. Then a schema registry with a
  compatibility mode enforced at publish time replaces the compiler as the check.
- A consumer outside this repository needs the contracts. A Java module cannot be its contract;
  a published schema can.
