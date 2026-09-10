# ADR 0006 — The trace context is a column, not a thread local

**Status:** accepted (M6 part 2)
**Supersedes:** nothing
**Related:** ADR 0004 (shared messaging library)

## Context

M6 part 1 added metrics. Metrics answer *how many* — how many sagas compensated, how deep the
outbox backlog is, how long the p99 saga takes. They cannot answer *why this one*. For that you
need the causal chain of a single payment: the HTTP POST, the reserve command, account-service's
handler, the gateway charge, the reply, the commit. Six spans in three processes, joined by a
trace id.

Distributed tracing normally requires no schema and very little code. The instrumentation reads
the current span off a thread local at the moment of an outbound call, writes a W3C `traceparent`
header onto the request, and the receiver extracts it. Spring Kafka does this for a plain
`kafkaTemplate.send()`; Spring MVC does it for HTTP. It works because **the outbound call happens
on the thread that is currently inside the span.**

The transactional outbox exists to make that untrue. Nothing is sent on the request thread. A row
is written inside the business transaction, and `OutboxRelay` turns it into a Kafka record later,
on a scheduled thread, in a different transaction. By the time the send happens the producing span
has ended.

## The failure this causes, if nothing is done

Not an error. Something worse: a plausible-looking trace that is wrong.

- With no context on the record, the consumer's listener observation starts a **new root trace**.
  One payment becomes four traces — one per hop — with no field joining them. Every trace looks
  healthy in isolation, and the question "where did this payment spend its 4 seconds" has no
  answer at all.
- With the relay's *own* context on the record, every message drained in one batch becomes a child
  of one `drainBatch` span, so unrelated payments are parented under each other. This is the worse
  of the two, because the trace renders and reads as real.

Neither logs anything. This is a defect that is only ever discovered by someone opening Jaeger
during an incident and finding that the thing they came for is not there.

## Decision

**Persist the trace context on the outbox row and restore it at publish time.**

- `OutboxWriter.append` calls `OutboxTracing.capture()` on the producing thread, inside the
  business transaction, and stores the result in `outbox.trace_parent` / `outbox.trace_state`. It
  therefore commits or rolls back with the message it describes — a row can never carry the trace
  of a transaction that was abandoned.
- `OutboxRelay.drainBatch` calls `OutboxTracing.beginPublish(...)`, which extracts that context,
  starts a PRODUCER span parented to it, and injects **that span** into the record headers.
- The consumer side is free: `spring.kafka.listener.observation-enabled: true` makes Spring Kafka
  extract `traceparent` into a consumer observation, so the handler and everything under it —
  including its own outbox write — continues the trace.

This is the outbox pattern applied one level up. Anything that must survive the gap between the
transaction and the send has to be in the row. Trace context is not an exception to that rule; it
is a second instance of it.

## Options considered

**1. Copy `trace_parent` verbatim onto the record.** Two lines, no span in the relay. Rejected:
it makes the consumer a direct child of the producing request and erases the relay hop entirely.
Relay lag — row committed at T, published at T+400ms — is one of the two intervals this milestone
exists to make visible, and a design that hides it attributes a broker stall to whichever service
happened to write the row.

**2. Let `KafkaTemplate` inject, via `spring.kafka.template.observation-enabled: true`.** The
default, free, and wrong here: the template injects whatever the *sending* thread holds, which for
the outbox is the relay's context, not the payment's. Explicitly set to `false` so that two
mechanisms are not writing the same header with different answers.

**3. Put the context in the `EventEnvelope` payload instead of in columns.** Rejected on layering:
the envelope is a published contract between services and every field in it is a promise. Trace
context is transport metadata — it belongs in headers on the wire and in columns at rest, where it
can change without a contract version.

**4. Store the whole propagation carrier as `jsonb`.** Propagator-agnostic, and rejected: it would
accept whatever keys the propagator of the day emits, so a switch from W3C to B3 would leave old
and new rows silently disagreeing with nobody obliged to notice. Two named columns make a
propagator change a migration, which is the visibility we want.
`management.tracing.propagation.type: w3c` is pinned for the same reason.

## Consequences

**A replayed dead letter re-enters the trace of the original payment.** The context is on the row,
so a replay hours later appears as a child of a request that finished long ago. That is either
exactly what is wanted — the operator sees the whole life of the message — or deeply confusing,
depending on whether they knew. It is worth knowing.

**Tracing must stay non-load-bearing, and is asserted so.** `OutboxTracing` falls back to
`Tracer.NOOP` / `Propagator.NOOP` when no bridge is on the classpath;
`OutboxTracingWithoutABridgeTest` proves capture, publish and inject are all no-ops in that case,
and no service depends on Jaeger in Compose. The rule is the one Redis is already held to: if
deleting the observability backend could make a payment fail, the instrumentation has joined the
payment path.

**Sampling is decided once, at the edge, and carried.** The sampled flag is the last byte of
`traceparent`, so it rides in the column along with everything else and downstream services do not
re-roll. If they did, a payment sampled at the orchestrator and dropped at account-service would
produce a trace that ends wherever the dice went the other way.

**Two more columns on the busiest table in each service, and no index on them.** Nothing ever
queries *by* trace context — it is read only for a row already claimed by primary key — and an
index on a high-cardinality column that no predicate mentions is write amplification on the
payment path.
