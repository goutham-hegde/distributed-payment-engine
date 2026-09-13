# ADR 0001 — The saga is orchestrated, not choreographed

**Status:** accepted (M3); amended by the M7 chaos fixes (the pivot, truthful replies)
**Supersedes:** nothing
**Related:** ADR 0003 (shared contracts), ADR 0004 (shared messaging library), ADR 0008
(reconciliation)

## Context

A transfer touches three services, each with its own database:

| Step | Owner | What it does |
|---|---|---|
| Reserve | `account-service` | debit the sender, credit CLEARING, record an `ACTIVE` hold |
| Charge | `payment-gateway` | ask the (simulated) payment provider to approve the charge |
| Commit | `account-service` | debit CLEARING, credit the recipient, mark the hold `COMMITTED` |
| Release (compensation) | `account-service` | debit CLEARING, credit the sender back, mark the hold `RELEASED` |

No single transaction can span them. Two-phase commit was never an option: it needs an XA
coordinator across three Postgres databases and the broker, it blocks every participant holding
locks while the coordinator is unreachable, and the payment provider — the one step that cannot be
undone by writing an opposite row — is an external party that will not join anyone's prepare phase.

So a transfer is a **saga**: a sequence of local transactions, each committed on its own, where a
failure after some of them have committed is repaired by *compensating* transactions rather than a
rollback. The open question is who decides what happens next.

## Options considered

**1. Choreography.** Each service reacts to the previous one's event: account-service publishes
`FundsReserved`, the gateway hears it and charges, account-service hears `GatewayApproved` and
commits. No central component, and each service only needs to know the events it reacts to.

**2. Orchestration.** One component holds the saga's state, sends each participant a command, and
decides the next step from the reply. Participants know nothing about each other.

**3. A workflow engine** (Temporal, Camunda, a Step Functions equivalent). Orchestration, with
durable timers, retries and history supplied by a separate stateful cluster.

## Decision

**Orchestration**, in `payment-orchestrator`, written by hand.

```
STARTED ──FundsReserved──▶ RESERVED ──GatewayApproved──▶ CHARGED ──FundsCommitted──▶ COMPLETED
   │                          │
   │ReserveRejected           │GatewayDeclined / timeout
   ▼                          ▼
 FAILED                   COMPENSATING ──FundsReleased──▶ COMPENSATED
```

- The saga is a row in `saga_instances` with a `status` column, and every transition writes two
  rows to `saga_steps`: one when the command goes out and one when its reply lands. A step with only
  the first row is a step whose reply never came.
- Every transition loads the saga `FOR UPDATE`, changes its status and writes the next command to
  the outbox, all in the one local transaction that also records the reply in the inbox.
- Commands go to per-participant command topics; replies come back on the participants' event
  topics. Each participant answers only the orchestrator.

### Why not choreography

The deciding reasons are all about failure, not the happy path — on the happy path choreography is
simpler.

- **The process has to exist somewhere.** In choreography the transfer is an emergent property of
  three services' subscriptions. Answering "where is transfer X, and what happens next?" means
  reading every service. Here it is one query against `saga_instances` and `saga_steps`, and the
  state machine is one class (`SagaOrchestrator`).
- **A timeout needs an owner.** The hard case is not a decline but silence: the provider never
  answers, or a reply is lost. Somebody has to hold a deadline for every in-flight transfer and act
  when it passes. In choreography no service owns the whole transfer, so either every service grows
  its own timers for other services' steps, or a stuck transfer is found by nobody. Here the
  sweeper runs one indexed query — non-terminal and past its deadline — against a partial index that
  contains only in-flight sagas.
- **Compensation depends on how far the saga got.** A rejected reserve moved no money and must *not*
  be compensated (a `ReleaseFunds` would name a hold that does not exist). A declined charge must be.
  After an approved charge, recovery may only go *forward* (see the pivot, below). These are
  decisions about the whole saga's history, and they are made in one place instead of being
  inferred by each participant from the one event it happened to receive.
- **Status is stored, not derived.** A status folded from an event log cannot be indexed, and the
  sweeper would scan every saga ever run. The column is the index; `saga_steps` is the history.

### Why not a workflow engine

An engine supplies durable timers, retries and a history view — roughly what the sweeper,
the outbox and `saga_steps` are. It also puts them behind an SDK, so the delivery guarantees this
system depends on (the outbox, the inbox, when a command is really durable) become the engine's
internals rather than code that can be read, tested and broken on purpose by the chaos suite. And
it is another stateful cluster on a machine where memory is the binding constraint (ADR 0012).
With one saga type and four steps, the engine's generality is not yet paying for itself.

## Consequences

**The orchestrator is a central component, but not a single point of loss.** If it is down, sagas
stop advancing; none is lost. Commands it committed sit in its outbox, replies wait on the broker,
and on restart the relay, the reply listener and the sweeper resume from the database. The sweeper
waits until the reply listener has held its partitions for a grace period (`listen-grace`, 10 s),
so it does not time out sagas whose replies are already waiting to be read.

**The orchestrator knows every participant's commands.** That is the coupling this pattern
accepts. It is contained by the contracts module (ADR 0003): the orchestrator depends on message
shapes, never on a participant's code or database.

**The terminal set is written down in seven places** — `SagaStatus`, the `saga_status_known` CHECK,
the in-flight partial index, `saga_completed_at_iff_terminal`, the sweeper's claim query, the
in-flight gauge's query and `verify-invariants.sh`. They must change together or invariant I4 stops
meaning anything. The native queries repeat the literal because the predicate must match the
partial index character for character or the planner ignores the index.

**Two rules the chaos suite (M7) added to the pattern**, both of which choreography would have made
harder to state:

- **The gateway charge is the pivot.** Before it, recovery runs backward: release the hold. After
  it, only forward: a timeout in `CHARGED` re-sends `CommitFunds` and keeps waiting, because
  releasing the hold would refund the customer out of the system's own books while the provider
  keeps the charge. Chaos scenario 2 found this with all five ledger invariants still green.
- **A participant always answers, and the answer is a statement of fact.** A commit or release for a
  hold that is no longer `ACTIVE` is answered with what actually happened to it (`FundsCommitted` or
  `FundsReleased`), not with silence. The orchestrator accepts `FundsCommitted` in `COMPENSATING`
  (the recipient has the money: `COMPLETED`) and `FundsReleased` in `RESERVED`/`CHARGED`.

**Compensation is not rollback.** A compensated transfer leaves four ledger entries — the reserve
and its reversal — not zero. The sender's statement shows the money leaving and coming back, which
is what happened.

## Revisit if

- A second saga type arrives with branching or long-lived waits (days, human approval). Durable
  timers and versioned workflow definitions are what an engine is good at, and hand-writing them
  twice is where it starts paying for itself.
- Participants need to react to each other's events for reasons unrelated to the transfer. Those are
  events, and choreography is the right tool for them; the saga stays orchestrated.
